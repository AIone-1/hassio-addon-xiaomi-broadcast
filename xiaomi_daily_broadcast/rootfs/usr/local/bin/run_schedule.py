#!/usr/bin/env python3
"""小爱每日播报 —— HAOS 加载项入口

- 60s 调度循环：decide_summary_type 判断到点，命中则播报
- stdlib http.server 提供 ingress Web UI + 触发 API
  GET  /            → ingress 页面（看日志 / 触发播报）
  GET  /logs        → 最近 N 行日志
  GET  /state       → 当前播报状态
  POST /trigger     → 立即播报（force=True，后台线程）
  POST /task        → 记录一条终端任务时间戳
  POST /memo        → 追加备忘
"""
import asyncio, json, os, threading, time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import daily_summary as ds

LOCK = threading.Lock()
LOG_TAIL = []           # 内存日志环形缓冲（最近 200 条）
LOG_MAX = 200
STATE_FILE = Path("/data/broadcast_state.json")
HISTORY_FILE = Path("/share/xiaomi_broadcast/broadcast_history.jsonl")


def load_history():
    """读播报历史 JSONL → [{date,time,text,count,type,sentences}]，按时间倒序。"""
    out = []
    try:
        if HISTORY_FILE.exists():
            for line in HISTORY_FILE.read_text().splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    out.append(r)
                except Exception:
                    pass
    except Exception as e:
        log(f"⚠️ 读历史失败: {e}")
    out.sort(key=lambda r: (r.get("date", ""), r.get("ts", "")), reverse=True)
    return out


def history_days(month):
    """某月（YYYY-MM）有记录的日期列表。"""
    days = set()
    for r in load_history():
        d = r.get("date", "")
        if d.startswith(month):
            days.add(d)
    return sorted(days, reverse=True)


def history_day(date_str):
    """某天的所有记录。"""
    return [r for r in load_history() if r.get("date") == date_str]


def log(msg):
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    LOG_TAIL.append(line)
    if len(LOG_TAIL) > LOG_MAX:
        del LOG_TAIL[:-LOG_MAX]


def get_webui():
    """读独立 webui/index.html；不存在则用内嵌 HTML。"""
    p = Path("/usr/local/bin/webui/index.html")
    try:
        if p.exists():
            return p.read_text()
    except Exception:
        pass
    return WEBUI_HTML


def ha_entities():
    """从 HA 拉全部实体状态列表（供配置页下拉选择）。返回 {ok, count, entities, error}。"""
    import urllib.request
    try:
        ws, api, token = ds._ha_endpoints()
        if not token:
            return {"ok": False, "count": 0, "entities": [], "error": "无 HA token（检查加载项 homeassistant_api 权限）"}
        req = urllib.request.Request(api.rstrip("/") + "/states",
                                     headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            states = json.loads(resp.read())
        out = []
        for s in states:
            eid = s.get("entity_id", "")
            out.append({
                "entity_id": eid,
                "name": s.get("attributes", {}).get("friendly_name", ""),
                "state": s.get("state", ""),
                "domain": eid.split(".")[0],
            })
        return {"ok": True, "count": len(out), "entities": out, "error": ""}
    except Exception as e:
        err = str(e)
        log(f"⚠️ 拉取 HA 实体失败: {err}")
        return {"ok": False, "count": 0, "entities": [], "error": err}


# 配置页可编辑的实体映射段（key → 配置 JSON 里的顶层键）
EDITABLE_SECTIONS = [
    {"key": "temp_sensors", "title": "🌡️ 温度传感器", "domains": ["sensor"], "room": True},
    {"key": "humidity_sensors", "title": "💧 湿度传感器", "domains": ["sensor"], "room": True},
    {"key": "power_sensors", "title": "⚡ 用电（power_cost_today）", "domains": ["sensor"], "room": True},
    {"key": "power_now", "title": "🔌 实时功率（可选）", "domains": ["sensor"], "room": True},
    {"key": "lights", "title": "💡 灯光", "domains": ["light"], "room": True},
    {"key": "doors", "title": "🚪 门窗（on=开）", "domains": ["binary_sensor"], "room": True},
    {"key": "important_devices", "title": "⚠️ 重要设备（离线提醒）", "domains": ["climate", "fan", "media_player", "switch"], "room": True},
]


def load_edit_cfg():
    """读配置（含 options 合并），供配置页展示。"""
    return ds.load_config()


def save_edit_cfg(updates):
    """原子写配置 JSON：updates 里的段整体替换，其余保留。
    读原始 JSON（不合并 options）——engine 段(含 DeepSeek key)由 options 管理，不落盘到可编辑文件。"""
    cfg = json.loads(ds.CONFIG_PATH.read_text()) if ds.CONFIG_PATH.exists() else {}
    for k, v in updates.items():
        if v is None:
            cfg.pop(k, None)
        else:
            cfg[k] = v
    tmp = str(ds.CONFIG_PATH) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, str(ds.CONFIG_PATH))
    return cfg


# 4 个播报按钮：每日/周/月/年。entity_id 由名字拼音生成（创建后按实际 id 更新）
BUTTONS = [
    {"type": "daily",   "name": "小爱播报·每日", "entity": "input_button.xiao_ai_bo_bao_mei_ri", "icon": "mdi:calendar-today"},
    {"type": "weekly",  "name": "小爱播报·周",   "entity": "input_button.xiao_ai_bo_bao_zhou",  "icon": "mdi:calendar-week"},
    {"type": "monthly", "name": "小爱播报·月",   "entity": "input_button.xiao_ai_bo_bao_yue",   "icon": "mdi:calendar-month"},
    {"type": "yearly",  "name": "小爱播报·年",   "entity": "input_button.xiao_ai_bo_bao_nian",  "icon": "mdi:calendar-star"},
]
# 兼容旧版每日按钮（xiao_ai_bo_bao_an_niu 名字是"小爱播报按钮"）
BUTTONS.append({"type": "daily", "name": "小爱播报按钮", "entity": "input_button.xiao_ai_bo_bao_an_niu", "icon": "mdi:volume-high"})


def _ws_connect():
    """建 WS 连接并认证，返回 (ws, token)。"""
    import websockets
    ws_url, api, token = ds._ha_endpoints()
    if not token:
        raise RuntimeError("无 HA token")
    return ws_url, token


def ensure_button_entity():
    """确保所有播报按钮存在（每日/周/月/年）。已存在跳过，避免重复创建堆积。"""
    import websockets
    try:
        ws_url, token = _ws_connect()
        async def _existing():
            async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                await ws.recv()
                await ws.send(json.dumps({"type": "auth", "access_token": token}))
                await ws.recv()
                await ws.send(json.dumps({"type": "get_states", "id": 1}))
                r = json.loads(await ws.recv())
                return set(s.get("entity_id") for s in r.get("result", []))
        existing = asyncio.run(_existing())
        ok_all = True
        for b in BUTTONS:
            if b["entity"] in existing:
                continue
            async def _create(name, icon):
                async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                    await ws.recv()
                    await ws.send(json.dumps({"type": "auth", "access_token": token}))
                    await ws.recv()
                    await ws.send(json.dumps({"id": 1, "type": "input_button/create",
                                              "name": name, "icon": icon}))
                    r = json.loads(await ws.recv())
                    # 返回的 result.id 是实际生成的 entity_id（如 xiao_ai_bo_bao_zhou）
                    if r.get("success") and isinstance(r.get("result"), dict):
                        b["entity"] = "input_button." + r["result"]["id"]
                    return r.get("success", False)
            ok = asyncio.run(_create(b["name"], b["icon"]))
            log(f"{'✅ 已创建' if ok else '⚠️ 创建失败'}播报按钮: {b['name']}")
            ok_all = ok_all and ok
        return ok_all
    except Exception as e:
        log(f"⚠️ 创建播报按钮失败: {e}")
        return False


def button_watcher():
    """监听所有播报按钮（每日/周/月/年）：被 press 时触发对应类型的语音播报。"""
    import websockets
    last = {}   # entity -> 上次 state
    log("🔔 播报按钮监听已启动")
    while True:
        try:
            ws_url, token = _ws_connect()
            async def _get_all():
                async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                    await ws.recv()
                    await ws.send(json.dumps({"type": "auth", "access_token": token}))
                    await ws.recv()
                    await ws.send(json.dumps({"type": "get_states", "id": 1}))
                    r = json.loads(await ws.recv())
                    states = {}
                    for s in r.get("result", []):
                        eid = s.get("entity_id")
                        if eid.startswith("input_button.") and "xiao_ai_bo_bao" in eid:
                            states[eid] = s.get("state", "unavailable")
                    return states
            states = asyncio.run(_get_all())
            for b in BUTTONS:
                eid = b["entity"]
                st = states.get(eid, "unavailable")
                # 首次轮询只同步，不触发
                if eid not in last:
                    last[eid] = st
                    continue
                # input_button 的 state 是最后按下时间戳，变化 = 被按过
                if st != last[eid] and st not in ("", "unavailable", "unknown", None) and last[eid] not in ("", "unavailable", "unknown", None):
                    log(f"🔔 播报按钮被按下（{b['name']}），触发{b['type']}播报")
                    run_broadcast(summary_type=b["type"], text_only=False)
                last[eid] = st
        except Exception as e:
            log(f"⚠️ 播报按钮监听错误: {e}")
        time.sleep(2)


_RUN_COUNT = 0

def run_broadcast(summary_type="daily", text_only=False):
    """在独立线程里跑播报（force=True）。语音播报用锁防双播；只看文字不发声，不抢锁。"""
    global _RUN_COUNT
    _RUN_COUNT += 1
    run_no = _RUN_COUNT
    def _run():
        locked = False
        if not text_only:
            if not LOCK.acquire(blocking=False):
                log(f"⚠️ [run{run_no}] 已有语音播报进行中，跳过本次触发")
                return
            locked = True
        try:
            log(f"📢 [run{run_no}] 手动触发播报: {summary_type}" + ("（只看文字）" if text_only else ""))
            asyncio.run(ds.main(force=True, text_only=text_only, summary_type=summary_type))
            log(f"✅ [run{run_no}] 播报线程结束")
        except Exception as e:
            log(f"❌ [run{run_no}] 播报出错: {e}")
            # 🔑 兜底：出错时写 done 状态，避免前端一直卡"生成中"
            try:
                import time as _t
                err_state = {"status": "done", "sentences": [{"text": f"播报出错：{e}", "icon": "text", "idx": 1, "total": 1}],
                             "played_to": 1, "run_id": str(int(_t.time() * 1000)), "mode": "speech",
                             "summary_type": summary_type, "phase": ""}
                STATE_FILE.write_text(json.dumps(err_state, ensure_ascii=False))
            except Exception:
                pass
        finally:
            if locked:
                LOCK.release()
    threading.Thread(target=_run, daemon=True).start()


def scheduler_loop():
    """60s 轮询：decide_summary_type 判断到点才播。"""
    last = {}
    log("⏰ 调度循环已启动")
    while True:
        try:
            cfg = ds.load_config()
            st = ds.decide_summary_type(datetime.now(), cfg)
            if st is not None:
                key = f"{st}:{datetime.now():%Y-%m-%d}"
                today = datetime.now().strftime("%Y-%m-%d")
                if last.get(key) != today:
                    last[key] = today
                    log(f"⏰ 到点播报: {st}")
                    if not LOCK.acquire(blocking=False):
                        log("⚠️ 已有播报进行中，跳过定时触发")
                        continue
                    try:
                        asyncio.run(ds.main(force=False, summary_type=st))
                    except Exception as e:
                        log(f"❌ 定时播报出错: {e}")
                    finally:
                        LOCK.release()
        except Exception as e:
            log(f"⚠️ 调度循环错误: {e}")
        time.sleep(60)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, mime="text/html; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        ln = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(ln) if ln else b""

    def do_GET(self):
        import urllib.parse as up
        path = self.path.split("?")[0]
        q = up.parse_qs(up.urlsplit(self.path).query)
        if path == "/" or path == "/index.html":
            self._send(200, get_webui())
        elif path == "/logs":
            self._send(200, json.dumps(LOG_TAIL[-100:], ensure_ascii=False), "application/json")
        elif path == "/state":
            try:
                self._send(200, STATE_FILE.read_text(), "application/json")
            except Exception:
                self._send(200, json.dumps({"status": "idle"}), "application/json")
        elif path in ("/cfg/entities", "/api/entities"):
            # /cfg/ 是新路径（避免 ingress 下 /api/ 嵌套被 HA 拦截）；/api/ 兼容旧版
            self._send(200, json.dumps(ha_entities(), ensure_ascii=False), "application/json")
        elif path in ("/cfg/config", "/api/config"):
            self._send(200, json.dumps(load_edit_cfg(), ensure_ascii=False), "application/json")
        elif path == "/theme":
            try:
                self._send(200, Path("/data/theme.json").read_text(), "application/json")
            except Exception:
                self._send(200, json.dumps({"theme": "dark"}), "application/json")
        elif path == "/history/days":
            month = q.get("month", [""])[0] or datetime.now().strftime("%Y-%m")
            self._send(200, json.dumps(history_days(month), ensure_ascii=False), "application/json")
        elif path == "/history/day":
            date_str = q.get("date", [""])[0]
            self._send(200, json.dumps(history_day(date_str), ensure_ascii=False), "application/json")
        else:
            self._send(404, "not found", "text/plain")

    def do_POST(self):
        path = self.path.split("?")[0]
        import urllib.parse as up
        q = up.parse_qs(up.urlsplit(self.path).query)
        if path == "/trigger":
            t = (q.get("type") or ["daily"])[0]
            # 用 startswith("true")：兼容历史 URL 里 Date.now() 粘在 true 后面变成 true1694 的情况
            to = (q.get("text_only") or ["false"])[0].lower().startswith("true")
            # 🔑 语音播报前检查锁：被占说明正在播，返回明确提示，避免"看起来卡死"
            if not to and LOCK.locked():
                self._send(409, json.dumps({"ok": False, "error": "已有语音播报进行中，请稍候"}), "application/json")
                return
            # 🔑 先清空状态，避免前端读到上一次播报的旧内容（旧 run_id + 全部句子）
            try:
                STATE_FILE.write_text(json.dumps({"status": "idle", "sentences": [], "played_to": 0,
                                                  "run_id": "", "mode": "speech",
                                                  "summary_type": t, "phase": ""}))
            except Exception:
                pass
            run_broadcast(summary_type=t, text_only=to)
            self._send(202, json.dumps({"ok": True, "type": t}), "application/json")
        elif path == "/task":
            try:
                with open(ds.TASK_LOG, "a") as f:
                    f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
                self._send(200, json.dumps({"ok": True}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/memo":
            try:
                data = json.loads(self._body() or b"{}")
                text = (data.get("text") or "").strip()
                if not text:
                    self._send(400, json.dumps({"ok": False, "error": "text required"}), "application/json")
                    return
                memos = json.loads(ds.MEMO_FILE.read_text()) if ds.MEMO_FILE.exists() else []
                memos.append({"id": datetime.now().strftime("%Y%m%d%H%M%S"),
                              "text": text, "done": False,
                              "created": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")})
                ds.MEMO_FILE.write_text(json.dumps(memos, ensure_ascii=False))
                self._send(200, json.dumps({"ok": True}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path in ("/cfg/config", "/api/config"):
            try:
                data = json.loads(self._body() or b"{}")
                save_edit_cfg(data.get("updates", {}))
                self._send(200, json.dumps({"ok": True}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/theme":
            try:
                data = json.loads(self._body() or b"{}")
                Path("/data/theme.json").write_text(json.dumps({"theme": data.get("theme", "dark")}))
                self._send(200, json.dumps({"ok": True}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/logs/clear":
            LOG_TAIL.clear()
            self._send(200, json.dumps({"ok": True}), "application/json")
        else:
            self._send(404, "not found", "text/plain")


WEBUI_HTML = """<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
<meta charset=\"UTF-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">
<title>小爱每日播报</title>
<style>
  :root{
    --bg:#0d1117; --bg2:#161b22; --bg3:#0a0e14; --bg-inset:#0d1117;
    --border:#30363d; --text:#c9d1d9; --title:#f0f6fc; --dim:#8b949e; --faint:#484f58;
    --accent:#1f6feb; --accent2:#58a6ff; --red:#f85149; --green:#3fb950;
  }
  body[data-theme=\"light\"]{
    --bg:#ffffff; --bg2:#f6f8fa; --bg3:#f6f8fa; --bg-inset:#ffffff;
    --border:#d0d7de; --text:#24292f; --title:#1f2328; --dim:#57606a; --faint:#8b949e;
    --accent:#0969da; --accent2:#0969da; --red:#cf222e; --green:#1a7f37;
  }
  body{background:var(--bg);color:var(--text);font-family:-apple-system,'PingFang SC',sans-serif;margin:0;padding:16px}
  h1{font-size:18px;color:var(--title)}
  .tabs{display:flex;gap:6px;margin-bottom:12px}
  .tab{padding:8px 18px;border-radius:8px;background:var(--bg2);border:1px solid var(--border);color:var(--dim);cursor:pointer;font-size:14px}
  .tab.on{background:var(--accent);color:#fff;border-color:var(--accent)}
  .card{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:12px}
  .row{display:flex;gap:8px;margin:10px 0;flex-wrap:wrap;align-items:center}
  button{padding:10px 16px;border:none;border-radius:8px;background:var(--accent);color:#fff;font-size:14px;cursor:pointer}
  button:active{opacity:.8}
  button.ghost{background:transparent;border:1px solid var(--accent);color:var(--accent2)}
  button.danger{background:transparent;border:1px solid var(--red);color:var(--red);padding:6px 10px;font-size:12px}
  select{padding:8px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border);max-width:320px}
  #type{padding:10px 16px;font-size:14px;border-radius:8px;max-width:none;height:40px;line-height:1}
  #btnSpeak,#btnText{transition:background .2s}
  #btnSpeak.active,#btnText.active{background:var(--accent);border:1px solid var(--accent);color:#fff}
  #btnSpeak:not(.active),#btnText:not(.active){background:transparent;border:1px solid var(--border);color:var(--accent2)}
  input[type=text]{padding:8px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border)}
  .entry{display:flex;gap:8px;align-items:center;margin-bottom:6px}
  .entry select{flex:1;min-width:200px}
  .entry input{flex:1;min-width:120px}
  .entry .del{background:transparent;border:none;color:var(--red);cursor:pointer;font-size:16px}
  .sec-title{font-size:13px;font-weight:600;color:var(--title);margin-bottom:4px}
  .sec-desc{font-size:11px;color:var(--dim);margin-bottom:10px}
  .add{background:transparent;border:1px dashed var(--border);color:var(--accent2);padding:6px 12px;font-size:12px;cursor:pointer;border-radius:6px}
  .search-row{margin:8px 0}
  .search-row input{width:100%;padding:8px 10px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border)}
  .search-row input:focus{border-color:var(--accent);outline:none}
  #log{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;max-height:400px;overflow-y:auto;background:var(--bg3);padding:10px;border-radius:8px}
  #status{font-size:12px;color:var(--dim)}
  .save-status{font-size:12px;color:var(--green);min-height:18px;margin-top:8px}
  .page{display:none}
  .page.on{display:block}
  .cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:4px;margin-top:10px}
  .cal-dow{text-align:center;font-size:10px;color:var(--faint);padding:4px 0}
  .cal-cell{height:36px;border-radius:6px;display:flex;flex-direction:column;align-items:center;justify-content:center;font-size:13px;cursor:pointer;border:1px solid transparent;color:var(--text);position:relative}
  .cal-cell:hover{background:var(--bg-inset);border-color:var(--border)}
  .cal-cell.empty{cursor:default}
  .cal-cell.has{color:var(--accent2);font-weight:600}
  .cal-cell.has .dot{width:5px;height:5px;border-radius:50%;background:var(--accent);position:absolute;bottom:3px}
  .cal-cell.today{border-color:var(--accent)}
  .cal-cell.selected{background:var(--accent);color:#fff}
  .cal-cell.selected .dot{background:#fff}
  .hist-entry{padding:10px 12px;margin-bottom:6px;border-radius:8px;background:var(--bg-inset);border:1px solid var(--border);cursor:pointer}
  .hist-entry:active{opacity:.8}
  .hist-entry .t{font-size:11px;color:var(--dim);margin-bottom:2px}
  .hist-entry .x{font-size:13px;line-height:1.5}
  .hist-entry .b{font-size:10px;color:var(--accent2);margin-top:2px}
  .hist-entry .type-badge{display:inline-block;font-size:10px;font-weight:600;border-radius:5px;padding:0 5px;margin-left:6px}
  .tb-daily{color:#58a6ff;border:1px solid #58a6ff55;background:#58a6ff18}
  .tb-weekly{color:#3fb950;border:1px solid #3fb95055;background:#3fb95018}
  .tb-monthly{color:#bc8cff;border:1px solid #bc8cff55;background:#bc8cff18}
  .tb-yearly{color:#f85149;border:1px solid #f8514955;background:#f8514918}
  .hist-full-line{padding:6px 8px;margin-bottom:3px;border-radius:6px;background:var(--bg-inset);font-size:13px;line-height:1.6}
</style>
</head>
<body data-theme=\"dark\">
<h1 style=\"display:flex;align-items:center;gap:8px\">🎙️ 小爱每日播报
  <button class=\"ghost\" id=\"themeBtn\" onclick=\"toggleTheme()\" style=\"margin-left:auto;padding:6px 12px;font-size:12px\">☀️ 日间</button>
</h1>
<div class=\"tabs\">
  <div class=\"tab on\" id=\"tabMain\" onclick=\"showPage('main')\">📢 播报</div>
  <div class=\"tab\" id=\"tabCfg\" onclick=\"showPage('cfg')\">⚙️ 传感器配置</div>
  <div class=\"tab\" id=\"tabHist\" onclick=\"showPage('hist')\">📜 历史</div>
</div>

<!-- 播报页 -->
<div class=\"page on\" id=\"page-main\">
  <div class=\"card\">
    <div class=\"row\">
      <select id=\"type\">
        <option value=\"daily\">📅 每日</option>
        <option value=\"weekly\">🗓️ 周</option>
        <option value=\"monthly\">📆 月</option>
        <option value=\"yearly\">🎆 年</option>
      </select>
      <button id=\"btnSpeak\" onclick=\"trigger(false)\">📢 立即播报</button>
      <button id=\"btnText\" class=\"ghost\" onclick=\"trigger(true)\">📝 只看文字</button>
      <button class=\"ghost\" onclick=\"clearLog()\">🗑️ 清空日志</button>
    </div>
    <div id=\"status\">就绪</div>
  </div>
  <div class=\"card\" id=\"textCard\" style=\"display:none\">
    <div class=\"sec-title\">📝 播报文字</div>
    <div id=\"textOut\" style=\"font-size:14px;line-height:1.8;white-space:pre-wrap\"></div>
  </div>
  <div class=\"card\">
    <div id=\"log\">日志加载中...</div>
  </div>
</div>

<!-- 传感器配置页 -->
<div class=\"page\" id=\"page-cfg\">
  <div class=\"card\">
    <div class=\"sec-title\">🎙️ 播报音箱</div>
    <div class=\"sec-desc\">选择小米音箱的 notify 实体（支持的所有音箱都会列出）</div>
    <div class=\"row\">
      <select id=\"cfg-speaker\" style=\"flex:1;max-width:360px\"></select>
    </div>
  </div>
  <div class=\"card\">
    <div class=\"sec-title\">🔧 实体映射</div>
    <div class=\"sec-desc\">从下拉选择你家的传感器实体，填房间名。保存后生效。</div>
    <div id=\"sections\"></div>
    <button onclick=\"saveCfg()\" style=\"margin-top:12px;width:100%\">💾 保存配置</button>
    <div class=\"save-status\" id=\"cfgStatus\"></div>
  </div>
</div>

<!-- 历史页 -->
<div class=\"page\" id=\"page-hist\">
  <div class=\"card\">
    <div class=\"row\" style=\"justify-content:center\">
      <button class=\"ghost\" onclick=\"histNav(-1)\">‹</button>
      <span id=\"histTitle\" style=\"font-size:15px;font-weight:600;min-width:120px;text-align:center\"></span>
      <button class=\"ghost\" onclick=\"histNav(1)\">›</button>
    </div>
    <div class=\"cal-grid\" id=\"histCal\"></div>
  </div>
  <div class=\"card\">
    <div id=\"histList\" style=\"font-size:12px;color:#8b949e\">选择日期查看播报记录</div>
  </div>
</div>

<script>
// 🔑 ingress 前缀：HA ingress 下页面 URL 是 /api/hassio_ingress/<token>/...
// 必须用相对路径（基于 location.pathname）而不是写死 /，否则请求打到 HA 主 API
var BASE = location.pathname.replace(/\\/[^/]*$/, '/');

/* ─── 主题（白天/夜晚）─── */
function setTheme(t){
  document.body.setAttribute('data-theme',t);
  document.getElementById('themeBtn').textContent=(t==='dark'?'☀️ 日间':'🌙 夜间');
}
function toggleTheme(){
  var cur=document.body.getAttribute('data-theme')||'dark';
  var next=(cur==='dark')?'light':'dark';
  setTheme(next);
  // 存后端 /data/theme.json（ingress 下 localStorage 不可用）
  try{ fetch(BASE+'theme',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({theme:next})}).catch(function(){}); }catch(e){}
}
(function(){
  try{
    fetch(BASE+'theme?'+Date.now()).then(function(r){return r.json()}).then(function(d){
      if(d&&d.theme) setTheme(d.theme);
    }).catch(function(){});
  }catch(e){}
})();

var ENTITIES=[], CONFIG={};
// kw: 关键词数组——只显示实体 id/名称含这些词的（区分温度/湿度/用电等，避免 500+ 混在一起）
var SECTIONS=[
  {key:'temp_sensors',title:'🌡️ 温度传感器',desc:'家里各房间温度',domains:['sensor'],kw:['temperature','温度','t2','temp'],room:true},
  {key:'humidity_sensors',title:'💧 湿度传感器',desc:'家里各房间湿度',domains:['sensor'],kw:['humidity','湿度','relative_humidity'],room:true},
  {key:'power_sensors',title:'⚡ 用电（今日电量）',desc:'用 *_power_cost_today 实体（今日累计 kWh）',domains:['sensor'],kw:['power_cost_today','今日电量','耗电'],room:true},
  {key:'power_now',title:'🔌 实时功率（可选）',desc:'用 *_electric_power 实体（单位 W，高功耗提醒）',domains:['sensor'],kw:['electric_power','power','功率'],room:true},
  {key:'lights',title:'💡 灯光',desc:'家里灯，播报\"没关灯\"提醒',domains:['light'],room:true},
  {key:'doors',title:'🚪 门窗',desc:'on=开，播报安全巡检',domains:['binary_sensor'],kw:['contact','门','窗','magnet'],room:true},
  {key:'important_devices',title:'⚠️ 重要设备',desc:'空调/风扇等，离线3天内播报提醒',domains:['climate','fan','media_player','switch'],room:true},
];

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}

function showPage(p){
  document.getElementById('tabMain').className='tab'+(p==='main'?' on':'');
  document.getElementById('tabCfg').className='tab'+(p==='cfg'?' on':'');
  document.getElementById('tabHist').className='tab'+(p==='hist'?' on':'');
  document.getElementById('page-main').className='page'+(p==='main'?' on':'');
  document.getElementById('page-cfg').className='page'+(p==='cfg'?' on':'');
  document.getElementById('page-hist').className='page'+(p==='hist'?' on':'');
  if(p==='cfg'){ loadCfgPage(); }
  if(p==='hist'){ loadHist(); }
}

function optsFor(domain, cur){
  var h='';
  ENTITIES.forEach(function(e){
    if(e.domain===domain || domain.indexOf(e.domain)>=0){
      var sel=(e.entity_id===cur)?' selected':'';
      var label=e.name?e.name+' ('+e.entity_id+')':e.entity_id;
      h+='<option value=\"'+esc(e.entity_id)+'\"'+sel+'>'+esc(label)+'</option>';
    }
  });
  return h;
}

// 填充音箱下拉：列出所有 notify. 实体（可播报的音箱）
function fillSpeakerSelect(cur){
  var sel=document.getElementById('cfg-speaker');
  var opts=[];
  ENTITIES.forEach(function(e){
    if(e.domain==='notify') opts.push(e);
  });
  if(!opts.length){ sel.innerHTML='<option value=\"\">(未找到 notify 实体)</option>'; return; }
  var h='<option value=\"\">选择音箱...</option>';
  opts.forEach(function(e){
    var name=e.name?e.name+' ('+e.entity_id+')':e.entity_id;
    var selFlag=(e.entity_id===cur)?' selected':'';
    h+='<option value=\"'+esc(e.entity_id)+'\"'+selFlag+'>'+esc(name)+'</option>';
  });
  sel.innerHTML=h;
}

function loadCfgPage(){
  document.getElementById('cfgStatus').textContent='加载中...';
  // /cfg/ 前缀避免 ingress 下 /api/ 嵌套被 HA 拦截
  Promise.all([
    fetch(BASE+'cfg/entities?'+Date.now()).then(function(r){return r.json()}).catch(function(err){return {ok:false,entities:[],error:'请求失败: '+(err&&err.message||'')}}),
    fetch(BASE+'cfg/config?'+Date.now()).then(function(r){return r.json()}).catch(function(){return{}}),
  ]).then(function(res){
    var er=res[0]||{}; CONFIG=res[1]||{};
    ENTITIES=er.entities||[];
    fillSpeakerSelect(CONFIG.speaker_notify||'');
    renderSections();
    if(er.ok){
      document.getElementById('cfgStatus').textContent='✅ 已加载 '+er.count+' 个实体';
    }else{
      document.getElementById('cfgStatus').textContent='❌ 实体加载失败: '+(er.error||'未知错误');
    }
  });
}

function renderSections(){
  var el=document.getElementById('sections');
  var h='';
  SECTIONS.forEach(function(sec){
    var items=CONFIG[sec.key]||{};
    var entries=[];
    for(var k in items){ if(k!=='_note') entries.push([k,items[k]]); }
    h+='<div class=\"sec-title\" style=\"margin-top:12px\">'+sec.title+'</div>';
    h+='<div class=\"sec-desc\">'+sec.desc+'</div>';
    h+='<div class=\"search-row\"><input type=\"text\" placeholder=\"🔍 点一下显示全部，输入关键词过滤（如 卧室/温度）\" onfocus=\"onSearch(\\''+sec.key+'\\')\" oninput=\"onSearch(\\''+sec.key+'\\')\" id=\"search-'+sec.key+'\"></div>';
    h+='<div id=\"sec-'+sec.key+'\">';
    entries.forEach(function(pair){
      h+=entryHTML(sec, pair[0], pair[1]);
    });
    h+='</div>';
    h+='<div id=\"cand-'+sec.key+'\"></div>';
    h+='<button class=\"add\" onclick=\"addEntry(\\''+sec.key+'\\')\">+ 手动添加</button>';
  });
  el.innerHTML=h;
}

// 搜索某个 section 的实体候选：按 kw 预过滤(区分温度/湿度/用电等) + 点开显示全部 + 输入再过滤（最多 30 个）
function onSearch(key){
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var q=(document.getElementById('search-'+key).value||'').toLowerCase().trim();
  var box=document.getElementById('cand-'+key);
  var used={};
  var box2=document.getElementById('sec-'+key);
  if(box2) box2.querySelectorAll('.entry input').forEach(function(s){used[(s.value||'').trim()]=true;});
  var matches=[];
  ENTITIES.forEach(function(e){
    if(sec.domains.indexOf(e.domain)<0) return;
    if(used[e.entity_id]) return;
    // kw 预过滤：实体 id/名称 必须含某关键词（区分温度/湿度/用电）
    if(sec.kw){
      var low=(e.entity_id+' '+(e.name||'')).toLowerCase();
      var kwHit=false;
      for(var i=0;i<sec.kw.length;i++){ if(low.indexOf(sec.kw[i])>=0){kwHit=true;break;} }
      if(!kwHit) return;
    }
    var hay=(e.entity_id+' '+(e.name||'')).toLowerCase();
    if(!q || hay.indexOf(q)>=0) matches.push(e);
  });
  if(!matches.length){ box.innerHTML='<div style=\"color:#8b949e;font-size:12px;padding:6px 0\">无匹配实体</div>'; return; }
  var total=matches.length;
  var h='<div style=\"font-size:11px;color:#8b949e;margin-top:8px;margin-bottom:4px\">共 '+total+' 个'+(q?'，匹配 “'+esc(q)+'”':'')+'（点击选择，可滚动）</div>';
  // 全部显示 + 限高可滚动（最多约 20 条可见，超出滚动查看）
  h+='<div style=\"max-height:320px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;padding:4px\">';
  matches.forEach(function(e){
    var label=(e.name?e.name+' — ':'')+e.entity_id;
    h+='<div style=\"padding:7px 10px;margin-bottom:3px;border-radius:6px;background:var(--bg-inset,#0d1117);border:1px solid #30363d;cursor:pointer;font-size:12px\" onclick=\"pickCandidate(\\''+key+'\\',\\''+e.entity_id+'\\')\">🔗 '+esc(label)+'</div>';
  });
  h+='</div>';
  box.innerHTML=h;
}

// 点击候选实体 → 添加到该 section 映射（房间名空，用户填）
function pickCandidate(key, eid){
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var box=document.getElementById('sec-'+key);
  box.insertAdjacentHTML('beforeend', entryHTML(sec, eid, ''));
  collectSection(key);
  document.getElementById('cand-'+key).innerHTML='';
  var si=document.getElementById('search-'+key); if(si) si.value='';
}

function entryHTML(sec, eid, room, editable){
  var ph=sec.room?'房间名':'标签';
  // editable=true：手动添加，第一个框可编辑填实体 id；否则只读（候选添加）
  var eidAttr = editable
    ? 'placeholder=\"填实体 id\" oninput=\"manualEid(this,\\''+sec.key+'\\')\"'
    : 'readonly';
  var eidStyle = editable
    ? 'flex:1.4;min-width:180px;background:var(--bg-inset);border:1px solid var(--border);color:var(--text)'
    : 'flex:1.4;min-width:180px;background:transparent;border:1px solid #30363d;color:#c9d1d9';
  return '<div class=\"entry\" data-raw=\"'+esc(eid)+'\">'
    +'<input type=\"text\" value=\"'+esc(eid)+'\" '+eidAttr+' style=\"'+eidStyle+'\">'
    +'<input type=\"text\" value=\"'+esc(room||'')+'\" placeholder=\"'+ph+'\" oninput=\"onRoom(this,\\''+sec.key+'\\')\">'
    +'<button class=\"del\" onclick=\"delEntry(\\''+sec.key+'\\',this)\">✕</button>'
    +'</div>';
}

// 收集每段当前条目到 CONFIG（读 DOM 里的 .entry）
function collectSection(key){
  var box=document.getElementById('sec-'+key);
  if(!box) return;
  var out={};
  box.querySelectorAll('.entry').forEach(function(e){
    var ins=e.querySelectorAll('input');
    var eid=(ins[0]&&ins[0].value||'').trim();
    var room=(ins[1]&&ins[1].value||'').trim();
    if(eid) out[eid]=room;
  });
  CONFIG[key]=out;
}

// 手动添加：追加一个可编辑空条目（实体 id + 房间名都可填），收回搜索候选
function addEntry(key){
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var box=document.getElementById('sec-'+key);
  box.insertAdjacentHTML('beforeend', entryHTML(sec,'','',true));
  hideAllCands();
}

// 收回所有搜索候选区
function hideAllCands(){
  SECTIONS.forEach(function(s){
    var c=document.getElementById('cand-'+s.key);
    if(c) c.innerHTML='';
    var si=document.getElementById('search-'+s.key);
    if(si) si.value='';
  });
}
// 点击页面空白处收回候选（点击非搜索框/非候选区域）
document.addEventListener('click',function(ev){
  if(ev.target && ev.target.id && ev.target.id.indexOf('search-')===0) return;
  if(ev.target && ev.target.closest && ev.target.closest('#page-cfg')) {
    // 在配置页内但不在搜索框上
    hideAllCands();
  }
});

// 手动条目实体 id 输入：同步更新 CONFIG（去掉空实体 id）
function manualEid(el,key){
  var box=document.getElementById('sec-'+key);
  var ins=box.querySelectorAll('.entry input');
  var eid=el.value.trim();
  // 收集该条目：实体 id + 房间名
  collectSection(key);
}

// 房间名输入：实时收集
function onRoom(el,key){ collectSection(key); }

// 删除条目：直接删 DOM，删除后重新收集 CONFIG（不再用索引，用按钮定位父元素）
function delEntry(key,btn){
  var entry=btn.closest('.entry');
  if(entry) entry.remove();
  collectSection(key);
}

function saveCfg(){
  document.getElementById('cfgStatus').textContent='保存中...';
  SECTIONS.forEach(function(sec){ collectSection(sec.key); });
  var updates={
    speaker_notify: document.getElementById('cfg-speaker').value,
    temp_sensors: CONFIG.temp_sensors||{},
    humidity_sensors: CONFIG.humidity_sensors||{},
    power_sensors: CONFIG.power_sensors||{},
    power_now: CONFIG.power_now||{},
    lights: CONFIG.lights||{},
    doors: CONFIG.doors||{},
    important_devices: CONFIG.important_devices||{},
  };
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:updates})})
    .then(function(r){return r.json()})
    .then(function(d){
      document.getElementById('cfgStatus').textContent = d.ok?'✅ 已保存':'❌ 保存失败';
    })
    .catch(function(){document.getElementById('cfgStatus').textContent='❌ 保存失败';});
}

/* ─── 播报页 ─── */
var lastTextRun=0;
function trigger(textOnly){
  var t=document.getElementById('type').value;
  // 🔑 按钮选中态：点了哪个哪个蓝底白字，另一个普通样式
  document.getElementById('btnSpeak').className = textOnly ? '' : 'active';
  document.getElementById('btnText').className = textOnly ? 'active' : '';
  lastTextRun=Date.now();  // 🔑 记录本次触发时间，pollText 用它忽略旧状态
  fetch(BASE+'trigger?type='+t+(textOnly?'&text_only=true':'')+'&_='+Date.now(),{method:'POST'})
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.ok){
        document.getElementById('status').textContent='✅ 已触发 '+(textOnly?'文字':'语音')+'播报';
      }else{
        document.getElementById('status').textContent='⚠️ '+(d.error||'已有播报进行中，请稍候');
      }
    })
    .catch(function(){document.getElementById('status').textContent='❌ 触发失败';});
  if(textOnly){ showTextCard(); }
  setTimeout(refreshLog,1000);
}
function showTextCard(){
  document.getElementById('textCard').style.display='block';
  document.getElementById('textOut').textContent='生成中...';
  textStartTime=Date.now();
  pollText();
}
var textStartTime=0;
var pollTimer=null;
function pollText(){
  if(pollTimer) clearTimeout(pollTimer);
  // ⏱️ 超时保护：超过 180 秒还没生成完就提示，避免一直卡\"生成中\"
  if(Date.now()-textStartTime > 180000){
    document.getElementById('textOut').textContent='⏱️ 生成超时，请查看日志';
    return;
  }
  fetch(BASE+'state?'+Date.now()).then(function(r){return r.json()}).then(function(st){
    var out=document.getElementById('textOut');
    if(st.status==='idle'||st.status==='preparing'||st.status==='broadcasting'){
      // 空闲/生成中：不显示旧内容，继续等（失败/异常也继续轮询）
      var ss=st.sentences||[];
      if(st.phase) out.textContent=st.phase+'...';
      else if(ss.length) out.textContent=ss.map(function(s){return s.text}).join('\\n');
      else out.textContent='生成中...';
      pollTimer=setTimeout(pollText,1000);
    }else if(st.status==='done'){
      var all=st.sentences||[];
      if(all.length) out.textContent=all.map(function(s){return s.text}).join('\\n');
      else out.textContent='(无文字内容)';
      // 显示完成状态
      document.getElementById('status').textContent='✅ 文字已生成';
    }
  }).catch(function(){
    // 请求失败：重试而不是静默停（避免\"一直生成中\"）
    pollTimer=setTimeout(pollText,1500);
  });
}
function refreshLog(){
  fetch(BASE+'logs?'+Date.now()).then(function(r){return r.json()}).then(function(a){
    document.getElementById('log').textContent=(a||[]).join('\\n')||'暂无日志';
    document.getElementById('log').scrollTop=document.getElementById('log').scrollHeight;
  });
}
function clearLog(){
  fetch(BASE+'logs/clear',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    document.getElementById('log').textContent='';
  }).catch(function(){document.getElementById('log').textContent='';});
}
/* ─── 历史记录 ─── */
var histYM={y:new Date().getFullYear(),m:new Date().getMonth()};
var histDays={}, histSel='';
function pad(n){return n<10?'0'+n:''+n;}
function histFmt(y,m){return y+'-'+pad(m+1);}
function histLoadCal(){
  var ym=histFmt(histYM.y,histYM.m);
  fetch(BASE+'history/days?month='+ym+'&_='+Date.now()).then(function(r){return r.json()}).then(function(days){
    histDays={}; (days||[]).forEach(function(d){histDays[d]=true;});
    histRenderCal();
  }).catch(function(){histDays={};histRenderCal();});
}
function histRenderCal(){
  var y=histYM.y,m=histYM.m;
  var first=new Date(y,m,1).getDay();
  var daysInMonth=new Date(y,m+1,0).getDate();
  var today=new Date();
  var todayStr=histFmt(today.getFullYear(),today.getMonth())+'-'+pad(today.getDate());
  if(histSel && !histSel.startsWith(histFmt(y,m))) histSel='';
  document.getElementById('histTitle').textContent=y+'年'+(m+1)+'月';
  var h='';
  ['日','一','二','三','四','五','六'].forEach(function(w){h+='<div class=\"cal-dow\">'+w+'</div>';});
  for(var i=0;i<first;i++) h+='<div class=\"cal-cell empty\"></div>';
  for(var d=1;d<=daysInMonth;d++){
    var ds=histFmt(y,m)+'-'+pad(d);
    var has=histDays[ds];
    var cls='cal-cell'+(has?' has':'')+(ds===todayStr?' today':'')+(ds===histSel?' selected':'');
    h+='<div class=\"'+cls+'\" onclick=\"histPick(\\''+ds+'\\')\">'+d+(has?'<span class=\"dot\"></span>':'')+'</div>';
  }
  document.getElementById('histCal').innerHTML=h;
}
function histNav(dir){
  histYM.m+=dir;
  if(histYM.m<0){histYM.m=11;histYM.y--;}
  if(histYM.m>11){histYM.m=0;histYM.y++;}
  histLoadCal();
}
function histPick(ds){
  histSel=ds;
  histRenderCal();
  var list=document.getElementById('histList');
  list.innerHTML='<div style=\"color:var(--dim)\">加载中...</div>';
  fetch(BASE+'history/day?date='+ds+'&_='+Date.now()).then(function(r){return r.json()}).then(function(entries){
    if(!entries.length){list.innerHTML='<div style=\"color:var(--dim)\">当天暂无播报</div>';return;}
    var tb={'daily':'日报','weekly':'周报','monthly':'月报','yearly':'年报'};
    var tc={'daily':'tb-daily','weekly':'tb-weekly','monthly':'tb-monthly','yearly':'tb-yearly'};
    // 统计各类型次数
    var typeOrder=['daily','weekly','monthly','yearly'];
    var cnt={daily:0,weekly:0,monthly:0,yearly:0};
    entries.forEach(function(e){cnt[e.type||'daily']=(cnt[e.type||'daily']||0)+1;});
    var h='<div style=\"font-size:12px;color:var(--dim);margin-bottom:8px\">'+ds+' · '+entries.length+'次播报';
    typeOrder.forEach(function(t){
      if(cnt[t]>0) h+=' <span class=\"type-badge '+tc[t]+'\">'+tb[t]+' '+cnt[t]+'</span>';
    });
    h+='</div>';
    entries.forEach(function(e,i){
      var t=e.type||'daily';
      h+='<div class=\"hist-entry\" onclick=\"histView(\\''+ds+'\\','+i+')\">';
      h+='<div class=\"t\">'+e.ts+' <span class=\"type-badge '+tc[t]+'\">'+tb[t]+'</span></div>';
      h+='<div class=\"x\">'+esc(e.text||'')+'</div>';
      h+='<div class=\"b\">'+e.count+'句 ›</div>';
      h+='</div>';
    });
    list.innerHTML=h;
  }).catch(function(){list.innerHTML='<div style=\"color:var(--red)\">加载失败</div>';});
}
function histView(ds,idx){
  var list=document.getElementById('histList');
  fetch(BASE+'history/day?date='+ds+'&_='+Date.now()).then(function(r){return r.json()}).then(function(entries){
    var e=entries[idx];
    var all=e.sentences||[];
    var tb={'daily':'日报','weekly':'周报','monthly':'月报','yearly':'年报'};
    var tc={'daily':'tb-daily','weekly':'tb-weekly','monthly':'tb-monthly','yearly':'tb-yearly'};
    var t=e.type||'daily';
    var h='<button class=\"ghost\" onclick=\"histPick(\\''+ds+'\\')\" style=\"margin-bottom:10px;padding:6px 12px;font-size:12px\">← 返回</button>';
    h+='<div style=\"font-size:12px;color:var(--dim);margin-bottom:8px\">'+ds+' '+e.ts+' · 完整内容 <span class=\"type-badge '+tc[t]+'\">'+tb[t]+'</span></div>';
    all.forEach(function(s){ h+='<div class=\"hist-full-line\">'+esc(s.text||'')+'</div>'; });
    list.innerHTML=h;
  });
}
function loadHist(){ histLoadCal(); }

setInterval(refreshLog,5000);
refreshLog();
// 初始默认选中\"立即播报\"
document.getElementById('btnSpeak').className='active';
</script>
</body>
</html>
"""


def main():
    port = int(os.environ.get("INGRESS_PORT") or os.environ.get("PORT") or "8099")
    # 调度循环在独立线程
    threading.Thread(target=scheduler_loop, daemon=True).start()
    # 播报按钮：创建 + 监听（按下触发播报）
    try:
        ensure_button_entity()
        threading.Thread(target=button_watcher, daemon=True).start()
    except Exception as e:
        log(f"⚠️ 播报按钮初始化失败: {e}")
    # Web 服务主线程
    log(f"🌐 Web UI 已启动 (port {port})")
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
