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
from datetime import datetime, timedelta
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
        # 🔑 累计差值用电（power_type=accumulate）：附带今日差值 = 当前累计 - 今日基准
        # 前端状态徽章显示"实际值 / 差值"，方便用户核对播报数字
        calc_map = {}
        try:
            cfg = ds.load_config()
            ps = cfg.get("power_sensors") or {}
            for eid, meta in ps.items():
                if eid.startswith("_"):
                    continue
                if isinstance(meta, dict) and meta.get("power_type") == "accumulate":
                    calc_map[eid] = True
        except Exception:
            pass
        bl = ds.load_power_baseline()
        # 🔑 基准过期/缺失：主动记录当前为基准（和播报 calc_device_energy 一致：当天从0开始算）
        # 否则跨天后 bl_map 空 → 累计差值不显示（"一开始好用后来不显示"的根因）
        if bl.get("date") != datetime.now().strftime("%Y-%m-%d"):
            try:
                _smap = {s.get("entity_id"): s for s in states}
                ds.record_power_baseline(_smap, ps)
                bl = ds.load_power_baseline()
            except Exception:
                pass
        bl_map = bl.get("baselines", {})
        out = []
        for s in states:
            eid = s.get("entity_id", "")
            item = {
                "entity_id": eid,
                "name": s.get("attributes", {}).get("friendly_name", ""),
                "state": s.get("state", ""),
                "domain": eid.split(".")[0],
            }
            if eid in calc_map:
                try:
                    cur = float(s.get("state", "")) if s.get("state", "") not in ("unavailable", "unknown", "") else None
                except (ValueError, TypeError):
                    cur = None
                base = bl_map.get(eid)
                if cur is not None and base is not None:
                    item["calc"] = round(max(0.0, cur - base), 2)
            out.append(item)
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
    """读配置供配置页展示。🔑 读原始 JSON（不规范化）——否则 usage/power_type 被 load_config 降成 room 字符串丢失。
    JSON 损坏/缺文件时返回空配置（不崩，让前端至少能打开）。"""
    try:
        if ds.CONFIG_PATH.exists():
            return json.loads(ds.CONFIG_PATH.read_text())
    except Exception as e:
        log(f"⚠️ 读取配置失败（返回空配置）: {e}")
    return {}


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


# 4 个播报按钮：日/周/月/年。entity_id 由名字拼音生成（创建后按实际 id 更新）
BUTTONS = [
    {"type": "daily",   "name": "传感器实体·日", "entity": "input_button.chuan_gan_qi_shi_ti_ri", "icon": "mdi:calendar-today"},
    {"type": "weekly",  "name": "传感器实体·周",   "entity": "input_button.chuan_gan_qi_shi_ti_zhou", "icon": "mdi:calendar-week"},
    {"type": "monthly", "name": "传感器实体·月",   "entity": "input_button.chuan_gan_qi_shi_ti_yue", "icon": "mdi:calendar-month"},
    {"type": "yearly",  "name": "传感器实体·年",   "entity": "input_button.chuan_gan_qi_shi_ti_nian", "icon": "mdi:calendar-star"},
]
# 旧版兼容按钮 an_niu 已移除（用户要求删掉，避免 5 个实体）


def _ws_connect():
    """建 WS 连接并认证，返回 (ws, token)。"""
    import websockets
    ws_url, api, token = ds._ha_endpoints()
    if not token:
        raise RuntimeError("无 HA token")
    return ws_url, token


def _fetch_all_states():
    """拉取 HA 全部实体状态 dict {entity_id: state}。失败返回 {}。"""
    import websockets
    try:
        ws_url, token = _ws_connect()
        async def _get():
            async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                await ws.recv()
                await ws.send(json.dumps({"type": "auth", "access_token": token}))
                await ws.recv()
                await ws.send(json.dumps({"type": "get_states", "id": 1}))
                r = json.loads(await ws.recv())
                return {s["entity_id"]: s for s in r.get("result", [])}
        return asyncio.run(_get())
    except Exception:
        return {}


def generate_greeting():
    """🤖 用大模型生成一句自然问候语（调 LLM）。失败返回默认问候。"""
    import urllib.request as _ur
    try:
        cfg = ds.load_config()
        llm = (cfg.get("engine", {}).get("llm") or {})
        base = (llm.get("base_url") or "").rstrip("/")
        key = llm.get("api_key") or ""
        if not base:
            return "你好呀，欢迎收听今天的小爱播报。"
        url = base + "/v1/messages"
        hdrs = {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
        if key:
            hdrs["x-api-key"] = key
        body = json.dumps({
            "model": llm.get("model", "deepseek-chat"),
            "max_tokens": 200,
            "system": "你是一个温暖亲切的小爱音箱播报助手。只输出一句简短自然的开场问候（10-25字），不要多余内容。",
            "messages": [{"role": "user", "content": "请生成一句今天的开场问候。"}],
        }).encode()
        req = _ur.Request(url, data=body, headers=hdrs)
        with _ur.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        if text:
            return text[:40]
    except Exception as e:
        log(f"⚠️ 生成问候语失败: {e}")
    return "你好呀，欢迎收听今天的小爱播报。"


# ── 一周 7 天文案（问候语/结束语/小贴士）：手动填写 + 大模型生成 ──
WEEKDAY_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

# 阳历固定节日（月-日 → 名称）。农历节日随年份浮动，不写死；靠日期/星期/天气让模型自行发挥
FESTIVALS = {
    "01-01": "元旦", "02-14": "情人节", "03-08": "妇女节", "03-12": "植树节",
    "04-01": "愚人节", "05-01": "劳动节", "05-04": "青年节", "06-01": "儿童节",
    "07-01": "建党节", "08-01": "建军节", "09-10": "教师节", "10-01": "国庆节",
    "12-24": "平安夜", "12-25": "圣诞节",
}

WEATHER_CN = {"clear-night": "晴", "sunny": "晴", "partlycloudy": "多云", "cloudy": "阴",
              "rainy": "小雨", "pouring": "大雨", "snowy": "雪", "fog": "雾", "windy": "风大"}

SECTION_META = {
    "greeting": ("问候语", "一句自然温暖的开场问候（15到45字）。这是整段播报唯一介绍当天日期的地方，自然带上星期几、节日、天气。注意：绝对不要写任何时间段词（早上好/早晨/清晨/上午/中午/下午/午后/傍晚/黄昏/晚上/夜晚/夜幕等——播报可能在任何时段进行，时段词由播报时自动加上，你写死会和实际时间矛盾），写'周六/周日'开头即可，如'周日的时光格外从容'"),
    "ending": ("结束语", "一句自然收尾的话（15到45字）。注意：①今天星期几、是不是周末、什么节日，开头问候语已经说过了，这里绝对不要再重复日期/星期/周末/节日；②绝对不要写任何时间段词（早上/早晨/上午/中午/下午/傍晚/晚上/夜晚/夜色/黄昏/晚安/好梦/睡觉等）——播报可能在任何时段进行，这些词会和实际时间不符，就写通用的收尾祝愿，如'愿这份温暖常伴你左右，一切顺心'"),
    "tip": ("小贴士", "一条任何时候都适用的实用生活小贴士（15到45字）。注意：不要提星期几/周末/节日/日期，也不要写'早上''晚上''睡前'等时段词（播报时会自动加'小贴士''午后小提示''睡前小提示'前缀），就纯说具体建议"),
    "enc": ("鼓励语", "一句温暖鼓励的话（15到45字）。注意：不要提星期几/周末/节日/日期，就纯说鼓励的话，如'辛苦了，早点休息吧'"),
}


def weather_forecast():
    """从 HA 拉天气实体预报（未来几天），返回 [{date, cond, temp}]；无天气实体/失败返回 []。"""
    try:
        states = _fetch_all_states()
        if not states:
            return []
        ent = None
        for eid, s in states.items():
            if eid.startswith("weather."):
                ent = s
                break
        if not ent:
            return []
        attr = ent.get("attributes") or {}
        out = []
        for f in (attr.get("forecast") or []):
            d = (f.get("datetime") or "")[:10]
            if not d:
                continue
            cond = WEATHER_CN.get(f.get("condition", ""), f.get("condition", ""))
            temp = f.get("temperature")
            out.append({"date": d, "cond": cond, "temp": temp})
        return out
    except Exception:
        return []


def generate_week(section, cfg, dates=None):
    """🤖 用大模型生成文案：问候语/结束语/小贴士。默认未来 7 天（今天起）；
    传 dates=[date] 可只生成某一天（自动更新用）。
    输入每天的实际日期、星期、节日、周末、天气，避免千篇一律。
    返回 {"ok": bool, "days": {"YYYY-MM-DD": text}, "error": str}。
    days 可能不满请求天数——缺的日期播报时自动用默认文案。"""
    if section not in SECTION_META:
        return {"ok": False, "days": {}, "error": "section 必须是 greeting/ending/tip"}
    name, req = SECTION_META[section]
    llm = cfg.get("engine", {}).get("llm") or {}
    base = (llm.get("base_url") or "").rstrip("/")
    key = llm.get("api_key") or ""
    if not base:
        return {"ok": False, "days": {}, "error": "未配置大模型引擎（base_url）"}
    try:
        import re as _re
        # 要生成的日期列表：默认未来 7 天
        if not dates:
            today = datetime.now().date()
            dates = [today + timedelta(days=i) for i in range(7)]
        n = len(dates)
        weather = weather_forecast()
        lines_ctx = []
        for d in dates:
            parts = f"{d:%Y-%m-%d} {WEEKDAY_CN[d.weekday()]}"
            fest = FESTIVALS.get(d.strftime("%m-%d"), "")
            if fest:
                parts += f"，是{fest}"
            if d.weekday() >= 5:
                parts += "，周末"
            wf = next((w for w in weather if w["date"] == d.strftime("%Y-%m-%d")), None)
            if wf:
                parts += f"，天气{wf['cond']}"
                if wf.get("temp") is not None:
                    parts += f"，{wf['temp']}度左右"
            lines_ctx.append(parts)

        system = (
            f"你是温暖亲切的家庭播报文案写手。请为下面 {n} 个日期各写{name}：{req}。\n"
            "格式要求：每天一行，行首必须是 YYYY-MM-DD 日期加冒号，后面接文案。\n"
            f"只输出 {n} 行，不要任何解释、空行、编号或 Markdown。\n"
            "口语化、适合语音朗读，不要表情符号、括号、引号、英文。"
        )
        # 🔑 非问候语（结束语/小贴士/鼓励语）绝不重复日期/星期/周末——开头问候语已介绍过，再说就啰嗦
        if section != "greeting":
            system += ("重要：文案里绝对不要出现'周六''周末''周一'等星期词，也不要出现日期和节日名称。"
                       "当天是星期几、什么节日，开头的问候语已经说过了，这里重复会让整段播报非常啰嗦。\n")
        user = "这些日期实际情况：\n" + "\n".join(lines_ctx) + f"\n\n请按格式输出 {n} 行{name}。"
        import urllib.request as _ur
        url = base + "/v1/messages"
        hdrs = {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
        if key:
            hdrs["x-api-key"] = key
        body = json.dumps({
            "model": llm.get("model", "claude-sonnet-4-5"),
            "max_tokens": int(llm.get("max_tokens", 2000)),
            "system": system,
            "messages": [{"role": "user", "content": user}],
        }).encode()
        req = _ur.Request(url, data=body, headers=hdrs)
        with _ur.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        if not text:
            return {"ok": False, "days": {}, "error": "模型返回空文本（可能是 max_tokens 不够）"}

        # 解析：每行 "YYYY-MM-DD：文案" → {date: text}
        days = {}
        for ln in text.splitlines():
            m = _re.match(r"(\d{4}-\d{2}-\d{2})[\s:：、\-]+(.+)$", ln.strip())
            if m:
                t = m.group(2).strip().strip("，。.！! ")
                if t:
                    days[m.group(1)] = t
        if not days:
            return {"ok": False, "days": {}, "error": "没解析出任何日期文案"}
        log(f"🤖 已生成{name}（共{len(days)}天文案）")
        return {"ok": True, "days": days, "error": ""}
    except Exception as e:
        log(f"⚠️ 生成{name}失败: {e}")
        return {"ok": False, "days": {}, "error": str(e)}


def generate_one(section, cfg, date_str):
    """🤖 只生成某一天的文案（自动更新用，每天零点刷新当天）。"""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return generate_week(section, cfg, [d])
    except ValueError:
        return {"ok": False, "days": {}, "error": f"日期格式不对: {date_str}"}


# 大模型文案自动更新：开自动更新=每天零点刷新当天；关=每 7 天（窗口滚出当天）更新一回
LLM_SECTIONS = ("greeting", "ending", "tip", "enc")

def llm_auto_refresh():
    """按配置刷新大模型生成的文案（greeting/ending/tip 的 <x>_llm_days）。
    - <x>_llm_autoupdate 开：生成当天这一条（日期变更时调用）
    - 关：当天不在 <x>_llm_days 里（7天窗口滚出）→ 重新生成未来 7 天
    生成结果合并进 <x>_llm_days（清掉早于今天的旧条目）。"""
    try:
        cfg = ds.load_config()
        ts = cfg.get("template_settings") or {}
        today = datetime.now().strftime("%Y-%m-%d")
        changed = False
        for prefix in LLM_SECTIONS:
            if ts.get(prefix + "_mode") != "llm":
                continue
            llm_days = ts.get(prefix + "_llm_days") or {}
            auto_val = ts.get(prefix + "_llm_autoupdate")
            # 自动更新方式：true=每天 / 'weekly'或false(旧)=每7天 / 'off'=关闭（互斥）
            if auto_val == 'off':
                continue  # 关闭自动更新
            auto = auto_val is True
            if auto:
                res = generate_one(prefix, cfg, today)      # 刷新当天
            elif today not in llm_days:
                res = generate_week(prefix, cfg)            # 7天窗口滚出 → 重新生成一周
            else:
                continue
            if res.get("ok") and res.get("days"):
                merged = dict(llm_days)
                for k, v in res["days"].items():
                    merged[k] = v
                # 清掉早于今天的旧条目（只留今天及以后）
                for k in [k for k in merged if k < today]:
                    del merged[k]
                ts[prefix + "_llm_days"] = merged
                changed = True
                log(f"🤖 自动更新{prefix}完成（{'每天模式·刷新当天' if auto else '7天模式·更新一周'}，目前共{len(merged)}天文案）")
        if changed:
            save_edit_cfg({"template_settings": ts})
    except Exception as e:
        log(f"⚠️ LLM 文案自动更新失败: {e}")


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
        # 🔑 迁移：旧命名（xiao_ai_bo_bao_zhou/yue/nian）删掉，统一成"传感器实体"前缀（chuan_gan_qi_shi_ti_*）
        OLD_ENTITIES = ("input_button.xiao_ai_bo_bao_zhou",
                        "input_button.xiao_ai_bo_bao_yue",
                        "input_button.xiao_ai_bo_bao_nian")
        for _old in OLD_ENTITIES:
            if _old in existing:
                try:
                    async def _del_old(entity_id):
                        async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                            await ws.recv()
                            await ws.send(json.dumps({"type": "auth", "access_token": token}))
                            await ws.recv()
                            await ws.send(json.dumps({"id": 5, "type": "config/entity_registry/remove",
                                                      "entity_id": entity_id}))
                            return json.loads(await ws.recv()).get("success", False)
                    if asyncio.run(_del_old(_old)):
                        log(f"🗑️ 已删除旧按钮实体（命名不统一）: {_old}")
                except Exception as e:
                    log(f"⚠️ 删除旧按钮失败 {_old}: {e}")
        ok_all = True
        for b in BUTTONS:
            if b["entity"] in existing:
                # 已存在：同步名字（改名后更新 HA 里的 friendly_name），并显示实体 id
                try:
                    async def _rename(entity_id, name):
                        async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                            await ws.recv()
                            await ws.send(json.dumps({"type": "auth", "access_token": token}))
                            await ws.recv()
                            await ws.send(json.dumps({"id": 2, "type": "config/entity_registry/update",
                                                      "entity_id": entity_id, "name": name}))
                            return json.loads(await ws.recv()).get("success", False)
                    asyncio.run(_rename(b["entity"], b["name"]))
                except Exception:
                    pass
                log(f"✅ 播报按钮已存在: {b['name']} → {b['entity']}")
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
            log(f"{'✅ 已创建' if ok else '⚠️ 创建失败'}播报按钮: {b['name']} → {b['entity']}")
            ok_all = ok_all and ok
        return ok_all
    except Exception as e:
        log(f"⚠️ 创建播报按钮失败: {e}")
        return False


def button_watcher():
    """监听所有播报按钮（每日/周/月/年）：被 press 时触发对应类型的语音播报。
    用 WebSocket 订阅 state_changed 事件（实时、无轮询首轮问题——之前轮询时间戳会有"第一次按不播"）。"""
    import websockets
    log("🔔 播报按钮监听已启动")
    # 实体名映射：entity -> type
    ENT2TYPE = {b["entity"]: b["type"] for b in BUTTONS}
    ENT2NAME = {b["entity"]: b["name"] for b in BUTTONS}
    while True:
        try:
            ws_url, token = _ws_connect()
            async def _watch():
                async with websockets.connect(ws_url, max_size=4 * 1024 * 1024) as ws:
                    await ws.recv()
                    await ws.send(json.dumps({"type": "auth", "access_token": token}))
                    r = json.loads(await ws.recv())
                    if r.get("type") != "auth_ok":
                        log("⚠️ 按钮监听认证失败")
                        return
                    # 订阅所有实体状态变化事件
                    await ws.send(json.dumps({"id": 1, "type": "subscribe_events", "event_type": "state_changed"}))
                    while True:
                        msg = json.loads(await ws.recv())
                        ev = msg.get("event", {}).get("data", {})
                        eid = ev.get("entity_id", "")
                        if eid in ENT2TYPE:
                            new_state = ev.get("new_state", {}) or {}
                            st = new_state.get("state", "")
                            old_state = ev.get("old_state", {}) or {}
                            old_st = old_state.get("state", "")
                            # input_button press：state 从时间戳变成新时间戳。实时事件，无需防重复。
                            if st != old_st and st and old_st and st not in ("unavailable", "unknown") and old_st not in ("unavailable", "unknown"):
                                log(f"🔔 播报按钮被按下（{ENT2NAME[eid]}），触发{ENT2TYPE[eid]}播报")
                                run_broadcast(summary_type=ENT2TYPE[eid], text_only=False)
            asyncio.run(_watch())
        except Exception as e:
            log(f"⚠️ 播报按钮监听错误（重连）: {e}")
        time.sleep(3)


_RUN_COUNT = 0

def run_broadcast(summary_type="daily", text_only=False):
    """在独立线程里跑播报（force=True）。语音播报用锁防双播；文字播报不发声，不抢锁。"""
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
            log(f"📢 [run{run_no}] 手动触发播报: {summary_type}" + ("（文字播报）" if text_only else ""))
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


def power_integral_loop():
    """🔌 常驻功率积分：每 5 分钟读配置 power_type=integral 的功率实体，
    累加 W × 间隔(kWh) 到当天 /data/power_integral.json。"""
    log("🔌 功率积分线程已启动")
    INTERVAL = 300  # 5 分钟
    while True:
        try:
            time.sleep(INTERVAL)
            cfg = ds.load_config()
            ps = cfg.get("power_sensors") or {}
            integ_entities = {}
            for eid, meta in ps.items():
                if eid.startswith("_"):
                    continue
                if isinstance(meta, dict) and meta.get("power_type") == "integral":
                    integ_entities[eid] = meta.get("room", eid)
            if not integ_entities:
                continue
            states = _fetch_all_states()
            if not states:
                continue
            today = datetime.now().strftime("%Y-%m-%d")
            integ = ds.load_power_integral()
            if integ.get("date") != today:
                integ = {"date": today, "kwh": {}}
            # W × 300s → kWh：W*秒/3.6e6
            factor = INTERVAL / 3.6e6
            for eid in integ_entities:
                v = ds.get_float(states, eid)
                if v is not None and v > 0:
                    integ["kwh"][eid] = integ["kwh"].get(eid, 0.0) + v * factor
            ds.save_power_integral(today, integ["kwh"])
        except Exception as e:
            log(f"⚠️ 功率积分错误: {e}")


def scheduler_loop():
    """后台循环：每天 0 点记累计差值基准 + LLM 文案自动更新（定时播报已移除，用自动化/实体触发）。"""
    last_base_date = ""
    log("⏰ 后台循环已启动（基准记录 + LLM 自动更新）")
    while True:
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            # 🔑 每天 0 点（或跨天后首次）记录 accumulate 设备的累计基准
            if today != last_base_date:
                try:
                    bl = ds.load_power_baseline()
                    if bl.get("date") != today:
                        states = _fetch_all_states()
                        if states:
                            ds.record_power_baseline(states, ds.load_config().get("power_sensors") or {})
                            log(f"📊 已记录今日累计电量基准 ({today})")
                except Exception as e:
                    log(f"⚠️ 记录电量基准失败: {e}")
                # 🔑 每天 0 点（日期变更）：LLM 文案自动更新（开=刷新当天，关=7天窗口滚出才重生成）
                try:
                    threading.Thread(target=llm_auto_refresh, daemon=True).start()
                except Exception as e:
                    log(f"⚠️ LLM 文案自动更新启动失败: {e}")
                last_base_date = today

            # 定时播报已移除（用自动化+实体触发）——本循环只做 0 点基准 + LLM 自动更新
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
        elif path == "/engine":
            # 引擎模式（template/llm）：读 /data/engine_mode（前端切换生效）
            try:
                self._send(200, Path("/data/engine_mode.json").read_text(), "application/json")
            except Exception:
                self._send(200, json.dumps({"mode": ds.load_config().get("engine", {}).get("mode", "template")}), "application/json")
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
        elif path == "/history/anomalies":
            # ⚠️ 检测异常传感器数据：某设备单日耗电超阈值（正常家庭不可能）→ 标记该天为异常
            try:
                snaps = ds.load_snapshots()
                threshold = float(q.get("threshold", ["30"])[0])  # 单日耗电超 30 度视为异常
                anomalies = {}  # {date: [设备名]}
                for date_str, s in snaps.items():
                    bad = []
                    p = s.get("power") or {}
                    by_dev = p.get("by_device") or {}
                    for label, kwh in by_dev.items():
                        try:
                            if float(kwh) > threshold:
                                bad.append(label)
                        except (ValueError, TypeError):
                            pass
                    if bad:
                        anomalies[date_str] = bad
                self._send(200, json.dumps({"anomalies": anomalies}, ensure_ascii=False), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/entities/buttons":
            # 播报按钮实体列表（供用户在自动化里引用）
            self._send(200, json.dumps([{"name": b["name"], "entity": b["entity"], "type": b["type"]} for b in BUTTONS], ensure_ascii=False), "application/json")
        elif path == "/stats":
            # 📊 统计：传感器总个数 / 离线传感器数 / 统计天数（日志页顶部显示）
            try:
                cfg = ds.load_config()
                sensor_segs = ("temp_sensors", "humidity_sensors", "power_sensors", "power_now",
                               "lights", "doors", "important_devices")
                total = 0
                for seg in sensor_segs:
                    total += len([k for k in (cfg.get(seg) or {}) if not k.startswith("_")])
                # 离线传感器：拉 HA states 统计 unavailable
                offline = 0
                try:
                    states = _fetch_all_states()
                    for seg in sensor_segs:
                        for eid in (cfg.get(seg) or {}):
                            if eid.startswith("_"): continue
                            s = states.get(eid)
                            if s and s.get("state") == "unavailable":
                                offline += 1
                except Exception:
                    pass
                days = len(ds.load_snapshots())
                self._send(200, json.dumps({"sensors": total, "offline": offline, "days": days}, ensure_ascii=False), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
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
        elif path == "/history/delete":
            # 🗑️ 删除某条历史记录（date+ts 匹配，POST）
            try:
                date_str = q.get("date", [""])[0]
                ts = q.get("ts", [""])[0]
                if HISTORY_FILE.exists():
                    lines = HISTORY_FILE.read_text().splitlines()
                    keep = []
                    for line in lines:
                        try:
                            r = json.loads(line)
                        except Exception:
                            keep.append(line)
                            continue
                        if r.get("date") == date_str and r.get("ts") == ts:
                            continue  # 删除这条
                        keep.append(line)
                    HISTORY_FILE.write_text("\n".join(keep) + ("\n" if keep else ""))
                    self._send(200, json.dumps({"ok": True, "msg": f"已删除 {date_str} {ts} 的记录"}), "application/json")
                else:
                    self._send(200, json.dumps({"ok": True, "msg": "无历史文件"}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/fix-anomaly":
            # 🛠️ 修复异常（POST）：异常设备的用电记录用前两天的平均值替换（不是删除）
            # 例：卧室电源2 某天 55 度（传感器异常）→ 用前两天正常值平均修正
            try:
                date_str = q.get("date", [""])[0]
                snaps = ds.load_snapshots()
                # 按日期找该设备前两天的正常值（排除当天、排除异常值>30）
                def _prior_avg(dev_label):
                    vals = []
                    for d in sorted(snaps.keys()):
                        if d >= date_str:
                            continue
                        dd = (snaps[d].get("power") or {}).get("by_device") or {}
                        v = dd.get(dev_label)
                        if isinstance(v, (int, float)) and 0 <= v <= 30:  # 只取正常值
                            vals.append(v)
                        if len(vals) >= 2:
                            break
                    return (sum(vals) / len(vals)) if vals else None
                if date_str in snaps:
                    s = snaps[date_str]
                    p = s.get("power") or {}
                    by_dev = p.get("by_device") or {}
                    # 找出异常的设备（单日耗电超30度）
                    bad_devs = [lb for lb, k in by_dev.items() if isinstance(k,(int,float)) and k > 30]
                    fixed = []
                    for lb in bad_devs:
                        avg = _prior_avg(lb)
                        if avg is not None:
                            by_dev[lb] = round(avg, 2)   # 用前两天平均值替换异常值
                            fixed.append(f"{lb}:{by_dev[lb]}度")
                        else:
                            # 无历史正常值 → 删除该设备当天记录
                            del by_dev[lb]
                            fixed.append(f"{lb}:无历史删除")
                    # 重算 total
                    p["total_kwh"] = round(sum(by_dev.values()), 1)
                    s["power"] = p
                    snaps[date_str] = s
                    ds.save_snapshots(snaps)
                    msg = f"已修复 {date_str}：{'、'.join(fixed) if fixed else '无异常设备'}"
                else:
                    msg = f"{date_str} 无快照数据"
                self._send(200, json.dumps({"ok": True, "msg": msg}), "application/json")
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
        elif path == "/log":
            # 📋 前端操作日志（切换自动更新等），写进日志界面
            try:
                data = json.loads(self._body() or b"{}")
                msg = (data.get("msg") or "").strip()
                if msg:
                    log(msg)
                    self._send(200, json.dumps({"ok": True}), "application/json")
                else:
                    self._send(400, json.dumps({"ok": False, "error": "msg required"}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/clear-snapshot":
            # 🗑️ 清空当天周期统计快照（daily_stats_history.json 里今天的记录）——当天数据异常时用
            try:
                snaps = ds.load_snapshots()
                today = datetime.now().strftime("%Y-%m-%d")
                if today in snaps:
                    del snaps[today]
                    ds.save_snapshots(snaps)
                    msg = f"已清除 {today} 的数据，从明天开始正常积累"
                else:
                    msg = f"今天({today})没有统计数据，无需清除"
                self._send(200, json.dumps({"ok": True, "msg": msg}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/theme":
            try:
                data = json.loads(self._body() or b"{}")
                Path("/data/theme.json").write_text(json.dumps({"theme": data.get("theme", "dark")}))
                self._send(200, json.dumps({"ok": True}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/tpl/greeting":
            # 🤖 生成问候语：调 LLM 生成自然问候，存到 template_settings.greeting_generated（旧版单条，保留兼容）
            try:
                greeting = generate_greeting()
                cfg = json.loads(ds.CONFIG_PATH.read_text()) if ds.CONFIG_PATH.exists() else {}
                cfg.setdefault("template_settings", {})["greeting_generated"] = greeting
                save_edit_cfg({"template_settings": cfg["template_settings"]})
                self._send(200, json.dumps({"ok": True, "greeting": greeting}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        elif path == "/tpl/week":
            # 🤖 大模型生成未来 7 天文案（问候语/结束语/小贴士），存入 template_settings.<section>_days
            try:
                data = json.loads(self._body() or b"{}")
                section = (data.get("section") or "").strip()
                res = generate_week(section, ds.load_config())
                if res.get("ok") and res.get("days"):
                    cfg = json.loads(ds.CONFIG_PATH.read_text()) if ds.CONFIG_PATH.exists() else {}
                    cfg.setdefault("template_settings", {})[section + "_llm_days"] = res["days"]
                    save_edit_cfg({"template_settings": cfg["template_settings"]})
                self._send(200, json.dumps(res, ensure_ascii=False), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "days": {}, "error": str(e)}), "application/json")
        elif path == "/tpl/one":
            # ✨ 只生成某一天的文案（单条），存入 template_settings.<section>_llm_days 对应日期
            try:
                data = json.loads(self._body() or b"{}")
                section = (data.get("section") or "").strip()
                date_str = (data.get("date") or "").strip()
                if not date_str:
                    self._send(400, json.dumps({"ok": False, "days": {}, "error": "date 必填"}), "application/json")
                    return
                res = generate_one(section, ds.load_config(), date_str)
                if res.get("ok") and res.get("days"):
                    cfg = json.loads(ds.CONFIG_PATH.read_text()) if ds.CONFIG_PATH.exists() else {}
                    key = section + "_llm_days"
                    llm_days = cfg.setdefault("template_settings", {}).get(key) or {}
                    llm_days.update(res["days"])
                    cfg["template_settings"][key] = llm_days
                    save_edit_cfg({"template_settings": cfg["template_settings"]})
                self._send(200, json.dumps(res, ensure_ascii=False), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "days": {}, "error": str(e)}), "application/json")
        elif path == "/logs/clear":
            LOG_TAIL.clear()
            self._send(200, json.dumps({"ok": True}), "application/json")
        elif path == "/engine":
            try:
                data = json.loads(self._body() or b"{}")
                mode = data.get("mode", "template")
                if mode not in ("template", "llm"):
                    mode = "template"
                Path("/data/engine_mode.json").write_text(json.dumps({"mode": mode}))
                self._send(200, json.dumps({"ok": True, "mode": mode}), "application/json")
            except Exception as e:
                self._send(500, json.dumps({"ok": False, "error": str(e)}), "application/json")
        else:
            self._send(404, "not found", "text/plain")


WEBUI_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>小爱每日播报</title>
<style>
  :root{
    --bg:#0d1117; --bg2:#161b22; --bg3:#0a0e14; --bg-inset:#0d1117;
    --border:#30363d; --text:#c9d1d9; --title:#f0f6fc; --dim:#8b949e; --faint:#484f58;
    --accent:#1f6feb; --accent2:#58a6ff; --red:#f85149; --green:#3fb950;
  }
  body[data-theme="light"]{
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
  #type,#engineSel{padding:10px 16px;font-size:14px;border-radius:8px;max-width:none;height:40px;line-height:1}
  /* 🔑 播报页控件统一样式：14px、40px 高、圆角8px */
  /* 语音/文字/清空/实体按钮：纯白色底（不管主题），深色文字，选中态蓝底 */
  .row button,.row select{font-size:14px;height:40px;border-radius:8px}
  .row button{background:#ffffff;color:#24292f;border:1px solid #d0d7de;padding:0 14px;line-height:1}
  .row button.active{background:var(--accent);color:#fff;border-color:var(--accent)}
  .row select{background:var(--bg-inset);color:var(--text);border:1px solid var(--border);padding:0 10px}
  #btnSpeak,#btnText,#btnClear,#btnEntities{transition:background .2s}
  #btnSpeak.active,#btnText.active,#btnClear.active,#btnEntities.active{background:var(--accent);border:1px solid var(--accent);color:#fff}
  #btnSpeak:not(.active),#btnText:not(.active),#btnClear:not(.active),#btnEntities:not(.active){background:transparent;border:1px solid var(--border);color:var(--accent2)}
  input[type=text]{padding:8px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border)}
  .entry{display:flex;gap:8px;align-items:center;margin-bottom:6px}
  .entry select{flex:1;min-width:200px}
  .entry input{flex:1;min-width:120px}
  .entry .del{background:transparent;border:none;color:var(--red);cursor:pointer;font-size:16px}
  .drag-handle{cursor:grab;color:var(--dim);font-size:14px;user-select:none;flex:0 0 auto}
  .entry[draggable=true] input{cursor:default}
  .entry.drag-over{border-color:var(--accent)}
  .mute-btn{background:transparent;border:1px solid var(--border);border-radius:6px;color:var(--dim);cursor:pointer;font-size:14px;padding:2px 7px;flex:0 0 auto;line-height:1.5}
  .mute-btn.muted{background:var(--bg-inset);border-color:var(--red);opacity:.7}
  .entry.muted-row{opacity:.55}
  .sec-title{font-size:13px;font-weight:600;color:var(--title);margin-bottom:4px}
  /* 🔑 模板配置页字段：label 一行、输入框一行占满，简洁紧凑 */
  .eng-label{display:block;font-size:12px;color:var(--dim);margin:8px 0 4px}
  .eng-input{width:100%;box-sizing:border-box;padding:8px 10px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border);font-size:13px}
  .eng-input:focus{border-color:var(--accent);outline:none}
  .eng-check{display:flex;align-items:center;gap:6px;padding:5px 0;font-size:13px;color:var(--text)}
  .eng-check input{width:16px;height:16px;flex:0 0 auto}
  .fmt-hint{font-size:11px;color:var(--faint);margin:-2px 0 4px;line-height:1.5}
  .sec-title-row{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:12px;margin-bottom:4px}
  .sec-title-row .sec-title{margin-top:0;margin-bottom:0}
  .ref-btn{background:transparent;border:1px solid var(--border);border-radius:6px;color:var(--accent2);padding:2px 9px;font-size:12px;cursor:pointer;line-height:1.5;flex:0 0 auto;transition:opacity .2s}
  .ref-btn:hover{border-color:var(--accent);color:var(--accent)}
  .ref-btn:active{opacity:.6}
  .sec-desc{font-size:11px;color:var(--dim);margin-bottom:10px}
  .add{background:transparent;border:1px dashed var(--border);color:var(--accent2);padding:6px 12px;font-size:12px;cursor:pointer;border-radius:6px}
  .search-row{margin:8px 0}
  .search-row input,.search-row select{width:98%;box-sizing:border-box;padding:8px 10px;height:36px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border);max-width:none;font-size:14px}
  .search-row select{appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%23888'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 12px center;padding-right:28px}
  .search-row input:focus{border-color:var(--accent);outline:none}
  #log{font-family:ui-monospace,monospace;font-size:12px;max-height:400px;overflow-y:auto;background:var(--bg3);padding:10px;border-radius:8px}
  #status{font-size:12px;color:var(--dim)}
  .save-status{font-size:12px;color:var(--green);min-height:18px;margin-top:8px}
  .week-tab{flex:1;text-align:center;padding:7px 0;border-radius:8px;background:var(--bg-inset);border:1px solid var(--border);color:var(--dim);cursor:pointer;font-size:13px;user-select:none}
  .week-tab.on{background:var(--accent);color:#fff;border-color:var(--accent)}
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
  .cal-cell.anom{color:#f85149}
  .cal-cell.anom .warn{font-size:9px;margin-left:1px;line-height:1}
  .cal-cell.selected .warn{filter:brightness(2)}
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
<body data-theme="dark">
<h1 style="display:flex;align-items:center;gap:8px">🎙️ 小爱每日播报
  <button class="ghost" id="fsBtn" onclick="toggleFullscreen()" style="margin-left:auto;padding:6px 12px;font-size:12px">⛶ 全屏</button>
  <button class="ghost" id="themeBtn" onclick="toggleTheme()" style="padding:6px 12px;font-size:12px">☀️ 日间</button>
</h1>
<div class="tabs">
  <div class="tab on" id="tabMain" onclick="showPage('main')">📢 播报</div>
  <div class="tab" id="tabSec" onclick="showPage('sec')">📋 播报栏目</div>
  <div class="tab" id="tabCfg" onclick="showPage('cfg')">⚙️ 传感器配置</div>
  <div class="tab" id="tabTpl" onclick="showPage('tpl')">🧩 模板配置</div>
  <div class="tab" id="tabLlm" onclick="showPage('llm')">🤖 大模型配置</div>
  <div class="tab" id="tabHist" onclick="showPage('hist')">📜 历史</div>
</div>

<!-- 播报页 -->
<div class="page on" id="page-main">
  <div class="card">
    <div class="row">
      <select id="type" title="播报类型">
        <option value="daily">📅 日</option>
        <option value="weekly">🗓️ 周</option>
        <option value="monthly">📆 月</option>
        <option value="yearly">🎆 年</option>
      </select>
      <select id="engineSel" onchange="changeEngine()" title="生成引擎">
        <option value="template">🧩 模板</option>
        <option value="llm">🤖 大模型</option>
      </select>
      <button id="btnSpeak" onclick="trigger(false)">📢 语音播报</button>
      <!-- ⏸️ 句间停顿：带文字说明，和模板页控件同款样式 -->
      <span style="display:flex;align-items:center;gap:4px;white-space:nowrap;font-size:14px;color:var(--dim)">
        停顿
        <select id="pauseSel" onchange="changePause()" title="播报时每句之间的停顿时间" style="padding:8px;border-radius:8px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border);max-width:none;height:40px;font-size:14px">
          <option value="0.3">0.3秒</option>
          <option value="0.5" selected>0.5秒</option>
          <option value="0.8">0.8秒</option>
          <option value="1.0">1.0秒</option>
          <option value="1.5">1.5秒</option>
        </select>
      </span>
      <button id="btnText" onclick="trigger(true)">📝 文字播报</button>
      <button id="btnClear" onclick="clearLog()">🗑️ 清空日志</button>
      <button id="btnEntities" onclick="showEntities()">🔑 传感器实体</button>
    </div>
    <div id="status">就绪</div>
  </div>
  <div class="card" id="entitiesBox" style="display:none"></div>
  <div class="card" id="textCard" style="display:none">
    <div class="sec-title">📝 播报文字</div>
    <div id="textOut" style="font-size:14px;line-height:1.8;white-space:pre-wrap"></div>
  </div>
  <div class="card">
    <div id="statsBar" style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;font-size:12px"></div>
    <div id="log">日志加载中...</div>
  </div>
</div>

<!-- 播报栏目页 -->
<div class="page" id="page-sec">
  <div class="card">
    <div class="sec-title">📋 播报栏目</div>
    <div class="sec-desc">选哪些内容播报（模板/大模型都生效），改完自动保存。</div>
    <div id="mainSections" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:4px;margin-top:8px"></div>
    <div class="save-status" id="secStatus"></div>
  </div>
</div>

<!-- 传感器配置页 -->
<div class="page" id="page-cfg">
  <div class="card">
    <div class="sec-title">🎙️ 播报音箱</div>
    <div class="sec-desc">选择要播报的音箱（只列出智能音箱）。如果找不到你的音箱，可在加载项设置里手动填 notify 实体。</div>
    <div class="search-row">
      <select id="cfg-speaker"></select>
    </div>
    <div class="fmt-hint">💡 只有名字含"智能音箱"的才是真正的音箱，手机/摄像机/电饭煲等不算。选错的话播报不会响。</div>
  </div>
  <div class="card">
    <div class="sec-title">🔧 实体映射</div>
    <div class="sec-desc">把家里的传感器配到各分类。每行：实体id → 播报名（可空）→ 房间名 → 用途。拖动 ⠿ 可调整播报顺序。状态徽章显示实时值。</div>
    <div id="sections"></div>
    <button onclick="saveCfg()" style="margin-top:12px;width:100%">💾 保存配置</button>
    <div class="save-status" id="cfgStatus"></div>
  </div>
</div>

<!-- 模板配置页 -->
<div class="page" id="page-tpl">
  <div class="card">
    <div class="sec-title">🧩 模板播报配置</div>
    <div class="sec-desc">播报文案（问候语/结束语/小贴士）、阈值、板块开关等参数。改完点"保存模板配置"。</div>
    <div id="tplBox"></div>
    <button onclick="saveTpl()" style="margin-top:12px;width:100%">💾 保存模板配置</button>
    <div class="save-status" id="tplStatus"></div>
  </div>
</div>

<!-- 大模型配置页 -->
<div class="page" id="page-llm">
  <div class="card">
    <div class="sec-title">🤖 大模型配置</div>
    <div class="sec-desc">配置大模型生成的提示语（system prompt）和参数。留空用默认。改完点"保存大模型配置"。</div>
    <div id="llmBox"></div>
    <button onclick="saveLlm()" style="margin-top:12px;width:100%">💾 保存大模型配置</button>
    <div class="save-status" id="llmStatus"></div>
  </div>
</div>

<!-- 历史页 -->
<div class="page" id="page-hist">
  <div class="card">
    <div class="row" style="justify-content:center">
      <button class="ghost" onclick="histNav(-1)">‹</button>
      <span id="histTitle" style="font-size:15px;font-weight:600;min-width:120px;text-align:center"></span>
      <button class="ghost" onclick="histNav(1)">›</button>
    </div>
    <div class="cal-grid" id="histCal"></div>
  </div>
  <!-- 🛠️ 修复异常放最上面（历史记录上方，一进来就看到） -->
  <div class="card">
    <button class="danger" onclick="fixAnomaly()">🛠️ 修复异常传感器数据</button>
    <div class="fmt-hint" style="margin-top:4px">日历上带 ⚠️ 的日期表示有传感器异常（如某设备一天耗电几十度，不正常）。选一个 ⚠️ 日期点这个修复（用该设备前两天正常值的平均值替换异常值）。</div>
    <div class="save-status" id="fixStatus"></div>
  </div>
  <div class="card">
    <div id="histList" style="font-size:12px;color:#8b949e">选择日期查看播报记录</div>
  </div>
  <div class="card">
    <button class="danger" onclick="clearSnapshot()">🗑️ 清除当天的统计数据</button>
    <div class="fmt-hint" style="margin-top:4px">周期统计（周/月/年）用的每日数据快照。如果今天的数据异常（比如测试过），点这个只清除今天的，不影响之前正常的数据。</div>
    <div class="save-status" id="snapStatus"></div>
  </div>
</div>

<script>
// 🔑 ingress 前缀：HA ingress 下页面 URL 是 /api/hassio_ingress/<token>/...
// 必须用相对路径（基于 location.pathname）而不是写死 /，否则请求打到 HA 主 API
var BASE = location.pathname.replace(/\\/[^/]*$/, '/');

/* ─── 全屏 ─── */
function toggleFullscreen(){
  var fsBtn=document.getElementById('fsBtn');
  if(document.fullscreenElement || document.webkitFullscreenElement){
    if(document.exitFullscreen) document.exitFullscreen();
    else if(document.webkitExitFullscreen) document.webkitExitFullscreen();
  }else{
    var el=document.documentElement;
    if(el.requestFullscreen) el.requestFullscreen();
    else if(el.webkitRequestFullscreen) el.webkitRequestFullscreen();
  }
}
document.addEventListener('fullscreenchange', function(){
  var fsBtn=document.getElementById('fsBtn');
  if(fsBtn) fsBtn.textContent=(document.fullscreenElement||document.webkitFullscreenElement)?'⛶ 退出全屏':'⛶ 全屏';
});
document.addEventListener('webkitfullscreenchange', function(){
  var fsBtn=document.getElementById('fsBtn');
  if(fsBtn) fsBtn.textContent=(document.fullscreenElement||document.webkitFullscreenElement)?'⛶ 退出全屏':'⛶ 全屏';
});
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
  {key:'temp_sensors',title:'🌡️ 温度传感器',desc:'各房间温度。填播报名可区分（如"客厅冰箱"/"客厅室温"），高温/低温报警用',domains:['sensor'],kw:['temperature','温度','t2','temp'],room:true},
  {key:'humidity_sensors',title:'💧 湿度传感器',desc:'各房间湿度，干燥/过湿提醒。常湿的房间（卫生间）可在模板配置里排除',domains:['sensor'],kw:['humidity','湿度','relative_humidity'],room:true},
  {key:'power_sensors',title:'⚡ 用电（今日电量）',desc:'用 *_power_cost_today 实体（今日累计 kWh），多个设备自动求和 + 耗电排行',domains:['sensor'],kw:['power_cost_today','今日电量','耗电'],room:true},
  {key:'power_now',title:'🔌 实时功率（可选）',desc:'用 *_electric_power 实体（单位 W），播报总功率 + 高功耗提醒。不填则不报',domains:['sensor'],kw:['electric_power','power','功率'],room:true},
  {key:'lights',title:'💡 灯光',desc:'家里灯，播报"没关灯"提醒。不填则不报',domains:['light'],room:true},
  {key:'doors',title:'🚪 门窗',desc:'on=开，播报安全巡检（门没关提醒）。不填则不报',domains:['binary_sensor'],kw:['contact','门','窗','magnet'],room:true},
  {key:'important_devices',title:'⚠️ 重要设备',desc:'空调/风扇等，离线时播报提醒（离线几天内提醒可在模板配置里调）',domains:['climate','fan','media_player','switch'],room:true},
];

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

var cfgDirty=false;   // 🔑 传感器配置页是否有未保存修改
var tplDirty=false;   // 🔑 模板配置页是否有未保存修改
var currentPage='main';  // 当前所在 tab（markDirty 按它提示对应的页面）
// 🔑 标记"有未保存的修改"：当前页的状态文字立刻变醒目提示（不再停留在"已保存"）
function markDirty(){
  if(currentPage==='cfg'){
    cfgDirty=true;
    var cs=document.getElementById('cfgStatus');
    if(cs){ cs.style.color='#e3b341'; cs.textContent='⚠️ 有未保存的修改'; }
  }else if(currentPage==='tpl'){
    tplDirty=true;
    var ts=document.getElementById('tplStatus');
    if(ts){ ts.style.color='#e3b341'; ts.textContent='⚠️ 有未保存的修改'; }
  }
}
function showPage(p){
  // 🔑 从配置页切走时，各自页面有未保存修改则提醒（cfg 和 tpl 分开，不再互相清标记）
  if(p!=='cfg' && cfgDirty){
    var ok=confirm('传感器配置有未保存的修改，要保存吗？');
    if(ok){ saveCfg(); }
    cfgDirty=false;
  }
  if(p!=='tpl' && tplDirty){
    var ok=confirm('模板配置有未保存的修改，要保存吗？');
    if(ok){ saveTpl(); }
    tplDirty=false;
  }
  currentPage=p;
  document.getElementById('tabMain').className='tab'+(p==='main'?' on':'');
  document.getElementById('tabSec').className='tab'+(p==='sec'?' on':'');
  document.getElementById('tabCfg').className='tab'+(p==='cfg'?' on':'');
  document.getElementById('tabTpl').className='tab'+(p==='tpl'?' on':'');
  document.getElementById('tabLlm').className='tab'+(p==='llm'?' on':'');
  document.getElementById('tabHist').className='tab'+(p==='hist'?' on':'');
  document.getElementById('page-main').className='page'+(p==='main'?' on':'');
  document.getElementById('page-sec').className='page'+(p==='sec'?' on':'');
  document.getElementById('page-cfg').className='page'+(p==='cfg'?' on':'');
  document.getElementById('page-tpl').className='page'+(p==='tpl'?' on':'');
  document.getElementById('page-llm').className='page'+(p==='llm'?' on':'');
  document.getElementById('page-hist').className='page'+(p==='hist'?' on':'');
  if(p==='cfg'){ loadCfgPage(); }
  else { stopStatePoll(); }
  if(p==='sec'){ loadMainSections(); }
  if(p==='tpl'){ loadTplPage(); }
  if(p==='llm'){ loadLlmPage(); }
  if(p==='hist'){ loadHist(); }
}

function optsFor(domain, cur){
  var h='';
  ENTITIES.forEach(function(e){
    if(e.domain===domain || domain.indexOf(e.domain)>=0){
      var sel=(e.entity_id===cur)?' selected':'';
      var label=e.name?e.name+' ('+e.entity_id+')':e.entity_id;
      h+='<option value="'+esc(e.entity_id)+'"'+sel+'>'+esc(label)+'</option>';
    }
  });
  return h;
}

// 填充音箱下拉：列出所有 notify. 实体（可播报的音箱）
function fillSpeakerSelect(cur){
  var sel=document.getElementById('cfg-speaker');
  // 🔑 只列出音箱：名字含"智能音箱"的 notify 实体（排除手机/摄像机/电饭煲等非音箱的 notify）
  var opts=[];
  ENTITIES.forEach(function(e){
    if(e.domain!=='notify') return;
    var name=e.name||'';
    var isSpeaker=(name.indexOf('智能音箱')>=0 || e.entity_id.indexOf('play_text')>=0);
    if(isSpeaker) opts.push(e);
  });
  if(!opts.length){ sel.innerHTML='<option value="">(未找到音箱，请在加载项设置里手动填 notify 实体)</option>'; return; }
  var h='<option value="">选择音箱...</option>';
  opts.forEach(function(e){
    var name=e.name?e.name+' ('+e.entity_id+')':e.entity_id;
    var selFlag=(e.entity_id===cur)?' selected':'';
    h+='<option value="'+esc(e.entity_id)+'"'+selFlag+'>'+esc(name)+'</option>';
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
    // 音箱下拉变更也算修改，标记 dirty
    var sp=document.getElementById('cfg-speaker');
    if(sp) sp.onchange=markDirty;
  });
  startStatePoll();
}
// 🔑 实时状态轮询：每5秒刷新实体状态，更新配置页里的状态徽章
var stateTimer=null;
function startStatePoll(){
  if(stateTimer) clearInterval(stateTimer);
  stateTimer=setInterval(function(){
    fetch(BASE+'cfg/entities?_='+Date.now()).then(function(r){return r.json()}).then(function(d){
      if(d && d.ok && d.entities){
        ENTITIES=d.entities;
        updateStateBadges();
      }
    }).catch(function(){});
  },5000);
}
function stopStatePoll(){
  if(stateTimer){ clearInterval(stateTimer); stateTimer=null; }
}
// 🔑 手动刷新某组的实时状态：立即拉最新实体状态，刷新所有徽章（按钮短暂反馈）
function refreshSection(key){
  var btn=document.getElementById('ref-'+key);
  if(btn){ btn.textContent='⏳'; btn.style.opacity='.5'; }
  fetch(BASE+'cfg/entities?_='+Date.now()).then(function(r){return r.json()}).then(function(d){
    if(d && d.ok && d.entities) ENTITIES=d.entities;
    updateStateBadges();
    if(btn){ btn.textContent='🔄'; btn.style.opacity=''; }
  }).catch(function(){
    if(btn){ btn.textContent='❌'; btn.style.opacity=''; setTimeout(function(){ btn.textContent='🔄'; },1500); }
  });
}
// 更新页面里所有状态徽章（.entry 里的 [data-state] 元素）
function updateStateBadges(){
  document.querySelectorAll('#page-cfg .entry').forEach(function(e){
    var eid=e.getAttribute('data-raw')||'';
    var badge=e.querySelector('[data-state-badge]');
    if(!badge) return;
    var st='', calc;
    for(var i=0;i<ENTITIES.length;i++){
      if(ENTITIES[i].entity_id===eid){ st=ENTITIES[i].state||''; calc=ENTITIES[i].calc; break; }
    }
    if(st==='on') st='✅ 开';
    else if(st==='off') st='⭕ 关';
    else if(st==='unavailable'||st==='unknown') st='⚠️ 无';
    // 🔑 累计差值用电：徽章显示"实际值 / 差值"
    if(calc!==undefined && st && !/^[✅⭕⚠️]/.test(st)) st+=' / '+calc;
    badge.textContent=st?st:'-';
  });
}

function renderSections(){
  var el=document.getElementById('sections');
  var h='';
  SECTIONS.forEach(function(sec){
    var items=CONFIG[sec.key]||{};
    var entries=[];
    for(var k in items){ if(k!=='_note') entries.push([k,items[k]]); }
    // 🔑 每组标题右侧加刷新按钮：点一下立即拉最新实时状态
    h+='<div class="sec-title-row">';
    h+='<div class="sec-title">'+sec.title+'</div>';
    h+='<button class="ref-btn" id="ref-'+sec.key+'" onclick="refreshSection(\\''+sec.key+'\\')" title="刷新实时状态">🔄</button>';
    h+='</div>';
    h+='<div class="sec-desc">'+sec.desc+'</div>';
    h+='<div class="search-row"><input type="text" placeholder="🔍 点一下显示全部，输入关键词过滤（如 卧室/温度）" onfocus="onSearch(\\''+sec.key+'\\')" oninput="onSearch(\\''+sec.key+'\\')" id="search-'+sec.key+'"></div>';
    h+='<div id="sec-'+sec.key+'">';
    entries.forEach(function(pair){
      // 兼容旧值(字符串=房间名)和新值({room,usage})
      var v=pair[1];
      var room=(typeof v==='string')?v:(v&&v.room||'');
      var usage=(typeof v==='object'&&v)?(v.usage||''):'';
      var label=(typeof v==='object'&&v)?(v.label||''):'';
      var ptype=(typeof v==='object'&&v)?(v.power_type||'daily'):'daily';
      var muted=(typeof v==='object'&&v)?(v.muted===true):false;
      h+=entryHTML(sec, pair[0], room, usage, label, ptype, false, muted);
    });
    h+='</div>';
    h+='<div id="cand-'+sec.key+'"></div>';
    h+='<button class="add" onclick="addEntry(\\''+sec.key+'\\')">+ 手动添加</button>';
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
  if(!matches.length){ box.innerHTML='<div style="color:#8b949e;font-size:12px;padding:6px 0">无匹配实体</div>'; return; }
  var total=matches.length;
  var h='<div style="font-size:11px;color:#8b949e;margin-top:8px;margin-bottom:4px">共 '+total+' 个'+(q?'，匹配 “'+esc(q)+'”':'')+'（点击选择，可滚动）</div>';
  // 全部显示 + 限高可滚动（最多约 20 条可见，超出滚动查看）
  h+='<div style="max-height:320px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;padding:4px">';
  matches.forEach(function(e){
    var label=(e.name?e.name+' — ':'')+e.entity_id;
    h+='<div style="padding:7px 10px;margin-bottom:3px;border-radius:6px;background:var(--bg-inset,#0d1117);border:1px solid #30363d;cursor:pointer;font-size:12px" onclick="pickCandidate(\\''+key+'\\',\\''+e.entity_id+'\\')">🔗 '+esc(label)+'</div>';
  });
  h+='</div>';
  box.innerHTML=h;
}

// 点击候选实体 → 添加到该 section 映射（房间名空，用户填）
function pickCandidate(key, eid){
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var box=document.getElementById('sec-'+key);
  box.insertAdjacentHTML('beforeend', entryHTML(sec, eid, '', '', ''));
  markDirty();
  collectSection(key);
  document.getElementById('cand-'+key).innerHTML='';
  var si=document.getElementById('search-'+key); if(si) si.value='';
}

function entryHTML(sec, eid, room, usage, label, ptype, editable, muted){
  var ph=sec.room?'房间名':'标签';
  // 🔑 editable=true：手动添加，第一个框可编辑填实体 id；否则只读（候选添加）
  // 但只读的已添加实体支持"双击编辑"（去掉 readonly 编辑，失焦恢复）
  var eidAttr = editable
    ? 'placeholder="填实体 id" oninput="manualEid(this,\\''+sec.key+'\\')"'
    : 'readonly ondblclick="editEid(this)" onblur="eidBlur(this,\\''+sec.key+'\\')"';
  var eidStyle = editable
    ? 'flex:1.4;min-width:180px;background:var(--bg-inset);border:1px solid var(--border);color:var(--text)'
    : 'flex:1.4;min-width:180px;background:transparent;border:1px solid var(--border);color:var(--text)';
  // 用电方式下拉（仅 power_sensors 段显示）
  var typeSel='';
  if(sec.key==='power_sensors'){
    var opts=[['daily','日电量'],['accumulate','累计差值'],['integral','功率积分']];
    var ohtml='';
    opts.forEach(function(o){
      ohtml+='<option value="'+o[0]+'"'+(ptype===o[0]?' selected':'')+'>'+o[1]+'</option>';
    });
    typeSel='<select style="flex:1;min-width:120px;height:34px;padding:0 8px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border)" onchange="onRoom(this,\\''+sec.key+'\\')">'+ohtml+'</select>';
  }
  // 🔑 实时状态：从 ENTITIES 找该实体的当前 state 显示（在用途前）
  var st='';
  var calcTxt='';
  if(eid){
    var found=null;
    for(var i=0;i<ENTITIES.length;i++){
      if(ENTITIES[i].entity_id===eid){ found=ENTITIES[i]; break; }
    }
    if(found){
      st=found.state||'';
      // 状态友好显示
      if(st==='on') st='✅ 开';
      else if(st==='off') st='⭕ 关';
      else if(st==='unavailable'||st==='unknown') st='⚠️ 无';
      // 🔑 累计差值用电：后端附带 calc（今日差值），徽章显示"实际值 / 差值"
      if(found.calc!==undefined && st && !/^[✅⭕⚠️]/.test(st)){
        calcTxt=' / '+found.calc;
      }
    }
  }
  // 🔑 状态徽章：所有 section 统一宽度(height 34px 同输入框, min-width 96px 显示更全面如"93.5 / 5.2")，布局整齐一致
  var stateHtml='<span data-state-badge style="flex:0 0 auto;min-width:96px;height:34px;display:inline-flex;align-items:center;justify-content:center;text-align:center;font-size:11px;color:var(--accent2);white-space:nowrap;padding:0 10px;border-radius:6px;background:var(--bg-inset);border:1px solid var(--border);box-sizing:border-box">'+(st?esc(st+calcTxt):'-')+'</span>';
  // 🔑 拖拽排序：每行可拖动改变顺序（决定播报顺序）
  return '<div class="entry'+(muted?' muted-row':'')+'" data-raw="'+esc(eid)+'" draggable="true" ondragstart="dragStart(event)" ondragover="dragOver(event)" ondrop="dragDrop(event,\\''+sec.key+'\\')" ondragend="dragEnd(event)">'
    +'<span class="drag-handle" title="拖动排序">⠿</span>'
    +'<input type="text" value="'+esc(eid)+'" '+eidAttr+' style="'+eidStyle+'">'
    +stateHtml
    +'<input type="text" value="'+esc(label||'')+'" placeholder="播报名" oninput="onRoom(this,\\''+sec.key+'\\')">'
    +'<input type="text" value="'+esc(room||'')+'" placeholder="房间名" oninput="onRoom(this,\\''+sec.key+'\\')">'
    +'<input type="text" value="'+esc(usage||'')+'" placeholder="用途" oninput="onRoom(this,\\''+sec.key+'\\')">'
    +typeSel
    +'<button class="mute-btn'+(muted?' muted':'')+'" onclick="toggleMute(this,\\''+sec.key+'\\')" title="'+(muted?'已屏蔽，点击恢复':'屏蔽，不参与播报')+'">'+(muted?'🔇':'🔔')+'</button>'
    +'<button class="del" onclick="delEntry(\\''+sec.key+'\\',this)">✕</button>'
    +'</div>';
}
// 🔑 双击已添加的实体 id → 允许编辑（去掉 readonly）；失焦恢复 + 同步配置
function editEid(inp){
  inp.readOnly=false;
  inp.focus();
  inp.select();
}
function eidBlur(inp,key){
  if(!inp.readOnly){
    inp.readOnly=true;
    markDirty();
    collectSection(key);
  }
}

// 收集每段当前条目到 CONFIG（读 DOM 里的 .entry）
function collectSection(key){
  var box=document.getElementById('sec-'+key);
  if(!box) return;
  var out={};
  box.querySelectorAll('.entry').forEach(function(e){
    var ins=e.querySelectorAll('input');
    var eid=(ins[0]&&ins[0].value||'').trim();
    var label=(ins[1]&&ins[1].value||'').trim();
    var room=(ins[2]&&ins[2].value||'').trim();
    var usage=(ins[3]&&ins[3].value||'').trim();
    var ptype='daily';
    var sel=e.querySelector('select');
    if(sel && key==='power_sensors') ptype=sel.value||'daily';
    if(eid){
      var obj={'room':room,'usage':usage,'label':label};
      var mbtn=e.querySelector('.mute-btn');
      if(mbtn && mbtn.classList.contains('muted')) obj.muted=true;
      if(key==='power_sensors') obj.power_type=ptype;
      out[eid]=obj;
    }
  });
  CONFIG[key]=out;
}

// 手动添加：追加一个可编辑空条目（实体 id + 房间名都可填），收回搜索候选
function addEntry(key){
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var box=document.getElementById('sec-'+key);
  // 🔑 参数顺序：(sec, eid, room, usage, ptype, editable)——第6个才是 editable！
  box.insertAdjacentHTML('beforeend', entryHTML(sec,'','','','','daily',true));
  markDirty();
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

/* ─── 模板播报配置 ─── */
var TPL_GROUPS=[
  {title:'播报文案',open:true,weekPanel:true,groups:[
    {name:'问候语',modeKey:'greeting_mode',weekKey:'greeting_days',llmWeekKey:'greeting_llm_days',loopKey:'greeting_loop_enabled',help:'手动和大模型内容各自独立，互不覆盖。留空的天播报时用下面的默认。',defFields:[
      {k:'workday_desc',label:'工作日描述（问候留空时用）',def:'工作日辛苦了',type:'text'},
      {k:'weekend_desc',label:'周末描述（问候留空时用）',def:'周末愉快',type:'text'},
    ]},
    {name:'结束语',modeKey:'ending_mode',weekKey:'ending_days',llmWeekKey:'ending_llm_days',loopKey:'ending_loop_enabled',help:'手动和大模型内容各自独立，互不覆盖。留空的天播报时用下面的默认。',defFields:[
      {k:'ending_text',label:'默认结束语（留空用按时间段的默认）',def:'',type:'textarea'},
    ]},
    {name:'小贴士',modeKey:'tip_mode',weekKey:'tip_days',llmWeekKey:'tip_llm_days',loopKey:'tip_loop_enabled',help:'手动和大模型内容各自独立，互不覆盖。留空的天播报时用默认小贴士。',defFields:[]},
    {name:'鼓励语',modeKey:'enc_mode',weekKey:'enc_days',llmWeekKey:'enc_llm_days',loopKey:'enc_loop_enabled',help:'说在小贴士之后、结束语/播报结束之前（倒数第2句）。留空的天播报时用默认鼓励语。',defFields:[]},
  ]},
  {title:'温度',fields:[
    {k:'temp_high_alert',label:'高温报警阈值(度)',ph:'温度超过这个值提醒高温（如 32=32度以上）',def:32,type:'number'},
    {k:'temp_low_alert',label:'低温报警阈值(度)',ph:'温度低于这个值提醒低温（如 10=10度以下）',def:10,type:'number'},
    {k:'temp_constant_diff',label:'恒温温差阈值(度)',ph:'全天温差小于这个值就说"全天X度"（不报高低）',def:1,type:'number'},
    {k:'temp_exclude_rooms',label:'不参与温度报警的房间',ph:'填房间名，多个用空格隔开。如：厨房 储物间',def:'',type:'text'},
  ]},
  {title:'湿度',fields:[
    {k:'humidity_dry',label:'干燥阈值(%)',ph:'湿度低于这个值提醒干燥',def:40,type:'number'},
    {k:'humidity_wet',label:'过湿阈值(%)',ph:'湿度高于这个值提醒过湿',def:80,type:'number'},
    {k:'humidity_exclude_rooms',label:'不参与湿度报警的房间',ph:'填房间名，多个用空格隔开。如：卫生间',def:'',type:'text'},
  ]},
  {title:'用电',fields:[
    {k:'power_show_threshold',label:'显示"共X度"阈值(kWh)',ph:'今日耗电量超过这个值才播"共X度"，低于则播"非常省电"',def:0.05,type:'number'},
    {k:'power_save_threshold',label:'"非常省电"阈值(kWh)',ph:'耗电量低于这个值播"非常省电"',def:0.1,type:'number'},
    {k:'power_top_n',label:'排行条数',ph:'耗电排行显示前几个设备',def:3,type:'number'},
    {k:'power_top_min',label:'排行最低门槛(kWh)',ph:'耗电低于这个值的设备不进入排行',def:0.01,type:'number'},
    {k:'power_now_top_n',label:'实时功率条数',ph:'实时功率按瓦数从大到小显示前几个',def:3,type:'number'},
    {k:'high_power_alert_w',label:'高功耗提醒阈值(W)',ph:'实时功率超过这个瓦数就提醒（如 200W）',def:200,type:'number'},
  ]},
  {title:'安全',fields:[
    {k:'door_night_hour',label:'晚间门提醒时段(点)',ph:'过了这个点门还开着就提醒（如 18=晚上6点后）',def:18,type:'number'},
  ]},
  {title:'空气质量 PM2.5',fields:[
    {k:'pm25_severe',label:'严重阈值',def:100,type:'number'},
    {k:'pm25_bad',label:'差阈值',def:50,type:'number'},
    {k:'pm25_good',label:'好阈值',def:10,type:'number'},
  ]},
  {title:'终端任务',fields:[
    {k:'task_high',label:'"效率很高"阈值(个)',def:10,type:'number'},
    {k:'task_mid',label:'"进度不错"阈值(个)',def:5,type:'number'},
  ]},
  {title:'待办/备忘',fields:[
    {k:'todo_preview',label:'待办预览条数',ph:'只报前几条待办，超出说"等共N项"（不是超过就不报）',def:5,type:'number'},
    {k:'memo_preview',label:'备忘预览条数',ph:'只报前几条备忘，超出说"等共N条"',def:8,type:'number'},
  ]},
  {title:'设备故障',fields:[
    {k:'fault_offline_days',label:'离线提醒时间窗(天)',ph:'几天内离线才提醒；超过这些天不提醒（长期关机不算故障）。如 1=一天内离线提醒',def:3,type:'number'},
  ]},
  {title:'句式',open:false,fields:[
    {k:'fmt_temp_prefix',label:'温度·开头',ph:'示例：温度：',def:'温度：',type:'text'},
    {k:'fmt_temp_item',label:'温度·每条',ph:'{label}房间+用途 {room}房间 {now}当前 {low}最低 {high}最高。示例：{label}现在{now}度，全天{low}到{high}度',def:'{label}当前{now}度，全天{low}到{high}度',type:'text'},
    {k:'fmt_temp_alert',label:'温度·高温提醒',ph:'{rooms}合并房间。示例：{rooms}温度过高',def:'{rooms}温度过高',type:'text'},
    {k:'fmt_temp_alert_low',label:'温度·低温提醒',ph:'{rooms}合并房间。示例：{rooms}温度偏低',def:'{rooms}温度偏低，建议注意保暖',type:'text'},
    {k:'fmt_humidity_prefix',label:'湿度·开头',ph:'示例：湿度：',def:'湿度：',type:'text'},
    {k:'fmt_humidity_item',label:'湿度·每条',ph:'{label}房间+用途 {room}房间 {hum}湿度。示例：{label}是{hum}%',def:'{label}{hum}%',type:'text'},
    {k:'fmt_humidity_dry',label:'湿度·干燥',ph:'{rooms}合并房间。示例：{rooms}比较干燥',def:'{rooms}比较干燥',type:'text'},
    {k:'fmt_humidity_wet',label:'湿度·过湿',ph:'{rooms}合并房间。示例：{rooms}湿度偏高',def:'{rooms}湿度偏高',type:'text'},
    {k:'fmt_power_prefix',label:'耗电·开头',ph:'{total}总量。示例：耗电量：共{total}度',def:'耗电量：共{total}度',type:'text'},
    {k:'fmt_power_top',label:'耗电·排名引导',ph:'{num}名次 {list}列表。示例：耗电前{num}：{list}',def:'耗电前{num}：{list}',type:'text'},
    {k:'fmt_power_top_item',label:'耗电·排名每条',ph:'{device}设备 {kwh}度。示例：{device}耗电{kwh}度',def:'{device}耗电{kwh}度',type:'text'},
    {k:'fmt_power_unread',label:'耗电·读不到',ph:'{count}个数 {items}设备。示例：另有{count}个设备没读到（{items}）',def:'另有{count}个用电设备读不到数据（{items}），未计入',type:'text'},
    {k:'fmt_power_now_prefix',label:'功率·开头',ph:'{total}总瓦数。示例：实时功率共{total}瓦',def:'实时功率共{total}瓦',type:'text'},
    {k:'fmt_power_now_top',label:'功率·排名引导',ph:'{num}名次 {list}列表。示例：耗电前{num}：{list}',def:'耗电前{num}：{list}',type:'text'},
    {k:'fmt_power_now_item',label:'功率·每条',ph:'{device}设备 {w}瓦。示例：{device}有{w}瓦',def:'{device} {w}瓦',type:'text'},
    {k:'fmt_power_now_alert',label:'功率·高功耗',ph:'{devices}合并设备。示例：{devices}功率较高',def:'{devices}功率较高，不用时可以关掉',type:'text'},
    {k:'fmt_lights_prefix',label:'灯光·开头',ph:'示例：灯光：',def:'灯光：',type:'text'},
    {k:'fmt_lights_item',label:'灯光·每条',ph:'{name}灯名。示例：{name}',def:'{name}',type:'text'},
    {k:'fmt_lights_suffix',label:'灯光·结尾',ph:'示例：还亮着，不需要的话可以关掉',def:'还亮着，不需要的话可以关掉',type:'text'},
    {k:'fmt_security',label:'安全巡检',ph:'{items}巡检内容。示例：安全巡检：{items}',def:'安全巡检：{items}',type:'text'},
    {k:'fmt_pm25',label:'空气质量',ph:'{pm}PM描述。示例：空气质量：{pm}',def:'空气质量：{pm}',type:'text'},
    {k:'fmt_task',label:'任务',ph:'{count}任务数 {extra}档位。示例：任务：完成{count}个终端任务{extra}',def:'任务：完成{count}个终端任务{extra}',type:'text'},
    {k:'fmt_todo',label:'待办',ph:'{items}待办列表。示例：待办与备忘：{items}',def:'待办与备忘：{items}',type:'text'},
    {k:'fmt_fault',label:'设备故障',ph:'{count}台数 {items}设备。示例：有{count}台设备离线：{items}，有空检查一下',def:'有{count}台设备离线：{items}，有空检查一下',type:'text'},
  ]},
];
/* ─── 大模型配置 ─── */
function loadLlmPage(){
  // 🔑 打开大模型配置页读最新配置并渲染
  document.getElementById('llmStatus').textContent='加载中...';
  var timer=setTimeout(function(){
    document.getElementById('llmStatus').textContent='⚠️ 加载超时';
  },10000);
  fetch(BASE+'cfg/config?'+Date.now()).then(function(r){
    if(!r.ok){ throw new Error('HTTP '+r.status); }
    return r.json();
  }).then(function(d){
    clearTimeout(timer);
    CONFIG=d||{};
    renderLlmCfg();
    document.getElementById('llmStatus').textContent='';
  }).catch(function(err){
    clearTimeout(timer);
    document.getElementById('llmStatus').textContent='⚠️ 加载失败: '+(err&&err.message||'');
    renderLlmCfg();
  });
}
// 🔑 默认提示语示例（与后端默认一致，点"示例"按钮回填）——v1.1.88 结合当下功能更新
var LLM_DEFAULT_DAILY = "你是家里的小爱音箱播报助手，在某个时刻向用户播报家中实时情况。\\n\\n【先看时间】现在是什么时段就问候什么：凌晨/早上/上午/中午/下午/晚上，例如现在中午就说\\"中午好\\"。只问候一次，整篇绝不重复出现时段词。\\n\\n【看数据顺序】用户给的实时数据里已经有整理好的板块：温度、湿度、耗电量、实时功率、安全、空气质量、灯光、任务、待办、设备故障。按数据出现的顺序自然带出，每块说一句，别漏也别乱。\\n\\n【名字要用准】数据里的设备名已经是\\"客厅冰箱\\"\\"卧室空调\\"这种房间+用途，直接用，不要说\\"客厅\\"\\"卧室\\"这种笼统的房间名。\\n\\n【异常要提醒】温度偏高/偏低、湿度干燥/过湿、功率过高、门窗没关、灯亮着、设备离线，都要自然提醒并给一句建议。多个房间同时异常就合并说：\\"卧室、厨房温度偏高，建议开空调\\"，不要一个一个重复建议。\\n\\n【结构】倒数第二句给一句温暖的鼓励语，最后一句说\\"播报结束\\"。\\n\\n【篇幅】全文120到220字，分成5到9句，每句独立成行，句号结尾，不要多余空行。\\n\\n【硬性】只输出播报稿本身，不要解释、前缀、引号、Markdown、括号、序号、表情、英文。";
var LLM_DEFAULT_PERIOD = "你是家里的小爱音箱播报助手，在某个时刻向用户播报一段周期的家中总结（周/月/年）。\\n\\n【开头】明确说这是周期总结，不是每日播报，用\\"这周/这个月/这一年家里……\\"的句式。\\n\\n【内容】自然带出：温度均值与最热最冷、用电总量与日均及耗电排行、任务完成数、空气质量与灯光亮点、门窗与故障等异常提醒。\\n\\n【诚实】数据里有 days_recorded 和 days_total，如果记录的日期少于总天数，必须诚实带过（如\\"这周只记录了几天数据，可能不够完整\\"），不许假装完整。\\n\\n【结构】结尾给一句温暖的鼓励语，最后一句说\\"播报结束\\"。\\n\\n【篇幅】全文150到280字，分成6到12句，每句独立成行，句号结尾，不要多余空行。\\n\\n【硬性】只输出播报稿本身，不要解释、前缀、引号、Markdown、括号、序号、表情、英文。";
function fillLlmExample(){
  document.getElementById('llm_daily_prompt').value=LLM_DEFAULT_DAILY;
  document.getElementById('llm_period_prompt').value=LLM_DEFAULT_PERIOD;
  document.getElementById('llmStatus').textContent='✅ 已填入示例提示语，点保存生效';
  markDirty();
}
function renderLlmCfg(){
  var box=document.getElementById('llmBox');
  if(!box) return;
  var ts=CONFIG.template_settings||{};
  var llm=ts.llm||{};
  var presets=ts.llm_presets||{};
  var cur=llm.current||'';
  var h='';
  h+='<div class="sec-desc" style="margin-top:4px">提示语是告诉大模型怎么组织播报的指令。可保存多份存档（命名），不合适可切换别的。留空用默认。</div>';
  // 🔑 存档管理：下拉切换当前档 + 保存为新档 + 删除
  h+='<div style="border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:12px;background:var(--bg-inset)">';
  h+='<div class="eng-label" style="margin-top:0">📚 提示语存档</div>';
  h+='<div style="display:flex;gap:8px;margin-bottom:8px">';
  h+='<select id="llm_preset_sel" style="flex:1;padding:8px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border)">';
  var names=Object.keys(presets);
  if(names.length) h+='<option value="">（当前档未保存）</option>';
  names.forEach(function(n){
    h+='<option value="'+esc(n)+'"'+(n===cur?' selected':'')+'>'+esc(n)+'</option>';
  });
  h+='</select>';
  h+='<button type="button" class="ghost" onclick="llmLoadPreset()" style="padding:6px 10px">📂 切换到此档</button>';
  h+='<button type="button" class="ghost" onclick="llmDeletePreset()" style="padding:6px 10px;color:var(--red)">🗑️ 删档</button>';
  h+='</div>';
  h+='<div style="display:flex;gap:8px">';
  h+='<input class="eng-input" type="text" id="llm_preset_name" placeholder="存档名字，如：温馨风 / 极简风" style="flex:1">';
  h+='<button type="button" class="btn-go" onclick="llmSavePreset()" style="padding:6px 14px">💾 存为新档</button>';
  h+='</div>';
  h+='</div>';
  h+='<div style="margin-bottom:10px"><button type="button" class="btn-go" onclick="fillLlmExample()" style="padding:6px 14px">📋 填入示例提示语</button><span class="fmt-hint" style="margin-left:8px">点这个自动填入默认示例，改坏了可一键回退</span></div>';
  h+='<label class="eng-label">每日播报提示语</label>';
  h+='<div class="fmt-hint">告诉大模型每日播报怎么写：语气、结构、要求。留空用默认。</div>';
  h+='<textarea class="eng-input" id="llm_daily_prompt" style="min-height:140px">'+esc(llm.daily_prompt||'')+'</textarea>';
  h+='<label class="eng-label">周期总结提示语（周/月/年）</label>';
  h+='<div class="fmt-hint">周/月/年总结的提示语。留空用默认。</div>';
  h+='<textarea class="eng-input" id="llm_period_prompt" style="min-height:140px">'+esc(llm.period_prompt||'')+'</textarea>';
  h+='<label class="eng-label">最大生成长度（max_tokens）</label>';
  h+='<div class="fmt-hint">生成播报稿的最大字数预算。默认 6000，生成太短可调大。</div>';
  h+='<input class="eng-input" type="number" id="llm_max_tokens" value="'+(llm.max_tokens||6000)+'">';
  box.innerHTML=h;
  box.querySelectorAll('input,textarea').forEach(function(inp){
    inp.addEventListener('input',markDirty);
  });
}
function saveLlm(){
  var ts=CONFIG.template_settings||{};
  if(!ts.llm) ts.llm={};
  ts.llm.daily_prompt=document.getElementById('llm_daily_prompt').value;
  ts.llm.period_prompt=document.getElementById('llm_period_prompt').value;
  ts.llm.max_tokens=parseInt(document.getElementById('llm_max_tokens').value)||6000;
  CONFIG.template_settings=ts;
  document.getElementById('llmStatus').textContent='保存中...';
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:{template_settings:ts}})})
    .then(function(r){return r.json()})
    .then(function(d){
      var s=document.getElementById('llmStatus');
      s.style.color=''; s.textContent = d.ok?'✅ 大模型配置已保存':'❌ 保存失败';
      if(d.ok) tplDirty=false;
    })
    .catch(function(){document.getElementById('llmStatus').textContent='❌ 保存失败';});
}
// 🔑 存档：把当前提示语存为一份命名档（多份可切换，不合适换一个）
function llmSavePreset(){
  var name=(document.getElementById('llm_preset_name').value||'').trim();
  if(!name){ document.getElementById('llmStatus').textContent='⚠️ 先填存档名字'; return; }
  var ts=CONFIG.template_settings||{};
  if(!ts.llm_presets) ts.llm_presets={};
  ts.llm_presets[name]={
    daily_prompt:document.getElementById('llm_daily_prompt').value,
    period_prompt:document.getElementById('llm_period_prompt').value,
    max_tokens:parseInt(document.getElementById('llm_max_tokens').value)||6000,
  };
  if(!ts.llm) ts.llm={};
  ts.llm.current=name;
  CONFIG.template_settings=ts;
  markDirty();
  document.getElementById('llmStatus').textContent='💾 正在保存存档「'+name+'」...';
  // 🔑 存档后直接提交到后端，真正保存成功
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:{template_settings:ts}})})
    .then(function(r){return r.json()})
    .then(function(d){
      var s=document.getElementById('llmStatus');
      if(d.ok){
        s.style.color=''; s.textContent='✅ 存档「'+name+'」保存成功';
        tplDirty=false;
      }else{
        s.textContent='❌ 保存失败';
      }
    })
    .catch(function(){document.getElementById('llmStatus').textContent='❌ 保存失败（网络）';});
  renderLlmCfg();
}
// 🔑 切换：加载选中的档到编辑框
function llmLoadPreset(){
  var name=document.getElementById('llm_preset_sel').value;
  if(!name) return;
  var ts=CONFIG.template_settings||{};
  var p=(ts.llm_presets||{})[name];
  if(!p) return;
  document.getElementById('llm_daily_prompt').value=p.daily_prompt||'';
  document.getElementById('llm_period_prompt').value=p.period_prompt||'';
  document.getElementById('llm_max_tokens').value=p.max_tokens||6000;
  if(!ts.llm) ts.llm={};
  ts.llm.current=name;
  CONFIG.template_settings=ts;
  markDirty();
  document.getElementById('llmStatus').textContent='📂 已切换到「'+name+'」，点保存生效';
}
// 🔑 删除存档
function llmDeletePreset(){
  var name=document.getElementById('llm_preset_sel').value;
  if(!name){ document.getElementById('llmStatus').textContent='⚠️ 先选择要删的存档'; return; }
  if(!confirm('确定删除存档「'+name+'」吗？')) return;
  var ts=CONFIG.template_settings||{};
  delete ts.llm_presets[name];
  if(ts.llm && ts.llm.current===name) delete ts.llm.current;
  CONFIG.template_settings=ts;
  markDirty();
  document.getElementById('llmStatus').textContent='🗑️ 已删除「'+name+'」，点保存生效';
  renderLlmCfg();
}
function loadTplPage(){
  // 🔑 打开模板配置页时读最新配置并渲染（加超时，避免一直"加载中"）
  document.getElementById('tplStatus').textContent='加载中...';
  var timer=setTimeout(function(){
    document.getElementById('tplStatus').textContent='⚠️ 加载超时';
  },10000);
  fetch(BASE+'cfg/config?'+Date.now()).then(function(r){
    if(!r.ok){ throw new Error('HTTP '+r.status); }
    return r.json();
  }).then(function(d){
    clearTimeout(timer);
    CONFIG=d||{};
    renderTplCfg();
    document.getElementById('tplStatus').textContent='';
  }).catch(function(err){
    clearTimeout(timer);
    document.getElementById('tplStatus').textContent='⚠️ 加载失败: '+(err&&err.message||'');
    renderTplCfg();
  });
}
function renderTplCfg(){
  var box=document.getElementById('tplBox');
  if(!box) return;
  var ts=CONFIG.template_settings||{};
  var secs=ts.sections||{};
  var h='';
  TPL_GROUPS.forEach(function(g,gi){
    // 🔑 播报文案面板：可折叠（默认收起），标题点击展开/收起
    if(g.weekPanel){
      var wpOpen=ts._weekPanelOpen===true;
      h+='<div style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden">';
      h+='<div onclick="toggleWeekPanel()" style="padding:10px 12px;background:var(--bg-inset);cursor:pointer;display:flex;justify-content:space-between;align-items:center">';
      h+='<span style="font-weight:600;font-size:13px">📝 播报文案</span><span>'+(wpOpen?'▾':'▸')+'</span></div>';
      if(wpOpen) h+=renderWeekPanel(ts);
      h+='</div>';
      return;
    }
    // 默认折叠（只有播报文案恒开）；用户展开过的保持展开
    var open = (ts['_fold'+gi]===true) || (g.open===true && ts['_fold'+gi]!==false);
    h+='<div style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden">';
    h+='<div onclick="toggleTplFold('+gi+')" style="padding:10px 12px;background:var(--bg-inset);cursor:pointer;display:flex;justify-content:space-between;align-items:center">';
    h+='<span style="font-weight:600;font-size:13px">'+g.title+'</span><span>'+(open?'▾':'▸')+'</span></div>';
    if(open){
      h+='<div style="padding:12px">';
      g.fields.forEach(function(f){
        var val;
        if(f.k.indexOf('sec_')===0){ var _sv=secs[f.k.slice(4)]; val=(_sv!==undefined)?_sv:f.def; }
        else if(f.k.indexOf('fmt_')===0){
          // 🔑 fmt_<key>_<sub> → formats.<key>.<sub>（字段级）；fmt_<key> → formats.<key>（整条字符串，兼容旧）
          var _rest=f.k.slice(4);
          var _fm=ts.formats||{};
          var _us=_rest.lastIndexOf('_');
          if(_us>0){
            var _k=_rest.slice(0,_us), _s=_rest.slice(_us+1);
            var _fd=_fm[_k];
            val=(_fd && typeof _fd==='object' && _fd[_s]!==undefined)?_fd[_s]:f.def;
          }else{
            val=(_fm[_rest]!==undefined)?_fm[_rest]:f.def;
          }
        }
        else{ val=(ts[f.k]!==undefined)?ts[f.k]:f.def; }
        if(f.type==='check'){
          h+='<label class="eng-check"><input type="checkbox" data-tpl="'+f.k+'" '+(val?'checked':'')+'> '+f.label+'</label>';
        }else if(f.type==='number'){
          h+='<label class="eng-label">'+f.label+'</label>';
          h+='<input class="eng-input" type="number" data-tpl="'+f.k+'" value="'+esc(val)+'">';
        }else if(f.type==='textarea'){
          h+='<label class="eng-label">'+f.label+'</label>';
          h+='<textarea class="eng-input" data-tpl="'+f.k+'" style="min-height:50px">'+esc(val)+'</textarea>';
        }else{
          h+='<label class="eng-label">'+f.label+'</label>';
          // 🔑 教程说明用灰色小字写在输入框上方；输入框 value 就是默认格式，点一下直接改
          if(f.ph) h+='<div class="fmt-hint">'+esc(f.ph)+'</div>';
          h+='<input class="eng-input" type="text" data-tpl="'+f.k+'" value="'+esc(val)+'">';
        }
      });
      h+='</div>';
    }
    h+='</div>';
  });
  box.innerHTML=h;
  box.querySelectorAll('input,textarea').forEach(function(inp){
    inp.addEventListener('input',markDirty);
  });
}
// 🔑 播报文案面板：问候语/结束语/小贴士 子 tab + 模式切换 + 未来 7 天一列 + 默认文案并入
function renderWeekPanel(ts){
  var sub=TPL_GROUPS[0].groups;
  var active=(typeof ts._activeWeek==='number')?ts._activeWeek:0;
  if(active<0||active>=sub.length) active=0;
  var g=sub[active];
  var mode=ts[g.modeKey]||'manual';
  var h='';
  h+='<div style="padding:12px">';
  // 子 tab（问候语/结束语/小贴士）
  h+='<div style="display:flex;gap:6px;margin-bottom:10px">';
  sub.forEach(function(s,si){
    h+='<div class="week-tab'+(si===active?' on':'')+'" onclick="weekTab('+si+')">'+s.name+'</div>';
  });
  h+='</div>';
  // 模式切换（当前 tab 的 modeKey）
  h+='<div style="display:flex;gap:6px;margin-bottom:10px">';
  h+='<button type="button" class="'+(mode==='manual'?'btn-go':'ghost')+'" style="flex:1;padding:6px" onclick="setTplMode('+active+',\\'manual\\')">✍️ 手动填写</button>';
  h+='<button type="button" class="'+(mode==='llm'?'btn-go':'ghost')+'" style="flex:1;padding:6px" onclick="setTplMode('+active+',\\'llm\\')">🤖 大模型生成</button>';
  h+='</div>';
  if(mode==='llm'){
    // 🔑 生成按钮 + 自动更新方式选择 放同一行（各一半）
    var autoKey=g.llmWeekKey.replace('_llm_days','_llm_autoupdate');
    var autoMode=ts[autoKey];
    // 兼容旧 bool：true=每天，false=每7天
    if(autoMode===true) autoMode='daily';
    else if(autoMode===false) autoMode='weekly';
    else if(autoMode!=='daily' && autoMode!=='weekly' && autoMode!=='off') autoMode='weekly';
    h+='<div style="display:flex;gap:8px;align-items:center;margin-bottom:6px">';
    // 🔑 生成按钮去底色(ghost) + 更新下拉，各占一半（flex:1）比例协调
    h+='<button type="button" class="ghost" style="flex:1;padding:7px;white-space:nowrap" onclick="genWeek('+active+')">🤖 生成7天'+g.name+'</button>';
    h+='<select data-tpl="'+autoKey+'" onchange="llmAutoMode(this,\\''+autoKey+'\\')" style="flex:1;padding:7px;border-radius:6px;background:var(--bg-inset);color:var(--text);border:1px solid var(--border);height:36px">'
      +'<option value="daily"'+(autoMode==='daily'?' selected':'')+'>每天更新</option>'
      +'<option value="weekly"'+(autoMode==='weekly'?' selected':'')+'>每7天更新</option>'
      +'<option value="off"'+(autoMode==='off'?' selected':'')+'>不更新</option>'
      +'</select>';
    h+='</div>';
    h+='<div style="font-size:11px;color:var(--dim);margin-bottom:8px">点按钮生成；自动更新：每天=刷新当天，每7天=更新一周，不更新=手动。</div>';
  }else{
    // 🔑 手动模式：7天循环开关（这7条每周循环用，不用每周改日期）
    var loopOn=ts[g.loopKey]===true;
    h+='<label class="eng-check" style="display:block;margin-bottom:8px"><input type="checkbox" data-tpl="'+g.loopKey+'" '+(loopOn?'checked':'')+' onchange="weekLoopToggle(this,\\''+g.loopKey+'\\')"> 7天循环（这7条每周重复用，不用每周改日期）</label>';
  }
  // 未来 7 天一列（日期 + 文案，清晰明了）：大模型读 llm_days，手动读 days
  var days=(mode==='llm')?(ts[g.llmWeekKey]||{}):(ts[g.weekKey]||{});
  var labels=weekLabels();
  h+='<div style="display:flex;flex-direction:column;gap:6px">';
  for(var i=0;i<7;i++){
    var w=labels[i];
    var val=days[w.ds]||'';
    h+='<div style="display:flex;gap:6px;align-items:center;min-width:0">';
    h+='<div style="flex:0 0 88px;font-size:11px;color:var(--dim);white-space:nowrap">'+w.lbl+'</div>';
    if(mode==='llm'){
      h+='<div style="flex:1;font-size:12px;color:var(--accent2);border:1px solid var(--border);border-radius:6px;padding:6px 8px;background:var(--bg-inset);overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+esc(val)+'">'+esc(val||'（未生成）')+'</div>';
      // 🔑 单条生成按钮：每个日期一个，点击只生成那一天
      h+='<button type="button" class="ghost" style="flex:0 0 auto;padding:3px 8px;font-size:11px" onclick="genOne('+active+',\\''+w.ds+'\\')" title="只生成这一天">✨ 生成</button>';
    }else{
      h+='<input class="eng-input" type="text" data-week="'+g.weekKey+'" data-date="'+w.ds+'" value="'+esc(val)+'" placeholder="留空=用默认" oninput="weekInput(this,\\''+g.weekKey+'\\',\\''+w.ds+'\\')" style="flex:1;min-width:0">';
    }
    h+='</div>';
  }
  h+='</div>';
  // 🔑 默认文案（合并到本面板：7条留空的天用）
  if(g.defFields && g.defFields.length){
    h+='<div style="border-top:1px dashed var(--border);margin-top:10px;padding-top:8px">';
    h+='<div style="font-size:11px;color:var(--dim);margin-bottom:6px">🎯 默认文案（上面留空的天用）</div>';
    g.defFields.forEach(function(f){
      var val=(ts[f.k]!==undefined)?ts[f.k]:f.def;
      h+='<label class="eng-label">'+f.label+'</label>';
      if(f.type==='textarea'){
        h+='<textarea class="eng-input" data-tpl="'+f.k+'" style="min-height:44px;width:100%;box-sizing:border-box">'+esc(val)+'</textarea>';
      }else{
        h+='<input class="eng-input" type="text" data-tpl="'+f.k+'" value="'+esc(val)+'" style="width:100%;box-sizing:border-box">';
      }
    });
    h+='</div>';
  }
  h+='<div style="font-size:11px;color:var(--dim);margin-top:8px">ℹ️ '+g.help+'</div>';
  h+='</div>';
  return h;
}
function toggleTplFold(gi){
  var ts=CONFIG.template_settings||{};
  ts['_fold'+gi] = !(ts['_fold'+gi]||false);
  CONFIG.template_settings=ts;
  renderTplCfg();
}
// 🔑 播报文案面板折叠开关（默认收起，点开才展开）
function toggleWeekPanel(){
  var ts=CONFIG.template_settings||{};
  ts._weekPanelOpen=!(ts._weekPanelOpen===true);
  CONFIG.template_settings=ts;
  renderTplCfg();
}
// 🔑 切换播报文案的子 tab（问候语/结束语/小贴士）
function weekTab(si){
  var ts=CONFIG.template_settings||{};
  ts._activeWeek=si;
  CONFIG.template_settings=ts;
  renderTplCfg();
}
// 模式切换（si = 播报文案子 tab 索引）
function setTplMode(si,mode){
  var g=TPL_GROUPS[0].groups[si];
  var ts=CONFIG.template_settings||{};
  ts[g.modeKey]=mode;
  ts._activeWeek=si;   // 🔑 切到对应 tab，让生成按钮/预览显示在正确的位置
  CONFIG.template_settings=ts;
  markDirty();
  renderTplCfg();
  // 🔑 切换模式不自动生成大模型文案；点"生成未来7天"按钮才生成
}
// 未来 7 天（从今天起）的日期 + 中文标签（紧凑：今天·周六 / 明天·周日 / 周一）
function weekLabels(){
  var wd=['周日','周一','周二','周三','周四','周五','周六'];
  var out=[];
  for(var i=0;i<7;i++){
    var d=new Date(); d.setDate(d.getDate()+i);
    var ds=d.getFullYear()+'-'+('0'+(d.getMonth()+1)).slice(-2)+'-'+('0'+d.getDate()).slice(-2);
    var lbl;
    if(i===0){ lbl='今天·'+wd[d.getDay()]; }
    else if(i===1){ lbl='明天·'+wd[d.getDay()]; }
    else { lbl=wd[d.getDay()]; }
    lbl+=' '+(d.getMonth()+1)+'/'+d.getDate();
    out.push({ds:ds,lbl:lbl});
  }
  return out;
}
// 🤖 调后端生成未来7天文案，存到大模型的独立存储（不影响手动填写的）
// ✨ 单条生成：只生成某一天，存入 llm_days 对应日期
function genOne(si,dateStr){
  var g=TPL_GROUPS[0].groups[si];
  var secName=g.llmWeekKey.replace('_llm_days','');
  var statusEl=document.getElementById('tplStatus');
  statusEl.textContent='✨ 正在生成 '+dateStr+' 的'+g.name+'...';
  fetch(BASE+'tpl/one',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({section:secName,date:dateStr})})
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.ok && d.days){
        var ts=CONFIG.template_settings||{};
        if(!ts[g.llmWeekKey]) ts[g.llmWeekKey]={};
        for(var k in d.days) ts[g.llmWeekKey][k]=d.days[k];
        CONFIG.template_settings=ts;
        renderTplCfg();
        statusEl.textContent='✅ 已生成 '+dateStr+' 的'+g.name;
      }else{
        statusEl.textContent='❌ 生成失败：'+(d.error||'未知错误');
      }
    })
    .catch(function(){statusEl.textContent='❌ 生成失败（网络或超时）';});
}
function genWeek(si){
  var g=TPL_GROUPS[0].groups[si];
  var secName=g.llmWeekKey.replace('_llm_days','');
  var statusEl=document.getElementById('tplStatus');
  statusEl.textContent='🤖 正在生成未来7天'+g.name+'（约30秒）...';
  fetch(BASE+'tpl/week',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({section:secName})})
    .then(function(r){return r.json()})
    .then(function(d){
      if(d.ok && d.days){
        var ts=CONFIG.template_settings||{};
        ts[g.llmWeekKey]=d.days;
        CONFIG.template_settings=ts;
        markDirty();
        renderTplCfg();
        statusEl.textContent='✅ 已生成未来7天'+g.name;
      }else{
        statusEl.textContent='❌ 生成失败：'+(d.error||'未知错误');
      }
    })
    .catch(function(){statusEl.textContent='❌ 生成失败（网络或超时）';});
}
// 🔑 大模型自动更新三选一：daily=每天 / weekly=每7天 / off=关闭
function llmAutoMode(el,autoKey){
  var ts=CONFIG.template_settings||{};
  var v=el.value;
  ts[autoKey]=(v==='daily')?true:v;
  CONFIG.template_settings=ts;
  markDirty();
  // 🔑 切换更新频率：前端状态栏提示 + 后端日志同步显示
  var modeName={daily:'每天自动更新',weekly:'每7天自动更新',off:'关闭自动更新'}[v]||v;
  var secName=autoKey.replace('_llm_autoupdate','');
  var labelMap={greeting:'问候语',ending:'结束语',tip:'小贴士',enc:'鼓励语'};
  var lbl=labelMap[secName]||secName;
  document.getElementById('tplStatus').textContent='🔁 '+lbl+' 已切换为：'+modeName+'（点保存生效）';
  try{
    fetch(BASE+'log',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({msg:'🔁 '+lbl+'自动更新方式已切换为：'+modeName})}).catch(function(){});
  }catch(e){}
}
function saveTpl(){
  // 🔑 收集模板配置并保存
  var ts=CONFIG.template_settings||{};
  var secs=ts.sections||{};
  var box=document.getElementById('tplBox');
  if(box){
    box.querySelectorAll('[data-tpl]').forEach(function(el){
      var k=el.getAttribute('data-tpl');
      if(el.type==='checkbox'){ if(k.indexOf('sec_')===0) secs[k.slice(4)]=el.checked; else ts[k]=el.checked; }
      else if(el.type==='number'){ ts[k]=parseFloat(el.value); }
      else if(k.indexOf('fmt_')===0){
        // 🔑 fmt_<key>_<sub> → formats.<key>.<sub>（字段级 dict）；fmt_<key> → formats.<key>（整条）
        // 空值不存（留空=后端默认），避免存一堆默认格式
        if(!ts.formats) ts.formats={};
        var _rest=k.slice(4);
        var _us=_rest.lastIndexOf('_');
        var _val=(el.value||'').trim();
        if(_us>0){
          var _k=_rest.slice(0,_us), _s=_rest.slice(_us+1);
          if(!ts.formats[_k] || typeof ts.formats[_k]!=='object') ts.formats[_k]={};
          if(_val) ts.formats[_k][_s]=_val; else delete ts.formats[_k][_s];
        }else{
          if(_val) ts.formats[_rest]=_val; else delete ts.formats[_rest];
        }
      }
      else{ ts[k]=el.value; }
    });
    // 🔑 一周 7 天文案（问候语/结束语/小贴士）：收集手动填写，空=删该天（播报时用默认）
    box.querySelectorAll('[data-week]').forEach(function(el){
      var wk=el.getAttribute('data-week');
      var dt=el.getAttribute('data-date');
      var v=(el.value||'').trim();
      if(!ts[wk]) ts[wk]={};
      if(v) ts[wk][dt]=v; else delete ts[wk][dt];
    });
  }
  CONFIG.template_settings=ts;
  if(secs) ts.sections=secs;
  document.getElementById('tplStatus').textContent='保存中...';
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:{template_settings:ts}})})
    .then(function(r){return r.json()})
    .then(function(d){
      var tplS=document.getElementById('tplStatus');
      tplS.style.color='';
      tplS.textContent = d.ok?'✅ 模板配置已保存':'❌ 保存失败';
      if(d.ok) tplDirty=false;
    })
    .catch(function(){document.getElementById('tplStatus').textContent='❌ 保存失败';});
}
// 🔑 7天循环开关：勾上=这7条每周循环用（实时写 CONFIG，切 tab 不丢）
function weekLoopToggle(el,loopKey){
  var ts=CONFIG.template_settings||{};
  ts[loopKey]=el.checked;
  CONFIG.template_settings=ts;
  markDirty();
}
// 🔑 一周 7 天文案手动输入：实时写 CONFIG（切 tab / 保存都拿到最新值；空=删该天用默认）
function weekInput(el,wk,date){
  var ts=CONFIG.template_settings||{};
  if(!ts[wk]) ts[wk]={};
  var v=(el.value||'').trim();
  if(v) ts[wk][date]=v; else delete ts[wk][date];
  CONFIG.template_settings=ts;
  markDirty();
}
// 手动条目实体 id 输入：同步更新 CONFIG（去掉空实体 id）
function manualEid(el,key){
  var box=document.getElementById('sec-'+key);
  var ins=box.querySelectorAll('.entry input');
  var eid=el.value.trim();
  // 收集该条目：实体 id + 房间名
  markDirty();
  collectSection(key);
}

// 房间名输入：实时收集
function onRoom(el,key){ markDirty(); collectSection(key); }

// 删除条目：直接删 DOM，删除后重新收集 CONFIG（不再用索引，用按钮定位父元素）
function delEntry(key,btn){
  var entry=btn.closest('.entry');
  if(entry) entry.remove();
  markDirty();
  collectSection(key);
}
// 🔑 屏蔽/恢复传感器：屏蔽后不参与播报（保留在配置里）。存 muted 字段
function toggleMute(btn,key){
  var entry=btn.closest('.entry');
  var muted=!btn.classList.contains('muted');
  btn.classList.toggle('muted',muted);
  if(muted) entry.classList.add('muted-row'); else entry.classList.remove('muted-row');
  btn.textContent=muted?'🔇':'🔔';
  btn.title=muted?'已屏蔽，点击恢复':'屏蔽，不参与播报';
  markDirty();
  collectSection(key);
}
// 🔑 拖拽排序：拖动实体条目改变顺序（播报顺序跟着变）
var _dragEntry=null;
function dragStart(ev){
  _dragEntry=ev.target.closest('.entry');
  ev.dataTransfer.effectAllowed='move';
  ev.dataTransfer.setData('text/plain','');
  _dragEntry.style.opacity='.4';
}
function dragOver(ev){
  ev.preventDefault();
  ev.dataTransfer.dropEffect='move';
}
function dragDrop(ev,key){
  ev.preventDefault();
  var target=ev.target.closest('.entry');
  if(!target || target===_dragEntry) return;
  var box=document.getElementById('sec-'+key);
  // 插入到目标前/后（根据鼠标位置）
  var rect=target.getBoundingClientRect();
  if(ev.clientY > rect.top + rect.height/2){
    target.after(_dragEntry);
  }else{
    target.before(_dragEntry);
  }
  markDirty();
  collectSection(key);
}
function dragEnd(ev){
  if(_dragEntry){ _dragEntry.style.opacity=''; _dragEntry=null; }
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
    template_settings: CONFIG.template_settings||{},
  };
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:updates})})
    .then(function(r){return r.json()})
    .then(function(d){
      var cfgS=document.getElementById('cfgStatus');
      cfgS.style.color='';
      cfgS.textContent = d.ok?'✅ 已保存':'❌ 保存失败';
      if(d.ok) cfgDirty=false;
    })
    .catch(function(){document.getElementById('cfgStatus').textContent='❌ 保存失败';});
}

/* ─── 播报页 ─── */
var lastTextRun=0;
/* ─── 引擎切换 ─── */
function loadEngine(){
  fetch(BASE+'engine?'+Date.now()).then(function(r){return r.json()}).then(function(d){
    if(d && d.mode) document.getElementById('engineSel').value=d.mode;
  }).catch(function(){});
}
// 📋 播报栏目（板块开关，模板/大模型都生效）——定义 + 渲染 + 变更保存
var MAIN_SECTIONS=[
  {k:'sec_greeting',label:'问候语',def:true},
  {k:'sec_ending',label:'结束语',def:true},
  {k:'sec_end_marker',label:'播报结束',def:false},
  {k:'sec_temp',label:'温度',def:true},
  {k:'sec_humidity',label:'湿度',def:true},
  {k:'sec_power',label:'耗电量',def:true},
  {k:'sec_power_now',label:'实时功率',def:true},
  {k:'sec_security',label:'安全',def:true},
  {k:'sec_pm25',label:'空气质量',def:true},
  {k:'sec_lights',label:'灯光',def:true},
  {k:'sec_task',label:'终端任务',def:true},
  {k:'sec_todo',label:'待办备忘',def:true},
  {k:'sec_fault',label:'设备故障',def:true},
  {k:'sec_enc',label:'鼓励语',def:false},
  {k:'sec_tip',label:'小贴士',def:true},
];
function renderMainSections(){
  var box=document.getElementById('mainSections');
  if(!box) return;
  var secs=((CONFIG&&CONFIG.template_settings)||{}).sections||{};
  var h='';
  MAIN_SECTIONS.forEach(function(s){
    var val=(secs[s.k.slice(4)]!==undefined)?secs[s.k.slice(4)]:s.def;
    h+='<label class="eng-check" style="justify-content:flex-start;padding:4px 6px"><input type="checkbox" data-sec="'+s.k+'" '+(val?'checked':'')+' onchange="saveMainSection(this)"> '+s.label+'</label>';
  });
  box.innerHTML=h;
}
// 🔑 改播报栏目立即保存（不用点按钮）
function saveMainSection(cb){
  var ts=CONFIG.template_settings||{};
  if(!ts.sections) ts.sections={};
  ts.sections[cb.getAttribute('data-sec').slice(4)]=cb.checked;
  CONFIG.template_settings=ts;
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:{template_settings:ts}})})
    .then(function(r){return r.json()})
    .then(function(d){
      document.getElementById('secStatus').textContent = d.ok?('✅ 已保存：'+(cb.checked?'开启':'关闭')):'❌ 保存失败';
    })
    .catch(function(){document.getElementById('secStatus').textContent='❌ 保存失败';});
}
// ⏸️ 播报句间停顿：存到配置 template_settings.tts_pause，播报时用
function changePause(){
  var v=document.getElementById('pauseSel').value;
  var ts=CONFIG.template_settings||{};
  ts.tts_pause=parseFloat(v);
  CONFIG.template_settings=ts;
  fetch(BASE+'cfg/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({updates:{template_settings:ts}})})
    .then(function(r){return r.json()})
    .then(function(d){
      document.getElementById('status').textContent = d.ok?('✅ 句间停顿已设为 '+v+'s'):'❌ 保存失败';
    })
    .catch(function(){document.getElementById('status').textContent='❌ 保存失败';});
}
function changeEngine(){
  var mode=document.getElementById('engineSel').value;
  fetch(BASE+'engine',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode:mode})})
    .then(function(r){return r.json()})
    .then(function(d){
      document.getElementById('status').textContent = d.ok ? ('✅ 已切换：'+(mode==='llm'?'大模型':'模板')) : '❌ 切换失败';
    })
    .catch(function(){document.getElementById('status').textContent='❌ 切换失败';});
}
// 🔑 按钮互斥高亮：点哪个哪个蓝底，其他恢复灰边
function setActive(id){
  ['btnSpeak','btnText','btnClear','btnEntities'].forEach(function(b){
    document.getElementById(b).className = (b===id) ? 'active' : '';
  });
}
function trigger(textOnly){
  var t=document.getElementById('type').value;
  // 🔑 按钮选中态：点了哪个哪个蓝底白字，另一个普通样式
  setActive(textOnly ? 'btnText' : 'btnSpeak');
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
  // 🔑 语音播报和文字播报都显示文字区（语音播报时实时显示逐句字幕）
  showTextCard();
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
  // ⏱️ 超时保护：超过 180 秒还没生成完就提示，避免一直卡"生成中"
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
    // 请求失败：重试而不是静默停（避免"一直生成中"）
    pollTimer=setTimeout(pollText,1500);
  });
}
// 📊 日志页统计条：传感器总个数 / 离线传感器数 / 统计天数
function loadStats(){
  var bar=document.getElementById('statsBar');
  if(!bar) return;
  fetch(BASE+'stats?'+Date.now()).then(function(r){return r.json()}).then(function(d){
    if(!d || d.sensors===undefined){ bar.innerHTML=''; return; }
    var offCls=(d.offline>0)?'color:var(--red)':'color:var(--green)';
    bar.innerHTML=
      '<span>📡 传感器共 <b>'+d.sensors+'</b> 个</span>'
      +'<span style="'+offCls+'">'+(d.offline>0?('⚠️ 离线 <b>'+d.offline+'</b> 个'):'✅ 全部在线')+'</span>'
      +'<span>📅 已统计 <b>'+d.days+'</b> 天</span>';
  }).catch(function(){});
}
// 🔑 日志显示优化：每行 = 时间(灰) + 图标 + 消息（按类型分色），一眼看懂
function logRowColor(msg){
  if(msg.indexOf('❌')>=0 || msg.indexOf('失败')>=0 || msg.indexOf('出错')>=0) return '#f85149';
  if(msg.indexOf('✅')>=0 || msg.indexOf('完成')>=0 || msg.indexOf('已')>=0) return '#3fb950';
  if(msg.indexOf('⚠️')>=0) return '#d29922';
  if(msg.indexOf('📢')>=0 || msg.indexOf('播报')>=0) return '#58a6ff';
  if(msg.indexOf('🤖')>=0) return '#bc8cff';
  if(msg.indexOf('📊')>=0 || msg.indexOf('⚡')>=0) return '#3fb950';
  return '';
}
function refreshLog(){
  fetch(BASE+'logs?'+Date.now()).then(function(r){return r.json()}).then(function(a){
    if(!a || !a.length){ document.getElementById('log').textContent='暂无日志'; return; }
    var h='';
    // 从旧到新显示（倒序方便看最新在底部？日志本就旧→新，保持顺序）
    a.forEach(function(line){
      // 拆 [时间] 和 消息
      var m=line.match(/^\\[([^\\]]+)\\]\\s*(.*)$/);
      var time=m?m[1]:'', msg=m?m[2]:line;
      var color=logRowColor(msg);
      h+='<div style="line-height:1.7"><span style="color:var(--faint);font-size:11px">'+esc(time)+'</span> '
        +'<span style="color:'+(color||'var(--text)')+'">'+esc(msg)+'</span></div>';
    });
    var el=document.getElementById('log');
    el.innerHTML=h;
    el.scrollTop=el.scrollHeight;
  });
}
function clearLog(){
  // 🔑 点击保持选中（蓝底），其他按钮恢复灰边
  setActive('btnClear');
  fetch(BASE+'logs/clear',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    document.getElementById('log').textContent='';
  }).catch(function(){document.getElementById('log').textContent='';});
}
/* ─── 查看播报按钮实体 ─── */
function showEntities(){
  var box=document.getElementById('entitiesBox');
  // 🔑 再点一次：如果已打开则关闭 + 按钮恢复灰边
  if(box && box.style.display==='block'){
    box.style.display='none';
    setActive('');  // 清掉所有高亮
    return;
  }
  // 点击高亮，保持选中
  setActive('btnEntities');
  fetch(BASE+'entities/buttons?'+Date.now()).then(function(r){return r.json()}).then(function(list){
    var box=document.getElementById('entitiesBox');
    if(!list || !list.length){ box.innerHTML='<div style="color:var(--dim)">暂无播报按钮实体</div>'; return; }
    var tb={'daily':'📅 日','weekly':'🗓️ 周','monthly':'📆 月','yearly':'🎆 年'};
    var h='<div style="font-size:12px;color:var(--dim);margin-bottom:8px">播报按钮实体（自动化里可引用，点复制）：</div>';
    list.forEach(function(b){
      h+='<div style="padding:8px 10px;margin-bottom:6px;border-radius:6px;background:var(--bg-inset);border:1px solid var(--border);cursor:pointer" onclick="copyEntity(\\''+b.entity+'\\')">'
        +'<div style="font-weight:600">'+(tb[b.type]||b.type)+' · '+esc(b.name)+'</div>'
        +'<div style="font-size:11px;color:var(--accent2)">'+esc(b.entity)+'</div>'
        +'</div>';
    });
    h+='<div style="font-size:11px;color:var(--dim);margin-top:4px">自动化示例：服务 input_button.press → 目标 '+(list[0]?esc(list[0].entity):'')+'</div>';
    box.innerHTML=h;
    box.style.display='block';
  }).catch(function(){
    var box=document.getElementById('entitiesBox');
    box.innerHTML='<div style="color:var(--red)">加载实体失败</div>';
    box.style.display='block';
  });
}
function copyEntity(eid){
  // 🔑 修复：ingress iframe 里 navigator.clipboard 被禁用（权限策略），
  // 改用临时 textarea + document.execCommand('copy')（iframe 里也能用）
  var ok=false;
  try{
    var ta=document.createElement('textarea');
    ta.value=eid;
    ta.style.position='fixed';
    ta.style.opacity='0';
    document.body.appendChild(ta);
    ta.focus(); ta.select();
    ok=document.execCommand('copy');
    document.body.removeChild(ta);
  }catch(e){ ok=false; }
  if(ok){
    document.getElementById('status').textContent='✅ 已复制: '+eid;
  }else{
    // fallback：显示可手动复制的文本
    document.getElementById('status').textContent='复制失败，请手动复制: '+eid;
  }
  // 复制后关闭实体弹窗 + 按钮恢复灰边
  var box=document.getElementById('entitiesBox');
  if(box) box.style.display='none';
  setActive('');
}
/* ─── 历史记录 ─── */
var histYM={y:new Date().getFullYear(),m:new Date().getMonth()};
var histDays={}, histSel='', histAnomalies={};
function pad(n){return n<10?'0'+n:''+n;}
function histFmt(y,m){return y+'-'+pad(m+1);}
function histLoadCal(){
  var ym=histFmt(histYM.y,histYM.m);
  // 🔑 同时拉异常日期（某设备单日耗电超阈值 = 传感器异常）
  Promise.all([
    fetch(BASE+'history/days?month='+ym+'&_='+Date.now()).then(function(r){return r.json()}).catch(function(){return[]}),
    fetch(BASE+'history/anomalies?month='+ym+'&_='+Date.now()).then(function(r){return r.json()}).catch(function(){return{anomalies:{}}}),
  ]).then(function(res){
    histDays={}; (res[0]||[]).forEach(function(d){histDays[d]=true;});
    histAnomalies=(res[1]&&res[1].anomalies)||{};
    histRenderCal();
  });
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
  ['日','一','二','三','四','五','六'].forEach(function(w){h+='<div class="cal-dow">'+w+'</div>';});
  for(var i=0;i<first;i++) h+='<div class="cal-cell empty"></div>';
  for(var d=1;d<=daysInMonth;d++){
    var ds=histFmt(y,m)+'-'+pad(d);
    var has=histDays[ds];
    var isAnom=histAnomalies[ds];
    var cls='cal-cell'+(has?' has':'')+(isAnom?' anom':'')+(ds===todayStr?' today':'')+(ds===histSel?' selected':'');
    // 🔑 异常标识 ⚠️ 放在日期后面紧挨着（有记录时在圆点前，一眼看到这天有问题）
    var mark = isAnom
      ? '<span class="warn">⚠️</span>'
      : (has ? '<span class="dot"></span>' : '');
    h+='<div class="'+cls+'" onclick="histPick(\\''+ds+'\\')" title="'+(isAnom?('⚠️ 传感器异常：'+histAnomalies[ds].join('、')):'')+'">'+d+mark+'</div>';
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
  list.innerHTML='<div style="color:var(--dim)">加载中...</div>';
  fetch(BASE+'history/day?date='+ds+'&_='+Date.now()).then(function(r){return r.json()}).then(function(entries){
    if(!entries.length){list.innerHTML='<div style="color:var(--dim)">当天暂无播报</div>';return;}
    var tb={'daily':'日报','weekly':'周报','monthly':'月报','yearly':'年报'};
    var tc={'daily':'tb-daily','weekly':'tb-weekly','monthly':'tb-monthly','yearly':'tb-yearly'};
    // 统计各类型次数
    var typeOrder=['daily','weekly','monthly','yearly'];
    var cnt={daily:0,weekly:0,monthly:0,yearly:0};
    entries.forEach(function(e){cnt[e.type||'daily']=(cnt[e.type||'daily']||0)+1;});
    var h='<div style="font-size:12px;color:var(--dim);margin-bottom:8px">'+ds+' · '+entries.length+'次播报';
    typeOrder.forEach(function(t){
      if(cnt[t]>0) h+=' <span class="type-badge '+tc[t]+'">'+tb[t]+' '+cnt[t]+'</span>';
    });
    h+='</div>';
    entries.forEach(function(e,i){
      var t=e.type||'daily';
      // 🔑 每条历史：点击看详情，点 ✕ 删除，右键也删除
      h+='<div class="hist-entry" onclick="histView(\\''+ds+'\\','+i+')" oncontextmenu="event.preventDefault();histDelete(\\''+ds+'\\','+i+');return false;">';
      h+='<div class="t" style="display:flex;align-items:center;justify-content:space-between">';
      h+='<span>'+e.ts+' <span class="type-badge '+tc[t]+'">'+tb[t]+'</span></span>';
      h+='<button class="del" onclick="event.stopPropagation();histDelete(\\''+ds+'\\','+i+');" title="删除这条记录">✕</button>';
      h+='</div>';
      h+='<div class="x">'+esc(e.text||'')+'</div>';
      h+='<div class="b">'+e.count+'句 ›</div>';
      h+='</div>';
    });
    list.innerHTML=h;
  }).catch(function(){list.innerHTML='<div style="color:var(--red)">加载失败</div>';});
}
// 🗑️ 删除某条历史记录（按日期+时间戳唯一，直接删不二次确认）
function histDelete(ds,idx){
  fetch(BASE+'history/day?date='+ds+'&_='+Date.now()).then(function(r){return r.json()}).then(function(entries){
    var e=entries[idx];
    if(!e) return;
    var ts=e.ts;
    fetch(BASE+'history/delete?date='+ds+'&ts='+encodeURIComponent(ts),{method:'POST'}).then(function(r){return r.json()}).then(function(d){
      if(d.ok){
        // 🔑 删除成功提示 + 刷新列表/日历
        document.getElementById('histList').innerHTML='<div style="color:var(--green)">✅ '+esc(d.msg||'已删除')+'</div>';
        setTimeout(function(){ histPick(ds); }, 600);
        histLoadCal();
      }else{
        document.getElementById('histList').innerHTML='<div style="color:var(--red)">❌ 删除失败</div>';
      }
    }).catch(function(){document.getElementById('histList').innerHTML='<div style="color:var(--red)">❌ 删除失败</div>';});
  });
}
function histView(ds,idx){
  var list=document.getElementById('histList');
  fetch(BASE+'history/day?date='+ds+'&_='+Date.now()).then(function(r){return r.json()}).then(function(entries){
    var e=entries[idx];
    var all=e.sentences||[];
    var tb={'daily':'日报','weekly':'周报','monthly':'月报','yearly':'年报'};
    var tc={'daily':'tb-daily','weekly':'tb-weekly','monthly':'tb-monthly','yearly':'tb-yearly'};
    var t=e.type||'daily';
    var h='<button class="ghost" onclick="histPick(\\''+ds+'\\')" style="margin-bottom:10px;padding:6px 12px;font-size:12px">← 返回</button>';
    h+='<div style="font-size:12px;color:var(--dim);margin-bottom:8px">'+ds+' '+e.ts+' · 完整内容 <span class="type-badge '+tc[t]+'">'+tb[t]+'</span></div>';
    all.forEach(function(s){ h+='<div class="hist-full-line">'+esc(s.text||'')+'</div>'; });
    list.innerHTML=h;
  });
}
function loadHist(){ histLoadCal(); }
// 🗑️ 清除当天统计数据快照（今天数据异常时用，不影响之前）
function clearSnapshot(){
  if(!confirm('确定清除今天的数据吗？只清除今天，之前正常的数据不受影响。')) return;
  document.getElementById('snapStatus').textContent='清除中...';
  fetch(BASE+'clear-snapshot',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    var s=document.getElementById('snapStatus');
    s.textContent = d.ok?('✅ '+d.msg):'❌ 清除失败';
  }).catch(function(){document.getElementById('snapStatus').textContent='❌ 清除失败';});
}
// 🛠️ 修复异常传感器数据：删掉选中日期里异常设备（单日耗电超30度）的用电记录
function fixAnomaly(){
  var date=histSel;
  if(!date){ document.getElementById('fixStatus').textContent='⚠️ 先在日历上选一个带 ⚠️ 的日期'; return; }
  if(!histAnomalies[date]){ document.getElementById('fixStatus').textContent='⚠️ '+date+' 没有检测到异常'; return; }
  if(!confirm('确定修复 '+date+' 吗？会删掉当天异常传感器（'+histAnomalies[date].join('、')+'）的用电记录，该天这些设备不再计入统计。')) return;
  document.getElementById('fixStatus').textContent='修复中...';
  fetch(BASE+'fix-anomaly?date='+date,{method:'POST'}).then(function(r){return r.json()}).then(function(d){
    var s=document.getElementById('fixStatus');
    s.textContent = d.ok?('✅ '+d.msg):'❌ 修复失败';
    histLoadCal();  // 刷新日历（移除 ⚠️）
  }).catch(function(){document.getElementById('fixStatus').textContent='❌ 修复失败';});
}

setInterval(refreshLog,5000);
refreshLog();
// 📊 日志页统计条：传感器个数/离线/统计天数（随日志一起刷新）
setInterval(loadStats,10000);
loadStats();
// 默认全部灰边（不选中任何按钮）
setActive('');
loadEngine();
// 📋 加载播报栏目开关（拉配置渲染，切到其他 tab 后回来也刷新）
function loadMainSections(){
  fetch(BASE+'cfg/config?'+Date.now()).then(function(r){return r.json()}).then(function(d){
    CONFIG=d||{};
    renderMainSections();
    // 🔑 读配置的句间停顿，设置下拉选中
    var pause=(CONFIG.template_settings||{}).tts_pause;
    if(pause!==undefined){
      var sel=document.getElementById('pauseSel');
      if(sel) sel.value=String(pause);
    }
  }).catch(function(){});
}
loadMainSections();
// 点击实体框以外区域关闭实体弹窗
document.addEventListener('click',function(ev){
  var box=document.getElementById('entitiesBox');
  if(box && box.style.display==='block'){
    var inBtn=(ev.target && ev.target.id==='btnEntities');
    var inBox=(ev.target && ev.target.closest && ev.target.closest('#entitiesBox'));
    if(!inBtn && !inBox){ box.style.display='none'; setActive(''); }
  }
});
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
    # 功率积分线程（配置了 power_type=integral 的插座）
    threading.Thread(target=power_integral_loop, daemon=True).start()
    # Web 服务主线程
    log(f"🌐 Web UI 已启动 (port {port})")
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
