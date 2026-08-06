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


def run_broadcast(summary_type="daily", text_only=False):
    """在独立线程里跑播报（force=True），用锁与调度串行化防双播。"""
    def _run():
        if not LOCK.acquire(blocking=False):
            log("⚠️ 已有播报进行中，跳过本次触发")
            return
        try:
            log(f"📢 手动触发播报: {summary_type}" + ("（只看文字）" if text_only else ""))
            asyncio.run(ds.main(force=True, text_only=text_only, summary_type=summary_type))
        except Exception as e:
            log(f"❌ 播报出错: {e}")
        finally:
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
        path = self.path.split("?")[0]
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
        else:
            self._send(404, "not found", "text/plain")

    def do_POST(self):
        path = self.path.split("?")[0]
        import urllib.parse as up
        q = up.parse_qs(up.urlsplit(self.path).query)
        if path == "/trigger":
            t = (q.get("type") or ["daily"])[0]
            to = (q.get("text_only") or ["false"])[0].lower() == "true"
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
        else:
            self._send(404, "not found", "text/plain")


WEBUI_HTML = """<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
<meta charset=\"UTF-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">
<title>小爱每日播报</title>
<style>
  body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,'PingFang SC',sans-serif;margin:0;padding:16px}
  h1{font-size:18px;color:#f0f6fc}
  .tabs{display:flex;gap:6px;margin-bottom:12px}
  .tab{padding:8px 18px;border-radius:8px;background:#161b22;border:1px solid #30363d;color:#8b949e;cursor:pointer;font-size:14px}
  .tab.on{background:#1f6feb;color:#fff;border-color:#1f6feb}
  .card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px;margin-bottom:12px}
  .row{display:flex;gap:8px;margin:10px 0;flex-wrap:wrap;align-items:center}
  button{padding:10px 16px;border:none;border-radius:8px;background:#1f6feb;color:#fff;font-size:14px;cursor:pointer}
  button:active{opacity:.8}
  button.ghost{background:transparent;border:1px solid #1f6feb;color:#58a6ff}
  button.danger{background:transparent;border:1px solid #f85149;color:#f85149;padding:6px 10px;font-size:12px}
  select{padding:8px;border-radius:6px;background:#0d1117;color:#c9d1d9;border:1px solid #30363d;max-width:320px}
  input[type=text]{padding:8px;border-radius:6px;background:#0d1117;color:#c9d1d9;border:1px solid #30363d}
  .entry{display:flex;gap:8px;align-items:center;margin-bottom:6px}
  .entry select{flex:1;min-width:200px}
  .entry input{flex:1;min-width:120px}
  .entry .del{background:transparent;border:none;color:#f85149;cursor:pointer;font-size:16px}
  .sec-title{font-size:13px;font-weight:600;color:#f0f6fc;margin-bottom:4px}
  .sec-desc{font-size:11px;color:#8b949e;margin-bottom:10px}
  .add{background:transparent;border:1px dashed #30363d;color:#58a6ff;padding:6px 12px;font-size:12px;cursor:pointer;border-radius:6px}
  #log{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;max-height:400px;overflow-y:auto;background:#0a0e14;padding:10px;border-radius:8px}
  #status{font-size:12px;color:#8b949e}
  .save-status{font-size:12px;color:#3fb950;min-height:18px;margin-top:8px}
  .page{display:none}
  .page.on{display:block}
</style>
</head>
<body>
<h1>🎙️ 小爱每日播报</h1>
<div class=\"tabs\">
  <div class=\"tab on\" id=\"tabMain\" onclick=\"showPage('main')\">📢 播报</div>
  <div class=\"tab\" id=\"tabCfg\" onclick=\"showPage('cfg')\">⚙️ 传感器配置</div>
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
      <button onclick=\"trigger(false)\">📢 立即播报</button>
      <button class=\"ghost\" onclick=\"trigger(true)\">📝 只看文字</button>
      <button class=\"ghost\" onclick=\"refreshLog()\">🔄 刷新日志</button>
    </div>
    <div id=\"status\">就绪</div>
  </div>
  <div class=\"card\">
    <div id=\"log\">日志加载中...</div>
  </div>
</div>

<!-- 传感器配置页 -->
<div class=\"page\" id=\"page-cfg\">
  <div class=\"card\">
    <div class=\"sec-title\">🎙️ 播报音箱</div>
    <div class=\"sec-desc\">小米音箱的 notify 实体（HA 开发工具 → 通知里能看到）</div>
    <div class=\"row\">
      <input type=\"text\" id=\"cfg-speaker\" style=\"flex:1\" placeholder=\"notify.xiaomi_xxx_play_text\">
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

<script>
// 🔑 ingress 前缀：HA ingress 下页面 URL 是 /api/hassio_ingress/<token>/...
// 必须用相对路径（基于 location.pathname）而不是写死 /，否则请求打到 HA 主 API
var BASE = location.pathname.replace(/\\/[^/]*$/, '/');
var ENTITIES=[], CONFIG={};
var SECTIONS=[
  {key:'temp_sensors',title:'🌡️ 温度传感器',desc:'家里各房间温度',domains:['sensor'],room:true},
  {key:'humidity_sensors',title:'💧 湿度传感器',desc:'家里各房间湿度',domains:['sensor'],room:true},
  {key:'power_sensors',title:'⚡ 用电（今日电量）',desc:'用 *_power_cost_today 实体（今日累计 kWh）',domains:['sensor'],room:true},
  {key:'power_now',title:'🔌 实时功率（可选）',desc:'用 *_electric_power 实体（单位 W，高功耗提醒）',domains:['sensor'],room:true},
  {key:'lights',title:'💡 灯光',desc:'家里灯，播报\"没关灯\"提醒',domains:['light'],room:true},
  {key:'doors',title:'🚪 门窗',desc:'on=开，播报安全巡检',domains:['binary_sensor'],room:true},
  {key:'important_devices',title:'⚠️ 重要设备',desc:'空调/风扇等，离线3天内播报提醒',domains:['climate','fan','media_player','switch'],room:true},
];

function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\"/g,'&quot;');}

function showPage(p){
  document.getElementById('tabMain').className='tab'+(p==='main'?' on':'');
  document.getElementById('tabCfg').className='tab'+(p==='cfg'?' on':'');
  document.getElementById('page-main').className='page'+(p==='main'?' on':'');
  document.getElementById('page-cfg').className='page'+(p==='cfg'?' on':'');
  if(p==='cfg'){ loadCfgPage(); }
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

function loadCfgPage(){
  document.getElementById('cfgStatus').textContent='加载中...';
  // /cfg/ 前缀避免 ingress 下 /api/ 嵌套被 HA 拦截
  Promise.all([
    fetch(BASE+'cfg/entities?'+Date.now()).then(function(r){return r.json()}).catch(function(err){return {ok:false,entities:[],error:'请求失败: '+(err&&err.message||'')}}),
    fetch(BASE+'cfg/config?'+Date.now()).then(function(r){return r.json()}).catch(function(){return{}}),
  ]).then(function(res){
    var er=res[0]||{}; CONFIG=res[1]||{};
    ENTITIES=er.entities||[];
    document.getElementById('cfg-speaker').value=CONFIG.speaker_notify||'';
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
    h+='<div id=\"sec-'+sec.key+'\">';
    entries.forEach(function(pair,i){
      h+=entryHTML(sec, pair[0], pair[1], i);
    });
    h+='</div>';
    h+='<button class=\"add\" onclick=\"addEntry(\\''+sec.key+'\\')\">+ 添加</button>';
  });
  el.innerHTML=h;
}

function entryHTML(sec, eid, room, i){
  var ph=sec.room?'房间名':'标签';
  return '<div class=\"entry\">'
    +'<select onchange=\"collectEntry(\\''+sec.key+'\\','+i+')\">'+optsFor(sec.domains[0], eid)+'</select>'
    +'<input type=\"text\" value=\"'+esc(room||'')+'\" placeholder=\"'+ph+'\" oninput=\"collectEntry(\\''+sec.key+'\\','+i+')\">'
    +'<button class=\"del\" onclick=\"delEntry(\\''+sec.key+'\\','+i+')\">✕</button>'
    +'</div>';
}

// 收集每段当前条目到 CONFIG
function collectSection(key){
  var box=document.getElementById('sec-'+key);
  if(!box) return;
  var out={};
  box.querySelectorAll('.entry').forEach(function(e){
    var eid=e.querySelector('select').value;
    var room=e.querySelector('input').value.trim();
    if(eid) out[eid]=room;
  });
  CONFIG[key]=out;
}

function addEntry(key){
  collectSection(key);
  var sec=SECTIONS.filter(function(s){return s.key===key})[0];
  var box=document.getElementById('sec-'+key);
  box.innerHTML+=entryHTML(sec,'','',box.querySelectorAll('.entry').length);
}

function delEntry(key,i){
  collectSection(key);
  var box=document.getElementById('sec-'+key);
  var entries=box.querySelectorAll('.entry');
  if(entries[i]) entries[i].remove();
  // 重新编号 onchange 的索引
  box.querySelectorAll('.entry').forEach(function(e,j){
    e.querySelector('select').setAttribute('onchange','collectEntry(\\''+key+'\\','+j+')');
  });
}

function collectEntry(key,i){
  collectSection(key);
}

function saveCfg(){
  document.getElementById('cfgStatus').textContent='保存中...';
  SECTIONS.forEach(function(sec){ collectSection(sec.key); });
  var updates={
    speaker_notify: document.getElementById('cfg-speaker').value.trim(),
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
function trigger(textOnly){
  var t=document.getElementById('type').value;
  fetch(BASE+'trigger?type='+t+(textOnly?'&text_only=true':'')+Date.now(),{method:'POST'})
    .then(function(r){return r.json()})
    .then(function(d){document.getElementById('status').textContent='✅ 已触发 '+(textOnly?'文字':'语音')+'播报';})
    .catch(function(){document.getElementById('status').textContent='❌ 触发失败';});
  setTimeout(refreshLog,1000);
}
function refreshLog(){
  fetch(BASE+'logs?'+Date.now()).then(function(r){return r.json()}).then(function(a){
    document.getElementById('log').textContent=(a||[]).join('\\n')||'暂无日志';
    document.getElementById('log').scrollTop=document.getElementById('log').scrollHeight;
  });
}
setInterval(refreshLog,5000);
refreshLog();
</script>
</body>
</html>
"""


def main():
    port = int(os.environ.get("INGRESS_PORT") or os.environ.get("PORT") or "8099")
    # 调度循环在独立线程
    threading.Thread(target=scheduler_loop, daemon=True).start()
    # Web 服务主线程
    log(f"🌐 Web UI 已启动 (port {port})")
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
