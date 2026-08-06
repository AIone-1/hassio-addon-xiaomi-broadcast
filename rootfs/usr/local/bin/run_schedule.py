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
        else:
            self._send(404, "not found", "text/plain")


WEBUI_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>小爱每日播报</title>
<style>
  body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,'PingFang SC',sans-serif;margin:0;padding:16px}
  h1{font-size:18px;color:#f0f6fc}
  .card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px;margin-bottom:12px}
  .row{display:flex;gap:8px;margin:10px 0;flex-wrap:wrap}
  button{padding:10px 16px;border:none;border-radius:8px;background:#1f6feb;color:#fff;font-size:14px;cursor:pointer}
  button:active{opacity:.8}
  button.ghost{background:transparent;border:1px solid #1f6feb;color:#58a6ff}
  select{padding:8px;border-radius:6px;background:#0d1117;color:#c9d1d9;border:1px solid #30363d}
  #log{font-family:ui-monospace,monospace;font-size:12px;white-space:pre-wrap;max-height:400px;overflow-y:auto;background:#0a0e14;padding:10px;border-radius:8px}
  #status{font-size:12px;color:#8b949e}
</style>
</head>
<body>
<h1>🎙️ 小爱每日播报</h1>
<div class="card">
  <div class="row">
    <select id="type">
      <option value="daily">📅 每日</option>
      <option value="weekly">🗓️ 周</option>
      <option value="monthly">📆 月</option>
      <option value="yearly">🎆 年</option>
    </select>
    <button onclick="trigger(false)">📢 立即播报</button>
    <button class="ghost" onclick="trigger(true)">📝 只看文字</button>
    <button class="ghost" onclick="refreshLog()">🔄 刷新日志</button>
  </div>
  <div id="status">就绪</div>
</div>
<div class="card">
  <div id="log">日志加载中...</div>
</div>
<script>
function trigger(textOnly){
  var t=document.getElementById('type').value;
  fetch('/trigger?type='+t+(textOnly?'&text_only=true':'')+Date.now(),{method:'POST'})
    .then(function(r){return r.json()})
    .then(function(d){document.getElementById('status').textContent='✅ 已触发 '+(textOnly?'文字':'语音')+'播报';})
    .catch(function(){document.getElementById('status').textContent='❌ 触发失败';});
  setTimeout(refreshLog,1000);
}
function refreshLog(){
  fetch('/logs?'+Date.now()).then(function(r){return r.json()}).then(function(a){
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
