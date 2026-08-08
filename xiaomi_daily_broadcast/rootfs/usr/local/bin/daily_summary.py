#!/usr/bin/env python3
"""
智能每日播报 V2 —— HAOS 加载项版
新增：温度日统计(高/低)、用电分析(排行+预估)、安全强化(多门窗+空调节能)、
      终端任务计数、购物清单+待办事项双清单
"""
import asyncio, json, websockets, urllib.request, urllib.parse, os, calendar, re
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── HAOS 加载项路径 ──
# 配置文件/数据放 /share/xiaomi_broadcast（Samba 可编辑、可备份）
# 状态/标记放 /data（容器持久卷）
DATA_DIR = Path("/share/xiaomi_broadcast")
CONFIG_PATH = DATA_DIR / "ha_daily_config.json"
TASK_LOG = DATA_DIR / ".terminal_tasks.log"
MEMO_FILE = DATA_DIR / "memos.json"
SNAPSHOT_FILE = DATA_DIR / "daily_stats_history.json"
BROADCAST_STATE = Path("/data/broadcast_state.json")
MARKER_DIR = Path("/data/markers")

# HA 连接（启动时由 _ha_endpoints() 更新，不硬编码）
HA_WS = "ws://supervisor/core/websocket"
HA_API = "http://supervisor/core/api"
TOKEN = ""


# ═══════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════

OPTIONS_PATH = Path("/data/options.json")


def load_options():
    """读加载项 options（Supervisor 写入 /data/options.json）"""
    try:
        if OPTIONS_PATH.exists():
            return json.loads(OPTIONS_PATH.read_text())
    except Exception:
        pass
    return {}


def _ha_endpoints():
    """根据 options 决定 HA 连接端点与 token。supervisor 模式自动用 SUPERVISOR_TOKEN。
    若拿不到 SUPERVISOR_TOKEN，fallback 到 options 里的 ha_token（手动兜底）。"""
    opt = load_options()
    host = (opt.get("ha_host") or "supervisor").strip()
    port = int(opt.get("ha_port") or 8123)
    if host in ("supervisor", ""):
        tok = os.environ.get("SUPERVISOR_TOKEN", "")
        if not tok:
            tok = opt.get("ha_token") or ""  # 兜底：用户手动填的 HA 长期 token
        return ("ws://supervisor/core/websocket",
                "http://supervisor/core/api",
                tok)
    token = opt.get("ha_token") or ""
    return (f"ws://{host}:{port}/api/websocket",
            f"http://{host}:{port}/api",
            token)


def _refresh_endpoints():
    global HA_WS, HA_API, TOKEN
    HA_WS, HA_API, TOKEN = _ha_endpoints()


def _merge_period(ps, ptype, opt, enabled, hour, day, also, month=None):
    """把 options 扁平键写入 period_summaries.<type> 嵌套结构。"""
    if enabled in opt:
        ps.setdefault(ptype, {})["enabled"] = bool(opt[enabled])
    if opt.get(hour) is not None:
        ps.setdefault(ptype, {})["hour"] = int(opt[hour])
    if opt.get(day) is not None:
        ps.setdefault(ptype, {})["day"] = int(opt[day])
    if month is not None and opt.get(month) is not None:
        ps.setdefault(ptype, {})["month"] = int(opt[month])
    if also in opt:
        ps.setdefault(ptype, {})["also_daily"] = bool(opt[also])


def load_config():
    """读 JSON 配置 + 合并加载项 options（options 覆盖 JSON 同名键）。"""
    cfg = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    opt = load_options()
    if not opt:
        return cfg

    # 音箱 / 语速
    if opt.get("speaker_notify"):
        cfg["speaker_notify"] = opt["speaker_notify"]
    if opt.get("tts_speed") is not None:
        cfg["tts_speed"] = float(opt["tts_speed"])

    # 引擎（LLM 直连 DeepSeek）
    engine = cfg.setdefault("engine", {})
    if opt.get("engine_mode"):
        engine["mode"] = opt["engine_mode"]
    llm = engine.setdefault("llm", {})
    if opt.get("deepseek_api_key"):
        llm["api_key"] = opt["deepseek_api_key"]
    if opt.get("deepseek_model"):
        llm["model"] = opt["deepseek_model"]
    if opt.get("deepseek_base_url"):
        llm["base_url"] = opt["deepseek_base_url"]
    if "fallback_to_template" in opt:
        llm["fallback_to_template"] = bool(opt["fallback_to_template"])
    llm.setdefault("provider", "deepseek")

    # 调度
    sched = cfg.setdefault("broadcast_schedule", {})
    if "broadcast_enabled" in opt:
        sched["enabled"] = bool(opt["broadcast_enabled"])
    if opt.get("weekday_hour") is not None:
        sched["weekday_hour"] = int(opt["weekday_hour"])
    if opt.get("weekend_hour") is not None:
        sched["weekend_hour"] = int(opt["weekend_hour"])

    # 周期总结
    ps = cfg.setdefault("period_summaries", {})
    _merge_period(ps, "weekly", opt, enabled="weekly_summary", hour="weekly_hour",
                  day="weekly_day", also="weekly_also_daily")
    _merge_period(ps, "monthly", opt, enabled="monthly_summary", hour="monthly_hour",
                  day="monthly_day", also="monthly_also_daily")
    _merge_period(ps, "yearly", opt, enabled="yearly_summary", hour="yearly_hour",
                  day="yearly_day", month="yearly_month", also="yearly_also_daily")

    # 🔑 前端切换的引擎模式（/data/engine_mode.json）优先于 options
    try:
        _em = Path("/data/engine_mode.json")
        if _em.exists():
            _mode = json.loads(_em.read_text()).get("mode", "")
            if _mode in ("template", "llm"):
                cfg.setdefault("engine", {})["mode"] = _mode
    except Exception:
        pass

    # 🔑 传感器配置值兼容：新格式 {eid: {room, usage, power_type}} 规范化
    # 普通段 → {eid: room}（usage 只存展示）；power_sensors → {eid: {room, power_type}}（用电三方案需要 power_type）
    _norm_segs = ("temp_sensors", "humidity_sensors", "power_now", "lights", "doors", "important_devices")
    for _seg in _norm_segs:
        _s = cfg.get(_seg)
        if isinstance(_s, dict):
            for _eid, _val in list(_s.items()):
                if isinstance(_val, dict):
                    _s[_eid] = _val.get("room", "")
    # power_sensors 保留 power_type（daily/accumulate/integral），默认 daily
    _ps = cfg.get("power_sensors")
    if isinstance(_ps, dict):
        for _eid, _val in list(_ps.items()):
            if isinstance(_val, dict):
                _pt = _val.get("power_type", "daily")
                if _pt not in ("daily", "accumulate", "integral"):
                    _pt = "daily"
                _ps[_eid] = {"room": _val.get("room", ""), "power_type": _pt}
    return cfg


def fetch_history(entity_ids, token, start_hour=0):
    """通过 REST API 查今日 state history。返回 {entity_id: [float values]}"""
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"{HA_API}/history/period/{today}T{start_hour:02d}:00:00"
    if isinstance(entity_ids, list):
        url += "?filter_entity_id=" + ",".join(entity_ids)
    result = {}
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            for entity_history in data:
                if not entity_history:
                    continue
                eid = entity_history[0].get("entity_id", "")
                values = []
                for entry in entity_history:
                    try:
                        v = float(entry.get("state", ""))
                        values.append(v)
                    except (ValueError, TypeError):
                        pass
                if values:
                    result[eid] = values
    except Exception as e:
        print(f"  ⚠️ 历史查询失败: {e}")
    return result


def fetch_history_range(entity_ids, token, start_date, end_date):
    """查 [start_date, end_date) 整段 state history，保留时间戳。
    返回 {entity_id: [(aware_dt, float), ...]}。start/end_date 是 date 对象。
    entity_ids 可以是 list 或 dict（取 keys）。
    ⚠️ HA 返回 UTC ISO 字符串，必须 .astimezone() 转本地，否则日界错 8 小时。"""
    ids = list(entity_ids.keys()) if isinstance(entity_ids, dict) else list(entity_ids or [])
    url = f"{HA_API}/history/period/{start_date}T00:00:00?end_time={end_date}T00:00:00"
    if ids:
        url += "&filter_entity_id=" + ",".join(ids)
    url += "&minimal_response"
    result = {}
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            for entity_history in data:
                if not entity_history:
                    continue
                eid = entity_history[0].get("entity_id", "")
                entries = []
                for entry in entity_history:
                    try:
                        v = float(entry.get("state", ""))
                        ts_str = entry.get("last_changed") or entry.get("last_updated")
                        if ts_str:
                            ts = datetime.fromisoformat(ts_str)
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                            entries.append((ts.astimezone(), v))
                    except (ValueError, TypeError):
                        pass
                if entries:
                    result[eid] = entries
    except Exception as e:
        print(f"  ⚠️ 历史范围查询失败: {e}")
    return result


def fetch_daily_power(power_config, token, date):
    """查某日各设备用电（kWh）。返回 {label: kwh}。
    - daily：取窗口最后一个值（power_cost_today 午夜归零，用最后值）
    - accumulate：取窗口首尾差值（power_consumption 累计，差值=当天增量）
    """
    eids = [e for e in power_config if not e.startswith("_")]
    hist = fetch_history_range(eids, token, date, date + timedelta(days=1))
    out = {}
    for eid, meta in power_config.items():
        if eid.startswith("_"):
            continue
        if isinstance(meta, dict):
            label = meta.get("room", "")
            ptype = meta.get("power_type", "daily")
        else:
            label = meta
            ptype = "daily"
        disp = label if label else eid
        entries = [v for _, v in hist.get(eid, [])]
        if not entries:
            continue
        if ptype == "accumulate":
            # 累计差值：窗口最后一个值 - 窗口第一个值
            out[disp] = max(0.0, entries[-1] - entries[0])
        else:
            out[disp] = max(0.0, entries[-1])
    return out


def fetch_daily_temperature(temp_sensors, token, day):
    """查某日各房间温度。返回 {room: {"low","high","now"}}。
    low=当天最小值, high=最大值, now=当天最后一个值。"""
    hist = fetch_history_range(temp_sensors, token, day, day + timedelta(days=1))
    out = {}
    for eid, room in temp_sensors.items():
        vals = [v for _, v in hist.get(eid, [])]
        if vals:
            out[room] = {"low": min(vals), "high": max(vals), "now": vals[-1]}
    return out


POWER_BASELINE = Path("/data/power_baseline.json")
POWER_INTEGRAL = Path("/data/power_integral.json")


def ts(config, key, default):
    """读 template_settings 配置项，带默认值兜底。"""
    try:
        v = (config.get("template_settings") or {}).get(key)
        if v is not None:
            return v
    except Exception:
        pass
    return default


def ts_on(config, section):
    """板块开关：sections.<section> 有配置按配置，没配置用默认。
    鼓励语(enc)/播报结束(end_marker) 默认关——用户没显式勾选就不播；其余板块默认开。"""
    try:
        secs = (config.get("template_settings") or {}).get("sections")
        if isinstance(secs, dict) and section in secs:
            return bool(secs[section])
    except Exception:
        pass
    if section in ("enc", "end_marker"):
        return False
    return True


def _week_loop_match(ts_cfg, prefix, today_str):
    """7 天循环：循环开关 <prefix>_loop_enabled 开启时，按星期几匹配 <prefix>_days 里同星期几那条（跨周一直循环用）。
    开关没开 / 没匹配到 → None。"""
    if not ts_cfg.get(prefix + "_loop_enabled"):
        return None
    days = ts_cfg.get(prefix + "_days") or {}
    if not isinstance(days, dict):
        return None
    try:
        wd = datetime.strptime(today_str, "%Y-%m-%d").weekday()
        for k, v in days.items():
            if isinstance(k, str) and len(k) == 10:
                try:
                    if datetime.strptime(k, "%Y-%m-%d").weekday() == wd \
                            and isinstance(v, str) and v.strip():
                        return v.strip()
                except ValueError:
                    continue
    except Exception:
        pass
    return None


def week_day_text(config, prefix, today_str):
    """读模板配置的文案取当天内容。手动和大模型**独立存储**，互不覆盖：
    - <prefix>_mode == "llm" → 读 <prefix>_llm_days[date]（大模型生成的）
    - 否则（manual）→ 读 <prefix>_days[date]（手动填的）> 7天循环（按星期几）
    prefix: greeting / ending / tip。都没有返回 None，上层回退默认逻辑。"""
    try:
        ts_cfg = config.get("template_settings") or {}
        mode = ts_cfg.get(prefix + "_mode", "manual")
        if mode == "llm":
            llm_days = ts_cfg.get(prefix + "_llm_days") or {}
            if isinstance(llm_days, dict):
                v = llm_days.get(today_str)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            return None
        # 手动填写
        days = ts_cfg.get(prefix + "_days") or {}
        if isinstance(days, dict):
            v = days.get(today_str)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return _week_loop_match(ts_cfg, prefix, today_str)
    except Exception:
        pass
    return None


def apply_section_filters(report, config):
    """🔑 大模型生成前：按模板配置的板块开关，清空 report 里关掉板块的数据。
    report 字段 → sections 键映射。"""
    _map = {
        "temperature": "temp", "temp_alerts": "temp", "humidity_dry": "humidity", "humidity_wet": "humidity",
        "power": "power", "security": "security", "pm25": "pm25",
        "lights_on": "lights", "tasks_done": "task", "todos": "todo", "memos": "todo",
        "faults": "fault", "tip": "tip",
    }
    for field, sec in _map.items():
        if not ts_on(config, sec):
            if isinstance(report.get(field), list):
                report[field] = []
            elif isinstance(report.get(field), dict):
                report[field] = {}
            else:
                report[field] = None


def load_power_baseline():
    """读每日累计差值基准：{"date": "2026-08-08", "baselines": {entity_id: value}}"""
    try:
        if POWER_BASELINE.exists():
            return json.loads(POWER_BASELINE.read_text())
    except Exception:
        pass
    return {"date": "", "baselines": {}}


def save_power_baseline(date_str, baselines):
    try:
        POWER_BASELINE.write_text(json.dumps({"date": date_str, "baselines": baselines}, ensure_ascii=False))
    except Exception:
        pass


def record_power_baseline(states, power_config):
    """把配置里 accumulate 设备的当前累计值记为今日基准（每天0点/缺失时调）。"""
    bl = load_power_baseline()
    today = datetime.now().strftime("%Y-%m-%d")
    bl["date"] = today
    bl.setdefault("baselines", {})
    for eid, meta in power_config.items():
        if eid.startswith("_"):
            continue
        if isinstance(meta, dict) and meta.get("power_type") == "accumulate":
            v = get_float(states, eid)
            if v is not None and v >= 0:
                bl["baselines"][eid] = v
    save_power_baseline(today, bl["baselines"])
    return bl


def load_power_integral():
    """读功率积分：{"date": "2026-08-08", "kwh": {entity_id: value}}"""
    try:
        if POWER_INTEGRAL.exists():
            return json.loads(POWER_INTEGRAL.read_text())
    except Exception:
        pass
    return {"date": "", "kwh": {}}


def save_power_integral(date_str, kwh):
    try:
        POWER_INTEGRAL.write_text(json.dumps({"date": date_str, "kwh": kwh}, ensure_ascii=False))
    except Exception:
        pass


def calc_device_energy(states, power_config):
    """用电三方案计算今日用电量 kWh：
    - daily（默认）：直接读 power_cost_today 实体
    - accumulate：读 power_consumption 累计实体，当前值 - 当日零点基准
    - integral：读功率积分文件（常驻线程累加 electric_power）
    """
    device_energy = {}
    bl = load_power_baseline()
    bl_date = bl.get("date", "")
    today = datetime.now().strftime("%Y-%m-%d")
    bl_map = bl.get("baselines", {}) if bl_date == today else {}
    integ = load_power_integral()
    integ_map = integ.get("kwh", {}) if integ.get("date") == today else {}

    for eid, meta in power_config.items():
        if eid.startswith("_"):
            continue
        # 旧格式字符串=daily；新格式 dict 带 power_type
        if isinstance(meta, dict):
            label = meta.get("room", "")
            ptype = meta.get("power_type", "daily")
        else:
            label = meta
            ptype = "daily"
        disp = label if label else eid

        if ptype == "daily":
            v = get_float(states, eid)
            if v is not None and v >= 0:
                device_energy[disp] = v
        elif ptype == "accumulate":
            cur = get_float(states, eid)
            base = bl_map.get(eid)
            if cur is None or cur < 0:
                continue
            if base is None:
                # 基准缺失/过期：先记当前为基准，今天从0开始算
                record_power_baseline(states, power_config)
                device_energy[disp] = 0.0
            else:
                device_energy[disp] = max(0.0, cur - base)
        elif ptype == "integral":
            v = integ_map.get(eid, 0.0)
            if v and v > 0:
                device_energy[disp] = v
    return device_energy


def count_today_tasks():
    """统计今日终端任务完成数"""
    return count_tasks_on(datetime.now().strftime("%Y-%m-%d"))


def count_tasks_on(date_str):
    """.terminal_tasks.log 中日期前缀 == date_str 的行数"""
    if not TASK_LOG.exists():
        return 0
    count = 0
    try:
        with open(TASK_LOG) as f:
            for line in f:
                if line.startswith(date_str):
                    count += 1
    except:
        pass
    return count


# ═══════════════════════════════════════════════════════
# 每日快照（长期数据源，周期总结依赖）
# ═══════════════════════════════════════════════════════

def load_snapshots():
    """读快照文件 → {date: snapshot}；文件不存在/损坏返回 {}"""
    if not SNAPSHOT_FILE.exists():
        return {}
    try:
        with open(SNAPSHOT_FILE) as f:
            data = json.load(f)
        return data.get("days", {})
    except:
        return {}


def save_snapshots(snaps):
    """写快照文件（含版本号）"""
    tmp = SNAPSHOT_FILE.with_suffix(".json.tmp")
    with open(tmp, 'w') as f:
        json.dump({"version": 1, "days": snaps}, f, ensure_ascii=False, indent=1)
    os.replace(tmp, SNAPSHOT_FILE)


def write_daily_snapshot(now, config, states, temp_history, device_energy,
                         task_count, pm25, lights_on, memos, open_doors, faults):
    """把当天采集结果落盘快照，同日期覆盖。temp_history 是 fetch_history() 的返回。"""
    snaps = load_snapshots()
    date_str = now.strftime("%Y-%m-%d")

    temp = {}
    for eid, room in config["temp_sensors"].items():
        cur = get_float(states, eid)
        hist = temp_history.get(eid, [])
        if cur is None and not hist:
            continue
        d = {}
        if hist:
            d["low"], d["high"] = round(min(hist), 1), round(max(hist), 1)
        if cur is not None:
            d["now"] = round(cur, 1)
        temp[room] = d

    humidity = {}
    for eid, room in config["humidity_sensors"].items():
        v = get_float(states, eid)
        if v is not None and v <= 100:
            humidity[room] = round(v, 1)

    snap = {
        "date": date_str,
        "ts": now.strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "snapshot",
        "temp": temp,
        "humidity": humidity,
        "pm25": pm25,
        "power": {
            "total_kwh": round(sum(device_energy.values()), 2),
            "by_device": {k: round(v, 2) for k, v in device_energy.items()},
        },
        "tasks_done": task_count,
        "lights_on": lights_on,
        "lights_all_off": len(lights_on) == 0,
        "active_memos": len(memos),
        "open_doors": len(open_doors),
        "faults": len(faults),
    }
    snaps[date_str] = snap
    save_snapshots(snaps)
    print(f"  📸 今日快照已记录")
    return snap


def load_active_memos():
    """读取未完成的备忘"""
    if not MEMO_FILE.exists():
        return []
    try:
        with open(MEMO_FILE) as f:
            memos = json.load(f)
        return [m for m in memos if not m.get('done')]
    except:
        return []


def classify_sentence(text):
    """根据文本内容返回 icon 类型（顺序重要：先匹配更具体的）"""
    if any(kw in text for kw in ['湿度', '干燥', '通风']):
        return 'air'
    if any(kw in text for kw in ['PM', '空气']):
        return 'air'
    if any(kw in text for kw in ['用电', '功耗', '排名', '耗电', '瓦']):
        return 'power'
    if any(kw in text for kw in ['安全', '门窗', '关好', '开着', '钥匙', '盖好']):
        return 'secure'
    if any(kw in text for kw in ['温度', '度']):
        return 'temp'
    if any(kw in text for kw in ['灯', '亮着', '省电']):
        return 'light'
    if any(kw in text for kw in ['备忘', '待办', '购物', '清单']):
        return 'todo'
    if any(kw in text for kw in ['终端任务']):
        return 'terminal'
    if any(kw in text for kw in ['离线', '故障']):
        return 'fault'
    if any(kw in text for kw in ['小贴士', '提示']):
        return 'tip'
    if any(kw in text for kw in ['加油', '辛苦', '晚安', '早安', '好梦', '努力', '收获', '休息', '发光', '开始', '顺利', '劳逸', '周末愉快', '工作日']):
        return 'encourage'
    return 'text'


def write_broadcast_state(status, sentences=None, played_to=0, run_id='', mode='speech', summary_type='daily', phase=''):
    """写入播报状态文件，供 UI 读取。mode: speech=播报 / text=只看文字。phase: 前端实时阶段提示（数据上传中/生成播报稿中/...）"""
    if not run_id:
        import time as _time
        run_id = str(int(_time.time()))
    state = {
        'status': status,
        'sentences': sentences or [],
        'played_to': played_to,
        'run_id': run_id,
        'mode': mode,
        'summary_type': summary_type,
        'phase': phase,
    }
    try:
        with open(BROADCAST_STATE, 'w') as f:
            json.dump(state, f, ensure_ascii=False)
    except:
        pass


def save_history(now, sentences, all_sentences, summary_type='daily'):
    """保存播报记录到本地 JSONL（加载项内，无外部 server）"""
    try:
        rec = {
            "date": now.strftime("%Y-%m-%d"),
            "text": sentences[0] if sentences else "",
            "count": len(sentences),
            "sentences": all_sentences,
            "type": summary_type,
            "ts": now.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        with open(DATA_DIR / "broadcast_history.jsonl", "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("  📜 已保存历史记录")
    except Exception as e:
        print(f"  ⚠️ 历史保存失败: {e}")


def get_float(states, eid):
    s = states.get(eid)
    if not s:
        return None
    v = s.get("state", "")
    if v in ("unavailable", "unknown", ""):
        return None
    try:
        return float(v)
    except:
        return None


def is_present(states, config):
    for eid in config["presence_sensors"]:
        s = states.get(eid)
        if s and s.get("state") == "on":
            return True
    return False


def should_broadcast(now, config):
    schedule = config.get("broadcast_schedule", {})
    if not schedule.get("enabled", True):
        return False
    wd = now.weekday()
    if wd <= 4:
        return now.hour == schedule.get("weekday_hour", 21)
    else:
        return now.hour == schedule.get("weekend_hour", 18)


def pick(items, seed):
    """支持普通数组和按时间段分组的 dict"""
    if isinstance(items, list):
        return items[seed % len(items)] if items else ""
    if isinstance(items, dict):
        # items 是 {"morning": [...], "afternoon": [...], "evening": [...]}
        # 合并所有条目然后挑
        all_items = []
        for k in items:
            if isinstance(items[k], list):
                all_items.extend(items[k])
        return all_items[seed % len(all_items)] if all_items else ""
    return ""


def pick_time(items, hour, seed):
    """根据小时选择对应时间段的条目"""
    if isinstance(items, list):
        return items[seed % len(items)] if items else ""
    if isinstance(items, dict):
        if hour < 12:
            bucket = items.get("morning", [])
        elif hour < 18:
            bucket = items.get("afternoon", [])
        else:
            bucket = items.get("evening", [])
        if not bucket:
            # fallback: pick from any non-empty bucket
            for v in items.values():
                if v:
                    bucket = v
                    break
        return bucket[seed % len(bucket)] if bucket else ""
    return ""


async def call_service(ws, domain, service, data=None, target=None, cid=1, return_response=False):
    msg = {"type": "call_service", "domain": domain, "service": service,
           "id": cid}
    if data:
        msg["service_data"] = data
    if target:
        msg["target"] = target
    if return_response:
        msg["return_response"] = True
    await ws.send(json.dumps(msg))
    return json.loads(await ws.recv())


async def speak(ws, text, config, cid=999):
    entity = config["speaker_notify"]
    # 音箱播放超时偶发，重试一次再放弃，并打印失败原因（不再静默）
    for attempt in (1, 2):
        try:
            r = await call_service(ws, "notify", "send_message",
                                   {"message": text},
                                   {"entity_id": entity}, cid=cid)
            if r.get("success"):
                return True
            err = (r.get("error") or {}).get("message", "")
            if attempt == 1:
                print(f"  ⚠️ 音箱播放失败（第{attempt}次）: {err}，重试...")
        except Exception as e:
            if attempt == 1:
                print(f"  ⚠️ 音箱播放异常（第{attempt}次）: {e}，重试...")
    print(f"  ❌ 音箱播放失败（已重试）: {entity}")
    return False


# ═══════════════════════════════════════════════════════
# 周期总结调度（周/月/年）+ 幂等
# ═══════════════════════════════════════════════════════

def _is_weekly_day(now, wcfg):
    return wcfg.get("enabled") and now.weekday() == wcfg.get("day_of_week", 6)


def _is_monthly_day(now, mcfg):
    if not mcfg.get("enabled"):
        return False
    day = mcfg.get("day", -1)
    last = calendar.monthrange(now.year, now.month)[1]
    if day == -1:
        return now.day == last
    return 1 <= day <= last and now.day == day


def _is_yearly_day(now, ycfg):
    return (ycfg.get("enabled") and now.month == ycfg.get("month", 12)
            and now.day == ycfg.get("day", 31))


def _is_period_day(now, cfg):
    """今天是不是某个周期总结日（只看日期，不看小时）"""
    if _is_yearly_day(now, cfg.get("yearly", {})): return True
    if _is_monthly_day(now, cfg.get("monthly", {})): return True
    if _is_weekly_day(now, cfg.get("weekly", {})): return True
    return False


def _also_daily_today(now, pcfg):
    """今天若是周期总结日，返回该类型的 also_daily；都不是返回 True（不拦每日）"""
    for key in ("yearly", "monthly", "weekly"):
        c = pcfg.get(key, {})
        if not c.get("enabled"):
            continue
        hit = _is_yearly_day(now, c) if key == "yearly" else (
              _is_monthly_day(now, c) if key == "monthly" else _is_weekly_day(now, c))
        if hit:
            return c.get("also_daily", False)
    return True


def decide_summary_type(now, config):
    """返回今天此刻应播的总结类型: 'yearly'|'monthly'|'weekly'|'daily'|None。
    优先级 yearly > monthly > weekly > daily。daily 分支被 also_daily=false 整日拦掉。"""
    pcfg = config.get("period_summaries", {})
    if _is_yearly_day(now, pcfg.get("yearly", {})) and now.hour == pcfg["yearly"].get("hour", 20):
        return "yearly"
    if _is_monthly_day(now, pcfg.get("monthly", {})) and now.hour == pcfg["monthly"].get("hour", 20):
        return "monthly"
    if _is_weekly_day(now, pcfg.get("weekly", {})) and now.hour == pcfg["weekly"].get("hour", 20):
        return "weekly"
    if not _also_daily_today(now, pcfg):
        return None
    sched = config.get("broadcast_schedule", {})
    if now.hour == sched.get("weekday_hour" if now.weekday() <= 4 else "weekend_hour", 21):
        return "daily"
    return None


def _marker_path(summary_type, date_str):
    try:
        MARKER_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return MARKER_DIR / f".summary_marker_{summary_type}_{date_str}"


def _cycle_done(summary_type):
    """今天这个类型的总结是否已播过（幂等标记）"""
    return _marker_path(summary_type, datetime.now().strftime("%Y-%m-%d")).exists()


def _mark_cycle_done(summary_type):
    """原子创建今日标记文件（O_EXCL），防手动+定时重复播"""
    try:
        fd = os.open(_marker_path(summary_type, datetime.now().strftime("%Y-%m-%d")),
                     os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, datetime.now().isoformat().encode())
        os.close(fd)
    except OSError:
        pass  # 已存在


def period_start(summary_type, now):
    """周期起始日（date）：本周一 / 本月1日 / 今年1月1日"""
    d = now.date()
    if summary_type == "weekly":
        return d - timedelta(days=d.weekday())
    if summary_type == "monthly":
        return d.replace(day=1)
    if summary_type == "yearly":
        return d.replace(month=1, day=1)
    return d


def date_range(start, end):
    """逐日 [start, end] 的 date 列表"""
    out, d = [], start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


# ═══════════════════════════════════════════════════════
# 大模型引擎（engine.mode = "llm" 时启用）
# ═══════════════════════════════════════════════════════

def _time_period(h):
    if h < 6: return "凌晨"
    if h < 9: return "早上"
    if h < 12: return "上午"
    if h < 14: return "中午"
    if h < 18: return "下午"
    return "晚上"


def generate_with_llm(report, config):
    """用大模型生成播报稿（Anthropic Messages API，纯标准库 urllib）。
    返回句子列表（每句一句、句号结尾）；失败返回 None，上层回退模板。"""
    import re
    llm = config.get("engine", {}).get("llm", {})
    base_url = (llm.get("base_url") or "").rstrip("/")
    model = llm.get("model") or "claude-sonnet-4-5"
    api_key = llm.get("api_key") or ""
    if not base_url:
        print("  ⚠️ LLM 未配置 base_url，回退模板")
        return None

    # 🔑 板块开关：问候语/结束语/鼓励语可关（LLM 模式也遵循）
    _greet_on = ts_on(config, "greeting")
    _end_on = ts_on(config, "ending")
    _enc_on = ts_on(config, "enc")
    if report.get("summary_type"):
        # 周期总结：用周期专用 prompt（按板块开关调整）
        system_prompt = PERIOD_SYSTEM_PROMPT
        if not _greet_on:
            system_prompt = system_prompt.replace(
                "1. 开头用\"这周/这个月/这一年家里……\"的句式，明确这是周期总结，不是每日播报\n",
                "1. 不要问候，直接开始播报周期总结内容\n")
        if _enc_on and _end_on:
            pass  # 默认：鼓励语 + 播报结束
        elif _enc_on:
            system_prompt = system_prompt.replace("4. 结尾给一句温暖的鼓励语，最后一句固定说播报结束\n",
                                                  "4. 结尾给一句温暖的鼓励语\n")
        elif _end_on:
            system_prompt = system_prompt.replace("4. 结尾给一句温暖的鼓励语，最后一句固定说播报结束\n",
                                                  "4. 最后一句固定说播报结束\n")
        else:
            system_prompt = system_prompt.replace("4. 结尾给一句温暖的鼓励语，最后一句固定说播报结束\n",
                                                  "4. 结尾自然收束\n")
        user_msg = (
            f"现在是{report['time']['weekday']}{report['time']['period']}，"
            f"请播报{report['summary_type']}总结。周期数据如下：\n"
            f"{json.dumps(report, ensure_ascii=False)}"
        )
    else:
        req_lines = [
            "1. 语气像家人朋友闲聊，自然亲切，不要说\"数据如下\"\"第一点\"这类书面语，不要念报告",
            "2. 开头按时间段问候一次（如\"中午好\"\"下午好\"），**整篇播报中问候语只能出现一次**，绝对不要重复出现\"中午好\"\"下午好\"等",
            "3. 然后自然带出温度、湿度、用电、安全、空气质量、灯光的情况",
            "4. 异常情况（温度偏高、门窗开着、灯还亮着、用电偏高、设备离线）必须自然提醒并给建议",
            "5. 倒数第二句给一句温暖的鼓励语（可参考数据里的鼓励语素材），最后一句固定说播报结束",
            "6. 全文120到220字，分成5到9句，每句独立成行，句号结尾，不要多余空行",
            "7. 只输出播报稿本身，不要任何解释、前缀、引号或Markdown",
            "8. 适合语音朗读：不要括号、列表序号、表情符号、英文",
        ]
        if not _greet_on:
            req_lines[1] = "2. 不要问候语，直接开始播报内容"
        # 鼓励语/结束语要求按开关动态调整
        if _enc_on and _end_on:
            pass  # 默认不变
        elif _enc_on:
            req_lines[4] = "5. 倒数第二句给一句温暖的鼓励语（可参考数据里的鼓励语素材）"
        elif _end_on:
            req_lines[4] = "5. 最后一句固定说播报结束"
        else:
            req_lines[4] = "5. 不需要鼓励语和结束语"
        system_prompt = ("你是家里的小爱音箱播报助手。根据用户提供的家里实时数据，"
                         "生成一段自然、亲切、口语化的中文播报稿。\n要求：\n"
                         + "\n".join(req_lines) + "\n")
        user_msg = (
            f"现在是{report['time']['weekday']}{report['time']['period']}，请播报家中情况。\n"
            f"家里实时数据如下：\n{json.dumps(report, ensure_ascii=False)}"
        )

    # deepseek 思考模式会先吃大量 token 用于 thinking，max_tokens 太小 → text 被挤空。
    # deepseek 思考模式先吃大量 token 做 thinking，max_tokens 太小 → text 被挤空。
    # 月/年 report 数据量大、thinking 更长，4000 会空文本重试很久；6000 更稳。
    max_tokens = int(llm.get("max_tokens", 6000))

    url = base_url + "/v1/messages"
    headers = {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
    if api_key:
        headers["x-api-key"] = api_key
    body = json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_msg}],
    }).encode()

    # 周期总结数据量大，失败快速回退模板（不值得等几分钟）；
    # 每日播报用户看重 AI 质量，保留长超时 + 多次重试。
    is_period = bool(report.get("summary_type"))
    timeout = 60 if is_period else 120
    retries = 1 if is_period else 3

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read())
            # 过滤 thinking 块，只取 text
            content = data.get("content", [])
            text = "".join(b.get("text", "") for b in content if b.get("type") == "text").strip()
            if not text:
                print(f"  ⚠️ LLM 返回空文本（第{attempt+1}次，thinking 吃满预算），重试...")
                continue
            break
        except Exception as e:
            print(f"  ⚠️ LLM 调用失败（第{attempt+1}次）: {e}")
            continue
    else:
        return None

    # 切成句子：按标点断句；过长的句再按逗号拆。
    # ⚠️ 只丢纯空串，不丢短句——否则"开灯了""温度舒适"等短句会消失导致内容不全
    chunks = re.split(r"[\n。！？!?；;]+", text)
    final = []
    for c in chunks:
        c = c.strip()
        if not c:
            continue
        if len(c) <= 60:
            final.append(c)
        else:
            final.extend(seg for seg in re.split(r"[，,、]+", c) if seg.strip())
    sentences = [s.strip().rstrip("。！？，,!? ") + "。" for s in final]

    # 🔑 问候语去重：整篇只保留第一个问候语（"中午好/下午好"等只出现一次）
    # 重复问候语只去掉问候词前缀，保留句子内容（避免丢信息）
    GREET_RE = re.compile(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*')
    seen_greet = False
    deduped = []
    for s in sentences:
        m = GREET_RE.match(s)
        if m:
            if seen_greet:
                rest = s[m.end():].strip().rstrip("。！？，,!? ")
                if rest:
                    deduped.append(rest + "。")
                continue  # 去掉重复问候前缀，保留内容
            seen_greet = True
        deduped.append(s)
    sentences = deduped

    print(f"  🤖 LLM 生成：{len(text)}字，{len(sentences)}句")
    return sentences


def build_ending(h):
    """按时间段返回播报结束语。模板和 LLM 引擎共用，保证每次都有收尾。
    手动模式：手动结束语(ending_text) > 7天循环结束语(按星期几) > 默认。
    LLM 模式：结束语由大模型生成（ending_llm_days，week_day_text 已读）；这里只兜底默认。"""
    cfg = load_config()
    try:
        ts2 = cfg.get("template_settings") or {}
        if ts2.get("ending_mode") != "llm":
            if ts2.get("ending_mode") == "manual":
                manual_end = (ts2.get("ending_text") or "").strip()
                if manual_end:
                    return manual_end
            # 7天循环结束语：按星期几匹配（循环开关开了才生效，仅手动模式）
            loop_end = _week_loop_match(ts2, "ending", datetime.now().strftime("%Y-%m-%d"))
            if loop_end:
                return loop_end
    except Exception:
        pass
    if h < 12:
        return "以上是今早的家中情况汇总，祝你今天一切顺利。播报结束。"
    elif h < 18:
        return "以上就是目前的家中情况，播报结束。"
    else:
        return "以上就是今晚的家中情况。好了，早点休息吧，播报结束，晚安。"


# ═══════════════════════════════════════════════════════
# 周期总结：HA 历史补齐 + 聚合
# ═══════════════════════════════════════════════════════

def backfill_snapshots(missing_dates, config, snaps):
    """对缺失日期做 HA 补齐（仅最近 ~10 天，受 recorder purge + 传感器存在期限制）。
    只补 temp/power/tasks_done；pm25/lights/memos 无法可靠回溯，标缺省。
    返回补齐天数。"""
    temp_eids = config["temp_sensors"]
    power_cfg = config["power_sensors"]
    filled = 0
    try:
        for d in missing_dates:
            date_str = d.strftime("%Y-%m-%d")
            if d > datetime.now().date() - timedelta(days=10):
                temp = fetch_daily_temperature(temp_eids, TOKEN, d)
                power = fetch_daily_power(power_cfg, TOKEN, d)
                tasks = count_tasks_on(date_str)
                if temp or power:
                    snaps[date_str] = {
                        "date": date_str,
                        "source": "backfill",
                        "temp": temp,
                        "power": {
                            "total_kwh": round(sum(power.values()), 2),
                            "by_device": {k: round(v, 2) for k, v in power.items()},
                        },
                        "tasks_done": tasks,
                    }
                    filled += 1
    except Exception as e:
        print(f"  ⚠️ 历史补齐失败: {e}")
    if filled:
        print(f"  🔁 已从 HA 补齐 {filled} 天数据")
    return filled


def _avg(vals):
    return sum(vals) / len(vals) if vals else None


def aggregate_period(summary_type, now, config):
    """聚合周期统计。返回 period_stats dict（含覆盖度，用于优雅降级）。"""
    start, end = period_start(summary_type, now), now.date()
    days_total = (end - start).days + 1
    snaps = load_snapshots()

    missing = [d for d in date_range(start, end) if d.strftime("%Y-%m-%d") not in snaps]
    backfilled = backfill_snapshots(missing, config, snaps) if missing else 0

    days = [snaps[d.strftime("%Y-%m-%d")] for d in date_range(start, end)
            if d.strftime("%Y-%m-%d") in snaps]
    if not days:
        return {"days_total": days_total, "days_recorded": 0, "backfilled_days": backfilled}

    # ── 温度 ──
    rooms = {}
    all_high, all_low, all_max_high, all_min_low = [], [], [], []
    for s in days:
        for room, t in s.get("temp", {}).items():
            r = rooms.setdefault(room, {"highs": [], "lows": [], "nows": []})
            if "high" in t: r["highs"].append(t["high"])
            if "low" in t: r["lows"].append(t["low"])
            if "now" in t: r["nows"].append(t["now"])
    temp_overall = None
    for room, r in rooms.items():
        if r["highs"]: all_high.extend(r["highs"])
        if r["lows"]: all_low.extend(r["lows"])
    if all_high and all_low:
        temp_overall = {
            "avg_high": _avg(all_high), "avg_low": _avg(all_low),
            "max_high": max(all_high), "min_low": min(all_low),
        }
    temp_by_room = {}
    for room, r in rooms.items():
        temp_by_room[room] = {
            "avg_high": _avg(r["highs"]) if r["highs"] else None,
            "avg_low": _avg(r["lows"]) if r["lows"] else None,
            "avg_now": _avg(r["nows"]) if r["nows"] else None,
        }

    # ── 用电 ──
    kwh_days = [s["power"]["total_kwh"] for s in days if s.get("power", {}).get("total_kwh") is not None]
    by_device = {}
    for s in days:
        for label, k in s.get("power", {}).get("by_device", {}).items():
            by_device[label] = by_device.get(label, 0.0) + k

    # ── 任务 / PM2.5 / 灯光 / 备忘 / 门 / 故障 ──
    tasks_vals = [s.get("tasks_done", 0) for s in days]
    pm25_vals = [s.get("pm25") for s in days if s.get("pm25") is not None]
    pm25_over100 = sum(1 for v in pm25_vals if v > 100)
    light_days = [s for s in days if "lights_all_off" in s]
    all_off_days = sum(1 for s in light_days if s.get("lights_all_off"))
    memos_now = days[-1].get("active_memos", 0)
    doors_days = sum(1 for s in days if s.get("open_doors", 0) > 0)
    faults_days = sum(1 for s in days if s.get("faults", 0) > 0)
    humidity_by_room = {}
    for s in days:
        for room, v in s.get("humidity", {}).items():
            humidity_by_room.setdefault(room, []).append(v)

    return {
        "days_total": days_total,
        "days_recorded": len(days),
        "backfilled_days": backfilled,
        "temp": {"overall": temp_overall, "by_room": temp_by_room},
        "power": {
            "total_kwh": round(sum(kwh_days), 1) if kwh_days else 0.0,
            "avg_daily": round(_avg(kwh_days), 2) if kwh_days else None,
            "days": len(kwh_days),
            "by_device": {k: round(v, 1) for k, v in by_device.items()},
        },
        "tasks": {"total": sum(tasks_vals), "days": len(tasks_vals),
                  "avg_daily": round(_avg(tasks_vals), 1) if tasks_vals else None},
        "pm25": {"avg": round(_avg(pm25_vals), 1) if pm25_vals else None,
                 "days": len(pm25_vals), "days_over_100": pm25_over100},
        "lights": {"days_recorded": len(light_days), "days_all_off": all_off_days},
        "humidity": {r: round(_avg(vs), 1) for r, vs in humidity_by_room.items()},
        "memos_active_now": memos_now,
        "doors_open_days": doors_days,
        "faults_days": faults_days,
    }


# ═══════════════════════════════════════════════════════
# 周期总结文案（模板引擎）
# ═══════════════════════════════════════════════════════

def build_period_text(summary_type, ps, now, config):
    """按周期类型生成模板文案（句子列表）。板块开关：问候语/结束语可关。"""
    period_label = {"weekly": "本周", "monthly": "这个月", "yearly": "这一年"}[summary_type]
    # 🔑 当天有手动/大模型生成的一周问候语缓存 → 整句使用；否则用默认
    lines = []
    if ts_on(config, "greeting"):
        _gcache = week_day_text(config, "greeting", now.strftime("%Y-%m-%d"))
        if _gcache:
            lines = [_gcache]
        else:
            lines = [f"晚上好！{period_label}的家中总结来了。"]

    t = ps["temp"]["overall"]
    if t:
        lines.append(f"{period_label}平均最高温{t['avg_high']:.0f}度，平均最低{t['avg_low']:.0f}度，最热到过{t['max_high']:.0f}度。")

    p = ps["power"]
    if p["days"]:
        lines.append(f"{period_label}一共用电{p['total_kwh']:.1f}度，平均每天{p['avg_daily']:.1f}度。")
        top = sorted(p["by_device"].items(), key=lambda x: -x[1])[:3]
        if top:
            lines.append("耗电最多：" + "，".join(f"{n}{k:.1f}度" for n, k in top) + "。")

    if ps["tasks"]["total"]:
        lines.append(f"{period_label}共完成了{ps['tasks']['total']}个终端任务，平均每天{ps['tasks']['avg_daily']:.1f}个。")

    if ps["pm25"]["days"] and ps["pm25"]["days_over_100"]:
        lines.append(f"有{ps['pm25']['days_over_100']}天空气质量较差，建议多开净化器。")

    if ps["lights"]["days_recorded"]:
        lines.append(f"晚间检查时，{ps['lights']['days_recorded']}天里{ps['lights']['days_all_off']}天主灯是全关的。")

    if ps["doors_open_days"]:
        lines.append(f"有{ps['doors_open_days']}天门窗是开着的，出门记得检查。")

    if ps["days_recorded"] < ps["days_total"]:
        lines.append(f"{period_label}共{ps['days_total']}天，记录了{ps['days_recorded']}天的数据，可能不够完整。")
    if ps["memos_active_now"]:
        lines.append(f"目前还有{ps['memos_active_now']}条备忘待处理。")

    # 结束语：板块开关可关；优先用当天配置的（手动/大模型一周缓存），否则按时间段默认
    if ts_on(config, "ending"):
        _ecache = week_day_text(config, "ending", now.strftime("%Y-%m-%d"))
        lines.append(_ecache if _ecache else build_ending(now.hour))
    return lines


def build_period_report(summary_type, period_stats, now, config, states):
    """构造周期总结的 report（供 LLM 用）"""
    label_map = {"weekly": "本周", "monthly": "本月", "yearly": "今年"}
    start = period_start(summary_type, now)
    return {
        "summary_type": summary_type,
        "period": {
            "label": label_map[summary_type],
            "start": start.strftime("%Y-%m-%d"),
            "end": now.strftime("%Y-%m-%d"),
            "days_total": period_stats["days_total"],
            "days_recorded": period_stats["days_recorded"],
            "backfilled_days": period_stats["backfilled_days"],
        },
        "period_stats": period_stats,
        "time": {"weekday": ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][now.weekday()],
                 "period": _time_period(now.hour)},
    }


# 周期总结专用 system prompt（LLM 引擎）
PERIOD_SYSTEM_PROMPT = (
    "你是家里的小爱音箱播报助手。根据用户提供的周期汇总数据，生成一段自然、亲切、口语化的中文周期总结播报稿。\n"
    "要求：\n"
    "1. 开头用\"这周/这个月/这一年家里……\"的句式，明确这是周期总结，不是每日播报\n"
    "2. 自然带出：温度均值与最热最冷、用电总量与日均及耗电排行、任务完成数、空气与灯光亮点、异常提醒\n"
    "3. 数据里有 days_recorded 和 days_total，如果记录的日期少于总天数，必须诚实带过（如\"这周只记录了几天数据，可能不够完整\"），不许假装完整\n"
    "4. 结尾给一句温暖的鼓励语，最后一句固定说播报结束\n"
    "5. 全文150到280字，分成6到12句，每句独立成行，句号结尾，不要多余空行\n"
    "6. 只输出播报稿本身，不要任何解释、前缀、引号或Markdown\n"
    "7. 适合语音朗读：不要括号、列表序号、表情符号、英文\n"
)


async def main(force=False, text_only=False, summary_type=None, print_report=False):
    import time as _t
    _t0 = _t.time()
    # 🔑 HAOS 加载项：启动时刷新 HA 连接端点（supervisor + SUPERVISOR_TOKEN）
    _refresh_endpoints()
    print(f"  ⏱️ [main] 起点 {_t.time()-_t0:.2f}s")
    config = load_config()
    now = datetime.now()
    wd = now.weekday()
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]

    # ── 调度检查（非强制模式）──
    if not force:
        # 周期总结或每日：decide_summary_type 统一判断（周期 > 每日，also_daily 整日生效）
        summary_type = decide_summary_type(now, config)
        if summary_type is None:
            print(f"  当前 {now.hour}:{now.minute:02d} 非播报时段，跳过")
            return
        if summary_type == "daily" and not config.get("broadcast_schedule", {}).get("enabled", True):
            print("  ⏸️ 定时播报已停用（网页设置可开启），跳过")
            return
        if summary_type != "daily" and _cycle_done(summary_type):
            print(f"  ⏸️ 今天已播过{summary_type}总结，跳过")
            return
    else:
        summary_type = summary_type or "daily"

    # ── Phase 1: 查询温度历史（REST API，在 WS 之前）──
    # 用电量直接读 power_cost_today 实体当前值，不需要历史积分
    write_broadcast_state('preparing', phase='数据上传中', summary_type=summary_type)
    temp_eids = list(config["temp_sensors"].keys())
    temp_history = fetch_history(temp_eids, TOKEN)

    # ── Phase 2: WebSocket 连接 + 获取当前状态 ──
    async with websockets.connect(HA_WS, max_size=4 * 1024 * 1024) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        r = json.loads(await ws.recv())
        if r.get("type") != "auth_ok":
            print("❌ 认证失败")
            return

        await ws.send(json.dumps({"type": "get_states", "id": 1}))
        states = {s["entity_id"]: s for s in json.loads(await ws.recv())["result"]}

        # ═══════════════════════════════════════════
        # 周期总结路径
        # ═══════════════════════════════════════════
        if summary_type in ("weekly", "monthly", "yearly"):
            await run_period_summary(summary_type, now, ws, states, config,
                                     text_only, print_report)
            return

        # ── 有人在家检查（移到采集后：无人天也落盘快照）──
        if not force:
            if not is_present(states, config):
                print("  没人在家，跳过播报")
                return
        else:
            print("  (强制模式)")

        # ═══════════════════════════════════════════
        # 构建播报内容
        # ═══════════════════════════════════════════

        # 根据实际时间判断问候语（词可配置；greeting_mode=llm 用大模型生成的问候）
        h = now.hour
        _gw = ts(config, "greeting_words", {})
        _gmode = ts(config, "greeting_mode", "manual")
        _gllm = ts(config, "greeting_generated", "")
        if _gmode == "llm" and _gllm:
            greeting = _gllm; period = "早上" if h < 12 else ("下午" if h < 18 else "晚上")
        elif h < 6:
            greeting = _gw.get("night", "凌晨好"); period = "凌晨"
        elif h < 9:
            greeting = _gw.get("morning", "早上好"); period = "早上"
        elif h < 12:
            greeting = _gw.get("am", "上午好"); period = "上午"
        elif h < 14:
            greeting = _gw.get("noon", "中午好"); period = "中午"
        elif h < 18:
            greeting = _gw.get("pm", "下午好"); period = "下午"
        else:
            greeting = _gw.get("evening", "晚上好"); period = "晚上"
        day_desc = ts(config, "weekend_desc", "周末愉快") if wd >= 5 else ts(config, "workday_desc", "工作日辛苦了")
        gfmt = ts(config, "greeting_format", "{greeting}！今天是{weekday}，{day_desc}。")
        # 🔑 当天有手动/大模型生成的一周问候语缓存 → 整句使用；否则用格式模板
        # 板块开关：问候语可关（sections.greeting=false 不播问候，直接从板块内容开始）
        lines = []
        if ts_on(config, "greeting"):
            _greet_cached = week_day_text(config, "greeting", now.strftime("%Y-%m-%d"))
            if _greet_cached:
                lines = [_greet_cached]
            else:
                lines = [gfmt.format(greeting=greeting, weekday=weekday_names[wd], day_desc=day_desc)]

        # 📌 检查是否配置了任何传感器实体：全空 → 播报提示"暂未添加传感器"
        _any_sensor = (
            (config.get("temp_sensors") or {})
            or (config.get("humidity_sensors") or {})
            or (config.get("power_sensors") or {})
            or (config.get("lights") or {})
            or (config.get("doors") or {})
            or (config.get("important_devices") or {})
        )
        if not _any_sensor:
            # 全空：直接播"暂未添加传感器"，跳过所有板块生成（避免缺键报错）
            lines.append(ts(config, "text_no_sensor", "暂未添加传感器，请先在传感器配置里添加要播报的设备。"))
            await broadcast_sentences(lines, now, ws, config, text_only, summary_type="daily")
            return

        # 结构化实时数据（LLM 引擎用，规则模板不用）
        report = {
            "time": {"weekday": weekday_names[wd], "period": period, "is_weekend": wd >= 5},
            "temperature": [], "temp_alerts": [],
            "humidity_dry": [], "humidity_wet": [],
            "power": {}, "security": {},
            "pm25": None, "lights_on": [],
            "tasks_done": 0, "todos": [], "memos": [],
            "faults": [], "tip": "",
        }

        # ─────────────────────────────────────────
        # 1. 🏠 温度 + 湿度（合并为一条）
        # ─────────────────────────────────────────
        temp_parts = []
        temp_alerts = []
        if ts_on(config, "temp"):
            temp_high = ts(config, "temp_high_alert", 32)
            temp_diff = ts(config, "temp_constant_diff", 1)
            excl_balcony = ts(config, "temp_exclude_balcony", True)
            for eid, room in config["temp_sensors"].items():
                cur = get_float(states, eid)
                hist = temp_history.get(eid, [])
                if cur is None:
                    continue
                entry = {"room": room, "now": round(cur, 1)}
                if hist:
                    hi, lo = max(hist), min(hist)
                    entry["low"], entry["high"] = round(lo, 1), round(hi, 1)
                    if hi - lo < temp_diff:
                        temp_parts.append(f"{room}全天{cur:.0f}度")
                    else:
                        temp_parts.append(f"{room}当前{cur:.0f}度，全天{lo:.0f}到{hi:.0f}度")
                else:
                    temp_parts.append(f"{room}当前{cur:.0f}度")
                if cur > temp_high and not (excl_balcony and "阳台" in room):
                    temp_alerts.append(f"{room} {cur:.0f}度偏高")
                report["temperature"].append(entry)

        hum_parts = []
        if ts_on(config, "humidity"):
            hums = {}
            for eid, room in config["humidity_sensors"].items():
                v = get_float(states, eid)
                if v is not None and v <= 100:
                    hums[room] = v
            dry_th = ts(config, "humidity_dry", 40)
            wet_th = ts(config, "humidity_wet", 80)
            dry = [r for r, h in hums.items() if h < dry_th]
            wet = [r for r, h in hums.items() if h > wet_th]
            if dry:
                hum_parts.append(f"{'、'.join(dry)}比较干燥，可以开加湿器")
            if wet:
                hum_parts.append(f"{'、'.join(wet)}湿度偏高，注意通风除湿")
            report["humidity_dry"] = dry
            report["humidity_wet"] = wet
        report["temp_alerts"] = temp_alerts

        temp_hum = ""
        if temp_parts:
            temp_hum = ts(config, "text_temp_prefix", "今日温度：") + "，".join(temp_parts)
        if temp_alerts:
            temp_hum += "，注意：" + "，".join(temp_alerts) + "，建议通风降温"
        if hum_parts:
            temp_hum += "，" + "，".join(hum_parts)
        if temp_hum:
            lines.append(temp_hum + "。")

        # ─────────────────────────────────────────
        # 2. ⚡ 用电（直接读今日电量，不推断）
        # ─────────────────────────────────────────
        device_energy = calc_device_energy(states, config["power_sensors"])
        total_kwh = sum(device_energy.values())
        print(f"  ⚡ 今日电量明细: {json.dumps(device_energy, ensure_ascii=False)}")

        power_line = ""
        if ts_on(config, "power") and device_energy:
            top_n = ts(config, "power_top_n", 3)
            top_min = ts(config, "power_top_min", 0.01)
            show_th = ts(config, "power_show_threshold", 0.05)
            save_th = ts(config, "power_save_threshold", 0.1)
            ranked = sorted(device_energy.items(), key=lambda x: -x[1])
            top3 = [(n, k) for n, k in ranked[:top_n] if k > top_min]
            if total_kwh > show_th:
                power_line = f"今日用电共{total_kwh:.1f}度"
                if top3:
                    top_parts = [f"{n}耗电{k:.1f}度" for n, k in top3]
                    power_line += "，排名前" + ("三" if top_n == 3 else str(top_n)) + "：" + "，".join(top_parts)
            elif total_kwh > 0:
                power_line = f"今日用电不到{min(0.1, save_th):.1f}度，非常省电"

            # 高功耗提醒用实时功率传感器（electric_power）
            high_alert = config.get("high_power_alert_w", 200)
            high_devices = []
            for eid, label in config.get("power_now", {}).items():
                v = get_float(states, eid)
                if v is not None and v > high_alert:
                    high_devices.append((label, v))
            if high_devices:
                hp = [f"{n} {p:.0f}瓦" for n, p in high_devices]
                power_line += "，提醒：" + "，".join(hp) + "当前功耗较高，不用时可以关掉"
            report["power"] = {
                "total_kwh": round(total_kwh, 2),
                "top3": [[n, round(k, 2)] for n, k in top3] if top3 else [],
                "high_power": [[n, p] for n, p in high_devices] if high_devices else [],
            }
        else:
            report["power"] = {}
        if power_line:
            lines.append(power_line + "。")

        # ─────────────────────────────────────────
        # 3. 🔒 安全巡检（门窗+钥匙+盖子合并为一条）
        # ─────────────────────────────────────────
        safety_parts = []
        open_doors = []
        door_parts = []
        special_parts = []
        # 没配置门窗传感器 → 整个安全板块跳过（不报"门窗都已关好"这种误报）
        doors_cfg = config.get("doors") or {}
        if ts_on(config, "security") and doors_cfg:
            night_hour = ts(config, "door_night_hour", 18)
            for eid, name in doors_cfg.items():
                if states.get(eid, {}).get("state") == "on":
                    if name.endswith("盖") or name.endswith("盖子"):
                        continue  # 家电盖子后面单独说
                    open_doors.append(name)

            for d in open_doors:
                if d in ("钥匙", "钥匙盒"):
                    special_parts.append("钥匙没放好")
                elif "外卖" in d:
                    special_parts.append("外卖还没取")
                else:
                    door_parts.append(d)

            if door_parts:
                if h >= night_hour:
                    safety_parts.append(f"{'、'.join(door_parts)}还开着，睡前记得关好")
                else:
                    safety_parts.append(f"{'、'.join(door_parts)}还开着")
            if not open_doors and not door_parts:
                safety_parts.append("门窗都已关好，家里很安全")
            if special_parts:
                safety_parts.append("，".join(special_parts))

        # 家电盖子
        open_lids = []
        for eid, name in config.get("doors", {}).items():
            if states.get(eid, {}).get("state") == "on":
                if name.endswith("盖") or name.endswith("盖子"):
                    open_lids.append(name)
        if open_lids:
            safety_parts.append(f"{'、'.join(open_lids)}没盖好，记得盖上")

        report["security"] = {
            "open": door_parts,
            "special": special_parts,
            "lids": open_lids,
            "all_closed": not open_doors and not special_parts,
        }
        if safety_parts:
            lines.append("安全巡检：" + "，".join(safety_parts) + "。")

        # ─────────────────────────────────────────
        # 4. 🌬️ 空气质量
        # ─────────────────────────────────────────
        pm25 = get_float(states, config.get("pm25_sensor", ""))
        report["pm25"] = pm25
        if ts_on(config, "pm25") and pm25 is not None:
            sev = ts(config, "pm25_severe", 100)
            bad = ts(config, "pm25_bad", 50)
            good = ts(config, "pm25_good", 10)
            if pm25 > sev:
                lines.append(f"PM2.5为{pm25:.0f}，严重污染，建议关闭门窗开净化器。")
            elif pm25 > bad:
                lines.append(f"PM2.5为{pm25:.0f}，空气质量差，建议开净化器。")
            elif pm25 < good:
                lines.append(f"PM2.5只有{pm25:.0f}，空气特别好，可以开窗通风。")

        # ─────────────────────────────────────────
        # 5. 💡 灯光
        # ─────────────────────────────────────────
        lights_cfg = config.get("lights") or {}
        lights_on = [n for eid, n in lights_cfg.items()
                     if states.get(eid, {}).get("state") == "on"]
        report["lights_on"] = lights_on
        if ts_on(config, "lights") and lights_cfg:
            if lights_on:
                lines.append(f"{'、'.join(lights_on)}还亮着，不需要的话可以关掉。")
            else:
                lines.append("所有主灯都已关闭，省电又环保。")

        # ─────────────────────────────────────────
        # 6. 💻 终端任务
        # ─────────────────────────────────────────
        task_count = count_today_tasks()
        report["tasks_done"] = task_count
        if ts_on(config, "task") and task_count > 0:
            t_high = ts(config, "task_high", 10)
            t_mid = ts(config, "task_mid", 5)
            if task_count >= t_high:
                lines.append(f"今天完成了{task_count}个终端任务，效率很高。")
            elif task_count >= t_mid:
                lines.append(f"今天完成了{task_count}个终端任务，进度不错。")
            else:
                lines.append(f"今天完成了{task_count}个终端任务。")

        # ─────────────────────────────────────────
        # 7. 📋 待办 + 备忘（合并为一条）
        # ─────────────────────────────────────────
        discovered_todos = {}
        for eid, s in states.items():
            if eid.startswith("todo."):
                discovered_todos[eid] = s["attributes"].get("friendly_name", eid)
        manual_todos = config.get("todo_lists", {})
        all_todos = {**discovered_todos, **manual_todos}

        todo_lines = []
        for idx, (todo_eid, todo_name) in enumerate(all_todos.items()):
            try:
                cid = 300 + idx
                resp = await call_service(ws, "todo", "get_items",
                                          {"entity_id": todo_eid}, None, cid=cid,
                                          return_response=True)
                raw_response = resp.get("result", {}).get("response", {})
                items_data = raw_response.get(todo_eid, {}).get("items", [])
                if not items_data and "items" in raw_response:
                    items_data = raw_response["items"]
                pending = [item["summary"] for item in items_data
                          if item.get("status") == "needs_action"]
                if pending:
                    preview = "，".join(pending[:5])
                    if len(pending) > 5:
                        preview += f"等共{len(pending)}项"
                    todo_lines.append(f"「{todo_name}」有{len(pending)}项：{preview}")
                    report["todos"].append({"list": todo_name, "count": len(pending), "items": pending[:8]})
            except Exception as e:
                print(f"  ⚠️ 读取{todo_name}失败: {e}")

        memos = load_active_memos()
        report["memos"] = [m['text'] for m in memos[:8]]
        if memos:
            memo_texts = [m['text'] for m in memos[:8]]
            if len(memos) > 8:
                memo_texts.append(f"等共{len(memos)}条备忘")
            todo_lines.append(f"手机备忘{len(memos)}条：{'，'.join(memo_texts)}")

        if todo_lines:
            lines.append("待办与备忘：" + "，".join(todo_lines) + "。")

        # ─────────────────────────────────────────
        # 8. ⚙️ 设备故障
        # ─────────────────────────────────────────
        # 🐛 faults 必须无条件初始化——板块开关关掉 fault 时下方 write_daily_snapshot 还会引用它
        faults = []
        if ts_on(config, "fault") and "important_devices" in config:
            from datetime import timedelta
            off_days = ts(config, "fault_offline_days", 3)
            cutoff = now - timedelta(days=off_days)
            for eid, label in config["important_devices"].items():
                s = states.get(eid)
                if s and s.get("state") == "unavailable":
                    # 超过 N 天离线的不提醒
                    lc = s.get("last_changed", "")
                    if lc:
                        try:
                            lc_dt = datetime.fromisoformat(lc)
                            if lc_dt.replace(tzinfo=None) < cutoff:
                                continue
                        except (ValueError, TypeError):
                            pass
                    faults.append(label)
            if faults:
                lines.append(f"有{len(faults)}台设备离线：{'、'.join(faults)}，有空检查一下。")
            report["faults"] = faults

        # ─────────────────────────────────────────
        # 9. 💡 鼓励语 + 小贴士（两个独立开关：enc=鼓励语 / tip=小贴士）
        # ─────────────────────────────────────────
        h = now.hour
        seed = wd * 7 + now.day
        # 鼓励语根据时间选：早上说早安、晚上说晚安。独立开关 sec_enc，不跟小贴士走
        encouragement = pick_time(config.get("encouragements", []), h, seed)
        if ts_on(config, "enc") and encouragement:
            # 🐛 去掉鼓励语里的时间段问候前缀（"早上好/上午好/中午好/下午好/晚上好"），
            # 避免和开头问候语重复（如"下午好...下午好！坚持住"）
            import re as _re
            encouragement = _re.sub(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*', '', encouragement)
            if encouragement:
                lines.append(encouragement)
        # 小贴士：优先用当天配置的（手动/大模型一周缓存），否则按时间段从库选
        tip = week_day_text(config, "tip", now.strftime("%Y-%m-%d"))
        if not tip:
            tip = pick_time(config.get("daily_tips", []), h, seed)
        report["tip"] = tip
        if ts_on(config, "tip") and tip:
            if h < 12:
                lines.append(f"小贴士：{tip}")
            elif h < 18:
                lines.append(f"午后小提示：{tip}")
            else:
                lines.append(f"睡前小提示：{tip}")

        # 结束语：板块开关可关（sections.ending=false 不播结束语）；优先用当天配置的，否则按时间段默认
        if ts_on(config, "ending"):
            _ending = week_day_text(config, "ending", now.strftime("%Y-%m-%d"))
            lines.append(_ending if _ending else build_ending(h))

        # 📸 每日快照落盘（周期总结的数据源，先于播报）
        write_daily_snapshot(now, config, states, temp_history, device_energy,
                             task_count, pm25, lights_on, memos, open_doors, faults)

        # 🐛 全空保护：所有板块+问候+结束语都关了 → 播提示，不播空稿
        if not lines:
            lines.append("当前没有开启任何播报板块，请到模板配置的板块开关里开启。")

        # ═══════════════════════════════════════════
        # 引擎选择：template=规则模板（原方案）/ llm=大模型生成
        # ═══════════════════════════════════════════
        engine_cfg = config.get("engine", {})
        engine_mode = engine_cfg.get("mode", "template")
        if engine_mode == "llm":
            # 把当前时间段的鼓励语素材交给 LLM 参考（鼓励语开关 sec_enc 开了才给）
            report["encouragement_options"] = []
            if ts_on(config, "enc"):
                enc_cfg = config.get("encouragements", {})
                if isinstance(enc_cfg, dict):
                    bucket = enc_cfg.get("morning" if h < 12 else ("afternoon" if h < 18 else "evening"), [])
                else:
                    bucket = enc_cfg if isinstance(enc_cfg, list) else []
                report["encouragement_options"] = bucket[:3]

            print("  🤖 大模型引擎生成播报...")
            write_broadcast_state('preparing', phase='生成播报稿中', summary_type=summary_type)
            # 🔑 大模型也遵循模板配置的板块开关：关掉的板块数据从 report 清空
            apply_section_filters(report, config)
            _t_llm = _t.time()
            llm_sentences = generate_with_llm(report, config)
            print(f"  ⏱️ [main] LLM 调用耗时 {_t.time()-_t_llm:.2f}s")
            if llm_sentences:
                lines = llm_sentences
                # 兜底：LLM 常漏掉鼓励语和结束语，固定补上（避免重复）
                seed = wd * 7 + now.day
                # 结束语去重：稿子里"播报结束/就到这里"类收尾句若不止一句，只保留最后一句
                END_KW = ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报")
                end_idx = [i for i, l in enumerate(lines) if any(k in l for k in END_KW)]
                if len(end_idx) > 1:
                    lines = [l for i, l in enumerate(lines) if i not in end_idx[:-1]]
                # 鼓励语兜底：鼓励语开关(sec_enc)开着、且整个稿子都没出现过鼓励词才补，避免重复
                enc = pick_time(config.get("encouragements", []), h, seed)
                enc_words = ("加油", "辛苦", "晚安", "好梦", "顺利", "感谢", "谢谢", "祝愿", "劳累", "努力", "休息", "美好", "坚持")
                # 🐛 如果稿子已有收尾句（"就到这里/播报结束"等），不补鼓励语——收尾后不该再有内容
                _has_ending = any(kw in "".join(lines) for kw in ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报", "结束"))
                if ts_on(config, "enc") and enc and not any(kw in "".join(lines) for kw in enc_words) and not _has_ending:
                    # 🐛 同样去问候前缀，避免和 LLM 稿子的问候语重复
                    import re as _re2
                    enc = _re2.sub(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*', '', enc)
                    if enc:
                        lines.append(enc)
                # 结束语兜底：板块开关结束语开着、且整个稿子都没有收尾句才补（识别各种收尾形式，避免重复）
                if ts_on(config, "ending") and not any(kw in "".join(lines) for kw in ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报", "结束")):
                    lines.append(build_ending(h))
            elif engine_cfg.get("llm", {}).get("fallback_to_template", True):
                print("  ⚠️ LLM 生成失败，回退到规则模板")
            else:
                print("  ❌ LLM 生成失败且未开启回退，本次不播报")
                return

        # ═══════════════════════════════════════════
        # 播报 + 逐句字幕（共用函数）
        # ═══════════════════════════════════════════
        print(f"  ⏱️ [main] 内容生成完成，总耗时 {_t.time()-_t0:.2f}s，进入播报")
        await broadcast_sentences(lines, now, ws, config, text_only,
                                  summary_type="daily")


async def broadcast_sentences(lines, now, ws, config, text_only, summary_type="daily"):
    """写状态文件 → 逐句播报（text_only 则跳过）→ 存历史。模板和周期共用。"""
    # 🔑 最后一道防线：全局问候去重——任何位置（不只句首）的问候词只保留第一个
    # 之前只匹配句首（^(晚上好)），漏掉 LLM 稿里"周五晚上好了"这种句中问候 → 重复
    GREET_PAT = r'凌晨好|早上好|上午好|中午好|下午好|晚上好'
    _seen_greet = False
    _final = []
    for line in lines:
        if re.search(GREET_PAT, line):
            if _seen_greet:
                # 已有问候：去掉本句所有问候词（"周五晚上好了"→"周五好了"）
                _no = re.sub(GREET_PAT, '', line).strip().rstrip("。！？，,!? ")
                if _no:
                    _final.append(_no + "。")
                continue
            _seen_greet = True
        _final.append(line)
    lines = _final

    # 🔑 最后一道防线：全局结束语去重，任何来源的收尾句只保留最后一句
    # 识别 LLM 常见的各种收尾形式："播报结束" / "就到这里" / "以上就是/以上是" / "本次播报"等
    _END_KW = ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报")
    _end_idx = [i for i, l in enumerate(lines) if any(k in l for k in _END_KW)]
    if len(_end_idx) > 1:
        lines = [l for i, l in enumerate(lines) if i not in _end_idx[:-1]]

    # 🔑 板块开关"播报结束"：勾选后播报以"播报结束"四字结尾（已有该字样则不重复加）
    if ts_on(config, "end_marker") and not any("播报结束" in l for l in lines):
        lines.append("播报结束。")

    clean_lines = []
    for line in lines:
        line = line.strip().rstrip("。！？，,!? ")
        if line:
            clean_lines.append(line)
    full_text = "。".join(clean_lines) + "。"

    # 按 lines 拆分句子（每个 line = 一条 TTS 消息），不做句号切分
    sentences = [line.strip().rstrip("。！？，,!? ") + "。" for line in lines if line.strip()]

    print(f"📝 {full_text}\n")
    print(f"  共 {len(full_text)} 字，{len(sentences)} 句")

    # 🔑 先把全部句子 + played_to=0 写入
    all_sentences = [{
        'text': s,
        'icon': classify_sentence(s),
        'idx': i + 1,
        'total': len(sentences),
    } for i, s in enumerate(sentences)]

    # 🔑 固定 run_id（毫秒级），同一轮播报所有状态共用，避免 UI 误判为新任务
    import time as _time
    run_id = str(int(_time.time() * 1000))

    if text_only:
        # 📝 只看文字：所有句子一次显示，不播报
        write_broadcast_state('done', all_sentences, played_to=len(sentences), run_id=run_id,
                              mode='text', summary_type=summary_type)
        print("✅ 文字生成完成（未播报）")
    else:
        # 🔑 逐句：显字幕 → 念 → 等念完 → 下一句
        write_broadcast_state('broadcasting', all_sentences, played_to=0, run_id=run_id,
                              mode='speech', summary_type=summary_type)
        SPEED = float(config.get("tts_speed", 4.5))

        for i, sentence in enumerate(sentences):
            # 1. 字幕先出来
            write_broadcast_state('broadcasting', all_sentences, played_to=i + 1, run_id=run_id,
                                  mode='speech', summary_type=summary_type)
            # 2. 发给音箱念这句
            await speak(ws, sentence, config, cid=9000 + i)
            # 3. 等这句念完
            wait = len(sentence) / SPEED
            await asyncio.sleep(wait)

        write_broadcast_state('done', all_sentences, played_to=len(sentences), run_id=run_id,
                              mode='speech', summary_type=summary_type)
        print(f"✅ 播报完成")

    save_history(now, sentences, all_sentences, summary_type=summary_type)


async def run_period_summary(summary_type, now, ws, states, config,
                             text_only, print_report):
    """周期总结路径：写今日快照 → 聚合 → 建 report → 引擎生成 → 播报 → 打幂等标记。"""
    config = load_config()

    # 采集今日临时数据（写快照用）
    temp_history = fetch_history(list((config.get("temp_sensors") or {}).keys()), TOKEN)
    device_energy = calc_device_energy(states, config.get("power_sensors") or {})
    lights_on = [n for eid, n in (config.get("lights") or {}).items()
                 if states.get(eid, {}).get("state") == "on"]
    open_doors = [n for eid, n in config.get("doors", {}).items()
                  if states.get(eid, {}).get("state") == "on"
                  and not n.endswith("盖") and not n.endswith("盖子")]
    faults = []
    from datetime import timedelta
    cutoff = now - timedelta(days=3)
    for eid, label in config.get("important_devices", {}).items():
        s = states.get(eid)
        if s and s.get("state") == "unavailable":
            lc = s.get("last_changed", "")
            if lc:
                try:
                    if datetime.fromisoformat(lc).replace(tzinfo=None) < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass
            faults.append(label)

    # 先写今日快照（周期才含今天）
    write_daily_snapshot(now, config, states, temp_history, device_energy,
                         count_today_tasks(), get_float(states, config.get("pm25_sensor", "")),
                         lights_on, load_active_memos(), open_doors, faults)

    # 聚合周期
    period_stats = aggregate_period(summary_type, now, config)

    # 引擎生成
    engine_mode = config.get("engine", {}).get("mode", "template")
    if engine_mode == "llm":
        report = build_period_report(summary_type, period_stats, now, config, states)
        if print_report:
            print("📊 period_report: " + json.dumps(report, ensure_ascii=False, indent=2))
        print("  🤖 大模型引擎生成周期总结...")
        write_broadcast_state('preparing', phase='生成播报稿中', summary_type=summary_type)
        llm_sentences = generate_with_llm(report, config)
        if llm_sentences:
            lines = llm_sentences
            # 周期路径同样做结束语去重/兜底
            END_KW = ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报")
            end_idx = [i for i, l in enumerate(lines) if any(k in l for k in END_KW)]
            if len(end_idx) > 1:
                lines = [l for i, l in enumerate(lines) if i not in end_idx[:-1]]
            # 结束语兜底：结束语开关开着、且整个稿子都没有收尾句才补
            if ts_on(config, "ending") and not any(k in "".join(lines) for k in ("播报结束", "播报完毕", "播报完成", "就到这里", "到此结束", "以上就是", "以上是", "本次播报", "结束")):
                lines.append(build_ending(now.hour))
        elif config.get("engine", {}).get("llm", {}).get("fallback_to_template", True):
            print("  ⚠️ LLM 生成失败，回退到规则模板")
            lines = build_period_text(summary_type, period_stats, now, config)
        else:
            print("  ❌ LLM 生成失败且未开启回退，本次不播报")
            return
    else:
        if print_report:
            print("📊 period_stats: " + json.dumps(period_stats, ensure_ascii=False, indent=2))
        lines = build_period_text(summary_type, period_stats, now, config)

    await broadcast_sentences(lines, now, ws, config, text_only, summary_type=summary_type)
    _mark_cycle_done(summary_type)
    print(f"  ✅ {summary_type}总结已完成并标记")


if __name__ == "__main__":
    import sys
    args = sys.argv
    def _parse_type(a):
        if "--type" in a:
            i = a.index("--type")
            if i + 1 < len(a):
                return a[i + 1]
        return None
    asyncio.run(main(force=("--force" in args),
                     text_only=("--text-only" in args),
                     summary_type=_parse_type(args),
                     print_report=("--print-report" in args)))
