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



def load_config():
    """读 JSON 配置 + 合并加载项 options（options 覆盖 JSON 同名键）。"""
    cfg = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    opt = load_options()
    if not opt:
        return cfg

    # 音箱 / 语速
    # 🔑 配置页保存的音箱（JSON）优先于 options——用户在前端切换音箱应该生效
    # options 只在 JSON 没有时兜底（首次安装场景）
    if not cfg.get("speaker_notify") and opt.get("speaker_notify"):
        cfg["speaker_notify"] = opt["speaker_notify"]
    # tts_speed 已移除：小米 TTS 不支持调语速，改为固定播报节奏（不打断）

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

    # 定时调度已移除（用自动化+实体触发）——broadcast_schedule/period_summaries 由 JSON 配置管理（如有）

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
    # 温度/湿度/实时功率/power 保留 {room, usage}——播报用 room+usage（"客厅冰箱"）避免"客厅10度"歧义
    # 其他段（lights/doors/important_devices）→ {eid: room} 字符串
    _keep_usage = ("temp_sensors", "humidity_sensors", "power_now")
    for _seg in _keep_usage:
        _s = cfg.get(_seg)
        if isinstance(_s, dict):
            for _eid, _val in list(_s.items()):
                if isinstance(_val, dict):
                    _s[_eid] = {"room": _val.get("room", ""), "usage": _val.get("usage", ""),
                                "label": _val.get("label", ""), "muted": _val.get("muted") is True}
    _norm_segs = ("lights", "doors", "important_devices")
    for _seg in _norm_segs:
        _s = cfg.get(_seg)
        if isinstance(_s, dict):
            for _eid, _val in list(_s.items()):
                if isinstance(_val, dict):
                    _s[_eid] = _val.get("room", "")
    # power_sensors 保留 power_type + usage（daily/accumulate/integral，默认 daily）
    # 🐛 usage 必须保留——calc_device_energy 用 room+usage 当 label，同房间多实体（卧室电源1/2）才不会互相覆盖
    _ps = cfg.get("power_sensors")
    if isinstance(_ps, dict):
        for _eid, _val in list(_ps.items()):
            if isinstance(_val, dict):
                _pt = _val.get("power_type", "daily")
                if _pt not in ("daily", "accumulate", "integral"):
                    _pt = "daily"
                _ps[_eid] = {
                    "room": _val.get("room", ""),
                    "usage": _val.get("usage", ""),
                    "label": _val.get("label", ""),
                    "muted": _val.get("muted") is True,
                    "power_type": _pt,
                }
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
            ptype = meta.get("power_type", "daily")
        else:
            ptype = "daily"
        # 🐛 用 room+usage 组合 label，同房间多实体不覆盖
        disp = _power_label(eid, meta)
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
    for eid, meta in temp_sensors.items():
        # 🔑 value 可能是 dict（{room,usage}）或旧字符串——取房间名
        room = meta.get("room", "") if isinstance(meta, dict) else meta
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


def ts_fmt(config, key, default, sub=None):
    """读句式模板（template_settings.formats.<key>）。
    formats.<key> 可以是字符串（旧格式，整条模板）或 dict（新格式，字段级配置）。
    sub 指定取 dict 的哪个字段（prefix/item/alert 等）；非 dict 时 sub 忽略。
    空/缺返回 default。"""
    try:
        v = (config.get("template_settings") or {}).get("formats", {}).get(key)
        if isinstance(v, dict):
            if sub:
                val = v.get(sub)
                if isinstance(val, str) and val.strip():
                    return val.strip()
                return default
            return v
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass
    return default


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
        "faults": "fault", "tip": "tip", "encouragement_options": "enc",
        "power_now": "power_now",
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


def _power_label(eid, meta):
    """用电实体的播报名。优先级：label（用户自定义播报名）> room+usage > room > eid。
    🐛 同房间多个用电实体（卧室电源1/2）必须区分——用 label 或 room+usage 避免互相覆盖漏算。"""
    if isinstance(meta, dict):
        label = (meta.get("label") or "").strip()
        if label:
            return label
        room = (meta.get("room") or "").strip()
        usage = (meta.get("usage") or "").strip()
        if room and usage:
            return f"{room}{usage}"
        if room:
            return room
        if usage:
            return usage
        return eid
    return (meta or eid).strip()


def _sensor_label(eid, meta):
    """温度/湿度等传感器的播报名。优先级：label（用户自定义播报名）> room+usage > room > eid。
    用户可单独填播报名（"冰箱"），或房间+用途（"客厅冰箱"），或只房间/只用途。"""
    if isinstance(meta, dict):
        label = (meta.get("label") or "").strip()
        if label:
            return label
        room = (meta.get("room") or "").strip()
        usage = (meta.get("usage") or "").strip()
        if room and usage:
            return f"{room}{usage}"
        if room:
            return room
        if usage:
            return usage
        return eid
    return (meta or eid).strip()


def _is_muted(meta):
    """🔑 传感器是否被屏蔽（配置条目 muted:true）——屏蔽后不参与播报，但保留在配置里。"""
    if isinstance(meta, dict):
        return meta.get("muted") is True
    return False


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
        if _is_muted(meta):
            continue  # 🔑 被屏蔽的用电传感器不参与
        # 旧格式字符串=daily；新格式 dict 带 power_type
        if isinstance(meta, dict):
            ptype = meta.get("power_type", "daily")
        else:
            ptype = "daily"
        disp = _power_label(eid, meta)

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
    for eid, meta in config["temp_sensors"].items():
        # 🔑 value 可能是 dict（{room,usage}）或旧字符串——统一取房间名
        room = meta.get("room", "") if isinstance(meta, dict) else meta
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
    for eid, meta in config["humidity_sensors"].items():
        room = meta.get("room", "") if isinstance(meta, dict) else meta
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

    # 🔑 大模型配置页可自定义提示语：template_settings.llm.daily_prompt / period_prompt（留空用默认）
    _ts_llm = (config.get("template_settings") or {}).get("llm") or {}
    _daily_prompt = (_ts_llm.get("daily_prompt") or "").strip()
    _period_prompt = (_ts_llm.get("period_prompt") or "").strip()
    _max_tokens_cfg = int(_ts_llm.get("max_tokens") or 0)
    if _max_tokens_cfg > 0:
        llm["max_tokens"] = _max_tokens_cfg

    # 🔑 板块开关：问候语/结束语/鼓励语可关（LLM 模式也遵循）
    _greet_on = ts_on(config, "greeting")
    _end_on = ts_on(config, "ending")
    _enc_on = ts_on(config, "enc")
    if report.get("summary_type"):
        # 周期总结：用周期专用 prompt（可用配置覆盖；按板块开关调整）
        system_prompt = _period_prompt or PERIOD_SYSTEM_PROMPT
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
            "3. 按数据出现的顺序自然带出各板块（温度、湿度、耗电量、实时功率、安全、空气质量、灯光、任务、待办、设备故障），每块说一句",
            "4. 设备名直接用数据里的\"客厅冰箱\"\"卧室空调\"这类房间+用途，不要只说笼统的房间名",
            "5. 异常情况（温度偏高/偏低、湿度干燥/过湿、功率过高、门窗没关、灯亮着、设备离线）自然提醒并给建议；多个房间同时异常要合并说（如\"卧室、厨房温度偏高，建议开空调\"），不要一个一个重复建议",
            "6. 倒数第二句给一句温暖的鼓励语（可参考数据里的鼓励语素材），最后一句固定说播报结束",
            "7. 全文120到220字，分成5到9句，每句独立成行，句号结尾，不要多余空行",
            "8. 只输出播报稿本身，不要任何解释、前缀、引号或Markdown",
            "9. 适合语音朗读：不要括号、列表序号、表情符号、英文",
        ]
        if not _greet_on:
            req_lines[1] = "2. 不要问候语，直接开始播报内容"
        # 鼓励语/结束语要求按开关动态调整（现在是第6条，索引5）
        if _enc_on and _end_on:
            pass  # 默认不变
        elif _enc_on:
            req_lines[5] = "6. 倒数第二句给一句温暖的鼓励语（可参考数据里的鼓励语素材）"
        elif _end_on:
            req_lines[5] = "6. 最后一句固定说播报结束"
        else:
            req_lines[5] = "6. 不需要鼓励语和结束语"
        # 🔑 用户自定义每日提示语优先；否则用默认+板块开关调整
        if _daily_prompt:
            system_prompt = _daily_prompt
        else:
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


def _ending_time_ok(text, h):
    """🔑 结束语缓存的时间词校验：文案带夜间词但当前不是晚上（或反之），则返回 False（改用当前时段默认）。
    大模型提前缓存的一周结束语可能写"夜色渐浓/晚安"（夜里生成的），早上播就明显不对。"""
    NIGHT_WORDS = ("夜色", "夜晚", "黄昏", "晚安", "好梦", "睡觉", "入眠", "梦乡", "夜深", "安睡")
    MORNING_WORDS = ("早晨", "早安", "晨曦", "清晨")
    if not text:
        return True
    is_night = h >= 18 or h < 6
    is_morning = 6 <= h < 12
    if any(w in text for w in NIGHT_WORDS) and not is_night:
        return False
    if any(w in text for w in MORNING_WORDS) and not is_morning:
        return False
    return True


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
        lines.append(f"{period_label}共耗电{p['total_kwh']:.1f}度，平均每天{p['avg_daily']:.1f}度。")
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
        # 🔑 结束语时间词校验：缓存带夜间词但当前不是晚上 → 用当前时段默认（避免早上播"夜色渐浓"）
        if _ecache and not _ending_time_ok(_ecache, now.hour):
            _ecache = ""
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

    # 定时调度已移除（用自动化+实体触发）。播报类型由触发方指定（force=手动/按钮/自动化）
    summary_type = summary_type or "daily"
    if not force and summary_type != "daily" and _cycle_done(summary_type):
        print(f"  ⏸️ 今天已播过{summary_type}总结，跳过")
        return

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

        # 根据实际时间判断问候语（词可配置）
        # 🐛 去掉旧的 greeting_generated（v1.1.53 单条遗留）——llm 模式的问候语来源是 greeting_llm_days（一周缓存）
        # 时段词（早上好/中午好/晚上好）始终由当前时间决定，缓存内容只负责日期/天气/内容
        h = now.hour
        _gw = ts(config, "greeting_words", {})
        if h < 6:
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
                # 🐛 缓存问候语可能带固定时段（"周六早上好"/"周日的清晨"/"周五的黄昏"），任意时段播都该换当前时段
                # ⚠️ 时段词可能在句中，必须 re.search 任意位置；变体（清晨/黄昏/午后/夜晚等）也要处理
                _PERIOD_PAT = r'早上好|上午好|中午好|下午好|晚上好|凌晨好|清晨|早晨|晨曦|午后|黄昏|傍晚|夜晚|夜幕|深夜|清晨'
                if re.search(_PERIOD_PAT, _greet_cached):
                    # 去掉第一个时段词，保留后面的标点（"周日的清晨，阳光"→"周日，阳光"，不吞逗号）
                    _g2 = re.sub(_PERIOD_PAT, '', _greet_cached, count=1).strip()
                    _g2 = re.sub(r'[！!]+', '，', _g2)  # "周六！周末"→"周六，周末"
                    _g2 = re.sub(r'^[，,。\s]+', '', _g2).strip()
                    # 🐛 "周日的清晨"去时段后剩"周日的，阳光"/"周五的格外温柔"——统一把"周X的"还原成"周X"
                    _g2 = re.sub(r'(周一|周二|周三|周四|周五|周六|周日)的', r'\1', _g2)
                    lines = [f"{greeting}，{_g2}"] if _g2 else [greeting]
                else:
                    # 缓存没有时段词（新生成的不带时段）→ 直接加当前时段
                    lines = [f"{greeting}，{_greet_cached}"]
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
            "power": {}, "power_now": {}, "security": {},
            "pm25": None, "lights_on": [],
            "tasks_done": 0, "todos": [], "memos": [],
            "faults": [], "tip": "",
        }

        # ─────────────────────────────────────────
        # 1. 🌡️ 温度（单独一条）
        # ─────────────────────────────────────────
        temp_parts = []
        temp_alerts = []
        temp_alerts_low = []
        temp_entries = []
        if ts_on(config, "temp"):
            temp_high = ts(config, "temp_high_alert", 32)
            temp_low = ts(config, "temp_low_alert", 10)
            temp_diff = ts(config, "temp_constant_diff", 1)
            # 🔑 不参与温度报警的房间（可多个，空格分隔）
            excl_rooms = [r.strip() for r in re.split(r'\s+', ts(config, "temp_exclude_rooms", "")) if r.strip()]
            for eid, meta in config["temp_sensors"].items():
                if _is_muted(meta):
                    continue  # 🔑 被屏蔽的传感器不参与播报
                room = meta.get("room", "") if isinstance(meta, dict) else meta
                label = _sensor_label(eid, meta)  # room+usage（客厅冰箱）
                cur = get_float(states, eid)
                hist = temp_history.get(eid, [])
                if cur is None:
                    continue
                entry = {"room": room, "label": label, "now": round(cur, 1)}
                hi = lo = None
                if hist:
                    hi, lo = max(hist), min(hist)
                    entry["low"], entry["high"] = round(lo, 1), round(hi, 1)
                    if hi - lo < temp_diff:
                        temp_parts.append(f"{label}全天{cur:.0f}度")
                    else:
                        temp_parts.append(f"{label}当前{cur:.0f}度，全天{lo:.0f}到{hi:.0f}度")
                else:
                    temp_parts.append(f"{label}当前{cur:.0f}度")
                # 排除的房间不参与报警（用房间名判断）
                if any(k in room for k in excl_rooms):
                    pass
                elif cur > temp_high:
                    temp_alerts.append((label, cur))
                elif cur < temp_low:
                    temp_alerts_low.append((label, cur))
                temp_entries.append(entry)
                report["temperature"].append(entry)
        report["temp_alerts"] = temp_alerts

        if temp_parts:
            # 🔑 字段级句式：prefix=前缀 item=每条内容 alert=高温提醒（dict）；旧字符串格式整条模板兼容
            _tp = ts_fmt(config, "temp", None)
            if isinstance(_tp, dict):
                _t_prefix = ts_fmt(config, "temp", "温度：", "prefix")
                _t_item = ts_fmt(config, "temp", "", "item")
                _t_alert = ts_fmt(config, "temp", "", "alert")
                if _t_item:
                    # 每条用用户模板：{room}=房间 {label}=房间+用途 {now} {low} {high}
                    _t_items = "，".join(
                        _t_item.replace("{room}", e.get("room", "")).replace("{label}", e.get("label", e.get("room", "")))
                                .replace("{now}", f"{e['now']:.0f}")
                                .replace("{low}", f"{e.get('low', 0):.0f}" if e.get('low') is not None else "")
                                .replace("{high}", f"{e.get('high', 0):.0f}" if e.get('high') is not None else "")
                        for e in temp_entries)
                else:
                    _t_items = "，".join(temp_parts)
                _t_extra = ""
                if temp_alerts:
                    # 🐛 高温提醒合并：卧室厨房、客厅温度过高，建议通风降温（不逐房间重复建议）
                    _t_rooms = "、".join(r for r, _ in temp_alerts)
                    if _t_alert:
                        _t_extra = "，" + "、".join(
                            _t_alert.replace("{room}", r).replace("{now}", f"{c:.0f}") for r, c in temp_alerts)
                        if "{rooms}" in _t_alert:
                            _t_extra = "，" + _t_alert.replace("{rooms}", _t_rooms).replace("{now}", "")
                    else:
                        _t_extra = f"，注意：{_t_rooms}温度过高，建议通风降温"
                # 🔑 低温提醒（合并 + 建议一次）
                if temp_alerts_low:
                    _t_low_rooms = "、".join(r for r, _ in temp_alerts_low)
                    _t_low_alert = ts_fmt(config, "temp", "", "alert_low")
                    if _t_low_alert:
                        _t_extra += "，" + _t_low_alert.replace("{rooms}", _t_low_rooms)
                    else:
                        _t_extra += f"，注意：{_t_low_rooms}温度偏低，建议注意保暖"
                lines.append(_t_prefix + _t_items + _t_extra + "。")
            else:
                # 旧格式字符串模板（整条）兼容
                _t_tpl = _tp or "温度：{items}{extra}"
                _t_extra = ""
                if temp_alerts:
                    _t_rooms = "、".join(r for r, _ in temp_alerts)
                    _t_extra = f"，注意：{_t_rooms}温度过高，建议通风降温"
                if temp_alerts_low:
                    _t_low_rooms = "、".join(r for r, _ in temp_alerts_low)
                    _t_extra += f"，注意：{_t_low_rooms}温度偏低，建议注意保暖"
                lines.append(_t_tpl.replace("{items}", "，".join(temp_parts)).replace("{extra}", _t_extra) + "。")

        # 2. 💧 湿度（单独一条）
        # ─────────────────────────────────────────
        hum_line = ""
        if ts_on(config, "humidity"):
            hums = {}  # label(room+usage) → value
            hum_rooms = {}  # label → 房间名（用于排除判断）
            for eid, meta in config["humidity_sensors"].items():
                if _is_muted(meta):
                    continue  # 🔑 被屏蔽的传感器不参与播报
                room = meta.get("room", "") if isinstance(meta, dict) else meta
                label = _sensor_label(eid, meta)
                v = get_float(states, eid)
                if v is not None and v <= 100:
                    hums[label] = v
                    hum_rooms[label] = room
            dry_th = ts(config, "humidity_dry", 40)
            wet_th = ts(config, "humidity_wet", 80)
            # 🔑 不参与湿度报警的房间（卫生间等湿度常高不算异常）
            hum_excl = [r.strip() for r in re.split(r'\s+', ts(config, "humidity_exclude_rooms", "")) if r.strip()]
            dry = [lb for lb, h in hums.items() if h < dry_th and not any(k in hum_rooms[lb] for k in hum_excl)]
            wet = [lb for lb, h in hums.items() if h > wet_th and not any(k in hum_rooms[lb] for k in hum_excl)]
            report["humidity_dry"] = dry
            report["humidity_wet"] = wet
            if hums:
                # 播报每个房间湿度值，如"湿度：客厅50%，卧室45%"
                _hp = ts_fmt(config, "humidity", None)
                if isinstance(_hp, dict):
                    _h_prefix = ts_fmt(config, "humidity", "湿度：", "prefix")
                    _h_item = ts_fmt(config, "humidity", "", "item")
                    _h_dry = ts_fmt(config, "humidity", "", "dry")
                    _h_wet = ts_fmt(config, "humidity", "", "wet")
                    if _h_item:
                        _h_items = "，".join(_h_item.replace("{room}", r).replace("{label}", r).replace("{hum}", f"{h:.0f}") for r, h in hums.items())
                    else:
                        _h_items = "，".join(f"{r}{h:.0f}%" for r, h in hums.items())
                    _h_extra = ""
                    _dry_rooms = "、".join(dry)
                    _wet_rooms = "、".join(wet)
                    if dry and _h_dry:
                        if "{rooms}" in _h_dry:
                            _h_extra += "，" + _h_dry.replace("{rooms}", _dry_rooms)
                        else:
                            _h_extra += "，" + "、".join(_h_dry.replace("{room}", r) for r in dry)
                    elif dry:
                        _h_extra += f"，{_dry_rooms}比较干燥，建议开加湿器"
                    if wet and _h_wet:
                        if "{rooms}" in _h_wet:
                            _h_extra += "，" + _h_wet.replace("{rooms}", _wet_rooms)
                        else:
                            _h_extra += "，" + "、".join(_h_wet.replace("{room}", r) for r in wet)
                    elif wet:
                        _h_extra += f"，{_wet_rooms}湿度偏高，建议通风除湿"
                    hum_line = _h_prefix + _h_items + _h_extra
                else:
                    _hum_tpl = _hp or "湿度：{items}{extra}"
                    _hum_items = "，".join(f"{r}{h:.0f}%" for r, h in hums.items())
                    _hum_extra = ""
                    if dry:
                        _hum_extra += f"，{'、'.join(dry)}比较干燥，建议开加湿器"
                    if wet:
                        _hum_extra += f"，{'、'.join(wet)}湿度偏高，建议通风除湿"
                    hum_line = _hum_tpl.replace("{items}", _hum_items).replace("{extra}", _hum_extra)
            elif dry or wet:
                hum_line = "，" + "，".join((["、".join(dry) + "比较干燥"] if dry else []) + (["、".join(wet) + "湿度偏高"] if wet else []))
            if hum_line:
                lines.append(hum_line + "。")

        # ─────────────────────────────────────────
        # 2. ⚡ 用电（直接读今日电量，不推断）
        # ─────────────────────────────────────────
        device_energy = calc_device_energy(states, config["power_sensors"])
        total_kwh = sum(device_energy.values())
        print(f"  ⚡ 今日电量明细: {json.dumps(device_energy, ensure_ascii=False)}")

        # 🔑 统计"读不到的用电设备"（unavailable/unknown 被跳过 → 总量偏小）——提示用户，避免"不准还不自知"
        unread = []
        for _eid, _meta in (config.get("power_sensors") or {}).items():
            if _eid.startswith("_"):
                continue
            if _is_muted(_meta):
                continue  # 🔑 被屏蔽的用电设备不算"读不到"
            _label = _power_label(_eid, _meta)
            if _label and _label not in device_energy:
                unread.append(_label)

        power_line = ""
        if ts_on(config, "power") and device_energy:
            top_n = ts(config, "power_top_n", 3)
            top_min = ts(config, "power_top_min", 0.01)
            show_th = ts(config, "power_show_threshold", 0.05)
            save_th = ts(config, "power_save_threshold", 0.1)
            ranked = sorted(device_energy.items(), key=lambda x: -x[1])
            top3 = [(n, k) for n, k in ranked[:top_n] if k > top_min]
            if total_kwh > show_th:
                _pp = ts_fmt(config, "power", None)
                if isinstance(_pp, dict):
                    _p_prefix = ts_fmt(config, "power", "耗电量：共{total}度", "prefix")
                    _p_top_item = ts_fmt(config, "power", "", "top_item")
                    _p_top_phrase = ts_fmt(config, "power", "耗电前{num}：{list}", "top")
                    _p_unread = ts_fmt(config, "power", "", "unread")
                    _pow_top = ""
                    if top3:
                        _top_list = "，".join(_p_top_item.replace("{device}", n).replace("{kwh}", f"{k:.1f}") for n, k in top3) if _p_top_item else "，".join(f"{n}耗电{k:.1f}度" for n, k in top3)
                        _p_phr = _p_top_phrase.replace("{num}", "三" if top_n == 3 else str(top_n))
                        if "{list}" in _p_phr:
                            _pow_top = "，" + _p_phr.replace("{list}", _top_list)
                        else:
                            _pow_top = "，" + _p_phr + "：" + _top_list
                    _pow_unread = ""
                    if unread and _p_unread:
                        _pow_unread = "，" + _p_unread.replace("{count}", str(len(unread))).replace("{items}", "、".join(unread[:4]))
                    elif unread:
                        _pow_unread = f"，另有{len(unread)}个用电设备读不到数据（{'、'.join(unread[:4])}" + ("等" if len(unread) > 4 else "") + "），未计入"
                    power_line = _p_prefix.replace("{total}", f"{total_kwh:.1f}") + _pow_top + _pow_unread
                else:
                    _pow_tpl = _pp or "耗电量：共{total}度{top}{unread}"
                    _pow_top = ""
                    if top3:
                        _pow_top = "，耗电前" + ("三" if top_n == 3 else str(top_n)) + "：" + "，".join(f"{n}耗电{k:.1f}度" for n, k in top3)
                    _pow_unread = ""
                    if unread:
                        _pow_unread = f"，另有{len(unread)}个用电设备读不到数据（{'、'.join(unread[:4])}" + ("等" if len(unread) > 4 else "") + "），未计入"
                    power_line = _pow_tpl.replace("{total}", f"{total_kwh:.1f}").replace("{top}", _pow_top).replace("{unread}", _pow_unread)
            elif total_kwh > 0:
                power_line = f"耗电量：不到{min(0.1, save_th):.1f}度，非常省电"
            report["power"] = {
                "total_kwh": round(total_kwh, 2),
                "top3": [[n, round(k, 2)] for n, k in top3] if top3 else [],
            }
        else:
            report["power"] = {}
        if power_line:
            lines.append(power_line + "。")

        # ─────────────────────────────────────────
        # 2.5 ⚡ 实时功率（独立一条 + 独立板块开关 power_now）
        # ─────────────────────────────────────────
        power_now_line = ""
        if ts_on(config, "power_now"):
            # 🔑 实时功率：按瓦数从大到小排序 + 限条数（power_now_top_n 可配，默认 3）
            now_top_n = int(ts(config, "power_now_top_n", 3))
            now_pairs = [(_power_label(eid, meta), get_float(states, eid))
                         for eid, meta in config.get("power_now", {}).items()
                         if not eid.startswith("_") and not _is_muted(meta)]  # 🔑 跳过屏蔽的
            now_pairs = [(n, w) for n, w in now_pairs if w is not None and w > 0]
            now_pairs.sort(key=lambda x: -x[1])  # 瓦数大的在前
            now_total = sum(w for _, w in now_pairs)
            now_top = now_pairs[:now_top_n]
            now_parts = [f"{n} {w:.0f}瓦" for n, w in now_top]
            high_alert = config.get("high_power_alert_w", 200)
            high_devices = [pair for pair in now_pairs if pair[1] > high_alert]
            if now_pairs:
                _np = ts_fmt(config, "power_now", None)
                if isinstance(_np, dict):
                    # 🔑 实时功率前缀含 {total}（共多少瓦），排名条数由 power_now_top_n 决定
                    _n_prefix = ts_fmt(config, "power_now", "实时功率：共{total}瓦", "prefix")
                    _n_item = ts_fmt(config, "power_now", "", "item")
                    _n_top = ts_fmt(config, "power_now", "耗电前{num}：{list}", "top")
                    _n_alert = ts_fmt(config, "power_now", "", "alert")
                    if _n_item:
                        _n_items = "，".join(_n_item.replace("{device}", n).replace("{w}", f"{w:.0f}") for n, w in now_top)
                    else:
                        _n_items = "，".join(f"{n} {w:.0f}瓦" for n, w in now_top)
                    _n_top_phrase = ""
                    if now_top:
                        _n_list = "，".join(f"{n} {w:.0f}瓦" for n, w in now_top)
                        _n_phr = _n_top.replace("{num}", "三" if now_top_n == 3 else str(now_top_n))
                        if "{list}" in _n_phr:
                            _n_top_phrase = "，" + _n_phr.replace("{list}", _n_list)
                        else:
                            _n_top_phrase = "，" + _n_phr + "：" + _n_list
                    _n_extra = ""
                    _dev_rooms = "、".join(n for n, _ in high_devices)
                    if high_devices:
                        if _n_alert:
                            if "{devices}" in _n_alert:
                                _n_extra = "，" + _n_alert.replace("{devices}", _dev_rooms)
                            else:
                                _n_extra = "，" + "、".join(_n_alert.replace("{device}", n).replace("{w}", f"{p:.0f}") for n, p in high_devices)
                        else:
                            _n_extra = f"，注意：{_dev_rooms}功率较高，不用时可以关掉"
                    power_now_line = _n_prefix.replace("{total}", f"{now_total:.0f}") + _n_top_phrase + _n_extra
                else:
                    # 🔑 默认格式：实时功率共{total}瓦，耗电前{num}：设备 瓦
                    _now_tpl = _np or "实时功率共{total}瓦，耗电前{num}：{list}{extra}"
                    _now_list = "，".join(f"{n} {w:.0f}瓦" for n, w in now_top)
                    _now_extra = ""
                    if high_devices:
                        _dev_rooms = "、".join(n for n, _ in high_devices)
                        _now_extra = f"，注意：{_dev_rooms}功率较高，不用时可以关掉"
                    power_now_line = _now_tpl.replace("{total}", f"{now_total:.0f}").replace("{num}", "三" if now_top_n == 3 else str(now_top_n)).replace("{list}", _now_list).replace("{extra}", _now_extra)
            report["power_now"] = {"now": [[n, p] for n, p in high_devices] if high_devices else [],
                                   "total_w": round(now_total, 0), "top": [[n, w] for n, w in now_top]}
        if power_now_line:
            lines.append(power_now_line + "。")

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
            _sec_tpl = ts_fmt(config, "security", "安全巡检：{items}")
            lines.append(_sec_tpl.replace("{items}", "，".join(safety_parts)) + "。")

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
                _pm_desc = f"PM2.5为{pm25:.0f}，严重污染，建议关闭门窗开净化器"
            elif pm25 > bad:
                _pm_desc = f"PM2.5为{pm25:.0f}，建议开净化器"
            elif pm25 < good:
                _pm_desc = f"PM2.5只有{pm25:.0f}，空气特别好，可以开窗通风"
            else:
                _pm_desc = None
            if _pm_desc:
                _pm_tpl = ts_fmt(config, "pm25", "空气质量：{pm}")
                lines.append(_pm_tpl.replace("{pm}", _pm_desc) + "。")

        # ─────────────────────────────────────────
        # 5. 💡 灯光
        # ─────────────────────────────────────────
        lights_cfg = config.get("lights") or {}
        lights_on = [n for eid, n in lights_cfg.items()
                     if states.get(eid, {}).get("state") == "on"]
        report["lights_on"] = lights_on
        if ts_on(config, "lights") and lights_cfg:
            if lights_on:
                _lp = ts_fmt(config, "lights", None)
                if isinstance(_lp, dict):
                    _l_prefix = ts_fmt(config, "lights", "灯光：", "prefix")
                    _l_item = ts_fmt(config, "lights", "", "item")
                    _l_suffix = ts_fmt(config, "lights", "", "suffix")
                    _l_items = "、".join(_l_item.replace("{name}", n) for n in lights_on) if _l_item else "、".join(lights_on)
                    _l_suf = _l_suffix or "还亮着，不需要的话可以关掉"
                    lines.append(_l_prefix + _l_items + _l_suf + "。")
                else:
                    _li_tpl = _lp or "灯光：{items}还亮着，不需要的话可以关掉"
                    lines.append(_li_tpl.replace("{items}", "、".join(lights_on)) + "。")
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
                _task_extra = "，效率很高"
            elif task_count >= t_mid:
                _task_extra = "，进度不错"
            else:
                _task_extra = ""
            _task_tpl = ts_fmt(config, "task", "任务：完成{count}个终端任务{extra}")
            lines.append(_task_tpl.replace("{count}", str(task_count)).replace("{extra}", _task_extra) + "。")

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
            _todo_tpl = ts_fmt(config, "todo", "待办与备忘：{items}")
            lines.append(_todo_tpl.replace("{items}", "，".join(todo_lines)) + "。")

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
                _fault_tpl = ts_fmt(config, "fault", "有{count}台设备离线：{items}，有空检查一下")
                lines.append(_fault_tpl.replace("{count}", str(len(faults))).replace("{items}", "、".join(faults)) + "。")
            report["faults"] = faults

        # ─────────────────────────────────────────
        # 9. 💡 小贴士（tip）→ 鼓励语（enc，倒数第2）→ 结束语（倒数第1）
        # ─────────────────────────────────────────
        h = now.hour
        seed = wd * 7 + now.day
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
        # 鼓励语：优先用当天配置的（手动/大模型一周缓存），否则按时间段从库选。独立开关 sec_enc
        encouragement = week_day_text(config, "enc", now.strftime("%Y-%m-%d"))
        if not encouragement:
            encouragement = pick_time(config.get("encouragements", []), h, seed)
        if ts_on(config, "enc") and encouragement:
            # 🐛 去掉鼓励语里的时间段问候前缀（"早上好/上午好/中午好/下午好/晚上好"），
            # 避免和开头问候语重复（如"下午好...下午好！坚持住"）
            import re as _re
            encouragement = _re.sub(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*', '', encouragement)
            if encouragement:
                lines.append(encouragement)

        # 结束语：板块开关可关（sections.ending=false 不播结束语）；优先用当天配置的，否则按时间段默认
        if ts_on(config, "ending"):
            _ending = week_day_text(config, "ending", now.strftime("%Y-%m-%d"))
            # 🔑 结束语时间词校验（同每日路径）
            if _ending and not _ending_time_ok(_ending, h):
                _ending = ""
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
                enc = week_day_text(config, "enc", now.strftime("%Y-%m-%d"))
                if not enc:
                    enc = pick_time(config.get("encouragements", []), h, seed)
                enc_words = ("加油", "辛苦", "晚安", "好梦", "顺利", "感谢", "谢谢", "祝愿", "劳累", "努力", "休息", "美好", "坚持")
                if ts_on(config, "enc") and enc and not any(kw in "".join(lines) for kw in enc_words):
                    # 🐛 同样去问候前缀，避免和 LLM 稿子的问候语重复
                    import re as _re2
                    enc = _re2.sub(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*', '', enc)
                    if enc:
                        # 🔑 鼓励语永远在结束语（收尾句）之前：有收尾句插入其前，无收尾句追加末尾
                        _end_pos = [i for i, l in enumerate(lines) if any(k in l for k in END_KW)]
                        if _end_pos:
                            lines.insert(_end_pos[-1], enc)
                        else:
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
        # 句间停顿可配（播报页 ⏸️ 下拉，存 template_settings.tts_pause，默认 0.5s）：
        # 每句等待 = 字数/4.5 + 停顿，下限 = 0.8s + 停顿——保证上一句念完才播下一句，不打断
        _pause = 0.5
        try:
            _pause = float((config.get("template_settings") or {}).get("tts_pause", 0.5))
        except Exception:
            pass
        for i, sentence in enumerate(sentences):
            # 1. 字幕先出来
            write_broadcast_state('broadcasting', all_sentences, played_to=i + 1, run_id=run_id,
                                  mode='speech', summary_type=summary_type)
            # 2. 发给音箱念这句
            await speak(ws, sentence, config, cid=9000 + i)
            # 3. 等这句念完（下限跟随停顿，不打断）
            wait = max(len(sentence) / 4.5 + _pause, 0.8 + _pause)
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
            # 鼓励语兜底：周期稿缺鼓励词、enc 开 → 在收尾句前补（鼓励语永远在结束语前）
            _penc = week_day_text(config, "enc", now.strftime("%Y-%m-%d"))
            if not _penc:
                _penc = pick_time(config.get("encouragements", []), now.hour, now.weekday() * 7 + now.day)
            _enc_words = ("加油", "辛苦", "晚安", "好梦", "顺利", "感谢", "谢谢", "祝愿", "劳累", "努力", "休息", "美好", "坚持")
            if ts_on(config, "enc") and _penc and not any(k in "".join(lines) for k in _enc_words):
                import re as _re3
                _penc = _re3.sub(r'^(凌晨好|早上好|上午好|中午好|下午好|晚上好)[！!，,。\s]*', '', _penc)
                if _penc:
                    _pe_pos = [i for i, l in enumerate(lines) if any(k in l for k in END_KW)]
                    if _pe_pos:
                        lines.insert(_pe_pos[-1], _penc)
                    else:
                        lines.append(_penc)
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
