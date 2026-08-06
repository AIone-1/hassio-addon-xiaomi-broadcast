#!/bin/bash
set -e

DATA_DIR="/share/xiaomi_broadcast"
mkdir -p "$DATA_DIR" /data/markers

# 首次启动：把默认配置种子到用户可编辑目录（Samba 可访问）
if [ ! -f "$DATA_DIR/ha_daily_config.json" ]; then
  cp /usr/local/bin/ha_daily_config.default.json "$DATA_DIR/ha_daily_config.json"
  echo "[init] 已生成默认配置: $DATA_DIR/ha_daily_config.json"
fi

# 确保数据文件存在
[ -f "$DATA_DIR/.terminal_tasks.log" ] || touch "$DATA_DIR/.terminal_tasks.log" 2>/dev/null || true
[ -f "$DATA_DIR/memos.json" ] || echo "[]" > "$DATA_DIR/memos.json"
[ -f "$DATA_DIR/daily_stats_history.json" ] || echo "{}" > "$DATA_DIR/daily_stats_history.json"

echo "[init] 启动小爱每日播报"
exec /opt/daily-venv/bin/python3 /usr/local/bin/run_schedule.py
