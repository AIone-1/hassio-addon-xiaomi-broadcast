ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

# Alpine 基础镜像自带 python3；用 venv 避免 PEP668 对系统 pip 的干扰
RUN apk add --no-cache python3 py3-pip ca-certificates \
    && python3 -m venv /opt/daily-venv \
    && /opt/daily-venv/bin/pip install --no-cache-dir websockets==13.1

COPY run.sh /run.sh
COPY rootfs/usr/local/bin/daily_summary.py /usr/local/bin/daily_summary.py
COPY rootfs/usr/local/bin/run_schedule.py /usr/local/bin/run_schedule.py
COPY rootfs/usr/local/bin/ha_daily_config.default.json /usr/local/bin/ha_daily_config.default.json
COPY rootfs/usr/local/bin/webui/index.html /usr/local/bin/webui/index.html

RUN chmod a+x /run.sh /usr/local/bin/*.py

WORKDIR /data
CMD ["/run.sh"]
