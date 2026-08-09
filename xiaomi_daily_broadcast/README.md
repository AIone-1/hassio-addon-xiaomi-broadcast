# 🎙️ 小爱每日播报 (Home Assistant Add-on)

让小米音箱播报家里的实时情况：温度、湿度、耗电量、实时功率、安全、空气质量、灯光、任务、待办、设备状态等。支持**每日/周/月/年**播报、**大模型引擎**，Web UI 可视化配置。

## ✨ 功能

- 🗣️ **多种播报类型**：日 / 周 / 月 / 年总结（通过加载项自动创建的按钮实体触发，或 HTTP 接口 / 自动化调用）
- 🤖 **大模型引擎**：直连 DeepSeek API 即兴组织语言（更自然），失败自动回退规则模板；提示语可自定义、可存档多份
- 🧩 **规则模板引擎**：阈值、板块开关、句式（每条播报的措辞）都可配置
- 🌐 **Web UI**：加载项侧边栏打开，一站式配置 + 播报 + 日志
  - **📢 播报**：语音/文字播报、日志、统计（传感器总数/离线数/统计天数）
  - **📋 播报栏目**：选哪些内容播报（问候语/温度/湿度/耗电量/实时功率/安全/空气质量/灯光/任务/待办/设备故障/鼓励语/小贴士/播报结束）
  - **⚙️ 传感器配置**：图形化选实体、填播报名/房间名/用途、拖动排序、屏蔽某个传感器、累计差值用电
  - **🧩 模板配置**：阈值、板块开关句式、一周文案（问候/结束/小贴士/鼓励），手动填或大模型生成（可批量生成一周或单条生成某天）
  - **🤖 大模型配置**：自定义提示语、生成长度、多份提示语存档
  - **📜 历史**：日历查看每天播报记录
- 🔇 **屏蔽传感器**：某个传感器不参与播报，但保留在配置里

## 📦 安装

1. Home Assistant → **设置 → 加载项 → 加载项商店**
2. 点右上角 **⋮ 菜单 → 仓库 (Repositories)**
3. 粘贴本仓库地址：`https://github.com/AIone-1/hassio-addon-xiaomi-broadcast`
4. 点击 **添加**，商店出现 "小爱每日播报"
5. 点开 → **安装**（首次现场构建镜像，约 1-3 分钟）→ **启动**
6. 打开侧边栏的 "小爱每日播报" 图标查看 Web UI

## ⚙️ 加载项设置

| 配置项 | 说明 | 默认 |
|--------|------|------|
| `ha_host` / `ha_port` / `ha_token` | HA 连接（默认 supervisor 自动） | supervisor |
| `speaker_notify` | 小米音箱的 notify 实体 | 需填 |
| `tts_speed` | 播报语速（字/秒） | 4.5 |
| `deepseek_api_key` / `model` / `base_url` | 大模型直连配置 | — |
| `fallback_to_template` | LLM 失败时是否回退模板 | true |

> 引擎切换（模板/大模型）在 Web UI 播报页顶栏；提示语等在大模型配置页。

## 🎙️ 音箱接入

加载项通过 HA 的 `notify.send_message` 让音箱说话：
- 音箱已接入 HA（Xiaomi Home / miot 集成）
- 有 `notify` 域的 `play_text` 实体（HA 开发工具 → 通知里能看到）
- 播报页"传感器配置"里可下拉选音箱（只列真正的智能音箱）

## 🔔 触发播报

### 方式一：加载项创建的按钮实体

加载项启动后自动创建 4 个按钮实体（`input_button.`），在自动化/仪表盘里按下即触发：

| 按钮实体 | 播报类型 |
|----------|---------|
| `input_button.chuan_gan_qi_shi_ti_ri` | 每日 |
| `input_button.chuan_gan_qi_shi_ti_zhou` | 每周 |
| `input_button.chuan_gan_qi_shi_ti_yue` | 每月 |
| `input_button.chuan_gan_qi_shi_ti_nian` | 每年 |

自动化示例：
```yaml
- id: xiaomi_daily_broadcast
  alias: 每晚播报
  trigger:
    - platform: time
      at: "21:00:00"
  action:
    - service: input_button.press
      target:
        entity_id: input_button.chuan_gan_qi_shi_ti_ri
```

### 方式二：HTTP 接口（rest_command / 脚本）

```
POST /trigger?type=daily|weekly|monthly|yearly
```

- 加 `&text_only=true` 只看文字不播报
- 自动化里可用 `rest_command` 或 `shell_command` 调用

### 方式三：Web UI 按钮

播报页点「📢 语音播报」或「📝 文字播报」。

## 📋 终端任务计数

Claude Code 等终端工具的 Stop hook 可把"完成任务"记到加载项（播报里念"今天完成N个任务"）：

```bash
curl -s -X POST http://<ha地址>:8099/task
```

## 🗑️ 周期统计数据

历史页有"清除当天的统计数据"按钮——周期统计（周/月/年）用的每日快照，某天数据异常可单独清除当天。

## 🛠️ 开发

```
addon_xiaomi_broadcast/
├── config.yaml              # 加载项元数据 + options/schema
├── Dockerfile               # 构建镜像（Python + websockets）
├── run.sh                   # 启动：种子配置 + 运行
└── rootfs/usr/local/bin/
    ├── daily_summary.py     # 播报核心（模板/LLM 双引擎）
    ├── run_schedule.py      # 后台循环 + Web 服务（HTTP + WebSocket）
    ├── ha_daily_config.default.json
    └── webui/index.html     # 侧边栏 Web UI
```

## 📄 License

MIT
