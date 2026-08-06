# 🎙️ 小爱每日播报 (Home Assistant Add-on)

让小米音箱定时播报家里的实时情况：温度、湿度、用电、安全、空气质量、灯光、待办、设备状态等。支持**每日播报**和**周/月/年周期总结**，大模型引擎直连 DeepSeek。

## ✨ 功能

- 📅 **每日播报**：定时（工作日/周末可分开设）把家里状态生成中文播报稿，推到小米音箱朗读
- 📊 **周/月/年总结**：聚合周期数据，自动生成周期总结（可选）
- 🤖 **大模型引擎**：直连 DeepSeek API 即兴组织语言（更自然），失败自动回退规则模板
- 🌐 **Web UI**：加载项侧边栏打开，可看实时日志、手动触发播报、只看文字
- 🛠️ **网页配置**：加载项设置页可配音箱、DeepSeek、调度时间；实体映射通过 Samba 编辑 JSON

## 📦 安装

1. Home Assistant → **设置 → 加载项 → 加载项商店**
2. 点右上角 **⋮ 菜单 → 仓库 (Repositories)**
3. 粘贴本仓库地址：
   ```
   https://github.com/AIone-1/hassio-addon-xiaomi-broadcast
   ```
4. 点击 **添加**，商店会出现 "小爱每日播报"
5. 点开 → **安装**（首次会现场构建镜像，约 1-3 分钟）→ **启动**
6. 打开侧边栏的 "小爱每日播报" 图标查看 Web UI

## ⚙️ 配置

### 加载项设置页（网页可配）

| 配置项 | 说明 | 默认 |
|--------|------|------|
| `speaker_notify` | 小米音箱的 notify 实体（如 `notify.xiaomi_cn_xxx_play_text`） | 需填 |
| `engine_mode` | `template` 规则模板 / `llm` 大模型 | `template` |
| `deepseek_api_key` | DeepSeek API Key（`sk-` 开头，留空则不直连） | 空 |
| `deepseek_model` | 模型名 | `deepseek-chat` |
| `fallback_to_template` | LLM 失败时是否回退模板 | `true` |
| `tts_speed` | 播报语速（字/秒） | `4.5` |
| `broadcast_enabled` | 定时播报开关 | `true` |
| `weekday_hour` / `weekend_hour` | 工作日/周末播报整点（0-23） | 21 / 18 |
| `weekly_*` `monthly_*` `yearly_*` | 周期总结开关与时间 | 关 |

### 实体映射（Samba 编辑 JSON）

设备传感器、门窗、灯光、用电等实体映射放在配置文件：

```
/share/xiaomi_broadcast/ha_daily_config.json
```

安装 Samba 插件后可通过网络邻居访问 `share` 共享，编辑该文件。首次安装会生成默认模板，把 `sensor.your_xxx` 替换成你自己的实体即可。

**关键实体段**：
- `speaker_notify`：音箱 notify 实体
- `temp_sensors` / `humidity_sensors`：`实体ID → 房间名`
- `power_sensors`：用电（用 `power_cost_today` 实体）
- `doors` / `windows`：门窗传感器（on=开）
- `lights`：灯光
- `important_devices`：重要设备（离线提醒）
- `encouragements` / `daily_tips`：鼓励语/小贴士

## 🎙️ 你的音箱怎么接入

加载项通过 HA 的 `notify.send_message` 让音箱说话。前提：
- 音箱已接入 HA（如 Xiaomi Home / miot 集成）
- 该集成提供 `notify` 域的 `play_text` 实体（在 HA 开发工具 → 通知里能看到）

## 🤖 DeepSeek 直连

在加载项配置里：
1. `engine_mode` 选 `llm`
2. 填入 `deepseek_api_key`（DeepSeek 开放平台创建，`sk-` 开头）
3. 保存重启

加载项直连 `https://api.deepseek.com/anthropic`，无需任何代理。

## 🕹️ 手动触发 / 集成到自动化

Web UI 有 "立即播报" 按钮。也可以用 `rest_command` 调加载项接口：

```yaml
rest_command:
  xiaomi_broadcast:
    url: "http://192.168.10.123:8099/trigger?type={{ type }}"
    method: POST
```

- `type`：`daily` / `weekly` / `monthly` / `yearly`
- 加 `&text_only=true` 只看文字不播报

## 📋 终端任务计数

Claude Code 等终端工具的 Stop hook 可把"完成任务"记到这个加载项（播报里会念"今天完成N个任务"）：

```bash
curl -s -X POST http://192.168.10.123:8099/task
```

## 🛠️ 开发

```
addon_xiaomi_broadcast/
├── config.yaml              # 加载项元数据 + options/schema
├── Dockerfile               # 构建镜像（Python + websockets）
├── run.sh                   # 启动：种子配置 + 运行
└── rootfs/usr/local/bin/
    ├── daily_summary.py     # 播报核心（改造自 Mac 版）
    ├── run_schedule.py      # 调度循环 + Web 服务
    ├── ha_daily_config.default.json
    └── webui/index.html     # 侧边栏 Web UI
```

## 📄 License

MIT
