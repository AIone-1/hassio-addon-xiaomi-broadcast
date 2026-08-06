# Changelog

## 1.0.4 (2026-08-06)

- 🐛 实体下拉改为 `/cfg/` 路径（避免 ingress 下 `/api/` 嵌套被 HA 拦截），兼容旧 `/api/`
- 🔍 实体加载失败时显示具体错误原因（此前静默返回空）

## 1.0.3 (2026-08-06)

- 🐛 修复 Web UI 实体下拉为空：ingress 下前端用相对路径（BASE 前缀），避免 `/api/` 请求被 HA 主 API 拦截

## 1.0.2 (2026-08-06)

- ⚙️ Web UI 新增「传感器配置」页面：在加载项网页上直接选实体、填房间名，保存即生效，无需 Samba 编辑 JSON
- 🔒 配置保存只写实体映射，DeepSeek key 由加载项设置页管理，不落盘到可编辑 JSON

## 1.0.1 (2026-08-06)

- 🐛 修复 Docker 构建：添加 build.yaml 指定 build_from 基础镜像

## 1.0.0 (2026-08-06)

- 🎉 首个版本：小爱每日播报加载项
- 每日播报 + 周/月/年周期总结
- 大模型引擎直连 DeepSeek（Anthropic 兼容端点）
- 加载项侧边栏 Web UI（日志 / 触发 / 只看文字）
- 支持 HA 自动化 rest_command 触发（/trigger）
- 终端任务计数接口（/task）
- 配置通过 Samba 编辑 `/share/xiaomi_broadcast/`，或加载项设置页
