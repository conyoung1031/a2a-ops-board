# A2A Ops Board

Local-first operations dashboard for a Hermes A2A fleet. **Zero mock data** — every
number comes from your own `state.db`, your task ledger, or a live network probe.

![panel](docs/panel.png)

## 它能给你什么

| 区块 | 内容 | 数据来源 |
|---|---|---|
| KPI 顶栏 | 本机/对端 gateway 延迟、A2A 往返、60s 活跃会话 | 实时探测 + state.db |
| 双机泳道 | MBP / Mini 两道、每个任务的**真实耗时**（账本时间戳）、跨机桥接、重叠自动分行 | 账本 `created_at→finished_at` |
| 工作流拓扑 | 编排器阶段实时点亮（接活→起草→审议→跨机→执行→六部→回传）、管道粒子 | `/api/flow` 推送 |
| 三省六部点名榜 | 每个 profile 的会话数/消息数/最后活动 + 灯色 | 全部 `profiles/*/state.db`（只读）|
| 联动矩阵 | 6 类软件的双端在线灯（红=仅单端、绿=两端通、虚线流动=有交互）| SSH/TCP/HTTP/进程 真实探测 |
| 任务账本 | 完成率、每条任务状态、点击看 result_text 全文 | `tasks.json` + SQLite 镜像 |
| Outbox | 对端离线时结果挂起、上线后自动回传 | 文件 + 心跳捎带 |

## 安装

```bash
# 方式 A: 一条命令装插件
hermes plugins install conyoung1031/a2a-ops-board

# 方式 B: 手动 clone
git clone https://github.com/conyoung1031/a2a-ops-board ~/.hermes/plugins/a2a-ops-board

# 然后跑安装向导（会扫描你的环境，几乎全自动）
python3 ~/.hermes/plugins/a2a-ops-board/installer.py
```

前置条件：本机已跑 Hermes（`hermes` 在 PATH、gateway 在 9900）。双机模式需要两台机器都装 Hermes 且能互通（Tailscale 或同网段）。

向导会：

1. **扫本机**：gateway 端口、A2A token、profiles 目录并自动映射角色（`shangshu`→尚书省 等）
2. **扫对端**：先从 `config.yaml` 的 `a2a_agents` 段读已配对节点，再用 `tailscale status` 交叉发现，逐个探测 9900 端口确认是不是 Hermes
3. **问三件事**：单机/双机、选哪台做对端、面板端口
4. **写配置 + 建库 + 装服务**（macOS launchd / Linux systemd 自适应）
5. **端到端自检**，全绿才算装好

非交互场景：`python3 installer.py --yes`（单机模式） / `--dry-run`（只扫描不落盘）。

## 配置

`config.json`（向导生成，也可手改）：

```json
{
  "mode": "dual",                       // single | dual
  "local_name": "Mini",
  "remote_name": "MBP",
  "web_port": 7844,
  "gateway_url": "http://127.0.0.1:9900",
  "remote": { "host": "100.x.x.x", "ssh_port": 22, "web_port": 7844 },
  "profiles": { "shangshu": "尚书省", "gongbu": "工部" },
  "dashboard_dir": ""
}
```

- `mode: single` → 单机模式，联动矩阵只显示本机端（对端灯灭，如实标注）
- `dashboard_dir` → 账本/outbox 存放目录，留空则用插件自带 `data/`
- 改完 **重启服务** 生效

## 数据与隐私

- 你的 Hermes `state.db` 全程 **只读**（`?mode=ro` 打开），不会被写入
- 面板自有数据库 `data/a2a_metrics.db`（SQLite）：`flow_events` 阶段事件、`stage_marks` 逐段耗时、`probe_samples` 探测采样、`push_samples` 对端延迟、`ledger` 账本镜像
- 探测采样保留 7 天，自动清理
- 不出网：所有探测都在本机/你的 Tailscale 网络内

## HTTP API

| 路径 | 说明 |
|---|---|
| `GET /` | 面板页面 |
| `GET /api/topology` | 全量状态（KPI/泳道/账本/联动/点名榜） |
| `GET /api/stream` | SSE 实时推送，2s/帧 |
| `GET /api/flow-events[?task=xx]` | 逐段耗时（`stage_marks`） |
| `GET /api/probe-history?hours=24` | 联动探测历史（降采样） |
| `GET /api/outbox` | 待回传结果 |
| `POST /api/task-report` | 登记/更新任务（需 Bearer token） |
| `POST /api/mbp-push` | 对端心跳上报（响应捎带 `pending_outbox`） |
| `POST /api/flow` | 编排器阶段推送 |

## 刷新节奏

- **≤2s**：账本、会话数、灯色、Outbox（SSE 每帧直读盘/库）
- **≤30s**：联动探测（刻意缓存，避免每 2s 打十几次 TCP）、对端上报、模型行

## 卸载

```bash
# macOS
launchctl unload ~/Library/LaunchAgents/com.hermes.a2a-web.plist
# Linux
systemctl --user disable --now com.hermes.a2a-web
rm -rf ~/.hermes/plugins/a2a-ops-board
```

## 已知限制

- 逐段耗时（泳道细分）需要 `/api/flow` 有真实推送才开始积累；首装时泳道按整单耗时显示
- 联动矩阵里 WorkBuddy 只显示 App 运行状态（它没有跨机数据通道，虚线不会流动）
- 建议 macOS / Linux；Windows 需手动跑服务
