# A2A Ops Board

面向 Hermes A2A 机群的本地优先运控台。**零 mock 数据** —— 面板上每个数字都来自你自己机器上的 `state.db`（只读打开）、任务账本，或一次真实的网络探测。

> 当前状态：v0.1.0。已在 macOS 双机（MacBook Pro M5 ↔ Mac mini M4，Tailscale 互联）日常运行；Linux 分支代码已写但**未在真机验证**。

## 面板内容（一屏 1440×900 零滚动）

| 区块 | 内容 | 数据来源 |
|---|---|---|
| KPI 顶栏 | 本机 gateway 延迟、A2A loopback 往返、60s 活跃会话数 | 实时 HTTP 探测 |
| Profile Sessions | 各 profile 会话数 top7 柱状图（最高柱标橙）| 全部 `state.db`（只读）|
| Stage Timer | 环形表盘（当前时间 + 5m 活跃）+ 每日自优化任务**真实状态**（launchd/systemd 实查，不写死）| 本机服务 |
| 双机节点 · Host Status | 两台机器 gateway 延迟、Session 数、60s 活跃；底部联动矩阵 | 本机探测 + 对端 30s 上报 |
| 任务账本 · Execution Strip | 完成率、每条任务状态、点击弹窗看 `result_text` 全文 | 账本 + SQLite 镜像 |
| 工作流实时拓扑 | 编排阶段逐节点点亮；运行中的边是**亮橙字符流**（快）、已完成的边是**暗橙余流**（慢）；`▶ 演示` 预览整链、`⇄` 切换编排图/Agent 图 | `/api/flow` 推送 + 扫描结果 |
| Agent 实时巡检榜 | 每个 agent/profile 的会话数、消息数、最后活动、模型、灯色 | 本机 ∪ 对端 activity |
| Setup Readiness · 节点就绪 | 就绪百分比圆环 + `编排 ×N` / `活跃 ×N` / `联动 N/M` 三条分类条 | 由上面各项汇总 |
| Outbox | 对端离线时结果挂起，对端上线后心跳自动回传 | 文件 + 心跳捎带 |

### 联动矩阵的四个灯态

扫描器（29 项配方库）扫到的每一类 agent/工具一行，格式 `●名称●`（左灯=对端，右灯=本机）：

| 灯 | 含义 |
|---|---|
| 🟢 实心绿（虚线流动）| 两端都在线，存在跨机数据通道 |
| 🔴 实心红（呼吸）| 只有一端在线 —— 链路不完整 |
| ⚪ 空心灰 | **装了但当前没在运行** |
| ○ 极淡 | 未安装 |

没有跨机通道的软件（如 WorkBuddy）两端都跑时虚线**不流动**——只表运行状态，不假装有数据交互。

## 安装

```bash
# 方式 A：一条命令装插件
hermes plugins install conyoung1031/a2a-ops-board

# 方式 B：手动 clone
git clone https://github.com/conyoung1031/a2a-ops-board ~/.hermes/plugins/a2a-ops-board

# 然后跑安装向导
python3 ~/.hermes/plugins/a2a-ops-board/installer.py
```

前置条件：本机已运行 Hermes（`hermes` 在 PATH、gateway 默认 9900）。双机模式需要两台机器都装 Hermes 且网络互通（Tailscale 或同网段）。

### 两种角色（谁常开、谁偶尔开机由你定）

装的时候向导会问这台机器在这次协同里的角色 —— **不预设机器名、也不预设数量**：

| 角色 | 谁适合 | 装什么服务 |
|---|---|---|
| **值守机** (`role: hub`) | 常开的 Mac mini / Mac Studio / NAS / 云主机 | 面板服务（常驻）+ 每日自检（每天 00:00）|
| **发单机** (`role: client`) | 偶尔开机的笔记本 | 状态上报（每 30s 推给值守机），不跑面板 |
| **单机** (`role: single`) | 只有一台机器 | 面板 + 每日自检 |

三台以上也可以：一台值守 + 多台发单，各自跑一遍向导填同一个值守机地址即可。反过来（笔记本值守、台式机发单）同样成立——代码里没有哪台机器"必须"是 Mini 或 MBP。

### 向导会做什么

1. **扫本机**：探测 gateway 端口（读 `config.yaml` + 探 9900）、读 A2A token（`.env` → `a2a_agents.*.auth.token` → `platforms.a2a.token`）、列 `profiles/*` 并按名字猜角色（`shangshu`→尚书省、`gongbu`→工部…）
2. **扫对端**：先用 `config.yaml` 里**已配对的 `a2a_agents`**（最准），再用 `tailscale status --json` 列节点并逐个探 `:9900/.well-known/agent.json` 确认是不是 Hermes
3. **问 7 项**：选对端编号、**这台机器的角色**、对端 SSH 端口、对端面板端口、本机显示名、对端显示名、本面板端口（`--yes` 时全部跳过用默认值）
4. **落盘**：写 `config.json`、建 `data/`、按角色装 1-2 个系统服务（macOS launchd / Linux systemd；`--no-service` 可跳过）
5. **自检**：curl `/` 和 `/api/topology`，两个都通才算装好

非交互：`python3 installer.py --yes`（单机模式）/ `--dry-run`（只扫描打印配置，不写任何文件）。

## 配置

`config.json`（向导生成；也可手改，**改完重启服务生效**）：

```json
{
  "mode": "dual",
  "role": "hub",
  "local_name": "Mini",
  "remote_name": "MBP",
  "web_port": 7844,
  "panel_url": "",
  "gateway_url": "http://127.0.0.1:9900",
  "gateway_port": 9900,
  "remote": { "host": "100.x.x.x", "gateway_port": 9900, "ssh_port": 22, "web_port": 7844 },
  "profiles": { "shangshu": "尚书省", "gongbu": "工部" },
  "orchestration": { "drafter": "zhongshu", "reviewer": "menxia", "dispatcher": "shangshu" },
  "services": { "prefix": "com.hermes", "web": true, "daily": true, "reporter": false },
  "dashboard_dir": ""
}
```

| 字段 | 说明 |
|---|---|
| `mode` | `single`（单机，对端灯全部熄灭并如实标注）或 `dual` |
| `local_name` / `remote_name` | 面板上显示的机器名 |
| `web_port` / `gateway_url` / `gateway_port` | 面板端口、本机 gateway 地址与端口 |
| `remote` | 对端地址；`dual` 模式下用于 SSH/端口探测 |
| `profiles` | profile 名 → 中文角色名映射；没有映射就显示 profile 原名 |
| `role` | `hub` 值守机 / `client` 发单机 / `single` 单机；只影响装哪些服务 |
| `orchestration` | `--mode pipeline` 用的三个角色 profile 名（起草/审议/调度）。你的审批架构不叫中书省尚书省就改这里 |
| `services.prefix` | 系统服务名前缀（默认 `com.hermes`）|
| `services.web` / `services.daily` / `services.reporter` | 分别控制：面板常驻、每日自检、向对端上报。值守机开前两个，发单机开第三个 |
| `panel_url` | 编排器把 flow 推给哪台面板。留空 = `http://127.0.0.1:<web_port>`；**双机且编排器跑在另一台时，必须填对端面板地址**（例如 `http://100.x.x.x:7844`），否则编排器推的 flow 到不了面板 |
| `dashboard_dir` | 账本 / outbox / 报告的存放目录；留空则用插件自带 `data/` |

`installer.py` 还会写入一个 `discovered` 字段（安装时扫描到的 agent 清单），面板的联动矩阵据此渲染——**不写死软件列表，你机器上装了什么就显示什么**。

## 数据与隐私

- Hermes 的 `state.db` 全程**只读**（`file:...?mode=ro` 打开），面板不会写入
- 面板自有数据库 `data/a2a_metrics.db`（SQLite，WAL）：`flow_events`（阶段事件）、`stage_marks`（每阶段起止）、`probe_samples`（联动探测采样）、`push_samples`（对端延迟采样）、`ledger`（账本镜像）
- `tasks.json` 与内存 flow 仍是主存储，SQLite 是**增量历史层**——任何写库失败只打日志，不影响面板运行
- 探测采样保留 7 天后自动清理
- 不出网：所有探测都在本机与你的 Tailscale/局域网内

## HTTP API

| 方法 | 路径 | 说明 | 鉴权 |
|---|---|---|---|
| GET | `/` | 面板页面 | 无 |
| GET | `/api/stream` | SSE 实时推送，2s/帧 | 无 |
| GET | `/api/topology` | 全量状态（KPI/账本/联动/巡检榜/flow）| 无 |
| GET | `/api/tasks` | 账本全量（只读）| 无 |
| GET | `/api/outbox` | 待回传结果（含全文）| 无 |
| GET | `/api/ping` | A2A loopback 往返计时（30s 节流）| 无 |
| GET | `/api/flow-events[?task=]` | 阶段事件与逐段耗时（`stage_marks`）| 无 |
| GET | `/api/probe-history?hours=24` | 联动探测历史（降采样至约 240 点）| 无 |
| GET | `/api/recent-artifacts?since=<epoch>&sub=reports` | 某时刻后新产生的产物文件（编排器超时回捞用）| 无 |
| POST | `/api/task-report` | 登记/更新任务（upsert by `id`）| Bearer |
| POST | `/api/outbox-ack` | 标记回传完成 `{"ids":[...]}` | Bearer |
| POST | `/api/mbp-push` | 对端心跳上报（响应捎带 `pending_outbox`）| Bearer |
| POST | `/api/flow` | 编排器阶段状态推送 | Bearer |

## 刷新节奏（三层，不要把探活也说成实时）

| 层级 | 数据 | 周期 |
|---|---|---|
| 时钟 | 时间 / 表盘 | 0.5s |
| 实时层 | 账本、会话数、灯色、Outbox —— 每帧直读磁盘与数据库，零缓存 | ≤2s（SSE 每帧）|
| 探测层 | 联动矩阵、模型行、A2A 往返 —— `_cached(ttl=30)` | ≤30s |
| 上报层 | 对端延迟、对端联动状态（launchd `StartInterval=30`）| ≤30s |

探活刻意不进实时层：否则每 2 秒要对十几项打一次 TCP，会把本机和对方搞烦。

## 附带的编排器（agent/）

`agent/orchestrator.py` 是可选的命令行编排器，三种模式：

| 模式 | 流程 |
|---|---|
| `pipeline` | 02 接活 → 03 起草 → 04 审议（可驳回）→ 05 A2A 跨机 → 06 尚书调度 → 07 六部执行 → 08 回传 |
| `direct` | 02 接活 → 05 跨机直达 → 07 执行 → 08 回传（不起草不审议，适合执行类任务）|
| `daily` | 本机自检：昨日会话统计 + 面板端点健康 + 历史层表行数 + 账本僵尸检测 → 日报落 `reports/YYYY-MM-DD-daily.md` |

```bash
python3 agent/orchestrator.py "统计今日各 profile 会话数" --mode direct
python3 agent/orchestrator.py "设计一个备份方案" --mode pipeline
python3 agent/orchestrator.py "每日自检" --mode daily
```

`daily` 刻意**只观测、不自动改代码**：无人值守自改一旦出错整个面板就没了，日报给人工 review。

## 卸载

```bash
# macOS（按你装过的服务删，通常是 web / a2a-opt / reporter 中的 1-2 个）
for s in com.hermes.a2a-web com.hermes.a2a-opt com.hermes.a2a-reporter; do
  launchctl unload ~/Library/LaunchAgents/$s.plist 2>/dev/null
done
# Linux
for s in com.hermes.a2a-web com.hermes.a2a-opt com.hermes.a2a-reporter; do
  systemctl --user disable --now $s 2>/dev/null
done
rm -rf ~/.hermes/plugins/a2a-ops-board
```

## 已知限制

- **工作流的时间轴视图已移除**：服务端仍在积累 `stage_marks`（每阶段起止），但前端没有时间轴 UI，需要逐段耗时请直接读 `GET /api/flow-events`
- **逐段耗时需要真实编排推送才开始积累**：首装时 `/api/flow-events` 返回空，跑过一次 pipeline 后才会有数据
- **Agent 图只在「没有配置编排体系」时自动启用**：`config.profiles` 非空（例如有三省六部）时默认显示编排骨架 + 待命态，要切 Agent 图点右上角 `⇄`
- **`▶ 演示` 是纯视觉预览**：不产生账本记录、不影响真实状态，播完自动还原
- **Linux systemd 分支未实测**：代码按 `systemctl --user` 写，但没有真机验证过
- **Windows**：无服务托管支持，需手动常驻运行 `server/a2a_web.py`
- **`--mode pipeline` 需要三个 profile**：起草（默认 `zhongshu`）、审议（`menxia`）、调度（`shangshu`）。没有这套审批架构就用 `--mode direct`（只需一个能接活的 agent），或改 `orchestration` 段指向你自己的 profile
- **对端离线判定 75 秒**：对端 reporter 超过 75s 没上报即视为离线，其对端灯与延迟归零（`MBP_STALE_S`）
