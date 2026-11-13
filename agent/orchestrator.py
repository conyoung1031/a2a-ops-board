#!/usr/bin/env python3
"""三省六部任务编排器 (MBP, 派活时手动运行, 跑完即退).

用法:
  python3 a2a_orchestrator.py "任务描述" --mode pipeline   # 需要方案的: 起草→审核→执行
  python3 a2a_orchestrator.py "任务描述" --mode direct     # 简单任务: 直达执行
  --mode auto (默认): 任务描述含 方案/规划/设计/计划 等词走 pipeline, 否则 direct

每一步转换都实时推送到 Mini 面板 (/api/flow), 面板跑马灯跟随真实步骤。
"""
import os
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

HOME = Path.home()
_HERE = Path(__file__).resolve().parent


def _load_conf():
    """读 config.json（插件根或同级）；读不到返回 {}（dashboard 布局照旧）。"""
    for cand in (_HERE.parent / "config.json", _HERE / "config.json"):
        try:
            if cand.is_file():
                return json.loads(cand.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
    return {}


CONF = _load_conf()


def _resolve_dash():
    """数据目录: env > config.dashboard_dir > 插件 data/ > 脚本同级目录。"""
    env = os.environ.get("A2A_DASH_DIR")
    if env:
        return Path(env)
    if CONF.get("dashboard_dir"):
        return Path(CONF["dashboard_dir"])
    if (_HERE.parent / "server").is_dir() or (_HERE.parent / "data").is_dir():
        return _HERE.parent / "data"      # 插件布局: agent/ 的同级 data/
    return _HERE                           # dashboard 布局: 脚本所在目录


DASH = _resolve_dash()


def _resolve_panel():
    """面板地址: env > config.panel_url > config 本机端口 > MBP→Mini 旧默认。"""
    env = os.environ.get("A2A_PANEL_URL")
    if env:
        return env.rstrip("/")
    if CONF.get("panel_url"):
        return str(CONF["panel_url"]).rstrip("/")
    if CONF:
        return "http://127.0.0.1:%s" % CONF.get("web_port", 7844)
    return "http://100.110.231.32:7844"


PANEL = _resolve_panel()
CONF = HOME / ".hermes" / "config.yaml"
ENV = HOME / ".hermes" / ".env"
MBP_GW = "http://127.0.0.1:9900"
MINI_GW = "http://100.110.231.32:9900"
MINI_FLOW = "http://100.110.231.32:7844/api/flow"
MINI_LEDGER = "http://100.110.231.32:7844/api/task-report"

PLAN_WORDS = ["方案", "规划", "设计", "计划", "策划", "架构", "蓝图", "proposal", "plan", "design"]


def read_token():
    try:
        m = re.search(r"A2A_BEARER_TOKEN=(\S+)", ENV.read_text())
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        m = re.search(r"mac-mini:.*?token:\s*(\S+)", CONF.read_text(), re.S)
        return m.group(1) if m else ""
    except Exception:
        return ""


TOKEN = read_token()


def a2a_call(url, text, timeout=900):
    """同步 message/send, 返回 (state, reply)."""
    payload = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000) % 10**9,
            "method": "message/send",
            "params": {
                "message": {
                    "role": "user",
                    "parts": [{"kind": "text", "text": text}],
                    "messageId": f"orch-{int(time.time()*1000)}",
                }
            },
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read())
    if "error" in body:
        return "ERROR", body["error"].get("message", "unknown")
    st = body["result"].get("status", {})
    reply = "".join(p.get("text", "") for p in (st.get("message") or {}).get("parts", []))
    return st.get("state", ""), reply


def push_flow(board):
    """把当前流程棋盘推给 Mini 面板."""
    payload = json.dumps(board, ensure_ascii=False).encode()
    req = urllib.request.Request(
        MINI_FLOW,
        data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except Exception as e:
        print("  (面板推送失败:", str(e)[:80], ")")


def board(task, mode, nodes, current, detail):
    return {
        "task": task[:80],
        "mode": mode,
        "nodes": nodes,
        "current": current,
        "detail": detail[:120],
        "updated_at": time.time(),
    }


# ── v28 超时回捞: 编排器等不到回报时, 去 Mini 面板查产物证明"活其实干完了" ──
START_TS = time.time()
MINI_PANEL = PANEL  # v30: 由 config/env 解析, 不再硬编码


def fetch_recent_artifacts(since, sub="reports"):
    """查 Mini 侧 since 之后新产生的产物文件 (证明任务实际完成)."""
    url = "%s/api/recent-artifacts?since=%d&sub=%s" % (MINI_PANEL, int(since), sub)
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
        with urllib.request.urlopen(req, timeout=8) as r:
            return (json.loads(r.read().decode()) or {}).get("files") or []
    except Exception as e:
        print(f"  (回捞查询失败: {str(e)[:70]})", flush=True)
        return []


# ── 任务账本 (v20): 协同任务的持久记录, MBP→Mini 谁在干、干到哪一步、完没完成 ──
LEDGER_ID = None
LEDGER_STEPS = []
CURRENT_TASK = ""
CURRENT_MODE = ""


LEDGER_QUEUE = DASH / "cache" / "ledger_queue.jsonl"  # v30: 跟数据目录走


def _post_ledger(payload, attempts=3):
    """POST 账本, 失败指数退避重试. 返回是否成功. 超时/连接断都算失败."""
    for i in range(attempts):
        req = urllib.request.Request(
            MINI_LEDGER,
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                r.read()
            return True
        except Exception:
            if i < attempts - 1:
                time.sleep(2 ** i)  # 1s, 2s
    return False


def _queue_ledger(payload):
    """账本推送最终失败: 追加到本地 jsonl, 不丢记录."""
    LEDGER_QUEUE.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER_QUEUE.open("a", encoding="utf-8") as f:
        f.write(payload.decode("utf-8") + "\n")


def flush_ledger_queue():
    """启动时补发队列: 成功的删除, 仍失败的保留. 只删已成功补发的那几行(不是整文件)."""
    if not LEDGER_QUEUE.exists():
        return 0
    lines = [l for l in LEDGER_QUEUE.read_text(encoding="utf-8").splitlines() if l.strip()]
    remain, sent = [], 0
    for l in lines:
        if _post_ledger(l.encode("utf-8"), attempts=2):
            sent += 1
        else:
            remain.append(l)
    if remain:
        LEDGER_QUEUE.write_text("\n".join(remain) + "\n", encoding="utf-8")
    else:
        LEDGER_QUEUE.unlink()
    return sent


def ledger(status, detail="", exec_host="mini"):
    """登记/更新当前任务到 Mini 账本. status: running|done|failed."""
    if not LEDGER_ID:
        return
    payload = json.dumps(
        {
            "id": LEDGER_ID,
            "task": CURRENT_TASK[:120],
            "source": "orchestrator",
            "mode": CURRENT_MODE,
            "dispatch_host": "mbp",
            "exec_host": exec_host,
            "status": status,
            "detail": detail[:200],
            "steps": list(LEDGER_STEPS),
        },
        ensure_ascii=False,
    ).encode()
    if not _post_ledger(payload):
        _queue_ledger(payload)
        print("  (账本登记失败, 已入本地队列, 下次启动补发)")


def step(label, host="mbp"):
    LEDGER_STEPS.append({"label": label[:40], "host": host, "at": time.time()})


PIPE_NODES = ["p_mbp", "p_zs", "p_mx", "p_a2a", "p_ss", "p_six"]
DIRECT_NODES = ["d_mbp", "d_a2a", "d_exec"]


def is_real_done(state, reply):
    """真完成判定 (v28 修正: 不再死抠 [SHANGSHU_DONE] 首行标记).

    gateway COMPLETED 只代表"收到并回复了", 所以要挡的是"转后台未收尾"这种假完成 —
    用话术特征识别, 而不是要求 agent 必须输出特定标记 (实测 shangshu 对简短任务
    从不加标记, 导致真完成被判失败, 面板整链显示 blocked 断流).
      ① 非 COMPLETED → 失败
      ② 空回复 → 失败
      ③ 首行 [SHANGSHU_PARTIAL] → 未收尾
      ④ 首行 [SHANGSHU_DONE] → 直接通过 (协议约定方式)
      ⑤ 含"转后台/等六部返回"等未收尾话术 → 失败
      ⑥ 其余有实质内容 → 真完成
    """
    if state != "TASK_STATE_COMPLETED":
        return False
    text = (reply or "").strip()
    if not text:
        return False
    first = text.splitlines()[0]
    if first.startswith("[SHANGSHU_PARTIAL]"):
        return False
    if first.startswith("[SHANGSHU_DONE]"):
        return True
    DEFER = ("转入后台", "转后台", "已转后台", "等待六部", "等六部返回", "等下回",
             "等返回后", "稍后继续", "收到完成通知后", "转后台执行")
    if any(w in text for w in DEFER):
        return False
    return True


def is_partial(reply):
    first = (reply or "").strip().splitlines()[0] if (reply or "").strip() else ""
    return first.startswith("[SHANGSHU_PARTIAL]")



# ── v30 --mode daily: 每日 00:00 本机自检 + 日报 (不自动改代码, 避免炸面板) ──
DAILY_NODES = ["p_mbp", "p_zs", "p_mx", "p_a2a", "p_ss", "p_six"]


def _dget(url, timeout=6):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "a2a-orchestrator/daily"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), int((time.time() - t0) * 1000)
    except Exception as e:
        return 0, str(e)[:150].encode(), int((time.time() - t0) * 1000)


def _daily_stats():
    """昨日各 profile 会话/消息数 (读本机 state.db + profiles/*/state.db)."""
    import sqlite3
    from datetime import datetime, timedelta
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    y0 = (today - timedelta(days=1)).timestamp()
    y1 = today.timestamp()
    dbs = [(HOME / ".hermes" / "state.db", "default")]
    pdir = HOME / ".hermes" / "profiles"
    if pdir.is_dir():
        for d in sorted(pdir.iterdir()):
            if (d / "state.db").is_file():
                dbs.append((d / "state.db", d.name))
    rows = []
    for db, name in dbs:
        try:
            con = sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True)
            n, m = con.execute(
                "SELECT COUNT(*), COALESCE(SUM(message_count),0) FROM sessions "
                "WHERE started_at >= ? AND started_at < ?", (y0, y1)).fetchone()
            con.close()
        except Exception:
            continue
        if n:
            rows.append((name, int(n), int(m)))
    return rows, datetime.fromtimestamp(y0).strftime("%Y-%m-%d")


def _daily_db_health():
    """a2a_metrics.db 各表行数 (面板自有历史层)."""
    import sqlite3
    out = {}
    db = DASH / "a2a_metrics.db"
    if not db.is_file():
        return out
    try:
        con = sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True)
        for t in ("flow_events", "stage_marks", "probe_samples", "push_samples", "ledger"):
            try:
                out[t] = con.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
            except Exception:
                out[t] = -1
        con.close()
    except Exception:
        pass
    return out


def daily_selfopt(task):
    """每日自检: 会话统计 + 面板健康 + DB 健康 + 告警 → 日报落 reports/, 账本登记.

    刻意不自动改代码/重启服务: 无人值守自改一旦出错整个面板就没了;
    自检报告给人工 review, 真正的自动修复留到有回滚闸之后再上.
    """
    from datetime import datetime
    day = datetime.now().strftime("%Y-%m-%d")
    nodes = {k: "pending" for k in DAILY_NODES}
    nodes["p_mbp"] = "running"
    push_flow(board(task, "daily", nodes, "02 每日自检启动", "本机自检: 统计/健康/告警"))
    step("02 每日自检启动", "mbp")
    ledger("running", "每日自检启动")

    warn = []
    nodes["p_mbp"] = "done"
    nodes["p_zs"] = "running"
    stats, yday = _daily_stats()
    total_s = sum(r[1] for r in stats)
    total_m = sum(r[2] for r in stats)
    push_flow(board(task, "daily", nodes, "03 会话统计", "%s 共 %d 会话 / %d 消息" % (yday, total_s, total_m)))
    step("03 会话统计完成", "mbp")

    nodes["p_zs"] = "done"
    nodes["p_mx"] = "running"
    checks = []
    for path, label in (("/", "面板页面"), ("/api/topology", "topology"), ("/api/outbox", "outbox")):
        code, body, ms = _dget(MINI_PANEL + path if "MINI_PANEL" in globals() else "http://127.0.0.1:7844" + path)
        ok = code == 200
        checks.append((label, code, ms, len(body)))
        if not ok:
            warn.append("面板 %s 返回 %s" % (label, code))
    push_flow(board(task, "daily", nodes, "04 面板健康检查", "%d/%d 端点正常" % (sum(1 for c in checks if c[1] == 200), len(checks))))
    step("04 面板健康检查完成", "mbp")

    nodes["p_mx"] = "done"
    nodes["p_a2a"] = "running"
    dbh = _daily_db_health()
    if not dbh:
        warn.append("a2a_metrics.db 不存在或不可读")
    push_flow(board(task, "daily", nodes, "05 历史层健康", "表: " + ", ".join("%s=%s" % kv for kv in dbh.items())))
    step("05 历史层检查完成", "mbp")

    nodes["p_a2a"] = "done"
    nodes["p_ss"] = "running"
    # 账本健康: running 超 10 分钟 = 僵尸
    try:
        cur = load_ledger_probe()
        stale = [x for x in cur if x.get("status") == "running" and time.time() - (x.get("updated_at") or 0) > 600]
        for x in stale:
            warn.append("账本僵尸: %s 卡在 running" % x.get("id"))
    except Exception as e:
        warn.append("账本读取失败: %s" % str(e)[:60])
    nodes["p_ss"] = "done"
    nodes["p_six"] = "done"

    # ── 落日报 ──
    rep_dir = DASH / "reports"
    try:
        rep_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    lines = []
    lines.append("# %s A2A 运控台每日自检报告" % day)
    lines.append("")
    lines.append("> 由 `com.hermes.a2a-opt` 于每日 00:00 触发 `a2a_orchestrator.py --mode daily` 自动生成。")
    lines.append("")
    lines.append("## 一、会话统计（%s）" % yday)
    lines.append("")
    lines.append("| Profile | 会话数 | 消息数 |")
    lines.append("|---|---|---|")
    for name, n, m in stats:
        lines.append("| %s | %d | %d |" % (name, n, m))
    if not stats:
        lines.append("| （昨日无会话） | 0 | 0 |")
    lines.append("| **合计** | **%d** | **%d** |" % (total_s, total_m))
    lines.append("")
    lines.append("## 二、面板健康")
    lines.append("")
    for label, code, ms, ln in checks:
        lines.append("- %s: HTTP %s (%dms, %dB)" % (label, code, ms, ln))
    lines.append("")
    lines.append("## 三、历史层（a2a_metrics.db）")
    lines.append("")
    if dbh:
        for k, v in dbh.items():
            lines.append("- %s: %s 行" % (k, v))
    else:
        lines.append("- 不可读")
    lines.append("")
    lines.append("## 四、告警")
    lines.append("")
    if warn:
        for w in warn:
            lines.append("- ⚠ " + w)
    else:
        lines.append("- 无")
    lines.append("")
    lines.append("## 五、待人工 review 的可改进点")
    lines.append("")
    lines.append("- 自检只做观测，不自动改代码（无人值守自改风险高，需先有回滚闸）")
    lines.append("- 若面板端点耗时 > 2000ms 或告警非空，请看上面告警段")
    lines.append("")
    text = "\n".join(lines) + "\n"
    fname = rep_dir / ("%s-daily.md" % day)
    try:
        fname.write_text(text, encoding="utf-8")
        saved = str(fname)
    except Exception as e:
        saved = "写入失败: " + str(e)[:80]
        warn.append("日报写入失败")

    nodes["p_six"] = "done"
    push_flow(board(task, "daily", nodes, "07 完成 · 每日自检", "报告: %s · 告警 %d 条" % (saved.split("/")[-1], len(warn))))
    ledger("done" if not warn else "done", "每日自检完成 · 报告 %s · 告警 %d 条 · %d 会话/%d 消息" % (saved.split("/")[-1], len(warn), total_s, total_m))
    print("[daily] 报告: %s" % saved)
    print("[daily] 会话 %d / 消息 %d · 告警 %d 条" % (total_s, total_m, len(warn)))
    for w in warn:
        print("  ⚠ " + w)
    return 0


def load_ledger_probe():
    """读本机面板账本 (供 daily 检查僵尸)."""
    code, body, _ = _dget((MINI_PANEL if "MINI_PANEL" in globals() else "http://127.0.0.1:7844") + "/api/topology", timeout=8)
    if code != 200:
        return []
    try:
        return (json.loads(body.decode()) or {}).get("ledger") or []
    except Exception:
        return []


def main():
    args = sys.argv[1:]
    mode = "auto"
    if "--mode" in args:
        i = args.index("--mode")
        mode = args[i + 1]
        args = args[:i] + args[i + 2 :]
    task = " ".join(args).strip()
    if not task:
        print("用法: a2a_orchestrator.py \"任务\" [--mode pipeline|direct|auto]")
        sys.exit(1)
    if mode == "auto":
        mode = "pipeline" if any(w in task.lower() for w in PLAN_WORDS) else "direct"
    n = flush_ledger_queue()
    if n:
        print(f"(已补发账本队列 {n} 条)")
    print(f"任务: {task}\n模式: {mode}")
    global LEDGER_ID, CURRENT_TASK, CURRENT_MODE
    CURRENT_TASK, CURRENT_MODE = task, mode
    LEDGER_ID = f"orch-{time.strftime('%Y%m%d-%H%M%S')}"
    LEDGER_STEPS.clear()

    if mode == "daily":
        sys.exit(daily_selfopt(task))
    elif mode == "pipeline":
        nodes = {k: "pending" for k in PIPE_NODES}
        nodes["p_mbp"] = "running"
        push_flow(board(task, mode, nodes, "02 MBP 接活", "编排模式启动"))
        step("02 MBP 接活", "mbp")
        ledger("running", "编排模式启动 · MBP 接活")
        nodes["p_mbp"] = "done"

        nodes["p_zs"] = "running"
        push_flow(board(task, mode, nodes, "03 中书省 · 起草方案", "zhongshu 起草中…"))
        step("03 中书省起草", "mbp")
        ledger("running", "中书省起草中")
        st, draft = a2a_call(MBP_GW + "/zhongshu", f"请为以下任务起草一份简明方案(300字内):\n{task}")
        print(f"[03 中书省] {st}")
        nodes["p_zs"] = "done" if st == "TASK_STATE_COMPLETED" else "blocked"
        push_flow(board(task, mode, nodes, "03 中书省 · 完成", draft))
        if st != "TASK_STATE_COMPLETED":
            push_flow(board(task, mode, nodes, "中断", draft))
            ledger("failed", f"中书省起草失败: {draft[:120]}")
            sys.exit(2)

        nodes["p_mx"] = "running"
        push_flow(board(task, mode, nodes, "04 门下省 · 审核", "menxia 审核中…"))
        step("04 门下省审核", "mbp")
        ledger("running", "门下省审核中")
        st, review = a2a_call(
            MBP_GW + "/menxia",
            f"请审核以下方案。若可用,第一行回复『通过』;若不可用,第一行回复『驳回』并说明理由。\n方案:\n{draft}",
        )
        print(f"[04 门下省] {st} | {'通过' if '通过' in review else '驳回/其他'}")
        approved = "通过" in review
        nodes["p_mx"] = "done" if approved and st == "TASK_STATE_COMPLETED" else "blocked"
        push_flow(board(task, mode, nodes, "04 门下省 · 审核结果", review))
        if not approved:
            print("方案被驳回, 流程终止。审核意见:\n", review[:300])
            ledger("failed", f"门下省驳回: {review[:120]}")
            sys.exit(3)

        nodes["p_a2a"] = "running"
        push_flow(board(task, mode, nodes, "05 A2A · 跨机派发", "方案发往 Mini 尚书省…"))
        step("05 A2A 跨机派发", "mbp")
        ledger("running", "方案发往 Mini 尚书省")
        nodes["p_a2a"] = "done"

        nodes["p_ss"] = "running"
        push_flow(board(task, mode, nodes, "06 尚书省 · 调度执行", "Mini 接单…"))
        step("06 尚书省执行", "mini")
        ledger("running", "Mini 尚书省接单执行中")
        st, out = a2a_call(
            MINI_GW + "/shangshu",
            f"以下方案已通过中书起草与门下审核, 请执行并给出结果:\n【任务】{task}\n【方案】{draft}",
            timeout=900,
        )
        print(f"[06 尚书省] {st}")
        # v24 修假完成: 只看协议状态会把"转后台/本轮结束"当成完成 (实测 2026-11-12
        # shangshu 回"t1 转入后台, 等户部返回后再派 t2", A2A 状态是 COMPLETED 但日报根本没写)。
        # 真完成 = COMPLETED 且首行 [SHANGSHU_DONE]; 转后台 = [SHANGSHU_PARTIAL] → 仍算未完成。
        ok = is_real_done(st, out)
        partial = (not ok) and is_partial(out)
        nodes["p_ss"] = "done" if ok else ("running" if partial else "blocked")
        nodes["p_six"] = nodes["p_ss"]
        if ok:
            step("07 执行完成", "mini")
            push_flow(board(task, mode, nodes, "07 完成", out))
            ledger("done", f"尚书省执行完成 · {(out or '')[:100]}")
        elif partial:
            step("07 执行中(转后台未收尾)", "mini")
            push_flow(board(task, mode, nodes, "07 执行中 · 转后台", out))
            ledger("failed", f"尚书省转后台未收尾(非真完成) · {(out or '')[:100]}")
        else:
            step("07 执行失败", "mini")
            push_flow(board(task, mode, nodes, "执行失败", out))
            ledger("failed", f"尚书省执行失败 · {(out or '')[:100]}")
        print("\n===== 最终结果 =====\n", out[:600])
    else:
        nodes = {k: "pending" for k in DIRECT_NODES}
        nodes["d_mbp"] = "running"
        push_flow(board(task, mode, nodes, "02 MBP 接活", "直达模式, 简单任务"))
        step("02 MBP 接活", "mbp")
        ledger("running", "直达模式 · MBP 接活")
        nodes["d_mbp"] = "done"
        nodes["d_a2a"] = "running"
        push_flow(board(task, mode, nodes, "05 A2A · 跨机派发", "直达 Mini 执行…"))
        step("05 A2A 跨机派发", "mbp")
        ledger("running", "直达派发 Mini 执行")
        st, out = a2a_call(MINI_GW + "/", task, timeout=900)
        ok = is_real_done(st, out)
        nodes["d_a2a"] = "done" if ok else "blocked"
        nodes["d_exec"] = nodes["d_a2a"]
        if not ok and st == "TASK_STATE_COMPLETED":
            print("[判定] gateway=COMPLETED 但回复无 [SHANGSHU_DONE] 标记 -> 视为未完成")
        step("07 执行完成" if ok else "07 执行失败", "mini")
        push_flow(board(task, mode, nodes, "07 完成" if ok else "执行失败", out))
        ledger("done" if ok else "failed", f"Mini 直达执行{'完成' if ok else '失败'} · {(out or '')[:100]}")
        print(f"[直达执行] {st}\n===== 结果 =====\n", out[:600])


if __name__ == "__main__":
    # 兜底: 任何未捕获异常 (超时/连接断/JSON错) 都先登记账本再退出,
    # 否则账本会永远卡在 running 变僵尸假状态 (2026-11-11 中书省 240s 超时实测崩过)
    # SystemExit(sys.exit(2)/(3) 的驳回/失败分支) 不是 Exception, 不会被这里覆盖
    try:
        main()
    except Exception as e:
        timed_out = isinstance(e, TimeoutError) or "timed out" in str(e).lower()
        harvested = False
        if timed_out:
            # v28: 超时不等于失败 — 先看 Mini 侧有没有新产物, 有就是"活干完了回报晚了"
            files = fetch_recent_artifacts(START_TS)
            if files:
                names = ", ".join(f["name"] for f in files[:3])
                msg = f"编排器等回报超时, 但 Mini 侧已产出 {len(files)} 个文件: {names}"
                print(f"[回捞] {msg}", flush=True)
                try:
                    ledger("done", msg)
                except Exception:
                    pass
                try:
                    done_nodes = {k: "done" for k in PIPE_NODES}
                    push_flow(board(CURRENT_TASK, CURRENT_MODE, done_nodes,
                                    "07 完成 · 超时回捞", msg[:110]))
                except Exception:
                    pass
                harvested = True
        if LEDGER_ID and not harvested:
            ledger("failed", f"编排器异常中断: {type(e).__name__}: {str(e)[:140]}")
        if harvested:
            sys.exit(0)
        raise
