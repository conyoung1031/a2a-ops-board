#!/usr/bin/env python3
"""工作机 → 值守机 状态上报器 (launchd/systemd timer 触发, 跑完即退, 非常驻服务).

采集本机真实状态 (gateway 探测 / profile 列表 / state.db SQL), POST 到值守机的
/api/mbp-push; 本机睡眠或关机时上报自然停止, 面板侧按时间戳判离线.

目标地址优先级: 环境变量 A2A_PUSH_URL > config.panel_url > config.remote.host:web_port
                > 本机 127.0.0.1:web_port (单机模式)
"""
import json
import os
import re
import socket
import sqlite3
import subprocess
import time
import urllib.request
from pathlib import Path

HOME = Path.home()
CONF = HOME / ".hermes" / "config.yaml"
ENV = HOME / ".hermes" / ".env"
_HERE = Path(__file__).resolve().parent


def _load_conf():
    for cand in (_HERE.parent / "config.json", _HERE / "config.json"):
        try:
            if cand.is_file():
                return json.loads(cand.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
    return {}


CONF = _load_conf()


def _resolve_push():
    """上报目标: env > panel_url > remote.host:web_port > 本机(单机)."""
    env = os.environ.get("A2A_PUSH_URL")
    if env:
        return env
    if CONF.get("panel_url"):
        return str(CONF["panel_url"]).rstrip("/") + "/api/mbp-push"
    r = CONF.get("remote") or {}
    if r.get("host"):
        return "http://%s:%s/api/mbp-push" % (r["host"], r.get("web_port", 7844))
    return "http://127.0.0.1:%s/api/mbp-push" % CONF.get("web_port", 7844)


PUSH_URL = _resolve_push()


def _peer():
    """对端 (值守机) 地址与 SSH 端口, 供探测项使用; 未配置返回 ("", 22)."""
    r = CONF.get("remote") or {}
    return (r.get("host") or "", int(r.get("ssh_port") or 22))
HERMES_BIN = str(HOME / ".hermes" / "hermes-agent" / "venv" / "bin" / "hermes")



def scan_softs():
    """用 agent_scanner 配方库探测本机全部已装 agent → {id: online} (失败回退旧 5 项)."""
    try:
        import sys as _s
        _s.path.insert(0, str(HOME / ".hermes" / "scripts"))
        from agent_scanner import scan_installed, probe_live  # type: ignore
        disc = scan_installed()
        out = {}
        live = {x["id"]: x for x in probe_live(disc)}
        for d in disc:
            if not d.get("installed"):
                continue
            lv = live.get(d["id"]) or {}
            out[d["id"]] = {"installed": True, "online": bool(lv.get("online"))}
        if out:
            return out
    except Exception as e:
        print(f"  (scan_softs fallback: {str(e)[:70]})", flush=True)
    return {
        "opencode": proc_ok("OpenCode"),
        "dsh": proc_ok("DeepSeek Harness"),
        "workbuddy": proc_ok("WorkBuddy.app"),
        "syncthing": tcp_ok("127.0.0.1", 8384),
        "peer_ssh": tcp_ok(*(lambda h, p: (h, p) if h else ("127.0.0.1", 22))(*_peer())),
    }


def read_token():
    """取 A2A token: env > 任意 a2a_agents.*.auth.token > platforms.a2a.token.

    不绑定具体 profile 名 —— 对端叫什么由用户在 config.yaml 里定.
    """
    env = os.environ.get("A2A_BEARER_TOKEN")
    if env:
        return env
    try:
        txt = (HOME / ".hermes" / "config.yaml").read_text()
    except Exception:
        return ""
    m = re.search(r"a2a_agents:(.*?)(?=\n[a-z_]+:|\Z)", txt, re.S)
    if m:
        t = re.search(r"token:\s*(\S+)", m.group(1))
        if t:
            return t.group(1)
    t = re.search(r"platforms:(.*?)token:\s*(\S+)", txt, re.S)
    return t.group(2) if t else ""


def timed_get(url, timeout=5):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "a2a-reporter/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read(256)
            return {"ok": True, "ms": round((time.time() - t0) * 1000), "http": r.status}
    except Exception as e:
        return {"ok": False, "ms": round((time.time() - t0) * 1000), "error": str(e)[:90]}


def mbp_profiles():
    try:
        out = subprocess.run(
            [HERMES_BIN, "profile", "list"], capture_output=True, text=True, timeout=25
        ).stdout
        profs = []
        for line in out.splitlines():
            if not line.strip() or "─" in line or "Profile" in line:
                continue
            parts = line.split()
            if len(parts) >= 3:
                profs.append(
                    {"name": parts[0].lstrip("◆● \t"), "model": parts[1], "gateway": parts[2]}
                )
        return profs
    except Exception as e:
        return [{"name": "error", "model": "-", "gateway": str(e)[:60]}]


def _all_mbp_dbs():
    out = [(HOME / ".hermes" / "state.db", None)]
    pdir = HOME / ".hermes" / "profiles"
    if pdir.is_dir():
        for d in sorted(pdir.iterdir()):
            db = d / "state.db"
            if d.is_dir() and db.is_file():
                out.append((db, d.name))
    return out


def db_activity():
    try:
        now = time.time()
        by = {}
        for db, label in _all_mbp_dbs():
            try:
                con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
                rows = con.execute(
                    "SELECT profile_name, last_activity_at, message_count FROM sessions"
                ).fetchall()
                con.close()
            except Exception:
                continue
            for p, la, mc in rows:
                key = label or p or "unknown"
                d = by.setdefault(key, {"sessions": 0, "active_now": 0, "last": 0.0, "msgs": 0})
                d["sessions"] += 1
                d["msgs"] += mc or 0
                if la and la > d["last"]:
                    d["last"] = la
                if la and now - la < 60:
                    d["active_now"] += 1
        return by
    except Exception as e:
        return {"error": str(e)[:90]}


def recent_tasks(max_age=900, limit=12):
    """MBP 本机任务明细 (零 mock): 15 分钟内有活动的真实会话, 60s 内活跃记 live.

    用于 dashboard "本地任务" 展示口径 —— 编排器只报跨机流程,
    本机直跑的会话必须靠这条 SQL 才能在面板上被看见。
    """
    try:
        con = sqlite3.connect(f"file:{HOME/'.hermes'/'state.db'}?mode=ro", uri=True)
        rows = con.execute(
            "SELECT profile_name, title, last_activity_at, message_count FROM sessions"
            " WHERE last_activity_at IS NOT NULL"
            " ORDER BY last_activity_at DESC LIMIT 60"
        ).fetchall()
        con.close()
    except Exception as e:
        return {"error": str(e)[:90]}
    now = time.time()
    out = []
    for p, title, la, mc in rows:
        age = now - (la or 0)
        if age > max_age:
            continue
        out.append(
            {
                "profile": p or "unknown",
                "title": (title or "").strip()[:80] or "(无标题会话)",
                "age": int(age),
                "live": age < 60,
                "msgs": mc or 0,
            }
        )
        if len(out) >= limit:
            break
    return out


def tcp_ok(host, port, timeout=1.5):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


def proc_ok(pat):
    try:
        return subprocess.run(["pgrep", "-f", pat], capture_output=True, timeout=5).returncode == 0
    except Exception:
        return False



def ack_outbox(ids):
    """标记结果已送达: POST /api/outbox-ack (与 push 同一 token)."""
    if not ids:
        return None
    url = PUSH_URL.replace("/api/mbp-push", "/api/outbox-ack")
    req = urllib.request.Request(
        url,
        data=json.dumps({"ids": ids}).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Bearer %s" % (read_token() or "")},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, r.read(300).decode()


def drain_outbox(body):
    """心跳响应捎带的待回传结果: 打印 + 落本地归档 + ack.

    这就是 Fire-and-Forget 的回程: 值守机在发单方离线时把结果挂起,
    发单方下次心跳顺手取走并确认, 面板上的「待回传」随之消失.
    """
    try:
        resp = json.loads(body) if isinstance(body, str) else body
    except Exception:
        return 0
    pend = (resp or {}).get("pending_outbox") or []
    if not pend:
        return 0
    ids = []
    arc = HOME / ".hermes" / "logs" / "a2a-outbox-received.log"
    try:
        arc.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    for x in pend:
        tid = x.get("id")
        if not tid:
            continue
        ids.append(tid)
        line = "[%s] %s | %s | %s | %s\n" % (
            time.strftime("%Y-%m-%d %H:%M:%S"), tid, x.get("task") or "",
            x.get("status") or "", (x.get("detail") or "").replace("\n", " ")[:200])
        print("  [取回] " + line.strip(), flush=True)
        try:
            with arc.open("a", encoding="utf-8") as f:
                f.write(line)
                if x.get("result_text"):
                    f.write("  result: %s\n" % str(x["result_text"])[:2000])
        except Exception:
            pass
    try:
        st, txt = ack_outbox(ids)
        print("  [ack] %s -> %s %s" % (ids, st, txt[:120]), flush=True)
    except Exception as e:
        print("  [ack] failed:", str(e)[:100], flush=True)
    return len(ids)


def main():
    payload = json.dumps(
        {
            "gateway": timed_get((CONF.get("gateway_url") or "http://127.0.0.1:9900").rstrip("/") + "/.well-known/agent.json"),
            "profiles": mbp_profiles(),
            "activity": db_activity(),
            "tasks": recent_tasks(),
            "softs": scan_softs(),
            "reported_at": time.time(),
        },
        ensure_ascii=False,
    ).encode()
    req = urllib.request.Request(
        PUSH_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {read_token()}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            status, body = r.status, r.read().decode()
        print("push:", PUSH_URL, status, body[:120])
        drain_outbox(body)          # v31: 取走待回传结果并 ack, 否则永远 pending
    except Exception as e:
        print("push failed:", str(e)[:120])


if __name__ == "__main__":
    main()
