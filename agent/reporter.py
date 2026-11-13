#!/usr/bin/env python3
"""MBP → Mini 状态上报器 (launchd StartInterval=30s 触发, 跑完即退, 非常驻服务).

采集 MBP 真实状态 (gateway 探测 / profile 列表 / state.db SQL),
POST 到 Mini dashboard; MBP 睡眠或关机时上报自然停止, 面板侧按时间戳判离线.
"""
import json
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
MINI_PUSH = "http://100.110.231.32:7844/api/mbp-push"
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
        "ssh_mini": tcp_ok("100.110.231.32", 22),
    }


def read_token():
    """MBP 侧 token 在 config.yaml a2a_agents.mac-mini.auth.token (与 Mini .env 共享同值)."""
    try:
        m = re.search(r"mac-mini:.*?token:\s*(\S+)", CONF.read_text(), re.S)
        return m.group(1) if m else ""
    except Exception:
        return ""


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


def main():
    payload = json.dumps(
        {
            "gateway": timed_get("http://127.0.0.1:9900/.well-known/agent.json"),
            "profiles": mbp_profiles(),
            "activity": db_activity(),
            "tasks": recent_tasks(),
            "softs": scan_softs(),
            "reported_at": time.time(),
        },
        ensure_ascii=False,
    ).encode()
    req = urllib.request.Request(
        MINI_PUSH,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {read_token()}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("push:", r.status, r.read(120).decode())
    except Exception as e:
        print("push failed:", str(e)[:120])


if __name__ == "__main__":
    main()
