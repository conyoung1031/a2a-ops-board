#!/usr/bin/env python3
"""a2a-ops-board 安装向导.

自动扫描本机 Hermes 状态 (gateway 端口 / token / profiles) 与局域网/Tailscale 对端,
生成 config.json, 建数据目录, 装系统服务, 最后端到端自检.

用法:
    python3 installer.py            # 交互式向导 (推荐)
    python3 installer.py --yes      # 全自动 (单机模式, 不提问)
    python3 installer.py --dry-run  # 只扫描并打印, 不写任何文件
"""
import argparse
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

HOME = Path.home()
HERE = Path(__file__).resolve().parent
HERMES = HOME / ".hermes"

# profile 名 → 中文角色 (关键词匹配, 猜不出的标为未识别让用户确认)
ROLE_KEYWORDS = [
    ("shangshu", "尚书省"), ("zhongshu", "中书省"), ("menxia", "门下省"),
    ("zhongshu".replace("zhongshu", "zhong"), "中书省"),
    ("lihu", "吏部"), ("hubu", "户部"), ("libu", "礼部"),
    ("bingbu", "兵部"), ("xingbu", "刑部"), ("gongbu", "工部"),
    ("reviewer", "审核"), ("researcher", "研究"), ("writer", "写作"),
    ("default", "默认"),
]

GREEN, YELLOW, RED, DIM, RESET = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m"


def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}!{RESET} {msg}")
def bad(msg):   print(f"  {RED}✗{RESET} {msg}")
def dim(msg):   print(f"  {DIM}{msg}{RESET}")


# ════════════ 扫描: 本机 Hermes ════════════

def read_yaml_text():
    p = HERMES / "config.yaml"
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def scan_gateway_port():
    """找本机 Hermes gateway 端口: config.yaml platforms.a2a.port → 探测常见端口."""
    txt = read_yaml_text()
    m = re.search(r"a2a:[\s\S]{0,400}?port:\s*(\d{2,5})", txt)
    cands = []
    if m:
        cands.append(int(m.group(1)))
    for p in (9900, 9901, 9902):
        if p not in cands:
            cands.append(p)
    for p in cands:
        if _http_ok(f"http://127.0.0.1:{p}/.well-known/agent.json", 1.5):
            return p, cands
    return None, cands


def scan_token():
    """找 A2A token: .env → a2a_agents.<peer>.auth.token → platforms.a2a.token."""
    env = HERMES / ".env"
    try:
        for line in env.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.strip().startswith("A2A_BEARER_TOKEN"):
                v = line.split("=", 1)[1].strip().strip("'\"")
                if v:
                    return v, ".env"
    except Exception:
        pass
    txt = read_yaml_text()
    # 跨机 peer 配置 (Hermes 自己配的 A2A 对端, 最准)
    m = re.search(r"a2a_agents:[\s\S]{0,600}?auth:\s*\n\s*token:\s*(\S+)", txt)
    if m:
        return m.group(1).strip("'\""), "config.yaml a2a_agents"
    m = re.search(r"a2a:[\s\S]{0,400}?token:\s*(\S+)", txt)
    if m:
        return m.group(1).strip("'\""), "config.yaml platforms.a2a"
    return "", None


def scan_a2a_agents():
    """从 config.yaml 的 a2a_agents 段读已配对的对端 (name/url/token) — 最准的发现源."""
    txt = read_yaml_text()
    out = []
    m = re.search(r"a2a_agents:\s*\n([\s\S]*?)(?:\n[a-z_]+:|\Z)", txt)
    if not m:
        return out
    block = m.group(1)
    for pm in re.finditer(r"(?:^|\n)  ([A-Za-z0-9_.-]+):\s*\n([\s\S]*?)(?=\n  [A-Za-z0-9_.-]+:\s*\n|\Z)", block):
        name, body = pm.group(1), pm.group(2)
        um = re.search(r"url:\s*(\S+)", body)
        if not um:
            continue
        url = um.group(1).strip("'\"")
        host = re.sub(r"^https?://", "", url).split(":")[0]
        port = 9900
        pm2 = re.search(r":(\d{2,5})", url.split(host)[-1] if host in url else "")
        if pm2:
            port = int(pm2.group(1))
        out.append({"name": name, "ip": host, "port": port, "src": "config.yaml a2a_agents"})
    return out


def scan_profiles():
    root = HERMES / "profiles"
    out = []
    if root.is_dir():
        for d in sorted(root.iterdir()):
            if d.is_dir() and (d / "state.db").exists() or (d.is_dir() and (d / "SOUL.md").exists()):
                out.append(d.name)
    return out


def guess_roles(profiles):
    mapping, unknown = {}, []
    for p in profiles:
        hit = next((zh for key, zh in ROLE_KEYWORDS if key in p.lower()), None)
        if hit:
            mapping[p] = hit
        else:
            unknown.append(p)
    return mapping, unknown


def scan_tailscale():
    """tailscale status → 在线节点 [{name, ip}] (排除本机)."""
    exe = shutil.which("tailscale") or "/Applications/Tailscale.app/Contents/MacOS/Tailscale"
    if not (shutil.which("tailscale") or Path(exe).exists()):
        return [], "tailscale 未安装"
    try:
        r = subprocess.run([exe, "status", "--json"], capture_output=True, text=True, timeout=8)
        if r.returncode != 0:
            return [], f"tailscale status 失败: {r.stderr[:60]}"
        data = json.loads(r.stdout)
        self_ip = (data.get("Self") or {}).get("TailscaleIPs") or []
        peers = []
        for _k, v in (data.get("Peer") or {}).items():
            ips = v.get("TailscaleIPs") or []
            if not ips:
                continue
            peers.append({
                "name": (v.get("HostName") or v.get("DNSName") or "peer").split(".")[0],
                "ip": ips[0],
                "online": bool(v.get("Online")),
            })
        return [p for p in peers if p["ip"] not in self_ip], None
    except Exception as e:
        return [], f"tailscale 扫描异常: {str(e)[:60]}"


def probe_hermes_node(ip, port=9900, timeout=2.5):
    """探测某 IP 是否是 Hermes gateway (agent.json 应答)."""
    url = f"http://{ip}:{port}/.well-known/agent.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "a2a-ops-board-installer"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8", "ignore"))
            return {"ok": True, "name": data.get("name") or "", "version": data.get("version") or ""}
    except Exception:
        return {"ok": False}


def _http_ok(url, timeout=2.0):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "installer"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


# ════════════ 服务安装 ════════════

def install_service(py, script, label, workdir, args="", log=None):
    """装后台服务: macOS→launchd, Linux→systemd. 返回服务名 或 None."""
    sysname = platform.system()
    if sysname == "Darwin":
        plist = HOME / "Library" / "LaunchAgents" / f"{label}.plist"
        plist.parent.mkdir(parents=True, exist_ok=True)
        log = log or (HERMES / "logs" / f"{label}.log")
        log.parent.mkdir(parents=True, exist_ok=True)
        body = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key><array>
    <string>{py}</string><string>{script}</string>{f"<string>{args}</string>" if args else ""}
  </array>
  <key>WorkingDirectory</key><string>{workdir}</string>
  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>{log}</string>
  <key>StandardErrorPath</key><string>{log}</string>
</dict></plist>"""
        plist.write_text(body, encoding="utf-8")
        subprocess.run(["launchctl", "unload", str(plist)], capture_output=True)
        r = subprocess.run(["launchctl", "load", "-w", str(plist)], capture_output=True, text=True)
        return label if r.returncode == 0 else None
    elif sysname == "Linux":
        unit_dir = Path("/etc/systemd/system") if os.geteuid() == 0 else (HOME / ".config/systemd/user")
        unit_dir.mkdir(parents=True, exist_ok=True)
        unit = unit_dir / f"{label}.service"
        unit.write_text(f"""[Unit]
Description={label} (a2a-ops-board)
After=network.target

[Service]
ExecStart={py} {script} {args}
WorkingDirectory={workdir}
Restart=always

[Install]
WantedBy=default.target
""", encoding="utf-8")
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
        r = subprocess.run(["systemctl", "--user", "enable", "--now", label], capture_output=True, text=True)
        return label if r.returncode == 0 else None
    return None


# ════════════ 主流程 ════════════

def ask(q, default=""):
    try:
        v = input(f"  {YELLOW}?{RESET} {q}" + (f" [{default}]" if default else "") + ": ").strip()
        return v or default
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true", help="全自动 (单机模式)")
    ap.add_argument("--dry-run", action="store_true", help="只扫描不写入")
    ap.add_argument("--no-service", action="store_true", help="不装系统服务")
    a = ap.parse_args()

    print(f"\n{'='*62}\n  a2a-ops-board 安装向导\n{'='*62}\n")

    # ── 1. 本机扫描 ──
    print("[1/5] 扫描本机 Hermes 环境")
    port, cands = scan_gateway_port()
    if port:
        ok(f"gateway 在线 :{port}")
    else:
        bad(f"未发现 Hermes gateway (试过 {cands}) — 请先确认 Hermes 已运行")
    token, token_src = scan_token()
    if token:
        ok(f"token 已获取 (来源 {token_src}, 长度 {len(token)})")
    else:
        warn("未找到 A2A_BEARER_TOKEN (跨机上报会 401, 单机模式可忽略)")

    profiles = scan_profiles()
    roles, unknown = guess_roles(profiles)
    if profiles:
        ok(f"发现 {len(profiles)} 个 profile: {', '.join(profiles)}")
        dim("角色映射: " + ", ".join(f"{k}→{v}" for k, v in roles.items()))
        if unknown:
            warn(f"未识别: {', '.join(unknown)} (可稍后在 config.json 补)")
    else:
        warn("未发现 profile — 将以单机会话模式运行")

    # ── 2. 对端扫描 ──
    print("\n[2/5] 扫描对端机器")
    configured = scan_a2a_agents()
    peers, err = scan_tailscale()
    if err:
        dim(f"tailscale: {err}")
    candidates = []
    seen = set()
    # 优先用 config.yaml 里已配对的 A2A 对端 (最准)
    for c in configured:
        if c["ip"] in seen:
            continue
        r = probe_hermes_node(c["ip"], c["port"])
        if r["ok"]:
            seen.add(c["ip"])
            candidates.append({**c, "online": True, "hermes": r})
            ok(f"已配置对端 {c['name']} ({c['ip']}:{c['port']}) {DIM}{r['name'][:40]}{RESET}")
        else:
            dim(f"已配置对端 {c['name']} ({c['ip']}) 当前不可达")
    if peers:
        ok(f"Tailscale 节点 {len(peers)} 个: " + ", ".join(f"{p['name']}({p['ip']})" for p in peers[:6]))
        for p in peers:
            if not p["online"] or p["ip"] in seen:
                continue
            r = probe_hermes_node(p["ip"], 9900)
            if r["ok"]:
                seen.add(p["ip"])
                candidates.append({**p, "port": 9900, "hermes": r, "src": "tailscale"})
                ok(f"{p['name']} ({p['ip']}) 是 Hermes 节点 {DIM}{r['name'][:40]}{RESET}")
        if not candidates and peers:
            dim("Tailscale 节点里没有探测到 Hermes gateway (9900 不通)")

    interactive = (not a.yes) and (not a.dry_run)
    mode = "single"
    remote = {"host": "", "gateway_port": 9900, "ssh_port": 22, "web_port": 7844}
    if candidates and interactive:
        print()
        for i, c in enumerate(candidates, 1):
            print(f"    [{i}] {c['name']}  {c['ip']}")
        print(f"    [0] 只用本机 (单机模式)")
        pick = ask("选择对端机器编号", "0")
        if pick.isdigit() and 1 <= int(pick) <= len(candidates):
            c = candidates[int(pick) - 1]
            mode = "dual"
            remote["host"] = c["ip"]
            remote["ssh_port"] = int(ask("对端 SSH 端口", "22"))
            remote["web_port"] = int(ask("对端面板端口 (同机上再跑一份 dashboard 时用)", "7844"))
    elif not interactive:
        dim("非交互模式: 使用单机模式" if not candidates else "非交互模式: 用第一个探测到的对端")
        if candidates and a.dry_run:
            mode = "dual"
            remote["host"] = candidates[0]["ip"]
            remote["ssh_port"] = 22

    local_name = ask("本机显示名", platform.node().split(".")[0] or "Local") if interactive else (platform.node().split(".")[0] or "Local")
    remote_name = (candidates[0]["name"] if (mode == "dual" and candidates) else "Remote") if interactive else ("Remote")
    if mode == "dual" and interactive:
        remote_name = ask("对端显示名", remote_name)

    web_port = int(ask("本面板端口", "7844")) if interactive else 7844

    conf = {
        "mode": mode,
        "local_name": local_name,
        "remote_name": remote_name,
        "web_port": web_port,
        "gateway_url": f"http://127.0.0.1:{port or 9900}",
        "gateway_port": port or 9900,
        "remote": remote,
        "profiles": roles,
        "services": {"prefix": "com.hermes"},
        "dashboard_dir": "",
    }

    # ── 3. 写配置 ──
    print("\n[3/5] 写入配置")
    if a.dry_run:
        print(json.dumps(conf, ensure_ascii=False, indent=2))
        print(f"\n{DIM}--dry-run: 未写入任何文件{RESET}")
        return
    (HERE / "config.json").write_text(json.dumps(conf, ensure_ascii=False, indent=2), encoding="utf-8")
    ok(f"config.json ({mode} 模式)")
    data_dir = HERE / "data"
    data_dir.mkdir(exist_ok=True)
    ok(f"数据目录 {data_dir}")

    # ── 4. 装服务 ──
    print("\n[4/5] 安装系统服务")
    py = sys.executable
    srv = HERE / "server" / "a2a_web.py"
    if a.no_service:
        dim("--no-service: 跳过")
        svc = None
    else:
        svc = install_service(py, srv, f"{(conf['services']['prefix'])}.a2a-web", str(HERE))
        if svc:
            ok(f"面板服务 {svc} 已启动 (: {web_port})")
            time.sleep(3)
        else:
            warn("服务安装失败 — 可手动运行:")
            dim(f"{py} {srv}")

    # ── 5. 自检 ──
    print("\n[5/5] 端到端自检")
    ip = "127.0.0.1"
    good = 0
    for label, path in (("面板页面", "/"), ("topology API", "/api/topology")):
        if _http_ok(f"http://{ip}:{web_port}{path}", 4):
            ok(label)
            good += 1
        else:
            bad(f"{label} 无响应")
    if good:
        try:
            with urllib.request.urlopen(f"http://{ip}:{web_port}/api/topology", timeout=5) as r:
                t = json.loads(r.read())
            act = (t.get("mini") or {}).get("activity") or {}
            ok(f"会话统计 {sum(v.get('sessions', 0) for v in act.values())} 条 · 联动 {len(t.get('softs') or [])} 项")
        except Exception:
            pass

    print(f"\n{'='*62}")
    if good == 2:
        host = socket.gethostbyname(socket.gethostname())
        print(f"  {GREEN}安装完成{RESET}  →  http://{host}:{web_port}")
    else:
        print(f"  {YELLOW}安装未完全通过{RESET} — 看日志: {HERMES}/logs/")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
