#!/usr/bin/env python3
"""agent_scanner — 扫描本机装了哪些 AI agent / 工具, 并给出实时探活配方.

设计: 一份配方 (RECIPES) 两种用法
  · 安装时  scan_installed()  → 装没装 (bin/app/dir/file 特征)
  · 运行时  probe_live()      → 活没活 (proc/http/tcp 探测)
装完写进 config.json 的 discovered 段, 面板按它动态渲染联动矩阵/点名榜,
不再写死软件清单.

用法:
    python3 agent_scanner.py            # 打印扫描结果
    python3 agent_scanner.py --json     # 输出 JSON (给 installer 用)
"""
import argparse
import glob
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path

HOME = Path.home()
IS_MAC = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"

# ════════════════════════════════════════════════════════════════
# 配方库: 每项的 probe 列表是「装了」的证据 (任一命中即算装了);
# live 列表是「活着」的判据 (运行时用).
#   bin   — PATH 里有没有这个命令
#   app   — 应用程序有没有装 (mac) / 常见安装路径 (linux)
#   dir   — 配置目录/数据目录
#   file  — 具体文件
#   proc  — 进程在跑 (pgrep -f)
#   http  — HTTP 端点应答 200
#   tcp   — TCP 端口可连
# ════════════════════════════════════════════════════════════════
RECIPES = [
    # ── Hermes 家族 ──
    {"id": "hermes", "name": "Hermes", "kind": "agent", "link": "gateway / A2A 路由",
     "probe": [{"type": "dir", "target": "~/.hermes"}, {"type": "bin", "target": "hermes"}],
     "live": [{"type": "http", "target": "http://127.0.0.1:9900/.well-known/agent.json"}]},

    # ── 通用 Agent 框架 (多机/多 agent 编排类) ──
    {"id": "openclaw", "name": "OpenClaw", "kind": "agent", "link": "Gateway WS :18789 / Nodes 多机",
     "probe": [{"type": "bin", "target": "openclaw"}, {"type": "dir", "target": "~/.openclaw"},
               {"type": "dir", "target": "~/.clawdbot"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:18789"}, {"type": "proc", "target": "openclaw"}]},
    {"id": "goose", "name": "Goose", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "goose"}, {"type": "dir", "target": "~/.config/goose"}],
     "live": [{"type": "proc", "target": "goose"}]},
    {"id": "crush", "name": "Crush", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "crush"}, {"type": "dir", "target": "~/.config/crush"}],
     "live": [{"type": "proc", "target": "crush"}]},
    {"id": "copilot-cli", "name": "Copilot CLI", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "github-copilot-cli"}, {"type": "bin", "target": "copilot"}],
     "live": [{"type": "proc", "target": "copilot"}]},

    # ── 编码 Agent CLI ──
    {"id": "claude-code", "name": "Claude Code", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "claude"}, {"type": "dir", "target": "~/.claude"}],
     "live": [{"type": "proc", "target": "claude"}]},
    {"id": "codex", "name": "Codex CLI", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "codex"}, {"type": "dir", "target": "~/.codex"}],
     "live": [{"type": "proc", "target": "codex"}]},
    {"id": "opencode", "name": "opencode", "kind": "agent", "link": "双向 Web UI",
     "probe": [{"type": "bin", "target": "opencode"}, {"type": "dir", "target": "~/.opencode"}],
     "live": [{"type": "http", "target": "http://127.0.0.1:49374/"}]},
    {"id": "dsh", "name": "DeepSeek Harness", "kind": "agent", "link": "Web UI / headless",
     "probe": [{"type": "bin", "target": "dsh"}, {"type": "dir", "target": "~/.dsh"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:9230"}]},
    {"id": "aider", "name": "Aider", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "aider"}, {"type": "dir", "target": "~/.aider"}],
     "live": [{"type": "proc", "target": "aider"}]},
    {"id": "gemini-cli", "name": "Gemini CLI", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "gemini"}, {"type": "dir", "target": "~/.gemini"}],
     "live": [{"type": "proc", "target": "gemini"}]},
    {"id": "qwen-code", "name": "Qwen Code", "kind": "agent", "link": "本地 CLI 会话",
     "probe": [{"type": "bin", "target": "qwen"}, {"type": "dir", "target": "~/.qwen"}],
     "live": [{"type": "proc", "target": "qwen"}]},
    {"id": "openhands", "name": "OpenHands", "kind": "agent", "link": "本地/容器",
     "probe": [{"type": "bin", "target": "openhands"}, {"type": "dir", "target": "~/.openhands"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:3000"}]},

    # ── AI IDE / 编辑器 ──
    {"id": "cursor", "name": "Cursor", "kind": "ide", "link": "编辑器内 Agent",
     "probe": [{"type": "app", "target": "Cursor"}, {"type": "dir", "target": "~/.cursor"}],
     "live": [{"type": "proc", "target": "Cursor.app"}]},
    {"id": "windsurf", "name": "Windsurf", "kind": "ide", "link": "编辑器内 Agent",
     "probe": [{"type": "app", "target": "Windsurf"}, {"type": "dir", "target": "~/.codeium"}],
     "live": [{"type": "proc", "target": "Windsurf.app"}]},
    {"id": "trae", "name": "Trae CN", "kind": "ide", "link": "SSH 互指 + Remote-SSH",
     "probe": [{"type": "app", "target": "Trae CN"}, {"type": "dir", "target": "~/.trae-cn"}],
     "live": [{"type": "proc", "target": "Trae"}]},
    {"id": "vscode-cline", "name": "VS Code + Cline/Roo", "kind": "ide", "link": "编辑器扩展",
     "probe": [{"type": "dir", "target": "~/.vscode/extensions/*cline*"},
               {"type": "dir", "target": "~/.vscode/extensions/*roo*"},
               {"type": "dir", "target": "~/.vscode-insiders/extensions/*cline*"},
               {"type": "file", "target": "~/Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev"}],
     "live": [{"type": "proc", "target": "Visual Studio Code"}]},
    {"id": "zed", "name": "Zed", "kind": "ide", "link": "编辑器内 Agent",
     "probe": [{"type": "app", "target": "Zed"}, {"type": "dir", "target": "~/.zed"}],
     "live": [{"type": "proc", "target": "Zed.app"}]},
    {"id": "jetbrains-ai", "name": "JetBrains AI", "kind": "ide", "link": "IDE 内 Agent",
     "probe": [{"type": "dir", "target": "~/Library/Application Support/JetBrains"}],
     "live": [{"type": "proc", "target": "idea"}]},

    # ── 本地模型运行时 (agent 的算力后端) ──
    {"id": "ollama", "name": "Ollama", "kind": "runtime", "link": "本地模型推理",
     "probe": [{"type": "bin", "target": "ollama"}, {"type": "dir", "target": "~/.ollama"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:11434"}]},
    {"id": "lmstudio", "name": "LM Studio", "kind": "runtime", "link": "本地模型推理",
     "probe": [{"type": "app", "target": "LM Studio"}, {"type": "dir", "target": "~/.lmstudio"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:1234"}]},
    {"id": "omlx", "name": "oMLX", "kind": "runtime", "link": "本地模型推理",
     "probe": [{"type": "dir", "target": "~/AI/models"}, {"type": "bin", "target": "omlx"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:8000"}]},
    {"id": "llamacpp", "name": "llama.cpp", "kind": "runtime", "link": "本地模型推理",
     "probe": [{"type": "bin", "target": "llama-server"}, {"type": "bin", "target": "llama-cli"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:8080"}]},
    {"id": "vllm", "name": "vLLM", "kind": "runtime", "link": "本地模型推理",
     "probe": [{"type": "bin", "target": "vllm"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:8001"}]},
    {"id": "openwebui", "name": "Open WebUI", "kind": "runtime", "link": "Web 聊天前端",
     "probe": [{"type": "dir", "target": "~/.open-webui"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:3001"}]},

    # ── 云端/桌面 agent 平台 ──
    {"id": "workbuddy", "name": "WorkBuddy", "kind": "platform", "link": "App 运行状态 · 无跨机通道",
     "probe": [{"type": "app", "target": "WorkBuddy"}, {"type": "dir", "target": "~/.workbuddy"}],
     "live": [{"type": "proc", "target": "WorkBuddy.app"}], "no_link": True},
    {"id": "coze", "name": "Coze", "kind": "platform", "link": "云端 Bot",
     "probe": [{"type": "bin", "target": "coze"}, {"type": "dir", "target": "~/.coze"}],
     "live": [{"type": "proc", "target": "coze"}]},
    {"id": "dify", "name": "Dify", "kind": "platform", "link": "工作流平台",
     "probe": [{"type": "dir", "target": "~/dify"}, {"type": "bin", "target": "dify"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:5001"}]},
    {"id": "n8n", "name": "n8n", "kind": "platform", "link": "自动化工作流",
     "probe": [{"type": "bin", "target": "n8n"}, {"type": "dir", "target": "~/.n8n"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:5678"}]},

    # ── 跨机协同通道 ──
    {"id": "syncthing", "name": "Syncthing", "kind": "sync", "link": "文件双向同步",
     "probe": [{"type": "bin", "target": "syncthing"}, {"type": "dir", "target": "~/Library/Application Support/Syncthing"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:8384"}]},
    {"id": "tailscale", "name": "Tailscale", "kind": "net", "link": "跨机组网",
     "probe": [{"type": "bin", "target": "tailscale"}, {"type": "app", "target": "Tailscale"}],
     "live": [{"type": "proc", "target": "Tailscale"}]},
    {"id": "ssh", "name": "SSH 远程", "kind": "net", "link": "跨机登录/执行",
     "probe": [{"type": "bin", "target": "ssh"}],
     "live": [{"type": "tcp", "target": "127.0.0.1:22"}]},
    {"id": "docker", "name": "Docker", "kind": "runtime", "link": "容器化 agent",
     "probe": [{"type": "bin", "target": "docker"}, {"type": "app", "target": "Docker"}],
     "live": [{"type": "proc", "target": "dockerd"}]},
]

APP_DIRS = ["/Applications", "/System/Applications", str(HOME / "Applications")]


def _bin_ok(name):
    return bool(shutil.which(name))


def _app_ok(name):
    for d in APP_DIRS:
        try:
            if Path(d, f"{name}.app").exists():
                return True
        except Exception:
            continue
    return False


def _dir_ok(target):
    """目录存在性; target 含 * 时按 glob 匹配 (用于精确判断某个扩展/插件是否真装了)."""
    t = os.path.expanduser(target)
    if "*" in t or "?" in t:
        try:
            return bool(glob.glob(t))
        except Exception:
            return False
    return Path(t).exists()


def _proc_ok(pat):
    try:
        return subprocess.run(["pgrep", "-f", pat], capture_output=True, timeout=4).returncode == 0
    except Exception:
        return False


def _http_ok(url, timeout=1.5):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "agent-scanner"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def _tcp_ok(spec, timeout=1.2):
    try:
        host, _, port = spec.partition(":")
        s = socket.create_connection((host or "127.0.0.1", int(port)), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


PROBES = {
    "bin": lambda t: (t, _bin_ok(t)),
    "app": lambda t: (t, _app_ok(t)),
    "dir": lambda t: (t, _dir_ok(t)),
    "file": lambda t: (t, Path(os.path.expanduser(t)).is_file()),
    "proc": lambda t: (t, _proc_ok(t)),
    "http": lambda t: (t, _http_ok(t)),
    "tcp": lambda t: (t, _tcp_ok(t)),
}


def scan_installed(recipes=None):
    """扫描装了哪些 (返回 [{id,name,kind,link,installed,evidence}])."""
    out = []
    for r in (recipes or RECIPES):
        ev = []
        for p in r.get("probe", []):
            fn = PROBES.get(p["type"])
            if not fn:
                continue
            target, hit = fn(p["target"])
            if hit:
                ev.append(f'{p["type"]}:{target}')
        out.append({
            "id": r["id"], "name": r["name"], "kind": r.get("kind", "agent"),
            "link": r.get("link", ""), "no_link": bool(r.get("no_link")),
            "installed": bool(ev), "evidence": ev,
        })
    return out


def probe_live(discovered, recipes=None):
    """对已装项做实时探活 (返回 [{id,name,link,online,no_link}])."""
    by_id = {r["id"]: r for r in (recipes or RECIPES)}
    out = []
    for d in discovered:
        if not d.get("installed"):
            continue
        r = by_id.get(d["id"]) or {}
        online = False
        for p in r.get("live", []):
            fn = PROBES.get(p["type"])
            if fn and fn(p["target"])[1]:
                online = True
                break
        out.append({"id": d["id"], "name": d["name"], "link": d.get("link", ""),
                    "online": online, "no_link": bool(d.get("no_link"))})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--live", action="store_true", help="同时对已装项做实时探活")
    a = ap.parse_args()
    res = scan_installed()
    if a.live:
        live = {x["id"]: x for x in probe_live(res)}
        for x in res:
            x["online"] = bool((live.get(x["id"]) or {}).get("online"))
    if a.json:
        print(json.dumps(res, ensure_ascii=False, indent=1))
        return
    by_kind = {}
    for x in res:
        if not x["installed"]:
            continue
        by_kind.setdefault(x["kind"], []).append(x)
    total = sum(len(v) for v in by_kind.values())
    print(f"\n扫描完成: 命中 {total} 项 (配方库共 {len(RECIPES)} 项)\n")
    for kind, items in by_kind.items():
        label = {"agent": "编码/自治 Agent", "ide": "AI IDE", "runtime": "本地模型运行时",
                 "platform": "Agent 平台", "sync": "跨机同步", "net": "组网/远程"}.get(kind, kind)
        print(f"  [{label}]")
        for x in items:
            on = " · 运行中" if x.get("online") else ""
            print(f"    · {x['name']:<18} {x['link']}{on}")
            print(f"      证据: {', '.join(x['evidence'][:3])}")
        print()
    missing = [r["name"] for r in RECIPES if r["id"] not in {x["id"] for x in res if x["installed"]}]
    if missing:
        print(f"  未装 ({len(missing)}): {', '.join(missing[:12])}{'…' if len(missing) > 12 else ''}\n")


if __name__ == "__main__":
    main()
