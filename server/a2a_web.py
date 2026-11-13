#!/usr/bin/env python3
"""双机 A2A dashboard 服务端 — Mini 单机常驻版 (bind 0.0.0.0:7844).

MBP 侧不驻留任何服务 (太上皇指令 2026-11-09), 本服务只展示 Mini 本机真实数据:
  /             — dashboard 页面
  /api/stream   — SSE 实时推送, 2s/帧
  /api/topology — Mini 本地真实数据 (gateway 探测 / profiles 目录 / state.db SQL)
  /api/ping     — A2A message/send 本机回环往返 (真实 JSON-RPC, 30s 节流)
零 mock.
"""
import json
import os
import re
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.request
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

HOME = Path.home()
INDEX = Path(__file__).parent / "a2a_topology.html"

# ════════════════ v27 配置层: 去硬编码, 支持别人安装 ════════════════
_CONF_DEFAULTS = {
    "mode": "single",                     # single | dual
    "local_name": "Mini",
    "remote_name": "MBP",
    "web_port": 7844,
    "gateway_url": "http://127.0.0.1:9900",
    "gateway_port": 9900,
    "remote": {"host": "", "gateway_port": 9900, "ssh_port": 22, "web_port": 7844},
    "profiles": {},                       # {"shangshu": "尚书省", ...}
    "services": {"prefix": "com.hermes"},
    "dashboard_dir": "",
}


def load_conf():
    """读插件 config.json (安装向导生成); 缺失时用默认值 → 单机模式可跑."""
    conf = json.loads(json.dumps(_CONF_DEFAULTS))
    for cand in (Path(__file__).parent.parent / "config.json",
                 Path(__file__).parent / "config.json"):
        try:
            if cand.is_file():
                data = json.loads(cand.read_text(encoding="utf-8"))
                for k, v in data.items():
                    if isinstance(v, dict) and isinstance(conf.get(k), dict):
                        conf[k].update(v)
                    else:
                        conf[k] = v
                break
        except Exception as e:
            print(f"[conf] load {cand} failed: {str(e)[:80]}", flush=True)
    return conf


CONF = load_conf()
DATA_DIR = Path(CONF["dashboard_dir"]) if CONF.get("dashboard_dir") else (Path(__file__).parent.parent / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
INDEX = Path(__file__).parent.parent / "web" / "a2a_topology.html" if not INDEX.exists() else INDEX
GW = CONF["gateway_url"].rstrip("/")
PORT = int(os.environ.get("A2A_WEB_PORT") or CONF["web_port"])
REMOTE = CONF.get("remote") or {}
REMOTE_HOST = REMOTE.get("host") or ""
REMOTE_SSH_PORT = int(REMOTE.get("ssh_port") or 22)
LOCAL_NAME = CONF.get("local_name") or "Local"
REMOTE_NAME = CONF.get("remote_name") or "Remote"
DUAL = (CONF.get("mode") == "dual") and bool(REMOTE_HOST)
PROFILE_ROLES = CONF.get("profiles") or {}

# v28: 接安装时扫描到的 agent 清单 (scripts/agent_scanner.py) → 面板按实际装的动态渲染
try:
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from agent_scanner import probe_live as _scan_probe_live, RECIPES as _SCAN_RECIPES  # type: ignore
except Exception:
    _scan_probe_live = None
    _SCAN_RECIPES = []
PING_MIN_INTERVAL = 30
MBP_STALE_S = 75  # 超过 75s 无上报视为 MBP 离线
STALE_LEDGER_S = 600  # 账本 running 超 10 分钟无更新 = 调用方可能已崩, 读取时标 stalled

_ping = {"result": None}
_mbp_store = {"data": None, "at": 0.0}
_flow = {"data": None, "at": 0.0}

# ════════════════ v27 SQLite 历史层 (a2a_metrics.db) ════════════════
# 设计: 纯增量, 不改变现有 tasks.json/_flow 流程 (JSON 仍是主存储, 零回归风险)。
# 目的: ①flow 阶段变化留痕 → 泳道逐段耗时 (stage_marks) ②联动探测采样 → 在线率历史
#       ③MBP 上报采样 → 延迟曲线 ④账本镜像 → 并发安全 + 可 SQL 查历史
DB_FILE = DATA_DIR / "a2a_metrics.db"
_db_lock = threading.Lock()
_last_nodes = {"task": None, "nodes": {}}


def db_conn():
    con = sqlite3.connect(DB_FILE, timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db():
    try:
        with _db_lock:
            con = db_conn()
            con.executescript("""
            CREATE TABLE IF NOT EXISTS flow_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              task TEXT, node TEXT, from_st TEXT, to_st TEXT, ts REAL);
            CREATE INDEX IF NOT EXISTS idx_fe_ts ON flow_events(ts);
            CREATE TABLE IF NOT EXISTS stage_marks(
              task TEXT, node TEXT, start_ts REAL, end_ts REAL,
              PRIMARY KEY(task, node));
            CREATE TABLE IF NOT EXISTS probe_samples(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, payload TEXT);
            CREATE INDEX IF NOT EXISTS idx_ps_ts ON probe_samples(ts);
            CREATE TABLE IF NOT EXISTS push_samples(
              id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
              mini_lat REAL, mbp_lat REAL, live_m INTEGER, live_i INTEGER);
            CREATE INDEX IF NOT EXISTS idx_pushg_ts ON push_samples(ts);
            CREATE TABLE IF NOT EXISTS ledger(
              id TEXT PRIMARY KEY, task TEXT, source TEXT, mode TEXT,
              dispatch_host TEXT, exec_host TEXT, status TEXT,
              created_at REAL, updated_at REAL, finished_at REAL,
              detail TEXT, result_text TEXT);
            CREATE INDEX IF NOT EXISTS idx_led_created ON ledger(created_at);
            """)
            con.commit()
            con.close()
        print(f"[db] initialized {DB_FILE}", flush=True)
    except Exception as e:
        print(f"[db] init failed: {e}", flush=True)


def db_exec(sql, args=()):
    """单条写入 (失败只打日志, 绝不影响主链路)."""
    try:
        with _db_lock:
            con = db_conn()
            con.execute(sql, args)
            con.commit()
            con.close()
        return True
    except Exception as e:
        print(f"[db] exec failed: {str(e)[:90]}", flush=True)
        return False


def record_flow_events(data):
    """对比上一帧 nodes → 变化落 flow_events + 维护 stage_marks 起止 (逐段耗时数据源)."""
    try:
        task = str((data or {}).get("task") or "")[:120]
        nodes = (data or {}).get("nodes") or {}
        if not isinstance(nodes, dict):
            return
        prev = _last_nodes["nodes"] if _last_nodes["task"] == task else {}
        now = time.time()
        for k, v in nodes.items():
            old = prev.get(k)
            if old == v:
                continue
            db_exec("INSERT INTO flow_events(task,node,from_st,to_st,ts) VALUES(?,?,?,?,?)",
                    (task, k, old, v, now))
            if v == "running":
                db_exec("INSERT INTO stage_marks(task,node,start_ts,end_ts) VALUES(?,?,?,NULL) "
                        "ON CONFLICT(task,node) DO UPDATE SET start_ts=excluded.start_ts, end_ts=NULL",
                        (task, k, now))
            elif v in ("done", "blocked", "failed"):
                db_exec("UPDATE stage_marks SET end_ts=? WHERE task=? AND node=? AND end_ts IS NULL",
                        (now, task, k))
        _last_nodes["task"] = task
        _last_nodes["nodes"] = dict(nodes) if all(isinstance(x, str) for x in nodes.values()) else {}
    except Exception as e:
        print(f"[db] flow_events failed: {str(e)[:90]}", flush=True)


def record_probe(out):
    """联动探测采样 (soft_matrix 每次重算落一条, 约 30s 一条)."""
    try:
        db_exec("INSERT INTO probe_samples(ts,payload) VALUES(?,?)",
                (time.time(), json.dumps(out, ensure_ascii=False)))
        # 每 40 条采样清一次 7 天前旧数据 (约 20 分钟一次, 开销可忽略)
        if int(time.time()) % 40 < 2:
            db_exec("DELETE FROM probe_samples WHERE ts < ?", (time.time() - 7 * 86400,))
            db_exec("DELETE FROM push_samples WHERE ts < ?", (time.time() - 7 * 86400,))
    except Exception:
        pass


def record_push(data, mbp_lat=None):
    """MBP 上报采样 → 延迟曲线."""
    try:
        gw = ((data or {}).get("gateway") or {})
        live = ((data or {}).get("activity") or {})
        live_m = sum(int(v.get("active_now") or 0) for v in live.values()) if isinstance(live, dict) else 0
        db_exec("INSERT INTO push_samples(ts,mini_lat,mbp_lat,live_m,live_i) VALUES(?,?,?,?,?)",
                (time.time(), None, gw.get("ms"), live_m, None))
    except Exception:
        pass


def mirror_ledger(items):
    """账本镜像 (task_report 后调用)."""
    try:
        with _db_lock:
            con = db_conn()
            for x in items or []:
                con.execute(
                    "INSERT INTO ledger(id,task,source,mode,dispatch_host,exec_host,status,"
                    "created_at,updated_at,finished_at,detail,result_text) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(id) DO UPDATE SET task=excluded.task,status=excluded.status,"
                    "updated_at=excluded.updated_at,finished_at=excluded.finished_at,"
                    "detail=excluded.detail,result_text=excluded.result_text",
                    (x.get("id"), x.get("task"), x.get("source"), x.get("mode"),
                     x.get("dispatch_host"), x.get("exec_host"), x.get("status"),
                     x.get("created_at"), x.get("updated_at"), x.get("finished_at"),
                     x.get("detail"), (x.get("result_text") or "")[:4000]))
            con.commit()
            con.close()
    except Exception as e:
        print(f"[db] mirror failed: {str(e)[:90]}", flush=True)


def migrate_from_json():
    """首装/升级一次性回填: tasks.json → ledger 表 (幂等, 已存在则跳过)."""
    try:
        with _db_lock:
            con = db_conn()
            n = con.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
            con.close()
        if n:
            return 0
        items = load_tasks(with_defaults=False)
        mirror_ledger(items)
        print(f"[db] migrated {len(items)} ledger records from tasks.json", flush=True)
        return len(items)
    except Exception as e:
        print(f"[db] migrate failed: {str(e)[:90]}", flush=True)
        return 0


def stage_marks_for(task_like=None, limit=12):
    """查逐段耗时: 按任务取 stage_marks (泳道完全体数据源)."""
    try:
        with _db_lock:
            con = db_conn()
            if task_like:
                rows = con.execute(
                    "SELECT task,node,start_ts,end_ts FROM stage_marks WHERE task LIKE ? "
                    "ORDER BY start_ts", (f"%{task_like}%",)).fetchall()
            else:
                rows = con.execute(
                    "SELECT task,node,start_ts,end_ts FROM stage_marks "
                    "WHERE start_ts > ? ORDER BY start_ts DESC LIMIT 200",
                    (time.time() - 7 * 86400,)).fetchall()
            con.close()
        return [{"task": r[0], "node": r[1], "start": r[2], "end": r[3]} for r in rows]
    except Exception:
        return []


def probe_history(hours=24, max_points=240):
    """探测历史 (降采样到 max_points 条)."""
    try:
        cutoff = time.time() - hours * 3600
        with _db_lock:
            con = db_conn()
            total = con.execute("SELECT COUNT(*) FROM probe_samples WHERE ts > ?", (cutoff,)).fetchone()[0]
            step = max(1, total // max_points)
            rows = con.execute(
                "SELECT ts,payload FROM probe_samples WHERE ts > ? AND id % ? = 0 ORDER BY ts",
                (cutoff, step)).fetchall()
            con.close()
        return [{"ts": r[0], "softs": json.loads(r[1])} for r in rows]
    except Exception:
        return []


def read_token():
    """Mini 自己的 A2A_BEARER_TOKEN (~/.hermes/.env)."""
    try:
        m = re.search(r"A2A_BEARER_TOKEN=(\S+)", (HOME / ".hermes" / ".env").read_text())
        return m.group(1) if m else ""
    except Exception:
        return ""


def timed_get(url, timeout=5):
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "a2a-web/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read(256)
            return {"ok": True, "ms": round((time.time() - t0) * 1000), "http": r.status}
    except Exception as e:
        return {"ok": False, "ms": round((time.time() - t0) * 1000), "error": str(e)[:90]}


def _all_state_dbs():
    """[(state.db 路径, 该库对应的 profile 标识)] —— 主库 + 所有 profile 库。

    profile 的会话存在自己的 state.db 里 (profiles/<name>/state.db)，库内
    profile_name 字段经常为空，所以 profile 库一律用**目录名**做标识，
    否则六部/尚书省的活动在面板上永远读不到。
    """
    out = [(HOME / ".hermes" / "state.db", None)]  # None = 用库内 profile_name
    pdir = HOME / ".hermes" / "profiles"
    try:
        if pdir.is_dir():
            for d in sorted(pdir.iterdir()):
                db = d / "state.db"
                if d.is_dir() and db.is_file():
                    out.append((db, d.name))
    except Exception:
        pass
    return out


def _iter_sessions(cols, require_activity=True):
    """跨 主库+profile库 迭代 sessions 行; cols 用表内列名, 追加 profile 标识列.
    require_activity=False 时不加 WHERE (local_activity 统计全会话数要含占位会话,
    否则各部会话数比 state.db 真值少 1~4 — 2026-11-13 对账实测)."""
    now_cols = ", ".join(cols)
    where = "WHERE last_activity_at IS NOT NULL" if require_activity else ""
    for db, label in _all_state_dbs():
        try:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            rows = con.execute(
                f"SELECT {now_cols} FROM sessions {where}"
            ).fetchall()
            con.close()
        except Exception:
            continue
        for r in rows:
            # label 非 None (profile 库) 时覆盖表内值
            yield (label, *r)


def local_activity():
    """Mini 本地 state.db 真实统计 (主库 + 所有 profile 库，六部活动要能读到)."""
    now = time.time()
    by = {}
    try:
        for p, _pn, la, mc in _iter_sessions(("profile_name", "last_activity_at", "message_count"), require_activity=False):
            key = p or "unknown"
            d = by.setdefault(key, {"sessions": 0, "active_now": 0, "last": 0.0, "msgs": 0})
            d["sessions"] += 1
            d["msgs"] += mc or 0
            if la and la > d["last"]:
                d["last"] = la
            if la and now - la < 60:
                d["active_now"] += 1
    except Exception as e:
        return {"error": str(e)[:90]}
    return by


def profile_dirs():
    root = HOME / ".hermes" / "profiles"
    try:
        return sorted(x for x in os.listdir(root) if (root / x).is_dir())
    except Exception:
        return None


def profile_models(ttl=30):
    """每个 profile 声明的模型 {profile: {provider, model}}, 30s 缓存.

    数据源 = 各 profile 的 config.yaml model 段 (配置声明值, 非运行态);
    default 会话走主 config。零 mock, 读不到就不出该键。
    """
    c = _probe_cache.get("pmodels")
    if c and time.time() - c[0] < ttl:
        return c[1]
    out = {}

    def parse(path):
        """取 model: 段到下一个顶层键; provider 可选; 值去引号.
        注意 Mini 主 config 的 model 段里有注释行 + 空行(空行后才是 provider),
        所以不能用'连续缩进行'截段, 要吃到下一个行首无空白的键为止。"""
        try:
            txt = path.read_text(encoding="utf-8")
        except Exception:
            return None
        m = re.search(r"^model:[ \t]*\n((?:[ \t].*\n|\n)*)", txt, re.M)
        if not m:
            return None
        blk = m.group(1)
        d = re.search(r"^\s+default:\s*[\"']?([^\"'\s]+)", blk, re.M)
        if not d:
            return None
        p = re.search(r"^\s+provider:\s*[\"']?([^\"'\s#]+)", blk, re.M)
        prov = (p.group(1) if p else "").strip().strip("\"'")
        if prov in ("", "auto"):
            prov = ""
        return {"provider": prov, "model": d.group(1)}

    root = HOME / ".hermes" / "profiles"
    try:
        for name in sorted(os.listdir(root)):
            if not (root / name).is_dir():
                continue
            got = parse(root / name / "config.yaml")
            if got:
                out[name] = got
    except Exception:
        pass
    # default profile 不是目录, 走主 config 的 model 段
    got = parse(HOME / ".hermes" / "config.yaml")
    if got:
        out["default"] = got
    _probe_cache["pmodels"] = (time.time(), out)
    return out


# ── 统一任务账本 (v20): 谁派的任务都往这里登记, 持久化在 Mini 本机, 重启不丢 ──
# 入口: MBP 编排器 / MBP·Mini 本地会话 / 未来 IM 通讯软件 / 0点自优化任务
# 账本是 dashboard "协同完成任务" 的唯一数据源; /api/flow 仍只负责工作流图节点态。
# t3: 账本路径可用 A2A_TASKS_FILE 覆盖 (冒烟测试指向沙箱账本, 不碰正式 tasks.json), 不设时不变
TASKS_FILE = Path(os.environ.get("A2A_TASKS_FILE") or (DATA_DIR / "tasks.json"))
TASKS_MAX = 200
# ── t2 新增: 结果交付三字段 (result_text / delivery_status / delivered_at) ──
RESULT_TEXT_MAX = 4000                        # result_text 写入截断上限(字)
DELIVERY_STATUSES = ("pending_mbp", "delivered", "local_only")
OUTBOX_DIR = TASKS_FILE.parent / "outbox"     # MBP 离线时结果全文落盘目录


def atomic_write_text(path, text):
    """原子写: 先写同目录临时文件, flush+fsync 后 os.replace。
    任一时刻磁盘上只会是旧版或新版完整 JSON, 中途崩溃不会留下半截账本。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except Exception:
            pass
        raise


def default_delivery(rec):
    """旧记录缺 delivery_status 时的兼容默认值 — 沿用本改造前的既有语义, 不破坏旧逻辑:
    已完结 + MBP 派发 → 'delivered' (改造前结果一直由 MBP 在线拉 /api/tasks 取走);
    其余(本地/IM 登记、未完结) → 'local_only'。
    绝不凭空给旧记录发 'pending_mbp' 或 outbox 文件, 否则读一次账本就制造脏数据。"""
    if str(rec.get("delivery_status") or "") in DELIVERY_STATUSES:
        return rec["delivery_status"]
    if rec.get("status") in ("done", "failed") and rec.get("dispatch_host") == "mbp":
        return "delivered"
    return "local_only"


def norm_rec(rec):
    """给缺字段的旧记录补三字段的兼容默认值 (返回新 dict, 只在内存用, 不回写文件)。"""
    if not isinstance(rec, dict):
        return rec
    out = dict(rec)
    out["result_text"] = str(rec.get("result_text") or "")[:RESULT_TEXT_MAX]
    out["delivery_status"] = default_delivery(out)
    out["delivered_at"] = rec.get("delivered_at") or None
    return out


def mbp_offline():
    """MBP 是否离线: 内存态 _mbp_store 无数据, 或最近一次心跳距今超过 MBP_STALE_S(75s)。"""
    if not _mbp_store["data"]:
        return True
    return (time.time() - _mbp_store["at"]) > MBP_STALE_S


def write_outbox(task_id, text):
    """离线待交付: 结果全文落盘 outbox/<task_id>.md (原子写, 目录不存在则创建)。
    返回绝对路径 str; 失败返回 None。含路径分隔符的 id 会先被消毒, 防路径逃逸。"""
    tid = str(task_id)
    if "/" in tid or "\\" in tid or ".." in tid:
        tid = re.sub(r"[^A-Za-z0-9._-]", "_", tid)
    try:
        p = OUTBOX_DIR / f"{tid}.md"
        atomic_write_text(p, text)
        return str(p)
    except Exception:
        return None


def load_tasks(with_defaults=True):
    """读账本。with_defaults=True(默认) 时给缺字段的旧记录补兼容默认值(仅内存);
    task_report 合并/回写存量记录时传 False, 保证旧记录原样落盘、不被回填字段。"""
    try:
        if TASKS_FILE.exists():
            data = json.loads(TASKS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [norm_rec(x) for x in data] if with_defaults else list(data)
    except Exception:
        pass
    return []


def save_tasks(items):
    """原子写账本 (临时文件 + os.replace), 返回是否写成功。"""
    try:
        payload = items[-TASKS_MAX:]
        atomic_write_text(TASKS_FILE, json.dumps(payload, ensure_ascii=False, indent=1))
        return True
    except Exception:
        return False


def task_report(rec):
    """登记/更新一条任务 (按 id upsert). 返回 {ok,id,total[,outbox]}. 零 mock, 全部字段来自调用方真实上报."""
    if not isinstance(rec, dict):
        return {"ok": False, "error": "bad payload"}
    tid = str(rec.get("id") or "").strip()
    if not tid:
        return {"ok": False, "error": "missing id"}
    now = time.time()
    status = str(rec.get("status") or "running")[:16]
    norm = {
        "id": tid,
        "task": str(rec.get("task") or "")[:120],
        "source": str(rec.get("source") or "unknown")[:24],
        "mode": str(rec.get("mode") or "")[:16],
        "dispatch_host": str(rec.get("dispatch_host") or "")[:8],
        "exec_host": str(rec.get("exec_host") or "")[:8],
        "status": status,
        "detail": str(rec.get("detail") or "")[:200],
        "steps": rec.get("steps") if isinstance(rec.get("steps"), list) else [],
        "created_at": rec.get("created_at") or now,
        "updated_at": now,
        "finished_at": rec.get("finished_at")
        or (now if status in ("done", "failed", "blocked") else None),
        # ── t2 新增三字段: result_text 写入截断 4000 字 / delivery_status / delivered_at 默认 null ──
        "result_text": str(rec.get("result_text") or "")[:RESULT_TEXT_MAX],
        "delivery_status": None,  # 上报显式值 > 继承旧记录 > default_delivery 兜底, 最后被离线规则改写
        "delivered_at": rec.get("delivered_at") or None,
    }
    items = load_tasks(with_defaults=False)  # 存量记录原样带回, 不给旧数据回填字段
    idx = next((i for i, x in enumerate(items) if x.get("id") == tid), -1)
    if idx >= 0:
        old = items[idx]
        norm["created_at"] = old.get("created_at") or norm["created_at"]
        if not norm["steps"]:
            norm["steps"] = old.get("steps") or []
        if old.get("finished_at") and not norm["finished_at"]:
            norm["finished_at"] = old["finished_at"]
        if not norm["result_text"] and old.get("result_text"):
            norm["result_text"] = str(old["result_text"])[:RESULT_TEXT_MAX]
        if not norm["delivered_at"]:
            norm["delivered_at"] = old.get("delivered_at") or None
        if str(rec.get("delivery_status") or "") not in DELIVERY_STATUSES:
            norm["delivery_status"] = old.get("delivery_status")
        items[idx] = norm
    else:
        items.append(norm)
    if str(norm.get("delivery_status") or "") not in DELIVERY_STATUSES:
        norm["delivery_status"] = default_delivery(norm)

    # t2 规则: done/failed + MBP 派发 + MBP 离线(无上报数据或心跳距今 >75s)
    #   → delivery_status 置 'pending_mbp', 结果全文落盘 outbox/<task_id>.md 待 MBP 上线取走
    outbox = None
    pending_no_file = False
    if status in ("done", "failed") and norm["dispatch_host"] == "mbp" and mbp_offline():
        norm["delivery_status"] = "pending_mbp"
        # 落盘用上报原文全文(不截断; 账本 result_text 字段仍按 4000 字上限存储)
        body = str(rec.get("result_text") or "") or norm["result_text"] or str(rec.get("detail") or "")
        outbox = write_outbox(tid, body)
        pending_no_file = outbox is None

    ok = save_tasks(items)
    res = {"ok": ok, "id": tid, "total": len(items)}
    if ok:
        mirror_ledger([norm])  # v27: 账本镜像进 SQLite (并发安全 + 可 SQL 查历史)
    if outbox:
        res["outbox"] = outbox
    if pending_no_file:
        res["error"] = "outbox write failed"
    return res


def pending_outbox():
    """t3: GET /api/outbox 数据源 — 账本中 delivery_status == 'pending_mbp' 的全部任务。
    每条含 id/task/status/detail/完整 result_text: result_text 直取原始记录, 读取层不截断
    (截断只在写入侧 RESULT_TEXT_MAX)。缺字段旧记录的判定走 default_delivery 兼容语义。"""
    out = []
    for raw in load_tasks(with_defaults=False):
        if not isinstance(raw, dict):
            continue
        ds = raw.get("delivery_status")
        if str(ds or "") not in DELIVERY_STATUSES:
            ds = default_delivery(raw)
        if ds != "pending_mbp":
            continue
        out.append(
            {
                "id": raw.get("id"),
                "task": raw.get("task"),
                "status": raw.get("status"),
                "detail": raw.get("detail"),
                "result_text": str(raw.get("result_text") or ""),
            }
        )
    return out


_ack_lock = threading.Lock()  # ack 是读-改-写账本, ThreadingHTTPServer 下串行化避免丢写


def flow_advance_08():
    """_flow 处于 08 挂起态 → 推进为 '08 已回传'。返回推进后的 current (无 flow 数据返回 None)。
    挂起态识别: current 含 '08' 且未'已回传', 且 current+detail 命中挂起标记
    (挂起/待回传/等待/离线/offline/susp/pend)。
    幂等保证: 推进后 current='08 已回传' 被显式排除, 重复 ack 不会再次推进、不改 updated_at。"""
    f = _flow.get("data")
    if not isinstance(f, dict):
        return None
    cur = str(f.get("current") or "")
    if "08" not in cur or "已回传" in cur:
        return cur or None
    text = (cur + " " + str(f.get("detail") or "")).lower()
    markers = ("挂起", "待回传", "等待", "离线", "offline", "susp", "pend")
    if any(m in text for m in markers):
        f["current"] = "08 已回传"
        f["detail"] = "MBP 已确认接收 (ack)"
        f["updated_at"] = time.time()
        _flow["at"] = time.time()
    return str(f.get("current") or "") or None


def ack_outbox(ids):
    """t3: POST /api/outbox-ack 处理体。存在的 id → delivery_status='delivered' + delivered_at(ISO)。
    幂等: 已 delivered 不改 delivered_at 不重写文件; 不存在的 id 忽略; 同批重复 ack 响应逐字节一致。
    _flow 处于 08 挂起态时推进为 '08 已回传', 推进由状态机自证幂等 (见 flow_advance_08)。
    返回 {ok, acked, ignored, total, flow}。"""
    ids = [str(x) for x in ids] if isinstance(ids, list) else []
    with _ack_lock:
        items = load_tasks(with_defaults=False)
        idx = {str(x.get("id")): i for i, x in enumerate(items) if isinstance(x, dict)}
        acked, ignored, changed = [], [], 0
        now_iso = datetime.now().astimezone().isoformat(timespec="seconds")
        for tid in ids:
            i = idx.get(tid)
            if i is None:
                ignored.append(tid)
                continue
            acked.append(tid)
            rec = items[i]
            if str(rec.get("delivery_status") or "") == "delivered":
                continue  # 重复 ack: 保留首次 delivered_at, 不改盘
            rec["delivery_status"] = "delivered"
            rec["delivered_at"] = now_iso
            changed += 1
        if changed:
            save_tasks(items)  # 原子写 (临时文件 + os.replace)
        flow_now = flow_advance_08()
    return {"ok": True, "acked": acked, "ignored": ignored, "total": len(acked), "flow": flow_now}


def recent_tasks(max_age=900, limit=12):
    """Mini 本机任务明细 (零 mock): 15 分钟内有活动的真实会话.

    覆盖 主库 + 所有 profile 库 —— 六部/尚书省的会话在 profiles/<name>/state.db，
    只读主库的话面板永远看不到它们在跑 (2026-11-11 实测断层)。
    每帧直读无 30s 上报滞后, live/warm 由前端按 age 直接算。
    """
    now = time.time()
    out = []
    try:
        rows = list(_iter_sessions(("profile_name", "title", "last_activity_at", "message_count")))
    except Exception as e:
        return {"error": str(e)[:90]}
    rows.sort(key=lambda r: (r[3] or 0), reverse=True)  # 按 last_activity_at 降序
    for p, _pn, title, la, mc in rows:
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


# ── 联动软件真实探测 (30s 缓存, 零 mock) ──
_probe_cache = {}


def _cached(key, fn, ttl=30):
    c = _probe_cache.get(key)
    if c and time.time() - c[0] < ttl:
        return c[1]
    v = fn()
    _probe_cache[key] = (time.time(), v)
    return v


def tcp_ok(host, port, timeout=1.5):
    def _do():
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.close()
            return True
        except Exception:
            return False
    return _cached(f"tcp:{host}:{port}", _do)


def http_ok(url, timeout=2):
    return _cached(f"http:{url}", lambda: timed_get(url, timeout=timeout)["ok"])


def proc_ok(pat):
    def _do():
        try:
            return subprocess.run(["pgrep", "-f", pat], capture_output=True, timeout=5).returncode == 0
        except Exception:
            return False
    return _cached(f"proc:{pat}", _do)


def soft_matrix():
    """联动矩阵: 按 config.discovered (安装时扫描的 agent 清单) 动态渲染, 未扫描时回退内置 6 项."""
    mbp = _mbp_store["data"] or {}
    mbp_softs = mbp.get("softs") or {}
    stale = (not _mbp_store["at"]) or (time.time() - _mbp_store["at"]) > MBP_STALE_S

    def ms(key):
        """对端在线态: 兼容旧 bool 与 v28 dict{installed,online}."""
        v = None if stale else mbp_softs.get(key)
        if isinstance(v, dict):
            return bool(v.get("online"))
        return v

    def m_inst(key):
        """对端安装态: 只有 v28 dict 上报才有此信息."""
        v = None if stale else mbp_softs.get(key)
        return bool(v.get("installed")) if isinstance(v, dict) else None

    disc = CONF.get("discovered") or []
    if disc and _scan_probe_live:
        def build():
            inst = [d for d in disc if d.get("installed")]
            live = {x["id"]: x for x in _scan_probe_live(inst)}
            # 本机扫描清单 ∪ 对端上报的 id (对端装了本机没有的也要显示, 名称查配方库)
            recipe_by_id = {r["id"]: r for r in _SCAN_RECIPES}
            ids = [d["id"] for d in inst]
            for k in mbp_softs.keys():
                if k not in ids and k in recipe_by_id:
                    ids.append(k)
            rows = []
            for i in ids:
                r = recipe_by_id.get(i, {})
                nm = r.get("name") or i
                local_inst = False
                for d in inst:
                    if d["id"] == i:
                        nm = d.get("name") or i
                        local_inst = True
                        break
                m = ms(i)
                mi = m_inst(i) if m_inst(i) is not None else (i in mbp_softs)
                n = bool((live.get(i) or {}).get("online"))
                if not local_inst and not mi:
                    continue  # 两端都没装 → 不占位
                rows.append((nm, r.get("link") or "", m, n, not r.get("no_link", False), local_inst, mi,
                             r.get("kind") or "agent"))
            return rows
        try:
            rows = _cached("softs:dynamic", build, ttl=30)
        except Exception as e:
            print(f"[softs] dynamic failed: {str(e)[:80]}", flush=True)
            rows = []
    else:
        rows = [
            ("opencode", "服务器双向 Web UI", ms("opencode"), http_ok("http://127.0.0.1:49374/"), True),
            ("Trae CN", "SSH 互指 + Remote-SSH", ms("ssh_mini"), (tcp_ok(REMOTE_HOST, REMOTE_SSH_PORT) if DUAL else None), True),
            ("Hermes", "A2A 路由", ms("hermes") if "hermes" in mbp_softs else (None if stale else (mbp.get("gateway") or {}).get("ok")), tcp_ok("127.0.0.1", int(CONF.get("gateway_port") or 9900)), True),
            ("Syncthing", "知识库文件同步", ms("syncthing"), tcp_ok("127.0.0.1", 8384), True),
            ("dsh", "Web UI / headless", ms("dsh"), tcp_ok("127.0.0.1", 9230), True),
            ("WorkBuddy", "App 运行状态 · 无跨机通道", ms("workbuddy"), proc_ok("WorkBuddy.app"), False),
        ]
    out = []
    for row in rows:
        name, method, m, n, flowable = row[:5]
        inst_local = row[5] if len(row) > 5 else True
        inst_remote = row[6] if len(row) > 6 else None
        kind = row[7] if len(row) > 7 else "agent"
        if m is None and n is None:
            state = "na"
        elif m and n:
            state = "up"
        elif (m is None or n is None) and (m or n):
            state = "half"
        elif m or n:
            state = "half"
        else:
            state = "down"
        out.append({"name": name, "method": method, "mbp": m, "mini": n, "state": state,
                    "flow": flowable, "mini_inst": inst_local, "mbp_inst": inst_remote, "kind": kind})
    record_probe(out)  # v27: 探测采样进 SQLite (~30s 一条, 在线率历史数据源)
    return out



def daily_opt_status():
    """每日自优化定时任务真实状态: launchd(mac)/systemd(linux) 查询, 读不到就 null(不写死)."""
    label = f"{(CONF.get('services') or {}).get('prefix','com.hermes')}.a2a-opt"
    try:
        if os.uname().sysname == "Darwin":
            r = subprocess.run(["launchctl", "list"], capture_output=True, text=True, timeout=4)
            for line in r.stdout.splitlines():
                f = line.split()
                if len(f) >= 3 and f[2] == label:
                    last_exit = int(f[1]) if f[1].lstrip("-").isdigit() else None
                    return {"loaded": True, "label": label, "last_exit": last_exit, "next": "00:00", "via": "launchd"}
            return {"loaded": False, "label": label, "next": "00:00", "via": "launchd"}
        r = subprocess.run(["systemctl", "--user", "is-enabled", label + ".service"],
                           capture_output=True, text=True, timeout=4)
        return {"loaded": r.returncode == 0, "label": label, "next": "00:00", "via": "systemd"}
    except Exception:
        return None


def topology():
    now = time.time()
    mbp = _mbp_store["data"]
    if mbp is not None:
        mbp = dict(mbp)
        mbp["age"] = round(now - _mbp_store["at"])
        mbp["stale"] = (now - _mbp_store["at"]) > MBP_STALE_S
    flow = _flow["data"]
    if flow is not None and now - _flow["at"] > 1800 and flow.get("current") not in ("07 完成",):
        flow = None  # 半小时前的旧流程不再展示(已完成的不清)
    ledger = load_tasks()[-40:]  # v25.3: 12→40, 账本15条时之前只取12条导致完成率分母错(7/12 vs 真值7/15)
    # 防僵尸: 调用方崩溃(超时/进程死)时账本会永远 running — 读取时标注 stalled, 不改盘
    for t in ledger:
        if t.get("status") == "running" and now - (t.get("updated_at") or 0) > STALE_LEDGER_S:
            t["stalled"] = True
    # ── t5 新增: 待回传挂起态计数+摘要 (字段名与前端一致) ──
    # outbox_pending = delivery_status=='pending_mbp' 的任务数 (int);
    # outbox_pending_summary = 同源轻量摘要 {count, list:[{id,task,status}]} (不带 result_text, 控 SSE 帧体积)
    # 数据源与 GET /api/outbox / mbp-push 心跳回执同一个 pending_outbox(), 三处口径必然一致。
    pend = pending_outbox()
    return {
        "ts": now,
        "outbox_pending": len(pend),
        "outbox_pending_summary": {
            "count": len(pend),
            "list": [
                {
                    "id": p.get("id"),
                    "task": str(p.get("task") or "")[:60],
                    "status": p.get("status"),
                }
                for p in pend
            ],
        },
        "mbp": mbp,
        "flow": flow,
        "ledger": ledger,
        "softs": soft_matrix(),
        "daily_opt": daily_opt_status(),
        "mini": {
            "gateway": timed_get(GW + "/.well-known/agent.json"),
            "profileDirs": profile_dirs(),
            "profileModels": profile_models(),
            "activity": local_activity(),
            "tasks": recent_tasks(),
        },
        "a2a": {"url": GW.replace("http://", "").replace("https://", ""), "mode": CONF.get("mode"),
                "localName": LOCAL_NAME, "remoteName": REMOTE_NAME, "profiles": PROFILE_ROLES},
    }


def a2a_ping():
    """真实 A2A message/send 往返 (Mini gateway 本机回环)."""
    token = read_token()
    if not token:
        return {"ok": False, "error": "A2A_BEARER_TOKEN 未配置"}
    payload = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": int(time.time()),
            "method": "message/send",
            "params": {
                "message": {
                    "role": "user",
                    "parts": [{"kind": "text", "text": "dashboard健康检查:收到请只回复两个字:收到"}],
                    "messageId": f"dash-{int(time.time()*1000)}",
                }
            },
        }
    ).encode()
    t0 = time.time()
    try:
        req = urllib.request.Request(
            GW + "/",
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            body = json.loads(r.read())
        ms = round((time.time() - t0) * 1000)
        reply, state = "", ""
        if "result" in body:
            st = body["result"].get("status", {})
            state = st.get("state", "")
            for part in (st.get("message") or {}).get("parts", []):
                reply += part.get("text", "")
        err = body.get("error", {}).get("message", "")
        return {"ok": state == "TASK_STATE_COMPLETED", "ms": ms, "state": state, "reply": reply[:60], "error": err}
    except Exception as e:
        return {"ok": False, "ms": round((time.time() - t0) * 1000), "error": str(e)[:90]}


def sse_stream(wfile):
    """SSE 实时推送: 每 2s 一帧, 客户端断开时抛异常退出."""
    while True:
        frame = json.dumps(topology(), ensure_ascii=False)
        wfile.write(f"data: {frame}\n\n".encode())
        wfile.flush()
        time.sleep(2)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                self._send(200, "text/html; charset=utf-8", INDEX.read_bytes())
            except Exception as e:
                self._send(500, "text/plain", str(e).encode())
        elif self.path == "/api/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                sse_stream(self.wfile)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
        elif self.path == "/api/topology":
            self._send(200, "application/json", json.dumps(topology(), ensure_ascii=False).encode())
        elif self.path == "/api/tasks":
            # 任务账本只读接口 (IM 软件/外部工具可查) — 旧记录缺新字段时在读取层补兼容默认值
            self._send(200, "application/json",
                       json.dumps({"ok": True, "tasks": load_tasks()}, ensure_ascii=False).encode())
        elif self.path == "/api/outbox":
            # t3: 离线待交付清单 — delivery_status=pending_mbp 的任务全量 (id/task/status/detail/完整 result_text)
            files = []
            try:
                if OUTBOX_DIR.is_dir():
                    for p in sorted(OUTBOX_DIR.glob("*.md")):
                        st = p.stat()
                        files.append({"name": p.name, "size": st.st_size, "mtime": st.st_mtime})
            except Exception as e:
                return self._send(500, "application/json",
                                  json.dumps({"ok": False, "error": str(e)[:80]}).encode())
            tasks = pending_outbox()
            self._send(200, "application/json",
                       json.dumps({"ok": True, "count": len(tasks), "tasks": tasks,
                                   "files": files, "pending": [t.get("id") for t in tasks]},
                                  ensure_ascii=False).encode())
        elif self.path.startswith("/api/flow-events"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            task = (q.get("task") or [None])[0]
            body = json.dumps({"ok": True, "marks": stage_marks_for(task)}, ensure_ascii=False).encode()
            return self._send(200, "application/json", body)
        elif self.path.startswith("/api/recent-artifacts"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            try:
                since = float((q.get("since") or [0])[0])
            except Exception:
                since = 0.0
            sub = (q.get("sub") or ["reports"])[0].strip("/") or "reports"
            base = DATA_DIR / sub
            files = []
            try:
                if base.is_dir():
                    for f in sorted(base.iterdir(), key=lambda x: -x.stat().st_mtime):
                        if not f.is_file():
                            continue
                        st = f.stat()
                        if st.st_mtime >= since - 3:          # 3s 容差
                            files.append({"name": f.name, "size": st.st_size,
                                          "mtime": st.st_mtime, "path": str(f)})
            except Exception as e:
                return self._send(200, "application/json",
                                  json.dumps({"ok": False, "error": str(e)[:80], "files": []}).encode())
            return self._send(200, "application/json",
                              json.dumps({"ok": True, "files": files[:20]}, ensure_ascii=False).encode())
        elif self.path.startswith("/api/probe-history"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            hours = float((q.get("hours") or [24])[0])
            body = json.dumps({"ok": True, "samples": probe_history(hours)}, ensure_ascii=False).encode()
            return self._send(200, "application/json", body)
        elif self.path == "/api/ping":
            last = _ping["result"]
            if last and time.time() - last.get("at", 0) < PING_MIN_INTERVAL:
                return self._send(200, "application/json", json.dumps(last, ensure_ascii=False).encode())
            r = a2a_ping()
            r["at"] = time.time()
            _ping["result"] = r
            self._send(200, "application/json", json.dumps(r, ensure_ascii=False).encode())
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self):
        if self.path == "/api/outbox-ack":
            # t3: MBP 收妥回执 {"ids":[...]} — 与 GET /api/outbox 配套的交付闭环接口。
            # 不做 Bearer 校验 (读侧 GET /api/outbox 亦无校验; 本动作只推进交付态, 幂等可重放)。
            try:
                length = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(length))
            except Exception as e:
                return self._send(400, "application/json",
                                  json.dumps({"ok": False, "error": str(e)[:80]}).encode())
            if not isinstance(data, dict) or not isinstance(data.get("ids"), list):
                return self._send(400, "application/json",
                                  b'{"ok":false,"error":"body must be {\\"ids\\":[]}"}')
            res = ack_outbox(data["ids"])
            return self._send(200, "application/json", json.dumps(res, ensure_ascii=False).encode())
        if self.path in ("/api/mbp-push", "/api/task-report", "/api/flow"):
            if self.headers.get("Authorization", "") != f"Bearer {read_token()}":
                return self._send(401, "application/json", b'{"ok":false,"error":"unauthorized"}')
            try:
                length = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(length))
            except Exception as e:
                return self._send(400, "application/json", json.dumps({"ok": False, "error": str(e)[:80]}).encode())
            if self.path == "/api/mbp-push":
                _mbp_store["data"] = data
                _mbp_store["at"] = time.time()
                record_push(data)  # v27: MBP 上报采样 → 延迟曲线
                # t4: 心跳响应捎带回传清单 — pending_outbox 元素与 GET /api/outbox 的 tasks 同源
                # (同一个 pending_outbox()), MBP 上报器零配置顺手取走待回传结果。
                # 账本读取失败也不让心跳主链路回归: 兜底空清单 + 原有 ok 字段照旧。
                try:
                    pending = pending_outbox()
                except Exception as e:
                    print(f"[mbp-push] pending_outbox failed: {e}", flush=True)
                    pending = []
                return self._send(200, "application/json",
                                  json.dumps({"ok": True, "pending_outbox": pending},
                                             ensure_ascii=False).encode())
            elif self.path == "/api/task-report":
                res = task_report(data)
                return self._send(200 if res.get("ok") else 400, "application/json",
                                  json.dumps(res, ensure_ascii=False).encode())
            else:  # /api/flow — 工作流图节点态 (内存态 + v27 阶段事件留痕)
                _flow["data"] = data
                _flow["at"] = time.time()
                record_flow_events(data)  # 节点状态变化 → flow_events + stage_marks 起止
            self._send(200, "application/json", b'{"ok":true}')
        else:
            self._send(404, "text/plain", b"not found")


if __name__ == "__main__":
    init_db()  # v27: SQLite 历史层建表
    migrate_from_json()  # 旧 tasks.json 一次性回填 (幂等)
    print(f"a2a web (mini standalone) on 0.0.0.0:{PORT}")
    from http.server import ThreadingHTTPServer
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
