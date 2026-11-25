"""Flask 侧的发布进程管理器：拉起 / 恢复 / 强杀 / 回收 / 重启恢复。

注意：PROCS 只在当前 Flask 进程内存里保存 Popen 句柄；权威状态全部在
SQLite。Flask / 容器重启后句柄丢失也没关系，可按 DB 里的 pid 发信号，
并通过 ``recover_publisher`` 把失联任务标记为 interrupted、认回租约 token。
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import config, db, publisher

_PROCS: dict[str, subprocess.Popen] = {}
_LOCK = threading.Lock()


def start_publish(
    *,
    request_id: str,
    holder: str,
    token: int,
    disabled: list[str],
    pause_at: str,
) -> dict[str, Any]:
    run_id = db.new_id()
    now_real = time.time()
    log_path = str(config.STATE_DIR / f"publisher-{run_id}.log")

    with db.session() as con:
        sim = db.sim_now(con)
        con.execute(
            "INSERT INTO publish_runs(run_id, request_id, holder, token, "
            "disabled_json, pause_at, phase, status, log_path, created_sim, "
            "started_real, updated_real) "
            "VALUES(?, ?, ?, ?, ?, ?, 'queued', 'running', ?, ?, ?, ?)",
            (run_id, request_id, holder, int(token),
             __import__("json").dumps(disabled, ensure_ascii=False),
             pause_at, log_path, sim, now_real, now_real),
        )

    config.ensure_dirs()
    log_fh = open(log_path, "ab", buffering=0)
    env = dict(os.environ)
    root = str(config.PROJECT_ROOT)
    env["PYTHONPATH"] = root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "drill.publisher", "--run-id", run_id],
        cwd=root,
        env=env,
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        # 独立进程组，便于自测 / 关闭时整组清理
        start_new_session=True,
    )
    log_fh.close()  # 子进程已 dup 该 fd，父进程关闭自己的副本避免句柄泄漏
    with _LOCK:
        _PROCS[run_id] = proc

    with db.session() as con:
        con.execute("UPDATE publish_runs SET pid=? WHERE run_id=?", (proc.pid, run_id))
        db.log_event(
            con, "publish_spawned",
            f"拉起发布端 pid={proc.pid} holder={holder} token={token} "
            f"req={request_id} 暂停点={pause_at}",
            holder=holder, request_id=request_id,
        )
    return {"run_id": run_id, "pid": proc.pid}


def reap() -> None:
    """非阻塞回收已退出的子进程，并把被强杀的任务落为 killed。"""
    with _LOCK:
        items = list(_PROCS.items())
    for run_id, proc in items:
        rc = proc.poll()
        if rc is None:
            continue
        with _LOCK:
            _PROCS.pop(run_id, None)
        with db.session() as con:
            row = con.execute(
                "SELECT status, phase FROM publish_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row and row["status"] in ("running", "paused", "queued"):
                if rc < 0 or rc not in (0, 2, 3):
                    sig = -rc if rc < 0 else None
                    con.execute(
                        "UPDATE publish_runs SET status='killed', phase='killed', "
                        "reason_code='publisher_killed', "
                        "reason_detail=?, updated_real=? WHERE run_id=?",
                        (f"发布端异常终止 rc={rc}" + (f" signal={sig}" if sig else ""),
                         time.time(), run_id),
                    )
                    db.log_event(
                        con, "publish_killed",
                        f"发布端 run={run_id[:8]} 未正常收尾即退出 rc={rc}，标记 killed",
                        level="error",
                    )


def _resolve_pid(run_id: str) -> int | None:
    with _LOCK:
        proc = _PROCS.get(run_id)
    if proc is not None and proc.poll() is None:
        return proc.pid
    with db.session() as con:
        row = con.execute("SELECT pid FROM publish_runs WHERE run_id=?", (run_id,)).fetchone()
        return int(row["pid"]) if row and row["pid"] else None


def resume_run(run_id: str) -> dict[str, Any]:
    pid = _resolve_pid(run_id)
    if pid is None:
        raise LookupError("找不到该任务的 PID")
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        raise LookupError(f"进程 {pid} 已不存在，无法恢复")
    with db.session() as con:
        db.log_event(con, "signal_cont", f"向 pid={pid} 发送 SIGCONT（run={run_id[:8]}）")
    return {"run_id": run_id, "pid": pid, "signal": "SIGCONT"}


def kill_run(run_id: str) -> dict[str, Any]:
    pid = _resolve_pid(run_id)
    if pid is None:
        raise LookupError("找不到该任务的 PID")
    # 对 SIGSTOP 的进程 SIGKILL 仍然有效
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    time.sleep(0.05)
    # 尝试 waitpid 回收（可能不是本进程 Popen 句柄，忽略错误）
    with _LOCK:
        proc = _PROCS.pop(run_id, None)
    if proc is not None:
        try:
            proc.wait(timeout=3)
        except Exception:
            pass
    with db.session() as con:
        con.execute(
            "UPDATE publish_runs SET status='killed', phase='killed', "
            "reason_code='publisher_sigkill', reason_detail=?, updated_real=? "
            "WHERE run_id=? AND status IN ('running','paused','queued')",
            ("演练台手动 SIGKILL（可用于“生效未回包时中断发布端”）", time.time(), run_id),
        )
        db.log_event(
            con, "signal_kill",
            f"向 pid={pid} 发送 SIGKILL（run={run_id[:8]}）", level="warn",
        )
    return {"run_id": run_id, "pid": pid, "signal": "SIGKILL"}


def recover_publisher() -> dict[str, Any]:
    """“重启发布端”按钮 / Flask 启动时调用。"""
    reap()
    return publisher.recover()


def shutdown_all() -> None:
    """Flask 退出时清理本进程拉起的所有发布端，避免留下 SIGSTOP 的孤儿。"""
    with _LOCK:
        procs = list(_PROCS.values())
    for proc in procs:
        try:
            pid = proc.pid
            os.kill(pid, signal.SIGCONT)  # 停止态先唤醒，TERM 才能被处理
            proc.terminate()
        except Exception:
            pass
    deadline = time.time() + 3
    for proc in procs:
        remain = max(0.0, deadline - time.time())
        try:
            proc.wait(timeout=remain)
        except Exception:
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except Exception:
                pass


def reset_all() -> None:
    """重置整个演练：杀进程 → 删库 / 卷文件 → 重新初始化。"""
    with _LOCK:
        run_ids = list(_PROCS.keys())
    for rid in run_ids:
        try:
            kill_run(rid)
        except Exception:
            pass
    shutdown_all()

    # 删除 SQLite 与 WAL/SHM
    for p in [config.DB_PATH, Path(str(config.DB_PATH) + "-wal"), Path(str(config.DB_PATH) + "-shm")]:
        try:
            p.unlink()
        except FileNotFoundError:
            pass
    # 清空 JSON 卷（只应含 current/staging 等）
    if config.DATA_DIR.exists():
        for child in config.DATA_DIR.iterdir():
            try:
                if child.is_file() or child.is_symlink():
                    child.unlink()
            except OSError:
                pass
    db.init_db()
    configstore_seed()
    # 路由目录也随演练重置：清掉侧车与内存快照后按新播种的 v0 重建
    from . import catalog
    catalog.reset()
    catalog.bootstrap()


def configstore_seed() -> None:
    from . import configstore
    configstore.seed_if_needed()
