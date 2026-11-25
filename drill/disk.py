"""本地卷磁盘核算。

演练约束：卷上最多保留 ``current.json``（当前配置）和 ``staging.json``
（一份暂存正文）。当旧发布进程暂停且仍打开 staging.json 时，备用进程
unlink 该名字再建新文件，旧 inode 变成“已删除但仍被打开”的文件——
目录里看不见，但磁盘 / inode 仍被占用，必须计入核算。

Linux 下通过 ``/proc/<pid>/fd/<n>`` 真实扫描这些句柄，而不只是记一笔账。
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from . import config, db


def _scan_proc_fds(root: Path) -> list[dict[str, Any]]:
    root_str = str(root.resolve())
    out: list[dict[str, Any]] = []
    proc = Path("/proc")
    if not proc.exists():
        return out

    run_index = _run_pid_index()

    for pid_dir in proc.glob("[0-9]*"):
        pid = int(pid_dir.name)
        fd_dir = pid_dir / "fd"
        try:
            fds = list(fd_dir.iterdir())
        except (PermissionError, FileNotFoundError, ProcessLookupError, OSError):
            continue
        for fd_path in fds:
            try:
                link = os.readlink(fd_path)
            except (OSError, PermissionError):
                continue
            deleted = link.endswith(" (deleted)")
            real = link[: -len(" (deleted)")] if deleted else link
            if not (real == root_str or real.startswith(root_str + os.sep)):
                continue
            try:
                st = os.stat(str(fd_path))  # 沿 fd fstat，已删除 inode 也能取大小
            except OSError:
                continue
            item: dict[str, Any] = {
                "pid": pid,
                "fd": fd_path.name,
                "path": real,
                "deleted_open": deleted,
                "size": st.st_size,
                "blocks": st.st_blocks * 512,
                "inode": st.st_ino,
                "dev": st.st_dev,
            }
            if pid in run_index:
                item.update(run_index[pid])
            out.append(item)
    return out


def _run_pid_index() -> dict[int, dict[str, Any]]:
    idx: dict[int, dict[str, Any]] = {}
    try:
        with db.session() as con:
            rows = con.execute(
                "SELECT pid, holder, request_id, run_id, phase, status, token "
                "FROM publish_runs WHERE pid IS NOT NULL "
                "AND status IN ('running','paused')"
            ).fetchall()
    except Exception:
        return idx
    for r in rows:
        idx[r["pid"]] = {
            "holder": r["holder"],
            "request_id": r["request_id"],
            "run_id": r["run_id"],
            "phase": r["phase"],
            "run_status": r["status"],
            "token": r["token"],
        }
    return idx


def snapshot() -> dict[str, Any]:
    config.ensure_dirs()
    root = config.DATA_DIR

    named: list[dict[str, Any]] = []
    inode_keys: set[tuple[int, int]] = set()
    body_names = {"current.json", "staging.json"}
    body_count = 0

    for p in sorted(root.iterdir()):
        if not p.is_file() and not p.is_symlink():
            continue
        try:
            st = p.stat()
        except OSError:
            continue
        is_body = p.name in body_names
        if is_body:
            body_count += 1
        item = {
            "name": p.name,
            "is_body": is_body,
            "size": st.st_size,
            "inode": st.st_ino,
            "dev": st.st_dev,
        }
        named.append(item)
        inode_keys.add((st.st_dev, st.st_ino))

    open_files = _scan_proc_fds(root)

    # 唯一 inode 口径：已删除但打开的文件没有目录项，需要额外加；
    # 未删除的打开句柄与 named 指向同一 inode，不重复计算。
    extra_bytes = 0
    ghosts: list[dict[str, Any]] = []
    for f in open_files:
        key = (f["dev"], f["inode"])
        if f["deleted_open"]:
            ghosts.append(f)
        if key not in inode_keys:
            extra_bytes += f["size"]
            inode_keys.add(key)

    named_bytes = sum(f["size"] for f in named)
    total_bytes = named_bytes + extra_bytes

    unexpected = [
        f["name"] for f in named
        if not f["is_body"] and not f["name"].endswith(".seed.tmp")
    ]
    slot_invariant_ok = body_count <= 2 and not unexpected

    return {
        "data_dir": str(root),
        "quota_bytes": config.DISK_QUOTA_BYTES,
        "named_files": named,
        "open_files": open_files,
        "ghost_files": ghosts,
        "ghost_bytes": extra_bytes,
        "named_bytes": named_bytes,
        "total_bytes": total_bytes,
        "quota_used_pct": round(100.0 * total_bytes / max(1, config.DISK_QUOTA_BYTES), 2),
        "body_file_count": body_count,
        "slot_invariant_ok": slot_invariant_ok,
        "unexpected_files": unexpected,
    }
