"""独立“发布端”进程：每次发布由 Flask 拉起一个新的 Python 子进程。

它有独立 PID / 文件描述符 / 内存，可以被真实地 SIGSTOP（暂停）、
SIGCONT（恢复）、SIGKILL（中断），从而让演练接近真实的旧进程 / 备用进程接管。

阶段：
    starting
      → lease_check（第一次 fencing）
      → staging（unlink 旧暂存 → 新建 staging.json → 写一半 → 【暂停点1】）
      → switch_pending（暂存完整，【暂停点2：切换已发起但未生效】）
      → commit（SQLite 事务：幂等回执 + fencing + 版本/回执落盘）
      → os.replace 原子切换 current.json（生效）
      → 【暂停点3：生效但回执未返回】
      → done

任何 fencing 失败都在改动 current.json 之前拒绝；网关始终只读 current.json。
"""
from __future__ import annotations

import argparse
import errno
import json
import os
import signal
import sys
import time
import traceback
from typing import Any

# 允许 `python /path/to/drill/publisher.py` 与 `python -m drill.publisher`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drill import config, configstore, db, lease  # noqa: E402


class FenceDenied(Exception):
    pass


class InjectedWriteError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    return p.parse_args()


class Publisher:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.con = db.connect()
        row = self.con.execute(
            "SELECT * FROM publish_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise SystemExit(f"run {run_id} 不存在")
        self.run = dict(row)
        self.request_id = self.run["request_id"]
        self.holder = self.run["holder"]
        self.token = int(self.run["token"])
        self.disabled = json.loads(self.run["disabled_json"] or "[]")
        self.pause_at = self.run["pause_at"] or "none"
        self.staging_fd: int | None = None
        self.bytes_total = 0
        self.bytes_written = 0

    # ---------- run / event helpers ----------

    def update(self, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_real"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [self.run_id]
        self.con.execute(f"UPDATE publish_runs SET {cols} WHERE run_id=?", vals)

    def event(self, kind: str, message: str, level: str = "info") -> None:
        db.log_event(
            self.con, kind, message, level=level,
            holder=self.holder, request_id=self.request_id,
        )

    def pause_here(self, phase: str, message: str) -> None:
        """把任务置为 paused 并真实 SIGSTOP 自己；收到 SIGCONT 后继续。"""
        self.update(phase=phase, status="paused")
        self.event("publish_paused", message, level="warn")
        print(f"[publisher {os.getpid()}] PAUSED at {phase}: {message}", flush=True)
        os.kill(os.getpid(), signal.SIGSTOP)
        # —— 被 SIGCONT 唤醒 ——
        self.update(status="running")
        db.log_event(
            self.con, "publish_resumed",
            f"{self.holder} 的发布从 {phase} 恢复（run={self.run_id[:8]}）",
            holder=self.holder, request_id=self.request_id,
        )
        print(f"[publisher {os.getpid()}] RESUMED at {phase}", flush=True)

    def terminate_run(self, status: str, code: str, detail: str, level: str = "error") -> int:
        try:
            if self.staging_fd is not None:
                os.close(self.staging_fd)
                self.staging_fd = None
        except OSError:
            pass
        self.update(status=status, phase=status, reason_code=code, reason_detail=detail)
        self.event("publish_terminal", f"{status}: {detail}", level=level)
        print(f"[publisher {os.getpid()}] {status}: {code} {detail}", flush=True)
        return 2 if status == "rejected" else 3

    # ---------- main state machine ----------

    def run_publish(self) -> int:
        self.update(pid=os.getpid(), phase="starting", status="running")
        print(
            f"[publisher {os.getpid()}] run={self.run_id} req={self.request_id} "
            f"holder={self.holder} token={self.token} pause_at={self.pause_at}",
            flush=True,
        )

        # 0) 同号回执已存在：直接回放原回执，绝不升版
        row = self.con.execute(
            "SELECT * FROM receipts WHERE request_id=?", (self.request_id,)
        ).fetchone()
        if row is not None:
            body = json.loads(row["body_json"])
            self.update(phase="done", status="done", reason_code="replayed_receipt")
            self.event(
                "publish_replay",
                f"同号请求 {self.request_id} 已有回执 v{row['version']}，直接回放，不升版",
                level="success",
            )
            print(f"[publisher] REPLAY v{row['version']}", flush=True)
            print("RECEIPT " + json.dumps(body, ensure_ascii=False), flush=True)
            return 0

        # 1) 第一次 fencing
        ok, why, _ = lease.fence_check(self.con, self.holder, self.token)
        if not ok:
            return self.terminate_run("rejected", "fence_denied", why, level="deny")

        # 2) 基于最近完整配置构造新配置
        current, from_cache = configstore.load_current()
        if from_cache:
            self.event("gateway_cache", "发布端读取 current.json 失败，使用了内存兜底副本", level="warn")
        cur_version = int(current.get("version", 0))
        new_version = cur_version + 1

        known_order = [r["path"] for r in config.SEED_ROUTES]
        disabled = sorted(set(self.disabled), key=lambda p: known_order.index(p) if p in known_order else 999)
        new_cfg: dict[str, Any] = {
            "version": new_version,
            "request_id": self.request_id,
            "holder": self.holder,
            "token": self.token,
            "published_sim": db.sim_now(self.con),
            "disabled": disabled,
            "routes": current.get("routes", config.SEED_ROUTES),
        }
        payload = json.dumps(new_cfg, ensure_ascii=False, indent=2).encode("utf-8")
        self.bytes_total = len(payload)
        self.update(bytes_total=self.bytes_total, phase="staging")

        # 3) 准备唯一的暂存正文：先 unlink 旧名字。
        #    若旧发布进程仍暂停并持有该 inode，它就变成“已删除但仍打开”的文件。
        try:
            if config.STAGING_PATH.exists() or os.path.lexists(config.STAGING_PATH):
                os.unlink(config.STAGING_PATH)
        except FileNotFoundError:
            pass

        fault = db.kv_get(self.con, "fault_next_write", "") or ""
        if fault:
            db.kv_set(self.con, "fault_next_write", "")
            self.event("fault_armed", f"已消费写入故障注入：{fault}", level="warn")

        fd = os.open(
            str(config.STAGING_PATH),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o644,
        )
        self.staging_fd = fd
        self.update(staging_path=str(config.STAGING_PATH))

        mid = max(1, len(payload) // 2)
        try:
            if fault in ("disk_full", "io_error"):
                code = errno.ENOSPC if fault == "disk_full" else errno.EIO
                name = errno.errorcode.get(code, "FAULT")
                raise OSError(code, f"[{name}] {os.strerror(code)}（演练注入）")

            written = os.write(fd, payload[:mid])
            self.bytes_written = written
            os.fsync(fd)  # 让“写了一半”的大小真实落盘可见
            self.update(bytes_written=self.bytes_written)

            # 暂停点 1：暂存写到一半，fd 仍然打开
            if self.pause_at == "stage_half":
                self.pause_here(
                    "stage_half",
                    f"已写入前半 {self.bytes_written}/{self.bytes_total} 字节，"
                    f"staging.json 的 fd={fd} 保持打开，进程暂停",
                )

            rest = payload[mid:]
            while rest:
                n = os.write(fd, rest)
                self.bytes_written += n
                rest = rest[n:]
            os.fsync(fd)
            os.close(fd)
            self.staging_fd = None
            self.update(bytes_written=self.bytes_written, phase="staged")
        except OSError as exc:
            # 写入失败：保留（可能只写了一半的）staging.json 作为唯一暂存正文，
            # 绝不动 current.json —— 网关继续按最近完整配置响应。
            return self.terminate_run(
                "failed", "write_failed",
                f"暂存写入失败：{exc.__class__.__name__}: {exc}",
            )

        # 4) 暂停点 2：切换已发起但未生效（rename/replace 尚未执行）
        self.update(phase="before_switch")
        if self.pause_at == "before_switch":
            self.pause_here(
                "before_switch",
                "暂存正文已完整并 fsync；切换意图已记录，但 os.replace 尚未执行，"
                "网关仍读取旧 current.json。进程暂停",
            )

        # 5) 提交事务：幂等 + 最终 fencing + 版本号 + 回执
        try:
            final_cfg, receipt = self.commit_transaction(new_cfg, payload)
        except FenceDenied as exc:
            return self.terminate_run("rejected", "fence_denied", str(exc), level="deny")

        final_bytes = json.dumps(final_cfg, ensure_ascii=False, indent=2).encode("utf-8")

        # 6) 若版本号在提交时被序列化成别的值（并发提交），对齐暂存正文
        if final_bytes != payload:
            with open(config.STAGING_PATH, "wb") as f:
                f.write(final_bytes)
                f.flush()
                os.fsync(f.fileno())

        # 7) 原子切换 —— 对网关来说配置在此刻生效
        os.replace(config.STAGING_PATH, config.CURRENT_PATH)
        configstore._fsync_dir(config.DATA_DIR)
        self.con.execute(
            "UPDATE versions SET applied=1 WHERE version=?", (receipt["version"],)
        )
        self.event(
            "publish_effective",
            f"current.json 已原子切换到 v{receipt['version']}（请求 {self.request_id}）",
            level="success",
        )

        # 8) 暂停点 3：已生效、回执已落库，但还没“回包”
        self.update(phase="effective_no_response")
        if self.pause_at == "effective_no_response":
            self.pause_here(
                "effective_no_response",
                f"v{receipt['version']} 已生效且回执已写 SQLite，但响应尚未返回；"
                f"此刻可强杀发布端，同号重发必须回放本原回执",
            )

        body = receipt
        self.update(phase="done", status="done", reason_code="")
        print("RECEIPT " + json.dumps(body, ensure_ascii=False), flush=True)
        print(f"[publisher {os.getpid()}] DONE v{receipt['version']}", flush=True)
        return 0

    def commit_transaction(self, candidate_cfg: dict[str, Any], payload: bytes):
        con = self.con
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                "SELECT * FROM receipts WHERE request_id=?", (self.request_id,)
            ).fetchone()
            if row is not None:
                body = json.loads(row["body_json"])
                con.execute("ROLLBACK")
                return json.loads(row["body_json"]).get("config", candidate_cfg), body

            ok, why, _ = lease.fence_check(con, self.holder, self.token)
            if not ok:
                con.execute("ROLLBACK")
                raise FenceDenied(why)

            next_v_row = con.execute("SELECT COALESCE(MAX(version), 0) + 1 AS v FROM versions").fetchone()
            next_v = int(next_v_row["v"])
            candidate_cfg["version"] = next_v
            candidate_cfg["published_sim"] = db.sim_now(con)
            cfg_json = json.dumps(candidate_cfg, ensure_ascii=False)

            now_real = time.time()
            con.execute(
                "INSERT INTO versions(version, request_id, holder, token, config_json, "
                "applied, created_sim, created_real) VALUES(?, ?, ?, ?, ?, 0, ?, ?)",
                (next_v, self.request_id, self.holder, self.token, cfg_json,
                 candidate_cfg["published_sim"], now_real),
            )
            body = {
                "request_id": self.request_id,
                "version": next_v,
                "holder": self.holder,
                "token": self.token,
                "config": candidate_cfg,
                "committed_real": now_real,
            }
            body_json = json.dumps(body, ensure_ascii=False)
            con.execute(
                "INSERT INTO receipts(request_id, version, holder, token, body_json, "
                "recovered, created_sim, created_real) VALUES(?, ?, ?, ?, ?, 0, ?, ?)",
                (self.request_id, next_v, self.holder, self.token, body_json,
                 candidate_cfg["published_sim"], now_real),
            )
            con.execute("COMMIT")
            return candidate_cfg, body
        except FenceDenied:
            raise
        except Exception:
            con.execute("ROLLBACK")
            raise


def recover() -> dict[str, Any]:
    """发布端重启后的恢复：认回租约 / token，补完未落盘的切换，清理失联任务。"""
    report: dict[str, Any] = {"recovered_runs": [], "pending_switches": [], "lease": None}
    with db.session() as con:
        now = db.sim_now(con)
        row = con.execute("SELECT * FROM lease WHERE id=1").fetchone()
        if row:
            d = dict(row)
            d["active"] = d["expires_at"] > now
            d["remaining"] = max(0, d["expires_at"] - now)
            report["lease"] = d

        # 补完“版本/回执已提交、文件切换未完成”的发布
        pending = con.execute(
            "SELECT version, request_id, config_json FROM versions WHERE applied=0"
        ).fetchall()
        for v in pending:
            cfg = json.loads(v["config_json"])
            configstore.write_current_bytes(
                json.dumps(cfg, ensure_ascii=False, indent=2).encode("utf-8")
            )
            con.execute("UPDATE versions SET applied=1 WHERE version=?", (v["version"],))
            report["pending_switches"].append(
                {"version": v["version"], "request_id": v["request_id"]}
            )

        # 失联的 running/paused 任务：pid 已死就标记 interrupted
        runs = con.execute(
            "SELECT run_id, pid, request_id, holder, phase, status FROM publish_runs "
            "WHERE status IN ('running','paused','queued')"
        ).fetchall()
        for r in runs:
            alive = False
            if r["pid"]:
                try:
                    os.kill(int(r["pid"]), 0)
                    alive = True
                except (ProcessLookupError, PermissionError):
                    alive = False
            if not alive:
                con.execute(
                    "UPDATE publish_runs SET status='interrupted', phase='interrupted', "
                    "reason_code=COALESCE(reason_code,'publisher_gone'), "
                    "reason_detail=COALESCE(reason_detail, ?), updated_real=? "
                    "WHERE run_id=?",
                    (f"发布端进程已不存在（重启恢复，run={r['run_id'][:8]}）", time.time(), r["run_id"]),
                )
                report["recovered_runs"].append(r["run_id"])

        db.log_event(
            con, "publisher_recover",
            f"发布端恢复完成：租约={row['holder'] if row else '无'} "
            f"token={row['token'] if row else '-'}，"
            f"补切换 {len(report['pending_switches'])} 个，"
            f"失联任务 {len(report['recovered_runs'])} 个",
            level="success",
            holder=row["holder"] if row else None,
        )
    return report


def main() -> int:
    args = parse_args()
    db.init_db()
    configstore.seed_if_needed()
    pub = Publisher(args.run_id)
    try:
        return pub.run_publish()
    except Exception as exc:  # noqa: BLE001 - 演练工具：任何异常都落库可见
        detail = f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc(limit=4)}"
        try:
            code = pub.terminate_run("failed", "unexpected_error", detail)
            print(detail, file=sys.stderr, flush=True)
            return code
        except Exception:
            print(detail, file=sys.stderr, flush=True)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
