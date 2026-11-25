"""租约申请 / 续租 / fencing 校验。

正确性要点：``acquire`` 在 ``BEGIN IMMEDIATE`` 事务里“读现行租约 → 判断 →
发新 token → 写回”，SQLite 的写锁把并发申请串行化，因此两个独立 worker
同时申请时恰好只有一个成功，其余拿到带当前持有者 / 期限的拒绝原因。
"""
from __future__ import annotations

import time
from typing import Any

from . import config, db


class LeaseDenied(Exception):
    def __init__(self, code: str, message: str, current: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.current = current or {}


def _lease_row(con) -> dict[str, Any] | None:
    row = con.execute("SELECT * FROM lease WHERE id=1").fetchone()
    return dict(row) if row else None


def _public_lease(row: dict[str, Any] | None, now: int) -> dict[str, Any] | None:
    if row is None:
        return None
    row = dict(row)
    row["active"] = row["expires_at"] > now
    row["remaining"] = max(0, row["expires_at"] - now)
    row["ttl"] = config.LEASE_TTL
    return row


def get_lease() -> dict[str, Any] | None:
    with db.session() as con:
        now = db.sim_now(con)
        return _public_lease(_lease_row(con), now)


def remember_worker_token(con, holder: str, token: int) -> None:
    con.execute(
        "INSERT INTO worker_state(holder, last_token, updated_real) VALUES(?, ?, ?) "
        "ON CONFLICT(holder) DO UPDATE SET last_token=excluded.last_token, "
        "updated_real=excluded.updated_real",
        (holder, int(token), time.time()),
    )


def worker_tokens() -> dict[str, int]:
    with db.session() as con:
        rows = con.execute("SELECT holder, last_token FROM worker_state").fetchall()
        return {r["holder"]: r["last_token"] for r in rows}


def acquire(holder: str) -> tuple[str, dict[str, Any]]:
    """返回 (granted|held, lease)。竞争失败抛 LeaseDenied。"""
    holder = (holder or "").strip()
    if not holder:
        raise ValueError("holder 不能为空")

    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        now = db.sim_now(con)
        row = _lease_row(con)

        if row is not None and row["expires_at"] > now:
            if row["holder"] == holder:
                remember_worker_token(con, holder, row["token"])
                db.log_event(
                    con, "lease_held",
                    f"{holder} 已持有有效租约 token={row['token']}，到期 t={row['expires_at']}",
                    holder=holder,
                )
                con.execute("COMMIT")
                return "held", _public_lease(row, now)  # type: ignore[return-value]

            current = _public_lease(row, now)
            reason = (
                f"租约被 {row['holder']} 持有（token={row['token']}，"
                f"t={row['expires_at']} 到期，剩余 {row['expires_at'] - now}s），"
                f"{holder} 的并发申请被拒绝"
            )
            db.log_event(con, "lease_denied", reason, level="deny", holder=holder)
            con.execute("COMMIT")
            raise LeaseDenied("held_by_other", reason, current)

        # 无租约或已过期：发放全新的递增 fencing token
        token = int(db.kv_get(con, "next_token", "1"))
        db.kv_set(con, "next_token", str(token + 1))
        expires = now + config.LEASE_TTL
        con.execute(
            "INSERT INTO lease(id, holder, token, expires_at, updated_real) "
            "VALUES(1, ?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "holder=excluded.holder, token=excluded.token, "
            "expires_at=excluded.expires_at, updated_real=excluded.updated_real",
            (holder, token, expires, time.time()),
        )
        remember_worker_token(con, holder, token)
        if row is None:
            msg = f"{holder} 获得新租约，fencing token={token}，期限 t={expires}"
        else:
            msg = (
                f"旧租约（{row['holder']} token={row['token']}）已于 t={row['expires_at']} "
                f"过期；{holder} 接管并获得新 fencing token={token}，期限 t={expires}"
            )
        db.log_event(con, "lease_acquired", msg, level="success", holder=holder)
        con.execute("COMMIT")

        return "granted", _public_lease(_lease_row(con), now)  # type: ignore[return-value]


def renew(holder: str, token: int | None) -> dict[str, Any]:
    holder = (holder or "").strip()
    with db.session() as con:
        con.execute("BEGIN IMMEDIATE")
        now = db.sim_now(con)
        row = _lease_row(con)

        def deny(code: str, detail: str) -> LeaseDenied:
            db.log_event(con, "renew_denied", detail, level="deny", holder=holder)
            con.execute("COMMIT")
            cur = _public_lease(row, now)
            return LeaseDenied(code, detail, cur)

        if row is None or row["expires_at"] <= now:
            raise deny("lease_expired", f"{holder} 续租失败：租约不存在或已在 t={now} 过期，需要重新申请（token 会递增）")
        if row["holder"] != holder:
            raise deny("not_holder", f"{holder} 续租失败：当前持有者是 {row['holder']}（token={row['token']}）")
        if token is not None and int(token) != row["token"]:
            raise deny(
                "token_mismatch",
                f"{holder} 续租失败：提交 token={token} 与现行 token={row['token']} 不一致（fencing 拒绝）",
            )

        expires = now + config.LEASE_TTL
        con.execute(
            "UPDATE lease SET expires_at=?, updated_real=? WHERE id=1",
            (expires, time.time()),
        )
        remember_worker_token(con, holder, row["token"])
        db.log_event(
            con, "lease_renewed",
            f"{holder} 续租成功 token={row['token']}，新期限 t={expires}",
            level="success", holder=holder,
        )
        con.execute("COMMIT")
        return _public_lease(_lease_row(con), now)  # type: ignore[return-value]


def fence_check(con, holder: str, token: int) -> tuple[bool, str, dict[str, Any] | None]:
    """发布端在关键节点调用的 fencing 校验。

    返回 (ok, reason_message, current_lease)。任何不匹配 / 过期都拒绝。
    """
    now = db.sim_now(con)
    row = _lease_row(con)
    cur = _public_lease(row, now)
    if row is None:
        return False, "fencing 拒绝：当前没有任何租约", cur
    if row["expires_at"] <= now:
        return False, (
            f"fencing 拒绝：{holder} 的租约已于 t={row['expires_at']} 过期"
            f"（当前 t={now}），其 token={token} 写入无效"
        ), cur
    if row["holder"] != holder:
        return False, (
            f"fencing 拒绝：持有者已变更为 {row['holder']}（现行 token={row['token']}），"
            f"旧持有者 {holder} token={token} 的写入必须丢弃"
        ), cur
    if int(token) != int(row["token"]):
        return False, (
            f"fencing 拒绝：token 过期，提交 token={token}，"
            f"现行 token={row['token']}（持有者 {row['holder']}）"
        ), cur
    return True, "", cur
