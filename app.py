#!/usr/bin/env python3
"""GSB 网关维护演练台 —— Flask 入口。

职责：
1. 渲染演练页；
2. ``/gw/...`` 网关：从本地卷读取 current.json，停用路由返回 503；
3. 演练控制 API：模拟时钟、两 worker 租约、发布子进程、暂停 / 恢复 / 强杀 /
   重启恢复、故障注入、磁盘核算、事件与回执查询。

启动：``python app.py``（单进程 + 多线程；发布端是独立子进程）。
"""
from __future__ import annotations

import atexit
import json
import signal
import time
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request, Response

from drill import catalog, config, configstore, db, disk, lease, runner

app = Flask(__name__, template_folder="templates", static_folder="static")


# ---------------------------------------------------------------------------
# 启动初始化与进程清理
# ---------------------------------------------------------------------------

def bootstrap() -> None:
    config.ensure_dirs()
    db.init_db()
    configstore.seed_if_needed()
    # 建立首代路由目录快照（含侧车装载 / 全量解析兜底）并启动后台巡检线程
    catalog.bootstrap()
    # 启动即做一次恢复：认回租约 / token，处理上次未完成的切换与失联任务
    try:
        runner.recover_publisher()
    except Exception as exc:  # noqa: BLE001
        print(f"[bootstrap] recover failed: {exc}", flush=True)


bootstrap()
atexit.register(runner.shutdown_all)
atexit.register(catalog.shutdown)


def _sigterm_handler(signum, frame):  # noqa: ANN001
    runner.shutdown_all()
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT, _sigterm_handler)


# ---------------------------------------------------------------------------
# 页面
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return render_template("index.html", lease_ttl=config.LEASE_TTL)


@app.get("/api/health")
def health():
    return jsonify(ok=True, ts=time.time())


# ---------------------------------------------------------------------------
# 状态总览
# ---------------------------------------------------------------------------

@app.get("/api/state")
def get_state():
    runner.reap()
    now = int(time.time())
    with db.session() as con:
        sim = db.sim_now(con)
        lease_row = con.execute("SELECT * FROM lease WHERE id=1").fetchone()
        lease_pub = lease._public_lease(dict(lease_row) if lease_row else None, sim)
        workers = {
            r["holder"]: r["last_token"]
            for r in con.execute("SELECT holder, last_token FROM worker_state")
        }
        runs = [
            dict(r) for r in con.execute(
                "SELECT * FROM publish_runs ORDER BY started_real DESC LIMIT 25"
            )
        ]
        receipts = [
            {k: r[k] for k in r.keys() if k != "body_json"} | {"body": json.loads(r["body_json"])}
            for r in con.execute(
                "SELECT * FROM receipts ORDER BY created_real DESC LIMIT 12"
            )
        ]
        versions = [
            {k: r[k] for k in r.keys() if k != "config_json"}
            for r in con.execute(
                "SELECT version, request_id, holder, token, applied, "
                "created_sim, created_real FROM versions ORDER BY version DESC LIMIT 12"
            )
        ]
        events = [dict(r) for r in con.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 40"
        )]
        fault = db.kv_get(con, "fault_next_write", "") or ""
        next_token = int(db.kv_get(con, "next_token", "1"))

    cfg, from_cache = catalog.current_config_dict()
    # 万条路由时每秒轮询的 /api/state 不把整表塞给前端：截断展示，全量在网关侧
    route_total = len(cfg.get("routes", []))
    cfg_view = dict(cfg)
    STATE_ROUTE_LIMIT = 200
    truncated = False
    if route_total > STATE_ROUTE_LIMIT:
        cfg_view["routes"] = cfg["routes"][:STATE_ROUTE_LIMIT]
        truncated = True
    try:
        disk_snap = disk.snapshot()
    except Exception as exc:  # noqa: BLE001
        disk_snap = {"error": f"{exc.__class__.__name__}: {exc}"}

    # 给运行中的任务补一个真实进程状态（/proc/<pid>/stat 的 R/S/T/Z）
    for run in runs:
        run["proc_state"] = _proc_state(run.get("pid"))

    return jsonify(
        server_real=now,
        sim_now=sim,
        lease_ttl=config.LEASE_TTL,
        next_token=next_token,
        lease=lease_pub,
        worker_tokens=workers,
        config=cfg_view,
        config_from_cache=from_cache,
        config_routes_total=route_total,
        config_routes_truncated=truncated,
        catalog=catalog.stats(),
        disk=disk_snap,
        runs=runs,
        receipts=receipts,
        versions=versions,
        events=events,
        fault_next_write=fault,
        pause_points=list(config.PAUSE_POINTS),
    )


def _proc_state(pid: int | None) -> str | None:
    if not pid:
        return None
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        # pid (comm) state ...
        rest = stat[stat.rfind(")") + 2:]
        return rest.split(" ", 1)[0]
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None


# ---------------------------------------------------------------------------
# 模拟时钟
# ---------------------------------------------------------------------------

@app.post("/api/clock/advance")
def clock_advance():
    data = request.get_json(silent=True) or {}
    with db.session() as con:
        now = db.sim_now(con)
        row = con.execute("SELECT expires_at FROM lease WHERE id=1").fetchone()

    if data.get("to_expire"):
        if not row:
            return jsonify(error="当前没有租约，无法按到期点推进"), 400
        seconds = max(1, int(row["expires_at"]) - now + 1)
    else:
        seconds = int(data.get("seconds", 0))
        if seconds <= 0:
            return jsonify(error="seconds 必须为正整数"), 400
    new_now = db.advance_clock(seconds)
    return jsonify(ok=True, advanced=seconds, sim_now=new_now)


# ---------------------------------------------------------------------------
# 租约
# ---------------------------------------------------------------------------

@app.post("/api/lease/acquire")
def api_acquire():
    data = request.get_json(silent=True) or {}
    holder = (data.get("holder") or "").strip()
    if not holder:
        return jsonify(error="holder 必填"), 400
    try:
        status, lic = lease.acquire(holder)
        return jsonify(ok=True, status=status, lease=lic)
    except lease.LeaseDenied as denied:
        return jsonify(
            ok=False,
            error_code=denied.code,
            error=denied.message,
            current=denied.current,
        ), 409


@app.post("/api/lease/renew")
def api_renew():
    data = request.get_json(silent=True) or {}
    holder = (data.get("holder") or "").strip()
    token = data.get("token")
    if not holder:
        return jsonify(error="holder 必填"), 400
    try:
        token_i = int(token) if token is not None and token != "" else None
    except (TypeError, ValueError):
        return jsonify(error="token 必须是整数"), 400
    try:
        lic = lease.renew(holder, token_i)
        return jsonify(ok=True, lease=lic)
    except lease.LeaseDenied as denied:
        return jsonify(
            ok=False,
            error_code=denied.code,
            error=denied.message,
            current=denied.current,
        ), 409


# ---------------------------------------------------------------------------
# 发布
# ---------------------------------------------------------------------------

@app.post("/api/publish")
def api_publish():
    data = request.get_json(silent=True) or {}
    holder = (data.get("holder") or "").strip()
    if not holder:
        return jsonify(error="holder 必填"), 400

    request_id = (data.get("request_id") or "").strip()
    if not request_id:
        request_id = f"req-{holder}-{int(time.time() * 1000)}-{db.new_id()[:6]}"

    disabled = data.get("disabled", [])
    if not isinstance(disabled, list) or not all(isinstance(x, str) for x in disabled):
        return jsonify(error="disabled 必须是字符串数组"), 400

    pause_at = data.get("pause_at", "none") or "none"
    if pause_at not in config.PAUSE_POINTS:
        return jsonify(error=f"pause_at 必须是 {config.PAUSE_POINTS} 之一"), 400

    with db.session() as con:
        # 1) 同号回执最高优先级：原进程哪怕仍暂停，也直接返回原始回执
        row = con.execute(
            "SELECT * FROM receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is not None:
            body = json.loads(row["body_json"])
            return jsonify(ok=True, replayed=True, receipt=body)

        # 2) 同号任务仍在途（运行 / 暂停）且没有回执：提示在途，不重复发布
        active = con.execute(
            "SELECT run_id, phase, status, pid FROM publish_runs "
            "WHERE request_id=? AND status IN ('running','paused','queued') "
            "ORDER BY started_real DESC LIMIT 1",
            (request_id,),
        ).fetchone()
        if active is not None:
            return jsonify(
                ok=False, error_code="request_in_flight",
                error=f"同号请求 {request_id} 仍在途（{active['status']}/{active['phase']}），"
                      f"请等待、恢复或强杀原发布端；生效后同号重发会回放原回执",
                run=dict(active),
            ), 409

        # 3) worker 自认 token（前端也可显式传入）
        token = data.get("token")
        if token is None or token == "":
            ws = con.execute(
                "SELECT last_token FROM worker_state WHERE holder=?", (holder,)
            ).fetchone()
            token = ws["last_token"] if ws else None
        try:
            token_i = int(token)
        except (TypeError, ValueError):
            token_i = -1

        # 4) 同步预 fencing，给出快速拒绝；发布端子进程还会在切换前再校验
        ok, why, current_lic = lease.fence_check(con, holder, token_i)
        if not ok:
            now_real = time.time()
            run_id = db.new_id()
            con.execute(
                "INSERT INTO publish_runs(run_id, request_id, holder, token, "
                "disabled_json, pause_at, phase, status, reason_code, reason_detail, "
                "created_sim, started_real, updated_real) "
                "VALUES(?, ?, ?, ?, ?, ?, 'rejected', 'rejected', 'fence_denied', ?, ?, ?, ?)",
                (run_id, request_id, holder, token_i,
                 json.dumps(disabled, ensure_ascii=False), pause_at, why,
                 db.sim_now(con), now_real, now_real),
            )
            db.log_event(con, "publish_denied", why, level="deny",
                         holder=holder, request_id=request_id)
            return jsonify(
                ok=False, error_code="fence_denied", error=why,
                current=current_lic, run_id=run_id,
            ), 409

    info = runner.start_publish(
        request_id=request_id, holder=holder, token=token_i,
        disabled=disabled, pause_at=pause_at,
    )
    return jsonify(ok=True, accepted=True, request_id=request_id, **info), 202


@app.get("/api/runs/<run_id>")
def api_run_detail(run_id: str):
    runner.reap()
    with db.session() as con:
        row = con.execute("SELECT * FROM publish_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row:
            return jsonify(error="run 不存在"), 404
        d = dict(row)
        d["proc_state"] = _proc_state(d.get("pid"))
    return jsonify(ok=True, run=d)


@app.post("/api/runs/<run_id>/resume")
def api_run_resume(run_id: str):
    try:
        return jsonify(ok=True, **runner.resume_run(run_id))
    except LookupError as exc:
        return jsonify(ok=False, error=str(exc)), 404


@app.post("/api/runs/<run_id>/kill")
def api_run_kill(run_id: str):
    try:
        return jsonify(ok=True, **runner.kill_run(run_id))
    except LookupError as exc:
        return jsonify(ok=False, error=str(exc)), 404


@app.get("/api/receipts/<path:request_id>")
def api_receipt(request_id: str):
    with db.session() as con:
        row = con.execute(
            "SELECT * FROM receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if not row:
            return jsonify(ok=False, error_code="not_found",
                           error=f"请求 {request_id} 暂无回执（可能尚未生效）"), 404
        return jsonify(ok=True, receipt=json.loads(row["body_json"]),
                       recovered=bool(row["recovered"]))


# ---------------------------------------------------------------------------
# 发布端重启恢复 / 故障注入 / 重置
# ---------------------------------------------------------------------------

@app.post("/api/publisher/recover")
def api_recover():
    report = runner.recover_publisher()
    return jsonify(ok=True, report=report)


@app.post("/api/fault")
def api_fault():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode", "")
    if mode not in ("", "disk_full", "io_error"):
        return jsonify(error="mode 只能是 '' | disk_full | io_error"), 400
    with db.session() as con:
        db.kv_set(con, "fault_next_write", mode)
        db.log_event(
            con, "fault_inject",
            "已武装下一次暂存写入失败：" + (mode or "清除"),
            level="warn",
        )
    return jsonify(ok=True, fault_next_write=mode)


@app.post("/api/admin/reset")
def api_reset():
    runner.reset_all()
    return jsonify(ok=True)


# ---------------------------------------------------------------------------
# 网关：/gw/... 只读本地卷 current.json（经路由目录做 O(1) 判定）
# ---------------------------------------------------------------------------

@app.route("/gw", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
@app.route("/gw/", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
@app.route("/gw/<path:subpath>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def gateway(subpath: str = ""):
    route_path = "/" + subpath if subpath else "/"
    if route_path != "/" and route_path.endswith("/"):
        route_path = route_path.rstrip("/")

    # 唯一判定点：一次 stat 指纹检查 + 两次 O(1) 集合查询。
    # 不重读整份配置、不遍历路由表 / 停用清单、不等待索引重建；
    # 返回的 snapshot 同时是状态码与 X-Config-Version 的唯一数据源。
    snap, decision = catalog.decide(route_path)
    version = snap.version

    headers = {"X-Config-Version": str(version), "Cache-Control": "no-store"}
    if snap.from_cache:
        headers["X-Config-Source"] = "last-good-cache"
    headers["X-Config-Index"] = snap.source

    if decision == "disabled":
        body = {
            "ok": False,
            "error": "route_disabled",
            "message": f"路由 {route_path} 在维护停用清单中，网关返回 503",
            "path": route_path,
            "config_version": version,
        }
        resp = jsonify(body)
        resp.status_code = 503
        resp.headers.update(headers)
        resp.headers["Retry-After"] = str(config.LEASE_TTL)
        return resp

    if decision == "not_found":
        resp = jsonify({
            "ok": False, "error": "route_not_found",
            "message": f"网关配置中没有路由 {route_path}",
            "path": route_path, "config_version": version,
        })
        resp.status_code = 404
        resp.headers.update(headers)
        return resp

    # “其余照常”：模拟上游正常应答
    payload = {
        "ok": True,
        "served_by": "mock-upstream",
        "method": request.method,
        "path": route_path,
        "query": request.args.to_dict(flat=True),
        "config_version": version,
        "echo_json": request.get_json(silent=True),
        "timestamp_real": time.time(),
    }
    if route_path == "/":
        payload["gateway"] = "GSB drill gateway"
        payload["routes"] = [dict(r) for r in snap.route_index.routes]
        payload["disabled"] = sorted(snap.disabled)
    resp = jsonify(payload)
    resp.status_code = 200
    resp.headers.update(headers)
    return resp


if __name__ == "__main__":
    print(f"[boot] data dir = {config.DATA_DIR}")
    print(f"[boot] db       = {config.DB_PATH}")
    # 关键：关闭 reloader（否则会有两份进程与重复子进程管理），threaded 支持并发申请
    app.run(host=config.HOST, port=config.PORT, threaded=True,
            debug=False, use_reloader=False)
