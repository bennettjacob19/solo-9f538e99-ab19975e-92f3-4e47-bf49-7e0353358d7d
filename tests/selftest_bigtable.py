#!/usr/bin/env python3
"""大路由表专项端到端自测（默认 12000 条路由）。

验证优化目标（题目逐条对应）：

1. 网关处理单个请求不再重读整份配置：热路径仅 stat + O(1) 集合判定
   （catalog 指标 hot_path_hits 增长；除冷启动建首代目录的一次全量解析外，
   运行期 full_parse_fallbacks 不再增加）；
2. 切换后的【首个请求】（关闭后台巡检，排除预热）不遍历路由表 / 停用清单、
   不等索引重建：sidecar_adoptions +1，full_parse_fallbacks 不增；
3. 只改停用清单时复用路由目录（route_index_reuses 增长、指纹不变）；
4. 发布暂停在切换前 / 写入失败时，网关仍按旧版响应（以 current.json 实际
   切换为准）；
5. 状态码与 X-Config-Version 取自同一快照（每个响应都核对一致性）；
6. 恢复切换 / 重置后首个请求即按实际生效配置响应，即使版号复用
   （重置后 v0 重新播种，再发布又从 v1 开始）；
7. 硬重启（SIGKILL 整个服务进程组后用同一卷重启）后首个请求同样 O(1) 正确。

只用标准库；由当前解释器真实启动 app.py（需要 Flask）。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
PASS_COUNT = 0
FAILURES: list[str] = []
ROUTE_COUNT = int(os.environ.get("DRILL_BIG_ROUTES", "12000"))
SEED_ROUTE_COUNT = 6  # 与 drill.config.SEED_ROUTES 内置条数保持一致


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT
    if cond:
        PASS_COUNT += 1
        print(f"  \033[32mPASS\033[0m {name}")
    else:
        FAILURES.append(name + (f" :: {detail}" if detail else ""))
        print(f"  \033[31mFAIL\033[0m {name} {detail}")


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Client:
    def __init__(self, base: str):
        self.base = base

    def call(self, method: str, path: str, body=None):
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode()
                return resp.status, dict(resp.headers), json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read().decode()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"raw": raw}
            return e.code, dict(e.headers), parsed

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body=None):
        return self.call("POST", path, body or {})

    def state(self):
        return self.get("/api/state")[2]

    def gw(self, path):
        return self.get("/gw" + path)

    def wait_run(self, run_id, timeout=40.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            _, _, d = self.get(f"/api/runs/{run_id}")
            last = d["run"]
            if last["status"] in ("paused", "done", "rejected", "failed",
                                  "killed", "interrupted"):
                return last
            time.sleep(0.1)
        raise AssertionError(f"wait_run timeout {run_id}: {last}")


class Server:
    def __init__(self, route_count: int, no_watcher: bool = False):
        self.tmp = tempfile.mkdtemp(prefix="drill-big-")
        self.data = Path(self.tmp) / "config"
        self.state_dir = Path(self.tmp) / "state"
        self.data.mkdir()
        self.state_dir.mkdir()
        self.port = free_port()
        self.log = open(Path(self.tmp) / "server.log", "ab")
        self.no_watcher = no_watcher
        self.proc = self._spawn(route_count, no_watcher)
        self.c = Client(f"http://127.0.0.1:{self.port}")
        self._wait_health()
        # 冷启动建首代目录的一次性全量解析基线；之后任何场景都不应再增加
        self.fallback_baseline = self.c.state()["catalog"]["full_parse_fallbacks"]

    def _env(self, route_count: int, no_watcher: bool):
        env = dict(os.environ)
        env.update({
            "DRILL_DATA_DIR": str(self.data),
            "DRILL_STATE_DIR": str(self.state_dir),
            "DRILL_PORT": str(self.port),
            "DRILL_LEASE_TTL": "30",
            "DRILL_ROUTE_COUNT": str(route_count),
            "DRILL_CATALOG_NO_WATCHER": "1" if no_watcher else "",
            "PYTHONPATH": str(ROOT),
        })
        return env

    def _spawn(self, route_count: int, no_watcher: bool):
        return subprocess.Popen(
            [sys.executable, str(ROOT / "app.py")],
            cwd=str(ROOT), env=self._env(route_count, no_watcher),
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True,
        )

    def _wait_health(self):
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("server exited early; log:\n" + self.log_text())
            try:
                if self.c.get("/api/health")[0] == 200:
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise RuntimeError("health timeout; log:\n" + self.log_text())

    def hard_restart(self, route_count: int | None = None):
        rc = ROUTE_COUNT if route_count is None else route_count
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=5)
        time.sleep(0.3)
        self.log.close()
        self.log = open(Path(self.tmp) / "server.log", "ab")
        self.proc = self._spawn(rc, self.no_watcher)
        self._wait_health()
        # 重启后若侧车可用应为 0；该值作为重启后场景的新基线
        self.fallback_baseline = self.c.state()["catalog"]["full_parse_fallbacks"]

    def log_text(self):
        self.log.flush()
        try:
            return (Path(self.tmp) / "server.log").read_text(errors="replace")[-5000:]
        except Exception:
            return "(no log)"

    def stop(self):
        try:
            self.c.post("/api/admin/reset", {})
            time.sleep(0.2)
        except Exception:
            pass
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
            self.proc.wait(timeout=5)
        except Exception:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except Exception:
                pass
        self.log.close()


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def acquire(s: Server, holder: str):
    return s.c.post("/api/lease/acquire", {"holder": holder})


def publish(s: Server, holder: str, token: int, req: str, disabled, pause="none"):
    st, _, d = s.c.post("/api/publish", {
        "holder": holder, "token": token, "request_id": req,
        "disabled": disabled, "pause_at": pause,
    })
    return st, d


def assert_same_version(s: Server, path: str, expect_status: int, expect_version):
    """状态码与 X-Config-Version 必须取自同一版：头版本 == 响应体 config_version。"""
    st, hdr, body = s.c.gw(path)
    check(f"{path} -> HTTP {expect_status} v{expect_version}",
          st == expect_status and hdr.get("X-Config-Version") == str(expect_version),
          f"got {st}/{hdr.get('X-Config-Version')}")
    check(f"{path} body config_version 与响应头同源",
          isinstance(body, dict) and body.get("config_version") == expect_version,
          str(body)[:120])
    return st, hdr, body


def gen_path(i: int) -> str:
    return f"/api/gen/{i:06d}"


# ---------------------------------------------------------------------------
# 场景
# ---------------------------------------------------------------------------

def test_seed_and_hot_path(s: Server):
    print(f"\n[1] 播种 {ROUTE_COUNT} 条路由：冷启动一次全量建目录，之后纯热路径")
    st = s.c.state()
    check("state 报告完整路由总数", st["config_routes_total"] == ROUTE_COUNT,
          str(st["config_routes_total"]))
    check("state 路由列表已截断展示", st["config_routes_truncated"] is True
          and len(st["config"]["routes"]) == 200)
    cat = st["catalog"]
    check("内存目录持有全量路由", cat["current"]["route_count"] == ROUTE_COUNT,
          str(cat["current"]))
    # 冷启动时尚无侧车指针，允许且仅允许 1 次全量解析用于建立首代目录
    check("冷启动仅 1 次全量解析（首代建目录）", cat["full_parse_fallbacks"] == 1, str(cat))
    check("启动建立了 1 份路由目录", cat["route_index_builds"] == 1, str(cat))
    fallback_baseline = cat["full_parse_fallbacks"]

    # 多个请求：含生成路由（头尾各一）、内置路由、未知路由
    samples = [gen_path(0), gen_path(ROUTE_COUNT - SEED_ROUTE_COUNT - 1),
               "/health", "/api/checkout", "/no/such/route"]
    t0 = time.perf_counter()
    for p in samples:
        s.c.gw(p)
    elapsed_ms = (time.perf_counter() - t0) * 1000 / len(samples)
    cat = s.c.state()["catalog"]
    check("全部请求走热路径命中", cat["hot_path_hits"] >= len(samples), str(cat))
    check("热路径期间零新增全量解析", cat["full_parse_fallbacks"] == fallback_baseline, str(cat))
    print(f"  -- 平均单请求网关耗时 {elapsed_ms:.3f} ms（{ROUTE_COUNT} 条路由）")

    assert_same_version(s, "/health", 200, 0)
    assert_same_version(s, gen_path(7), 200, 0)
    assert_same_version(s, "/no/such/route", 404, 0)


def test_switch_first_request(s: Server):
    print("\n[2] 切换后的首个请求（无后台巡检）：O(1) 侧车装载，不遍历/不等待")
    acquire(s, "A")
    target = gen_path(123)
    before = s.c.state()["catalog"]
    fp_before = before["current"]["route_fingerprint"]
    adopt0 = before["sidecar_adoptions"]
    change0 = before["sig_changes_detected"]
    fallback0 = before["full_parse_fallbacks"]

    st, d = publish(s, "A", 1, "req-big-1", [target])
    check("publish accepted", st == 202, str(d)[:200])
    # wait_run 只访问 /api/runs，不触碰网关目录，保证下一个 /gw 是切换后首请求
    run = s.c.wait_run(d["run_id"])
    check("run done", run["status"] == "done", run.get("reason_detail", ""))

    t0 = time.perf_counter()
    st, hdr, body = s.c.gw(target)
    first_ms = (time.perf_counter() - t0) * 1000
    check("首个请求即 503 且版本头=1",
          st == 503 and hdr.get("X-Config-Version") == "1"
          and body.get("config_version") == 1, f"{st}/{hdr}")
    print(f"  -- 切换后首个请求（含装载）耗时 {first_ms:.3f} ms")

    cat = s.c.state()["catalog"]
    check("首个请求自身恰好完成 1 次代际检测与侧车装载（无后台预热）",
          cat["sig_changes_detected"] == change0 + 1
          and cat["sidecar_adoptions"] == adopt0 + 1, str(cat))
    check("首个请求未走任何全量解析", cat["full_parse_fallbacks"] == fallback0, str(cat))
    check("新快照版本为 v1", cat["current"]["version"] == 1, str(cat["current"]))
    check("停用集合已生效（503 判定数据来自指针）",
          target in s.c.state()["config"]["disabled"])

    # 第二个请求回到纯热路径
    s.c.gw(target)
    cat2 = s.c.state()["catalog"]
    check("后续请求仅增加热路径命中",
          cat2["hot_path_hits"] > cat["hot_path_hits"]
          and cat2["full_parse_fallbacks"] == s.fallback_baseline, str(cat2))

    # 未知路由在新版仍 404，且头/体同源
    assert_same_version(s, "/totally/unknown", 404, 1)
    assert_same_version(s, "/health", 200, 1)
    return fp_before


def test_disabled_only_reuses_index(s: Server, fp_before: str):
    print("\n[3] 只改停用清单：路由目录指纹不变并复用，停用判定即时切换")
    st, d = publish(s, "A", 1, "req-big-2", [gen_path(1), gen_path(2)])
    run = s.c.wait_run(d["run_id"])
    check("second publish done", run["status"] == "done", run.get("reason_detail", ""))
    assert_same_version(s, gen_path(1), 503, 2)
    assert_same_version(s, gen_path(123), 200, 2)  # 上一版停用的路由现在恢复

    cat = s.c.state()["catalog"]
    check("路由指纹未变（只改停用清单）",
          cat["current"]["route_fingerprint"] == fp_before, str(cat["current"]))
    check("路由目录被复用（reuse 计数增长）",
          cat["route_index_reuses"] >= 1, str(cat))
    check("全程仍无新增全量解析",
          cat["full_parse_fallbacks"] == s.fallback_baseline, str(cat))


def test_pause_before_switch_keeps_old(s: Server):
    print("\n[4] 暂停在切换前：current.json 未换，网关继续按旧版响应")
    st, d = publish(s, "A", 1, "req-big-pause", [gen_path(9)], pause="before_switch")
    run = s.c.wait_run(d["run_id"])
    check("paused before_switch", run["status"] == "paused" and run["phase"] == "before_switch")
    # 切换未发生：仍 v2、目标路由仍 200
    assert_same_version(s, gen_path(9), 200, 2)
    cat = s.c.state()["catalog"]
    check("暂停期间未检测到切换", cat["current"]["version"] == 2, str(cat["current"]))
    # 恢复后完成切换
    st, _, _ = s.c.post(f"/api/runs/{d['run_id']}/resume", {})
    check("resume accepted", st == 200)
    run = s.c.wait_run(d["run_id"])
    check("run done after resume", run["status"] == "done", str(run))
    assert_same_version(s, gen_path(9), 503, 3)
    assert_same_version(s, gen_path(1), 200, 3)


def test_write_failure_keeps_old(s: Server):
    print("\n[5] 写入失败：current.json 不动，网关仍按旧版，随后可正常发布")
    s.c.post("/api/fault", {"mode": "disk_full"})
    st, d = publish(s, "A", 1, "req-big-fail", [gen_path(10)])
    run = s.c.wait_run(d["run_id"])
    check("write failed run", run["status"] == "failed"
          and "write_failed" in (run["reason_code"] or ""), str(run))
    assert_same_version(s, gen_path(10), 200, 3)
    cat = s.c.state()["catalog"]
    check("失败后无新增全量降级",
          cat["full_parse_fallbacks"] == s.fallback_baseline, str(cat))

    st, d = publish(s, "A", 1, "req-big-ok", [gen_path(10)])
    run = s.c.wait_run(d["run_id"])
    check("next publish done", run["status"] == "done", str(run))
    assert_same_version(s, gen_path(10), 503, 4)


def test_reset_version_reuse(s: Server):
    print("\n[6] 重置演练：版号复用（回到 v0），首个请求即按新实际配置响应")
    st, _, _ = s.c.post("/api/admin/reset", {})
    check("reset 200", st == 200)
    for _ in range(50):
        if s.c.state()["sim_now"] == 1000:
            break
        time.sleep(0.05)
    cat = s.c.state()["catalog"]
    check("重置后目录版本回到 v0（版号复用）",
          cat["current"]["version"] == 0, str(cat["current"]))
    check("重置后累计指标已清零并重新播种（无历史切换）",
          cat["sig_changes_detected"] == 0 and cat["sidecar_adoptions"] == 0
          and cat["route_index_builds"] == 1 and cat["snapshot_installs"] >= 1,
          str(cat))
    # 重置后重建首代目录允许一次全量解析，并把它作为后续场景的新基线
    check("重置后仅 1 次全量解析用于重新播种",
          cat["full_parse_fallbacks"] == 1, str(cat))
    s.fallback_baseline = cat["full_parse_fallbacks"]
    # 重置后首个请求：v0、全量路由仍在、无停用
    assert_same_version(s, gen_path(555), 200, 0)
    assert_same_version(s, "/api/checkout", 200, 0)
    cat = s.c.state()["catalog"]
    check("重置后请求为热路径且无新增全量解析",
          cat["hot_path_hits"] >= 1
          and cat["full_parse_fallbacks"] == s.fallback_baseline,
          str(cat))

    # 重新发布又得到 v1（版号复用），网关必须认实际生效的新 v1 而非旧记忆
    acquire(s, "A")
    st, d = publish(s, "A", 1, "req-after-reset-1", ["/api/checkout"])
    s.c.wait_run(d["run_id"])
    assert_same_version(s, "/api/checkout", 503, 1)
    assert_same_version(s, gen_path(555), 200, 1)


def test_hard_restart(s: Server):
    print("\n[7] 硬重启（SIGKILL 进程组 + 同一卷）：首个请求即正确，优先侧车冷装载")
    st, d = publish(s, "A", 1, "req-big-restart", [gen_path(77)])
    s.c.wait_run(d["run_id"])
    assert_same_version(s, gen_path(77), 503, 2)
    s.hard_restart()

    # 重启后首个请求
    st, hdr, body = s.c.gw(gen_path(77))
    check("重启后首个请求 503 且版本正确",
          st == 503 and hdr.get("X-Config-Version") == "2"
          and body.get("config_version") == 2, f"{st}/{hdr}")
    assert_same_version(s, "/health", 200, 2)
    cat = s.c.state()["catalog"]
    check("重启后目录持有全量路由", cat["current"]["route_count"] == ROUTE_COUNT,
          str(cat["current"]))
    check("重启后经侧车装载（无新增全量解析）",
          cat["sidecar_adoptions"] >= 1
          and cat["full_parse_fallbacks"] == s.fallback_baseline, str(cat))
    check("回执在重启后仍可回放",
          s.c.get("/api/receipts/req-big-restart")[0] == 200)


def test_latency_scale(s_small: Server, s_big: Server):
    print("\n[8] 规模对比：万条路由的热路径请求延迟不应随表增大而升高")
    # 两个服务都先打一轮让目录稳定
    for s in (s_small, s_big):
        for _ in range(20):
            s.c.gw("/health")

    def measure(s):
        # 用一条生成路由（小表没有则退化成 /health）
        path = gen_path(3)
        ts = []
        for _ in range(200):
            t0 = time.perf_counter()
            s.c.gw(path)
            ts.append((time.perf_counter() - t0) * 1000)
        ts.sort()
        return ts[len(ts) // 2], ts[int(len(ts) * 0.95)]

    p50_s, p95_s = measure(s_small)
    p50_b, p95_b = measure(s_big)
    print(f"  -- 小表({6} 条): p50={p50_s:.3f}ms p95={p95_s:.3f}ms")
    print(f"  -- 大表({ROUTE_COUNT} 条): p50={p50_b:.3f}ms p95={p95_b:.3f}ms")
    check("大表 p50 与小表同量级（比值 < 3）", p50_b < max(0.05, p50_s * 3),
          f"{p50_b} vs {p50_s}")
    check("大表全流程无新增全量解析",
          s_big.c.state()["catalog"]["full_parse_fallbacks"] == s_big.fallback_baseline)


def main():
    print(f"项目根: {ROOT}  路由规模: {ROUTE_COUNT}")
    s_big = Server(ROUTE_COUNT, no_watcher=True)
    s_small = Server(6, no_watcher=True)
    print(f"大表服务: http://127.0.0.1:{s_big.port}  tmp={s_big.tmp}")
    try:
        test_seed_and_hot_path(s_big)
        fp = test_switch_first_request(s_big)
        test_disabled_only_reuses_index(s_big, fp)
        test_pause_before_switch_keeps_old(s_big)
        test_write_failure_keeps_old(s_big)
        test_reset_version_reuse(s_big)
        test_hard_restart(s_big)
        test_latency_scale(s_small, s_big)
    except Exception:
        import traceback
        traceback.print_exc()
        FAILURES.append("uncaught exception (see traceback)")
        print("\n------ big server log tail ------")
        print(s_big.log_text())
    finally:
        s_big.stop()
        s_small.stop()

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"\033[31m{len(FAILURES)} 组断言失败\033[0m；通过 {PASS_COUNT} 项")
        for f in FAILURES:
            print(" - " + f)
        sys.exit(1)
    print(f"\033[32m大路由表自测全部通过：{PASS_COUNT} 项断言\033[0m")
    sys.exit(0)


if __name__ == "__main__":
    main()
