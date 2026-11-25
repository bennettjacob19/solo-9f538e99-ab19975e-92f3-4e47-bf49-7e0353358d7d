"""网关路由目录：让 ``/gw/<path>`` 的单次请求只做 O(1) 判定。

背景
----
旧实现每个请求都 ``open + json.load`` 整份 current.json，再把上万条 routes
和 disabled 各遍历一遍构造成 set —— 请求成本与路由表规模同阶（O(N)）。

本模块的不变量
~~~~~~~~~~~~~~
1. **请求热路径零全表读取**：网关线程只做一次 ``stat``（比对 current.json
   的 inode/mtime/size 指纹）。指纹不变时直接复用内存里的不可变
   :class:`Snapshot`；指纹变化（发生切换）时，只读两份小文件确认生效版本
   并装载本路由所需的目录条目，*不* 遍历路由表 / 停用清单，*不* 等待任何
   全表索引重建。
2. **侧车索引（发布端预制）**：发布端子进程在 ``os.replace`` 切换之前，
   先把 content-addressed 的路由目录 ``route-<fingerprint>.json`` 与代际指针
   ``active.json`` 原子写到 **state 目录**（配置卷仍只放 current/staging，
   沿用旧卷与磁盘核算口径）。切换后首个请求据此 O(1) 装载新代际。
3. **只改停用清单 → 复用路由目录**：路由集合的规范化哈希不变就复用同一个
   :class:`RouteIndex`，只换 disabled 集合（``route_index_reuses`` 计数）。
4. **状态码与版本头同源**：一次请求的 503/404/200 判定和
   ``X-Config-Version`` 头取自同一个 :class:`Snapshot` 对象，不可能跨版本。
5. **降级仍正确**：侧车缺失 / 指针陈旧 / current.json 损坏时，退回一次性
   全量解析（计入 ``full_parse_fallbacks``），并保留 last-good 快照继续服务；
   任何降级都不改变应答正确性，只损失性能。

侧车文件位于 ``STATE_DIR/catalog/``（不是配置卷，不参与“一正文一暂存”核算）：

* ``route-<sha256>.json`` —— 不可变、按内容寻址的路由目录；
* ``active.json`` —— 当前代际指针（版本 / 停用清单 / 路由指纹 / 正文大小），
  永远经临时文件 + ``os.replace`` 原子写入；
* ``*.tmp`` —— 写入中间态，启动时清理。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

from . import config, configstore

# 代际指针 / 侧车目录
CATALOG_DIR = config.STATE_DIR / "catalog"
ACTIVE_POINTER = CATALOG_DIR / "active.json"
_POINTER_TMP = CATALOG_DIR / "active.json.tmp"

# 切换后确认版本只读取文件头部的有限字节，与路由表规模无关
HEAD_READ_BYTES = 8192
_VERSION_RE = re.compile(rb'"version"\s*:\s*(\d+)')

# 守护巡检间隔：主动预热，让切换通常在首个请求到来前完成装载
WATCH_INTERVAL_SEC = 0.1
# 自测开关：DRILL_CATALOG_NO_WATCHER=1 时不启动后台巡检，
# 用于确定性地验证“切换后的首个请求自身”即可 O(1) 装载，不依赖预热。
_NO_WATCHER = bool(os.environ.get("DRILL_CATALOG_NO_WATCHER"))


# ---------------------------------------------------------------------------
# 不可变数据结构
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FileSig:
    """current.json 的廉价身份指纹，一次 stat 即可取得，O(1)。"""
    dev: int
    ino: int
    mtime_ns: int
    size: int


@dataclass(frozen=True)
class RouteIndex:
    """一份不可变路由目录，可被多个代际（Snapshot）按指纹共享。"""
    fingerprint: str
    paths: frozenset[str]
    routes: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class Snapshot:
    """某一代实际生效配置的完整只读视图。请求只持有它，不直接读文件。"""
    version: int
    request_id: str
    sig: FileSig
    route_index: RouteIndex
    disabled: frozenset[str]
    source: str           # sidecar | parse | bootstrap | recover | seed
    from_cache: bool      # current.json 不可读、来自 last-good 兜底时为 True


# ---------------------------------------------------------------------------
# 进程内状态
# ---------------------------------------------------------------------------

_LOCK = threading.RLock()
_CURRENT: Snapshot | None = None
_ROUTE_INDEXES: dict[str, RouteIndex] = {}
_WATCHER_STARTED = False
_STOP = threading.Event()

# 可观测指标：自测用它们证明热路径没有全表遍历 / 没有等待索引重建
_METRICS: dict[str, int] = {
    "requests": 0,                 # 网关请求进入目录的次数
    "hot_path_hits": 0,            # stat 指纹未变，纯内存 O(1) 命中
    "sig_changes_detected": 0,     # 检测到 current.json 代际切换
    "head_version_reads": 0,       # 切换后读取文件头确认版本的次数（有界读取）
    "sidecar_adoptions": 0,        # 经发布端预制指针完成的代际装载
    "full_parse_fallbacks": 0,     # 降级：一次性全量解析的次数（唯一 O(N) 路径）
    "last_good_served": 0,         # current 不可读、沿用旧快照的次数
    "route_index_builds": 0,       # 新建路由目录（含侧车落盘）次数
    "route_index_reuses": 0,       # 路由指纹未变、复用目录次数
    "snapshot_installs": 0,        # 装载新代际快照次数
    "pointer_writes": 0,           # 代际指针写入次数
    "pointer_rejected": 0,         # 指针校验未过、转降级的次数
}


def stats() -> dict[str, Any]:
    """供 /api/state 与自测使用的指标快照。"""
    with _LOCK:
        out: dict[str, Any] = dict(_METRICS)
        snap = _CURRENT
        out["catalog_dir"] = str(CATALOG_DIR)
        out["active_pointer_exists"] = ACTIVE_POINTER.exists()
        if snap is not None:
            out["current"] = {
                "version": snap.version,
                "request_id": snap.request_id,
                "source": snap.source,
                "from_cache": snap.from_cache,
                "route_count": len(snap.route_index.paths),
                "disabled_count": len(snap.disabled),
                "route_fingerprint": snap.route_index.fingerprint,
                "sig": {"ino": snap.sig.ino, "mtime_ns": snap.sig.mtime_ns,
                        "size": snap.sig.size},
            }
        else:
            out["current"] = None
        return out


def _bump(name: str, n: int = 1) -> None:
    _METRICS[name] = _METRICS.get(name, 0) + n


def _reset_metrics() -> None:
    for k in _METRICS:
        _METRICS[k] = 0


# ---------------------------------------------------------------------------
# 文件指纹 / 原子写
# ---------------------------------------------------------------------------

def stat_current() -> FileSig | None:
    try:
        st = config.CURRENT_PATH.stat()
    except FileNotFoundError:
        return None
    return FileSig(st.st_dev, st.st_ino, st.st_mtime_ns, st.st_size)


def _atomic_write_json(path, obj: Any) -> None:
    tmp = path.with_name(path.name + ".tmp")
    data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _cleanup_tmp() -> None:
    if not CATALOG_DIR.exists():
        return
    for p in CATALOG_DIR.iterdir():
        if p.name.endswith(".tmp"):
            try:
                p.unlink()
            except OSError:
                pass


def routes_fingerprint(routes: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> str:
    """路由集合的规范化哈希：只改停用清单时它保持不变。"""
    canon = json.dumps(list(routes), ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]


def _sidecar_path(fingerprint: str):
    return CATALOG_DIR / f"route-{fingerprint}.json"


# ---------------------------------------------------------------------------
# RouteIndex：内存复用 + 侧车落盘 / 冷装载
# ---------------------------------------------------------------------------

def _get_or_build_index(routes: list[dict[str, Any]], fp: str | None = None) -> RouteIndex:
    """按指纹获取内存目录；命中既有目录记一次复用，否则新建（含侧车落盘）。"""
    if fp is None:
        fp = routes_fingerprint(routes)
    existing = _ROUTE_INDEXES.get(fp)
    if existing is not None:
        _bump("route_index_reuses")
        return existing

    # 内存未命中：先看发布端 / 上一代进程是否已落侧车
    sidecar = _sidecar_path(fp)
    loaded: list[dict[str, Any]] | None = None
    try:
        with open(sidecar, "r", encoding="utf-8") as f:
            doc = json.load(f)
        if doc.get("fingerprint") == fp and isinstance(doc.get("routes"), list):
            loaded = doc["routes"]
    except (OSError, ValueError):
        loaded = None

    use_routes = loaded if loaded is not None else routes
    idx = RouteIndex(
        fingerprint=fp,
        paths=frozenset(r["path"] for r in use_routes),
        routes=tuple(use_routes),
    )
    _ROUTE_INDEXES[fp] = idx
    _bump("route_index_builds")

    if loaded is None:
        # 本进程首次见到该路由集合：补写不可变侧车（已存在则内容必然相同）
        try:
            CATALOG_DIR.mkdir(parents=True, exist_ok=True)
            if not sidecar.exists():
                _atomic_write_json(sidecar, {"fingerprint": fp, "routes": routes})
        except OSError:
            # 侧车只是性能加速器，落盘失败不影响正确性
            pass
    return idx


def _build_route_index(routes: list[dict[str, Any]]) -> RouteIndex:
    """按指纹获取 / 新建路由目录。同指纹永远复用，不重复建表。"""
    return _get_or_build_index(routes)


def _write_route_sidecar(routes: list[dict[str, Any]]) -> str:
    """发布端进程使用：确保路由目录侧车存在，返回指纹。"""
    fp = routes_fingerprint(routes)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    sidecar = _sidecar_path(fp)
    if not sidecar.exists():
        _atomic_write_json(sidecar, {"fingerprint": fp, "routes": routes})
    return fp


# ---------------------------------------------------------------------------
# Snapshot 装载
# ---------------------------------------------------------------------------

def _install(cfg: dict[str, Any], sig: FileSig | None, source: str,
             from_cache: bool) -> Snapshot:
    routes = list(cfg.get("routes", config.seed_routes()))
    idx = _build_route_index(routes)
    snap = Snapshot(
        version=int(cfg.get("version", 0)),
        request_id=str(cfg.get("request_id", "")),
        sig=sig or FileSig(0, 0, 0, 0),
        route_index=idx,
        disabled=frozenset(cfg.get("disabled", []) or []),
        source=source,
        from_cache=from_cache,
    )
    global _CURRENT
    _CURRENT = snap
    _bump("snapshot_installs")
    return snap


def install_from_cfg(cfg: dict[str, Any], source: str,
                     from_cache: bool = False) -> Snapshot:
    """恢复 / 播种路径：已知完整 cfg，直接装载并把指针补齐。"""
    with _LOCK:
        sig = stat_current()
        snap = _install(cfg, sig, source, from_cache)
        _write_pointer(snap, payload_size=sig.size if sig else 0,
                       payload_sha256=None)
        return snap


def _write_pointer(snap: Snapshot, payload_size: int,
                   payload_sha256: str | None) -> None:
    try:
        CATALOG_DIR.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(_POINTER_TMP, {
            "version": snap.version,
            "request_id": snap.request_id,
            "disabled": sorted(snap.disabled),
            "route_fingerprint": snap.route_index.fingerprint,
            "payload_size": int(payload_size),
            "payload_sha256": payload_sha256,
        })
        os.replace(_POINTER_TMP, ACTIVE_POINTER)
        _bump("pointer_writes")
    except OSError:
        pass


def publisher_publish(cfg: dict[str, Any], payload_size: int,
                      payload_sha256: str | None = None) -> None:
    """发布端子进程在 os.replace 切换 current.json **之前** 调用。

    顺序保证：先落不可变路由侧车 → 再原子代际指针 → 最后才 replace 正文。
    因此网关在新正文出现的同一时刻就一定能读到匹配的指针；若本函数中途
    崩溃，指针缺失只会让网关走一次降级全量解析，应答仍然正确。
    """
    with _LOCK:
        fp = _write_route_sidecar(list(cfg.get("routes", [])))
        idx = _ROUTE_INDEXES.get(fp)
        if idx is None:
            idx = _build_route_index(list(cfg.get("routes", [])))
        snap = Snapshot(
            version=int(cfg["version"]),
            request_id=str(cfg.get("request_id", "")),
            sig=FileSig(0, 0, 0, 0),  # 替换后由网关按真实 stat 校正
            route_index=idx,
            disabled=frozenset(cfg.get("disabled", []) or []),
            source="sidecar",
            from_cache=False,
        )
        _write_pointer(snap, payload_size, payload_sha256)


# ---------------------------------------------------------------------------
# 切换检测：有界头部读取 + 指针校验，杜绝全表遍历
# ---------------------------------------------------------------------------

def _read_head_version() -> int | None:
    """只读 current.json 头部有限字节确认生效版本，与表大小无关。"""
    try:
        with open(config.CURRENT_PATH, "rb") as f:
            head = f.read(HEAD_READ_BYTES)
    except OSError:
        return None
    m = _VERSION_RE.search(head)
    if m:
        _bump("head_version_reads")
        return int(m.group(1))
    return None


def _read_pointer() -> dict[str, Any] | None:
    """读代际指针。文件不存在返回 None；存在但损坏抛 ValueError。"""
    try:
        with open(ACTIVE_POINTER, "r", encoding="utf-8") as f:
            doc = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise ValueError(f"active pointer unreadable: {exc.__class__.__name__}") from exc
    if isinstance(doc, dict) and "version" in doc:
        return doc
    raise ValueError("active pointer missing version field")


def _adopt_by_sidecar(sig: FileSig) -> Snapshot | None:
    """切换后的首选装载路径：指针 + 路由侧车，全程 O(路由条目哈希查找)。

    不遍历整张路由表：侧车按指纹直接命中内存目录；停用清单只取本代际指针
    里的那一份构造成集合（一次性 O(|disabled|)，与路由表规模无关）。

    指针文件不存在（如首次播种、刚重置）返回 None 走正常装载，不计入
    pointer_rejected；只有指针存在但与 current.json 对不上时才计拒绝。
    """
    try:
        pointer = _read_pointer()
    except ValueError:
        _bump("pointer_rejected")
        return None
    if pointer is None:
        return None
    head_version = _read_head_version()
    # 校验：正文大小 + 头部版本双双吻合，才承认指针描述的是当前代际
    if head_version is None or int(pointer.get("payload_size", -1)) != sig.size \
            or int(pointer.get("version", -1)) != head_version:
        _bump("pointer_rejected")
        return None

    fp = pointer.get("route_fingerprint")
    idx = _ROUTE_INDEXES.get(fp) if fp else None
    if idx is None:
        # 冷启动 / 新进程：按指纹读对应侧车（只读取该代际目录这一个文件）
        sidecar = _sidecar_path(fp) if fp else None
        try:
            with open(sidecar, "r", encoding="utf-8") as f:
                doc = json.load(f)
            if doc.get("fingerprint") != fp:
                raise ValueError("fingerprint mismatch")
            idx = _get_or_build_index(list(doc["routes"]), fp)
        except (OSError, ValueError, TypeError):
            _bump("pointer_rejected")
            return None
    else:
        # 内存里已有同一指纹的目录：只改停用清单时直接复用，不重建
        _bump("route_index_reuses")

    snap = Snapshot(
        version=head_version,
        request_id=str(pointer.get("request_id", "")),
        sig=sig,
        route_index=idx,
        disabled=frozenset(pointer.get("disabled", []) or []),
        source="sidecar",
        from_cache=False,
    )
    global _CURRENT
    _CURRENT = snap
    _bump("sidecar_adoptions")
    _bump("snapshot_installs")
    return snap


def _adopt_by_full_parse(sig: FileSig | None) -> Snapshot:
    """降级路径：侧车不可用时一次性全量解析（唯一的 O(N) 路径，计入指标）。"""
    cfg, from_cache = configstore.load_current()
    snap = _install(cfg, sig or stat_current(),
                    source="parse", from_cache=from_cache)
    _bump("full_parse_fallbacks")
    # 顺手把指针补齐，后续切换 / 重启不必再降级
    real_sig = stat_current()
    if real_sig is not None:
        _write_pointer(snap, real_sig.size, None)
    return snap


def refresh() -> Snapshot | None:
    """检测 current.json 是否切换并装载新代际。每次网关请求只付出一次 stat。"""
    with _LOCK:
        sig = stat_current()
        if sig is None:
            # 正文暂时不可读：有 last-good 就继续按旧版服务
            if _CURRENT is not None:
                _bump("last_good_served")
                return _CURRENT
            return _adopt_by_full_parse(None)

        if _CURRENT is not None and _CURRENT.sig == sig:
            # 热路径：无文件内容读取、无任何表遍历，仅一次 stat + 对象比较
            _bump("hot_path_hits")
            return _CURRENT

        _bump("sig_changes_detected")
        snap = _adopt_by_sidecar(sig)
        if snap is None:
            snap = _adopt_by_full_parse(sig)
        return snap


def current_snapshot() -> Snapshot:
    """网关请求入口：保证返回与 current.json 实际内容一致的快照。"""
    with _LOCK:
        _bump("requests")
        snap = refresh()
        if snap is not None:
            return snap
        # 理论不可达：_adopt_by_full_parse 还有内置 v0 兜底
        return _adopt_by_full_parse(None)


def decide(route_path: str) -> tuple[Snapshot, str]:
    """单次请求的唯一判定点，返回 (快照, 决策)。

    决策 ∈ {"disabled", "not_found", "ok"}。两次集合查询均为 O(1)，
    状态码与 X-Config-Version 由调用方取自同一个快照。
    """
    with _LOCK:
        snap = current_snapshot()
        if route_path in snap.disabled:
            return snap, "disabled"
        if route_path != "/" and snap.route_index.paths \
                and route_path not in snap.route_index.paths:
            return snap, "not_found"
        return snap, "ok"


def current_config_dict() -> tuple[dict[str, Any], bool]:
    """供 /api/state 展示：从内存快照还原配置视图，不重读整份文件。"""
    with _LOCK:
        snap = current_snapshot()
        return (
            {
                "version": snap.version,
                "request_id": snap.request_id,
                "disabled": sorted(snap.disabled),
                "routes": [dict(r) for r in snap.route_index.routes],
            },
            snap.from_cache,
        )


# ---------------------------------------------------------------------------
# 生命周期：启动 / 重置 / 后台巡检
# ---------------------------------------------------------------------------

def bootstrap() -> Snapshot:
    """Flask 启动时调用一次：建立首代快照并启动后台巡检线程。"""
    global _WATCHER_STARTED
    with _LOCK:
        CATALOG_DIR.mkdir(parents=True, exist_ok=True)
        _cleanup_tmp()
        sig = stat_current()
        snap: Snapshot | None = None
        if sig is not None:
            snap = _adopt_by_sidecar(sig)
        if snap is None:
            # 侧车缺失 / 指针陈旧 / 冷启动：一次性全量解析兜底（计入降级指标）
            cfg, from_cache = configstore.load_current()
            snap = _install(cfg, sig or stat_current(),
                            source="bootstrap", from_cache=from_cache)
            _bump("full_parse_fallbacks")
            real_sig = stat_current()
            _write_pointer(snap, real_sig.size if real_sig else 0, None)
        if not _WATCHER_STARTED and not _NO_WATCHER:
            t = threading.Thread(target=_watch_loop, name="catalog-watcher",
                                 daemon=True)
            t.start()
            _WATCHER_STARTED = True
        return snap


def reset() -> None:
    """重置演练：清空侧车与内存目录，调用方随后重新播种并 bootstrap。"""
    with _LOCK:
        global _CURRENT
        _CURRENT = None
        _ROUTE_INDEXES.clear()
        _reset_metrics()
        try:
            if CATALOG_DIR.exists():
                for p in CATALOG_DIR.iterdir():
                    try:
                        p.unlink()
                    except OSError:
                        pass
        except OSError:
            pass


def _watch_loop() -> None:
    """后台守护：主动发现切换并预热新代际，请求线程因此通常无需等待。"""
    while not _STOP.wait(WATCH_INTERVAL_SEC):
        try:
            with _LOCK:
                refresh()
        except Exception:  # noqa: BLE001 - 巡检线程永不退出
            time.sleep(0.5)


def shutdown() -> None:
    _STOP.set()
