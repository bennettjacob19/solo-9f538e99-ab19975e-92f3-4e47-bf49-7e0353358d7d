"""当前生效配置的读取 / 播种。网关只读 current.json，永远不读暂存文件。"""
from __future__ import annotations

import json
import os
from typing import Any

from . import config

# 进程内“最近一次完整配置”兜底：极端情况下 current.json 不可读时，
# 网关仍按最近发布的完整配置响应。
_LAST_GOOD: dict[str, Any] | None = None


def seed_if_needed() -> None:
    config.ensure_dirs()
    if not config.CURRENT_PATH.exists():
        cfg = {
            "version": 0,
            "request_id": "seed",
            "published_sim": config.START_NOW,
            "disabled": [],
            "routes": config.seed_routes(),
        }
        # 播种直接写 current.json（它本身就是完整配置）
        tmp = config.CURRENT_PATH.with_suffix(".seed.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, config.CURRENT_PATH)


def load_current() -> tuple[dict[str, Any], bool]:
    """返回 (config, from_cache)。"""
    global _LAST_GOOD
    try:
        with open(config.CURRENT_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict) or "version" not in cfg:
            raise ValueError("current.json 缺少 version 字段")
        _LAST_GOOD = cfg
        return cfg, False
    except Exception:
        if _LAST_GOOD is not None:
            return _LAST_GOOD, True
        # 最后兜底：内置 v0
        cfg = {
            "version": 0,
            "request_id": "builtin-fallback",
            "disabled": [],
            "routes": config.seed_routes(),
        }
        _LAST_GOOD = cfg
        return cfg, True


def write_current_bytes(data: bytes) -> None:
    """恢复未完成切换时使用：经 staging 原子替换，保持“一份暂存”约束。"""
    config.ensure_dirs()
    with open(config.STAGING_PATH, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(config.STAGING_PATH, config.CURRENT_PATH)
    _fsync_dir(config.DATA_DIR)


def _fsync_dir(path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
