"""Path utilities for PyInstaller compatibility.

- resource_dir: read-only bundled files (templates, static, npc_stations.json)
- app_dir:      writable runtime dir next to the exe (database, logs, tokens)
"""

import sys
import os
from pathlib import Path


def is_frozen() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def get_resource_dir() -> Path:
    if is_frozen():
        return Path(sys._MEIPASS)
    return Path(__file__).parent


def get_app_dir() -> Path:
    if is_frozen():
        return Path(sys.executable).parent
    return Path(__file__).parent


def get_data_dir() -> Path:
    d = get_app_dir() / "data"
    d.mkdir(exist_ok=True)
    return d


def get_log_dir() -> Path:
    d = get_app_dir() / "logs"
    d.mkdir(exist_ok=True)
    return d


def get_etag_dir() -> Path:
    d = get_data_dir() / "etags"
    d.mkdir(exist_ok=True)
    return d


def ensure_npc_stations() -> Path:
    src = get_resource_dir() / "data" / "sde" / "npc_stations.json"
    dst_dir = get_data_dir() / "sde"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / "npc_stations.json"
    if src.exists() and not dst.exists():
        import shutil
        shutil.copy2(str(src), str(dst))
    return dst
