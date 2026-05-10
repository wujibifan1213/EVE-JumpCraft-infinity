"""System, stargate, and station data fetching from ESI."""

import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from esi.client import get_json, make_esi_url
from graph.geometry import system_distance_ly
from config import setup_logging

_log = setup_logging("esi.universe")

BAR_WIDTH = 30
MAX_WORKERS = 100


def _render_bar(done: int, total: int, speed: float, elapsed: float) -> str:
    pct = done / total if total else 0
    filled = int(BAR_WIDTH * pct)
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
    eta = (total - done) / speed if speed > 0 else 0
    eta_m = int(eta // 60)
    eta_s = int(eta % 60)
    return (
        f"\r  [{bar}] {pct*100:5.1f}%  {done}/{total}"
        f"  {speed:5.1f} req/s  ETA: {eta_m}m{eta_s:02d}s  "
    )


def get_all_system_ids() -> list[int]:
    _log.info("Fetching all system IDs from ESI...")
    url = make_esi_url("/universe/systems/")
    result = get_json(url, ttl=86400) or []
    _log.info("Got %d system IDs", len(result))
    return result


def get_system_info(system_id: int) -> dict:
    url = make_esi_url(f"/universe/systems/{system_id}/")
    return get_json(url, ttl=86400) or {}


def get_stargate_info(stargate_id: int) -> dict:
    url = make_esi_url(f"/universe/stargates/{stargate_id}/")
    return get_json(url, ttl=86400) or {}


def get_station_info(station_id: int) -> dict:
    url = make_esi_url(f"/universe/stations/{station_id}/")
    return get_json(url, ttl=86400) or {}


def _concurrent_fetch(
    items: list[int],
    fetch_func,
    label: str,
    progress_callback=None,
) -> dict[int, dict]:
    results: dict[int, dict] = {}
    total = len(items)
    done = 0
    failed = 0
    lock = threading.Lock()
    t0 = time.time()

    def _worker(item_id: int):
        try:
            data = fetch_func(item_id)
            return (item_id, data, None)
        except Exception as e:
            return (item_id, None, str(e))

    _log.info("Batch fetching %d %s with %d workers...", total, label, MAX_WORKERS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_worker, i): i for i in items}
        for future in as_completed(futures):
            item_id, data, error = future.result()
            with lock:
                done += 1
                if data:
                    results[item_id] = data
                else:
                    failed += 1
                if error:
                    _log.warning("Failed %s %d: %s", label, item_id, error)
                if done % 200 == 0 or done == total:
                    elapsed = time.time() - t0
                    speed = done / elapsed if elapsed > 0 else 0
                    sys.stdout.write(_render_bar(done, total, speed, elapsed))
                    sys.stdout.flush()
                if progress_callback and done % 500 == 0:
                    progress_callback(done, total)

    elapsed = time.time() - t0
    sys.stdout.write(
        _render_bar(total, total, total / elapsed if elapsed > 0 else 0, elapsed) + "\n"
    )
    sys.stdout.flush()
    _log.info(
        "%s fetch complete: %d/%d succeeded, %d failed, %.1fs",
        label, len(results), total, failed, elapsed,
    )
    return results


def batch_fetch_systems(system_ids: list[int], progress_callback=None) -> dict[int, dict]:
    return _concurrent_fetch(system_ids, get_system_info, "systems", progress_callback)


def batch_fetch_stargates(stargate_ids: list[int], progress_callback=None) -> dict[int, dict]:
    return _concurrent_fetch(stargate_ids, get_stargate_info, "stargates", progress_callback)


def batch_validate_stations(station_ids: list[int], progress_callback=None) -> dict[int, dict]:
    return _concurrent_fetch(station_ids, get_station_info, "stations", progress_callback)


def resolve_system_en_names(zh_names: list[str]) -> dict[int, str]:
    """Resolve Chinese system names to English via POST /universe/ids/.
    Returns dict mapping system_id -> English name."""
    from esi.client import make_esi_url, _get_bearer_token, _rate_limit, _session

    if not zh_names:
        return {}

    unique = list(set(zh_names))
    url = make_esi_url("/universe/ids/")
    token = _get_bearer_token()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    _rate_limit()
    try:
        resp = _session.post(url, json=unique, headers=headers, timeout=30)
        resp.raise_for_status()
        result = {}
        for s in resp.json().get("systems", []):
            result[s["id"]] = s.get("name", "")
        return result
    except Exception as e:
        _log.warning("Failed to resolve English names: %s", e)
        return {}





def get_type_name(type_id: int) -> str:
    """Resolve type_id to Chinese type name (e.g. 35834 -> '星城').
    Uses ETag caching via get_json with 30 day TTL (types rarely change)."""
    from esi.client import get_json, make_esi_url
    url = make_esi_url(f"/universe/types/{type_id}/")
    url_with_lang = f"{url}&language=zh"
    data = get_json(url_with_lang, ttl=2592000)
    if data and "name" in data:
        return data["name"]
    return ""


def ensure_npc_stations_loaded(system_id: int):
    """Lazily load NPC stations for a system from ESI if not yet cached in DB.
    NPC station data (name, type_id) is stored in system_buildings for reuse."""
    from cache.storage import get_buildings_for_system, upsert_building

    buildings = get_buildings_for_system(system_id)
    has_npc = any(
        b.get("building_type") == "npc_station" for b in buildings
    )
    if has_npc:
        return

    sys_data = get_system_info(system_id)
    if not sys_data:
        return

    station_ids = sys_data.get("stations", [])
    if not station_ids:
        return

    _log.info("Lazy-loading %d NPC stations for system %d...", len(station_ids), system_id)
    loaded = 0
    for sid in station_ids:
        try:
            info = get_station_info(sid)
            if info:
                upsert_building(
                    system_id=system_id,
                    building_id=sid,
                    building_name=info.get("name", ""),
                    building_type="npc_station",
                    owner_name=str(info.get("owner", "")),
                    structure_type_id=info.get("type_id"),
                )
                loaded += 1
        except Exception as e:
            _log.warning("Failed to load NPC station %d: %s", sid, e)
    _log.info("Loaded %d/%d NPC stations for system %d", loaded, len(station_ids), system_id)