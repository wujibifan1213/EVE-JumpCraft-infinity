"""Shared star map data pull and rebuild logic.

Used by both main.py (CLI) and web/app.py (API rebuild endpoint)
to avoid code duplication.
"""

import time
import json
import os

from config import setup_logging, NPC_STATIONS_PATH

_log = setup_logging("services.sync")


def pull_full_map(progress_callback=None):
    """Pull full star map data from ESI and store in DB.

    Args:
        progress_callback: Optional callable(msg) for progress updates.

    Returns:
        dict with counts: systems, stargates, deleted.
    """
    from esi.universe import get_all_system_ids, batch_fetch_systems, batch_fetch_stargates
    from cache.storage import upsert_system, upsert_stargate, set_meta

    _log.info("====== Full Map Pull Started ======")
    t_start = time.time()

    system_ids = get_all_system_ids()
    _log.info("Got %d system IDs", len(system_ids))
    if progress_callback:
        progress_callback(f"获取到 {len(system_ids)} 个星系 ID，正在拉取数据...")

    if not system_ids:
        _log.error("No system IDs returned. Check ESI connectivity.")
        return {"systems": 0, "stargates": 0, "deleted": 0}

    systems = batch_fetch_systems(system_ids)
    _log.info("Fetched %d systems", len(systems))
    if progress_callback:
        progress_callback(f"已拉取 {len(systems)} 个星系数据，正在存储...")

    for sid, data in systems.items():
        upsert_system(data)

    stargate_ids = []
    for sid, data in systems.items():
        for sg_id in data.get("stargates", []):
            stargate_ids.append(sg_id)

    _log.info("Found %d stargate references to fetch", len(stargate_ids))
    if progress_callback:
        progress_callback(f"正在拉取 {len(stargate_ids)} 个星球...")

    stargate_data = batch_fetch_stargates(stargate_ids)

    stargate_rows = []
    for sg_id, sg_info in stargate_data.items():
        dest = sg_info.get("destination", {})
        to_system_id = dest.get("system_id") if dest else None
        from_system_id = sg_info.get("system_id")
        if from_system_id and to_system_id:
            stargate_rows.append({
                "stargate_id": sg_id,
                "name": sg_info.get("name", ""),
                "from_system_id": from_system_id,
                "to_system_id": to_system_id,
            })

    from cache.storage import batch_upsert_stargates
    batch_upsert_stargates(stargate_rows)
    stored = len(stargate_rows)

    set_meta("last_full_pull", str(time.time()))
    elapsed = time.time() - t_start
    _log.info("Stored %d stargates (fetched %d total)", stored, len(stargate_data))
    _log.info("====== Full Map Pull Complete (%.1fs) ======", elapsed)
    if progress_callback:
        progress_callback(f"数据拉取完成（{stored} 个星门），正在筛选不可达星系...")

    return {"systems": len(systems), "stargates": stored, "deleted": 0}


def filter_unreachable(progress_callback=None):
    """Filter unreachable systems from the graph via ESI verification.

    Returns:
        dict from filter_unreachable_systems with deleted count etc.
    """
    from graph.builder import build_graph as _build_tmp
    from graph.validator import filter_unreachable_systems

    _log.info("====== Unreachable Filter Started ======")
    tmp_G = _build_tmp()
    result = filter_unreachable_systems(tmp_G, esi_verify=True)
    for line in result.get("logs", []):
        _log.info("  " + line)
    if result["deleted"] > 0:
        _log.info("Removed %d unreachable systems. Remaining: %d", result["deleted"], result["kept"])
    _log.info("====== Unreachable Filter Complete ======")
    if progress_callback:
        progress_callback(f"已删除 {result['deleted']} 个不可达星系")
    return result


def load_npc_stations(progress_callback=None):
    """Load NPC stations from SDE JSON into DB.

    Returns:
        int: number of stations loaded.
    """
    from cache.storage import upsert_building

    if not os.path.exists(NPC_STATIONS_PATH):
        _log.warning("NPC stations file not found: %s", NPC_STATIONS_PATH)
        _log.info("Run scripts/build_npc_stations.py to generate it.")
        return 0

    with open(NPC_STATIONS_PATH, "r", encoding="utf-8") as f:
        stations = json.load(f)

    count = 0
    for s in stations:
        upsert_building(
            system_id=s["system_id"],
            building_id=s["station_id"],
            building_name=s.get("station_name", ""),
            building_type="npc_station",
            owner_name=None,
        )
        count += 1

    _log.info("Loaded %d NPC stations into system_buildings", count)
    if progress_callback:
        progress_callback(f"已加载 {count} 个 NPC 空间站")

    return count


def full_rebuild(progress_callback=None):
    """Complete rebuild: pull map, filter unreachable, load NPC stations, rebuild graph.

    Args:
        progress_callback: Optional callable(msg) for progress updates.

    Returns:
        tuple of (graph, result_dict).
    """
    pull_result = pull_full_map(progress_callback)
    filter_result = filter_unreachable(progress_callback)
    load_npc_stations(progress_callback)

    from graph.builder import build_graph
    from graph.validator import get_unreachable_systems, filter_unreachable_systems
    from graph.routes import clear_capital_cache

    G = build_graph()
    remaining = get_unreachable_systems(G)
    if remaining:
        _log.info("Final cleanup: %d systems still unreachable after ESI verification, deleting...",
                  len(remaining))
        cleanup_result = filter_unreachable_systems(G, esi_verify=False)
        filter_result["deleted"] += cleanup_result["deleted"]
        for line in cleanup_result.get("logs", []):
            _log.info("  " + line)
        G = build_graph()

    clear_capital_cache()

    return G, {
        "systems": G.number_of_nodes(),
        "stargates": G.number_of_edges(),
        "deleted": filter_result.get("deleted", 0),
        "logs": filter_result.get("logs", []),
    }