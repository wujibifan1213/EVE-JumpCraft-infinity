"""Connectivity validation for the star graph."""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import networkx as nx
from cache.storage import get_system, delete_systems
from config import setup_logging

_log = setup_logging("graph.validator")


def get_unreachable_systems(G: nx.Graph) -> list[int]:
    if G.number_of_nodes() == 0:
        return []

    components = list(nx.connected_components(G))
    if len(components) <= 1:
        return []

    largest = max(components, key=len)
    all_nodes = set(G.nodes())
    unreachable = all_nodes - set(largest)
    return sorted(list(unreachable))


def get_unreachable_details(G: nx.Graph) -> list[dict]:
    unreachable_ids = get_unreachable_systems(G)
    details = []
    for sid in unreachable_ids:
        node = G.nodes[sid]
        details.append({
            "id": sid,
            "name": node.get("name", str(sid)),
            "security": node.get("security", 0) or 0,
            "region_id": node.get("region_id"),
            "constellation_id": node.get("constellation_id"),
        })
    return details


def validate_node(G: nx.Graph, system_id: int) -> dict:
    if not hasattr(validate_node, '_largest_cache'):
        validate_node._largest_cache = (None, None)

    cache_graph, cache_largest = validate_node._largest_cache
    if cache_graph is not G or cache_largest is None:
        cache_largest = max(nx.connected_components(G), key=len) if G.number_of_nodes() > 0 else set()
        validate_node._largest_cache = (G, cache_largest)

    if system_id not in G:
        return {"exists": False, "reachable": False}

    reachable = system_id in cache_largest

    return {
        "exists": True,
        "reachable": reachable,
        "name": G.nodes[system_id].get("name", ""),
        "degree": G.degree(system_id),
        "security": G.nodes[system_id].get("security", 0) or 0,
    }


def filter_unreachable_systems(G: nx.Graph,
                               esi_verify: bool = True,
                               dry_run: bool = False) -> dict:
    """Find and remove systems not reachable from the largest connected component.

    For each disconnected component, checks one representative system via the
    ESI /route/ endpoint. If ESI confirms it's unreachable, the entire component
    is deleted. If ESI shows it IS reachable, the component is kept (our stargate
    data is incomplete for that area).

    Args:
        G: The star graph.
        esi_verify: If True, verify each component via ESI route API.
        dry_run: If True, only report, don't delete from DB.

    Returns:
        dict with counts, deleted_ids, and log lines.
    """

    total_nodes = G.number_of_nodes()
    components = list(nx.connected_components(G))

    if len(components) <= 1:
        _log.info("Graph is fully connected (%d systems). No filtering needed.", total_nodes)
        return {"deleted": 0, "total": total_nodes, "kept": total_nodes, "logs": []}

    largest = max(components, key=len)
    hub = _find_hub(G, largest)
    hub_name = G.nodes[hub].get("name", str(hub))

    # Separate components: largest is kept, others need verification
    other_components = [c for c in components if c != largest]
    total_other = sum(len(c) for c in other_components)

    _log.info("Largest component: %d systems (%s). Other components: %d (%d systems).",
              len(largest), hub_name, len(other_components), total_other)

    logs: list[str] = []
    logs.append(f"主连通分量: {len(largest)} 星系 (枢纽: {hub_name})")
    logs.append(f"待验证分量: {len(other_components)} 个，共 {total_other} 星系")

    to_delete: set[int] = set()
    to_keep: set[int] = set()
    esi_checks = 0
    esi_reachable = 0

    if not esi_verify:
        for comp in other_components:
            to_delete.update(comp)
        logs.append(f"跳过 ESI 验证，将删除全部 {total_other} 个不可达星系")
    else:
        from config import ESI_BASE_URL, ESI_DATASOURCE
        import requests as req_lib

        session = req_lib.Session()
        session.mount("https://", req_lib.adapters.HTTPAdapter(
            pool_connections=200, pool_maxsize=200, max_retries=1,
        ))
        session.mount("http://", req_lib.adapters.HTTPAdapter(
            pool_connections=200, pool_maxsize=200, max_retries=1,
        ))
        _session_headers = {"Accept": "application/json"}

        tasks = []
        for i, comp in enumerate(other_components):
            rep = next(iter(comp))
            rep_name = G.nodes[rep].get("name", str(rep))
            url = f"{ESI_BASE_URL}/route/{hub}/{rep}/?datasource={ESI_DATASOURCE}"
            tasks.append((i + 1, rep, rep_name, comp, len(comp), url))

        total_tasks = len(tasks)
        max_workers = min(200, total_tasks)
        _log.info("Starting parallel ESI verification: %d components with %d workers",
                  total_tasks, max_workers)
        logs.append(f"并行 ESI 验证: {total_tasks} 个分量, {max_workers} 并发")

        results_lock = threading.Lock()
        esi_checks = 0
        esi_reachable = 0
        completed = 0

        def _check_one(task):
            idx, rep_id, rep_name, comp, comp_size, url = task
            try:
                resp = session.get(url, headers=_session_headers, timeout=10)
                if resp.status_code == 200:
                    route = resp.json()
                    if isinstance(route, list) and len(route) > 0:
                        return (comp, True, f"  ⚠ 分量{idx}: {rep_name} ({comp_size}星系) — ESI可达({len(route)}跳)，保留")
                    else:
                        return (comp, False, f"  ✗ 分量{idx}: {rep_name} ({comp_size}星系) — 空路由，删除")
                elif resp.status_code == 404 or resp.status_code == 503:
                    return (comp, False, f"  ✗ 分量{idx}: {rep_name} ({comp_size}星系) — ESI {resp.status_code}，删除")
                else:
                    return (comp, False, f"  ✗ 分量{idx}: {rep_name} ({comp_size}星系) — ESI {resp.status_code}，删除")
            except Exception as e:
                return (comp, False, f"  ✗ 分量{idx}: {rep_name} ({comp_size}星系) — ESI 错误: {e}，删除")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_check_one, t): t for t in tasks}

            for future in as_completed(futures):
                comp, reachable, msg = future.result()
                with results_lock:
                    esi_checks += 1
                    completed += 1
                    if reachable:
                        to_keep.update(comp)
                        esi_reachable += 1
                    else:
                        to_delete.update(comp)
                    if completed % 200 == 0 or completed == total_tasks:
                        _log.info("ESI verify progress: %d/%d (%.0f%%), %d reachable",
                                  completed, total_tasks,
                                  completed / total_tasks * 100, esi_reachable)
                    logs.append(msg)

        logs.append(
            f"ESI 验证完成: {esi_checks} 次检测, {esi_reachable} 个分量可达(保留), "
            f"{esi_checks - esi_reachable} 个不可达(删除)")

    if dry_run:
        _log.info("[DRY RUN] Would delete %d systems, keep %d.", len(to_delete), total_nodes - len(to_delete))
        logs.append(f"预演: 将删除 {len(to_delete)} 个不可达星系，保留 {total_nodes - len(to_delete)}")
        return {"deleted": 0, "total": total_nodes, "kept": total_nodes,
                "unreachable_count": total_other, "logs": logs}

    # Delete confirmed unreachable systems
    delete_list = sorted(to_delete)
    if delete_list:
        batch_size = 500
        for i in range(0, len(delete_list), batch_size):
            batch = delete_list[i:i + batch_size]
            n = delete_systems(batch)
            _log.info("  Batch %d/%d: %d rows", i // batch_size + 1,
                      (len(delete_list) + batch_size - 1) // batch_size, n)
            time.sleep(0.1)

        logs.append(f"已删除 {len(delete_list)} 个不可达星系")
    else:
        logs.append("无需删除")

    kept = total_nodes - len(delete_list)
    _log.info("Filter complete: kept %d, deleted %d", kept, len(delete_list))
    logs.append(f"筛选完成: 保留 {kept} 个星系，删除 {len(delete_list)} 个")

    return {
        "deleted": len(delete_list),
        "total": total_nodes,
        "kept": kept,
        "unreachable_count": total_other,
        "deleted_ids": delete_list,
        "esi_checks": esi_checks,
        "esi_reachable": esi_reachable,
        "logs": logs,
    }


def _find_hub(G: nx.Graph, component: set) -> int:
    """Find the system with highest degree in the given component as a routing hub."""
    best = None
    best_deg = -1
    for sid in component:
        deg = G.degree(sid)
        if deg > best_deg:
            best_deg = deg
            best = sid
    # Fallback: pick Jita (30000142) if it's in the component
    if 30000142 in component:
        return 30000142
    return best if best is not None else next(iter(component))
