"""Capital jump routing with player structure enrichment and dirt-road support.

Routing strategy:
  1. Direct route: capital jumps + stargates between building systems
  2. Dirt-road route: allows stargate jumps at source/target to reach building systems
     - If source has no building, but a neighbor (or neighbor-of-neighbor) does,
       take a stargate to reach it before jumping
     - If target has no building, but a neighbor (or neighbor-of-neighbor) does,
       take a stargate after landing to reach it
     - Max prefix/suffix stargate jumps controlled by MAX_DIRT_ROAD_GATE_JUMPS
  3. Building-preferred routing: prefer routes through systems with buildings
     as intermediate waypoints
"""

import threading
import networkx as nx
from config import STRUCTURE_SEARCH_WORKERS, setup_logging
from cache.storage import (
    get_buildings_for_system, upsert_building, delete_player_structures_for_system,
    get_all_building_systems,
)
from graph.geometry import distance_ly, LY_IN_METERS

_log = setup_logging("graph.routes")


CAPITAL_JUMP_RANGES = [6.0, 7.0, 8.0, 10.0, 12.0]
MAX_CAPITAL_RANGE = max(CAPITAL_JUMP_RANGES)
MAX_DIRT_ROAD_GATE_JUMPS = 2

_capital_edges_cache: dict[float, list] = {}
_cache_lock = threading.Lock()


def clear_capital_cache():
    with _cache_lock:
        _capital_edges_cache.clear()


def _precompute_capital_edges(G: nx.Graph):
    with _cache_lock:
        if MAX_CAPITAL_RANGE in _capital_edges_cache:
            return _capital_edges_cache[MAX_CAPITAL_RANGE]

    nodes_list = list(G.nodes(data=True))
    bucket_size = MAX_CAPITAL_RANGE
    grid: dict[tuple, list] = {}

    for sid, data in nodes_list:
        x = (data.get("x", 0) or 0) / LY_IN_METERS
        y = (data.get("y", 0) or 0) / LY_IN_METERS
        z = (data.get("z", 0) or 0) / LY_IN_METERS
        sec = data.get("security", 0) or 0
        bx = int(x // bucket_size)
        by = int(y // bucket_size)
        bz = int(z // bucket_size)
        key = (bx, by, bz)
        grid.setdefault(key, []).append((sid, x, y, z, sec))

    range_sq = MAX_CAPITAL_RANGE * MAX_CAPITAL_RANGE
    edges = []

    for sid, data in nodes_list:
        sx = (data.get("x", 0) or 0) / LY_IN_METERS
        sy = (data.get("y", 0) or 0) / LY_IN_METERS
        sz = (data.get("z", 0) or 0) / LY_IN_METERS
        ssec = data.get("security", 0) or 0
        bx = int(sx // bucket_size)
        by = int(sy // bucket_size)
        bz = int(sz // bucket_size)

        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    key = (bx + dx, by + dy, bz + dz)
                    if key not in grid:
                        continue
                    for tid, tx, ty, tz, tsec in grid[key]:
                        if tid <= sid:
                            continue
                        d2 = (sx - tx) ** 2 + (sy - ty) ** 2 + (sz - tz) ** 2
                        if d2 > range_sq:
                            continue
                        dist = d2 ** 0.5
                        if tsec < 0.1:
                            edges.append((sid, tid, dist))
                        if ssec < 0.1:
                            edges.append((tid, sid, dist))

    with _cache_lock:
        _capital_edges_cache[MAX_CAPITAL_RANGE] = edges
    return edges


def _build_capital_graph(G: nx.Graph, jump_range: float) -> nx.DiGraph:
    """Build directed graph with stargates + capital jump edges."""
    DG = nx.DiGraph()

    for nid, ndata in G.nodes(data=True):
        DG.add_node(nid, **ndata)

    for u, v, data in G.edges(data=True):
        DG.add_edge(u, v, weight=1.0, distance_ly=data.get("distance_ly", 0),
                    edge_type="stargate")
        DG.add_edge(v, u, weight=1.0, distance_ly=data.get("distance_ly", 0),
                    edge_type="stargate")

    _precompute_capital_edges(G)
    all_edges = _capital_edges_cache.get(MAX_CAPITAL_RANGE, [])

    for u, v, dist in all_edges:
        if dist <= jump_range and u in DG and v in DG:
            DG.add_edge(u, v, weight=1.0, distance_ly=dist, edge_type="capital")

    return DG


def route_capital_jump(G: nx.Graph, source: int, target: int,
                       jump_range: float = 10.0) -> dict:
    """Compute capital-jump-aware route (stargates + nullsec jumps)."""
    DG = _build_capital_graph(G, jump_range)

    if source not in DG or target not in DG:
        return {"error": "起点或终点不在图中", "path": []}

    try:
        path = nx.shortest_path(DG, source, target, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return {"error": "没有找到旗舰跳跃路线", "path": []}

    return _build_route_result(path, G, DG, jump_range)


def _refresh_path_buildings(path: list) -> None:
    """Update buildings from DB for all path nodes. Ensures buildings
    are always fresh regardless of graph cache state."""
    from esi.universe import ensure_npc_stations_loaded
    for node in path:
        sid = node["id"]
        ensure_npc_stations_loaded(sid)
        buildings = get_buildings_for_system(sid)
        node["has_building"] = len(buildings) > 0
        node["buildings"] = [
            {
                "type": b.get("building_type", "npc_station"),
                "name": b.get("building_name", ""),
                "id": b.get("building_id"),
                "owner": b.get("owner_name"),
            }
            for b in buildings
        ]


def _build_route_result(path: list, G: nx.Graph, DG: nx.DiGraph,
                         jump_range: float, prefix_len: int = 0,
                         suffix_len: int = 0, is_dirt_road: bool = False) -> dict:
    """Build a route result dict from a path.

    When is_dirt_road=True, prefix_len/suffix_len define how many edges
    at the start/end are dirt-road stargate segments.
    """
    mode = f"capital_dirt_{jump_range}ly" if is_dirt_road else f"capital_{jump_range}ly"
    result = {
        "path": [],
        "total_ly": 0,
        "mode": mode,
        "actual_jumps": len(path) - 1,
        "stargate_jumps": 0,
        "capital_jumps": 0,
        "jump_range": jump_range,
    }
    if is_dirt_road:
        result["dirt_road_jumps"] = 0

    total_ly = 0

    for i, sid in enumerate(path):
        node = G.nodes.get(sid) or DG.nodes.get(sid, {})

        edge_type = "stargate"
        edge_subtype = None
        distance = 0

        if i > 0:
            prev = path[i - 1]
            in_prefix = i <= prefix_len
            in_suffix = i > (len(path) - 1 - suffix_len)

            if is_dirt_road and (in_prefix or in_suffix):
                if G.has_edge(prev, sid):
                    distance = G[prev][sid].get("distance_ly", 0)
                edge_type = "stargate"
                edge_subtype = "dirt_road"
                result["dirt_road_jumps"] += 1
                result["stargate_jumps"] += 1
            elif DG.has_edge(prev, sid):
                edge_data = DG[prev][sid]
                distance = edge_data.get("distance_ly", 0)
                edge_type = edge_data.get("edge_type", "stargate")
                if edge_type == "capital":
                    result["capital_jumps"] += 1
                else:
                    result["stargate_jumps"] += 1
            elif G.has_edge(prev, sid):
                distance = G[prev][sid].get("distance_ly", 0)
                result["stargate_jumps"] += 1

            total_ly += distance

        result["path"].append({
            "id": sid,
            "name": node.get("name", str(sid)),
            "security": node.get("security", 0) or 0,
            "has_building": node.get("has_building", False),
            "buildings": [],
            "distance_from_prev": round(distance, 2) if i > 0 else 0,
            "edge_type": edge_type if i > 0 else None,
            "edge_subtype": edge_subtype if i > 0 else None,
        })

    result["total_ly"] = round(total_ly, 2)
    return result


# ── Player Structure Enrichment ─────────────────────────────────────────────

def enrich_route_with_player_structures(route_result: dict, character_id: int) -> dict:
    """Query ESI for player structures along route using multi-keyword search
    with solar_system_id filtering. Always queries fresh (no cache).

      1. Generate keywords (en_name + zh_name + prefix) per system
      2. Concurrent search across all keywords
      3. Concurrent detail fetch for all candidate IDs
      4. Filter by solar_system_id to eliminate cross-system false positives
      5. Handle 403 (ACL expired) and 404 (destroyed) gracefully
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from esi.search import build_keywords, search_character_structures, public_search_structures, get_structure_info
    from esi.universe import resolve_system_en_names

    path = route_result.get("path", [])
    if not path or not character_id:
        _log.info("Enrich: no path or no character_id, skipping")
        _refresh_path_buildings(path)
        return route_result

    systems_to_query = []
    for node in path:
        sid = node["id"]
        delete_player_structures_for_system(sid)
        systems_to_query.append((sid, node["name"]))

    _log.info("Enrich: querying player structures for %d systems...", len(systems_to_query))

    zh_names = {sid: name for sid, name in systems_to_query}
    en_name_map = resolve_system_en_names(list(zh_names.values()))
    _log.debug("Enrich: resolved %d English names", len(en_name_map))

    keyword_tasks = []
    for sid, zh_name in systems_to_query:
        en_name = en_name_map.get(sid, zh_name)
        for kw in build_keywords(en_name, zh_name):
            keyword_tasks.append(kw)

    candidate_ids = set()
    search_failed = 0
    _log.info("Enrich: searching %d keywords across %d systems (char + public)...", len(keyword_tasks), len(systems_to_query))

    with ThreadPoolExecutor(max_workers=STRUCTURE_SEARCH_WORKERS) as executor:
        futures = {}
        for kw in keyword_tasks:
            if character_id:
                futures[executor.submit(search_character_structures, character_id, kw)] = ("char", kw)
            futures[executor.submit(public_search_structures, kw)] = ("public", kw)

        for future in as_completed(futures):
            scope, kw = futures[future]
            try:
                ids = future.result()
                candidate_ids.update(ids)
            except Exception as e:
                _log.warning("Enrich: %s search failed for '%s': %s", scope, kw, e)
                search_failed += 1

    if not candidate_ids:
        _log.info("Enrich: no candidate structures found")
        _refresh_path_buildings(path)
        return route_result

    _log.info("Enrich: %d unique candidate structure IDs, fetching details...", len(candidate_ids))

    detail_map = {}
    detail_failures = 0
    with ThreadPoolExecutor(max_workers=STRUCTURE_SEARCH_WORKERS) as executor:
        futures = {
            executor.submit(get_structure_info, sid): sid
            for sid in candidate_ids
        }
        for future in as_completed(futures):
            sid = futures[future]
            try:
                info = future.result()
                if info:
                    detail_map[sid] = info
                else:
                    detail_failures += 1
            except Exception as e:
                _log.warning("Enrich: detail fetch failed for %d: %s", sid, e)
                detail_failures += 1

    _log.info("Enrich: fetched %d details, %d skipped (403/404/error)",
              len(detail_map), detail_failures)

    new_structures = 0
    for sid, zh_name in systems_to_query:
        in_system = [
            (struct_id, d) for struct_id, d in detail_map.items()
            if d.get("solar_system_id") == sid
        ]
        for struct_id, s in in_system:
            upsert_building(
                system_id=sid,
                building_id=struct_id,
                building_name=s.get("name", ""),
                building_type="player_structure",
                owner_name=str(s.get("owner_id", "")),
                structure_type_id=s.get("type_id"),
            )
            new_structures += 1

    _log.info("Enrich: added %d player structures (filtered by system_id)", new_structures)

    _refresh_path_buildings(path)
    return route_result


def route_capital_dirt_road(G: nx.Graph, source: int, target: int,
                            jump_range: float = 10.0) -> dict:
    """Capital jump routing with dirt-road support.

    Strategy:
      1. If source/target lacks a building (needed for capital ship docking),
         find building-system candidates reachable within MAX_DIRT_ROAD_GATE_JUMPS
         stargate jumps.
      2. Enumerate all (launch, land) candidate pairs and find the optimal route
         via the capital graph.
      3. Dirt-road segments (extra stargate jumps to reach buildings) are marked
         with edge_subtype="dirt_road".

    Optimization priority:
        1. Minimise capital jumps  (fuel cost)
        2. Tie-break: total jumps  (time)
        3. Tie-break: total LY     (distance)
    """
    building_systems = get_all_building_systems()
    DG = _build_capital_graph(G, jump_range)

    if source not in DG or target not in DG:
        return {"error": "起点或终点不在图中", "path": []}

    max_gate = MAX_DIRT_ROAD_GATE_JUMPS

    def _candidates(center: int, exclude: int = None) -> list[tuple[int, int]]:
        """Find (system_id, gate_distance) pairs for building-reachable launch/landing.

        Includes center itself if it has a building, plus all building-system
        neighbors within max_gate stargate hops. Excludes the `exclude` node
        to prevent degenerate cycle paths.
        """
        cands = []
        if center in building_systems:
            cands.append((center, 0))

        visited = {center}
        if exclude is not None:
            visited.add(exclude)
        frontier = [center]
        for hop in range(1, max_gate + 1):
            next_frontier = []
            for node in frontier:
                for nb in G.neighbors(node):
                    if nb in visited or nb not in DG:
                        continue
                    visited.add(nb)
                    if nb in building_systems:
                        cands.append((nb, hop))
                    next_frontier.append(nb)
            frontier = next_frontier
        return cands

    s_candidates = _candidates(source, exclude=target)
    t_candidates = _candidates(target, exclude=source)

    if not s_candidates or not t_candidates:
        return route_capital_jump(G, source, target, jump_range)

    direct = route_capital_jump(G, source, target, jump_range)
    direct_has_error = direct.get("error") is not None
    direct_capital = direct.get("capital_jumps", 0) if not direct_has_error else 999_999

    source_has_building = source in building_systems
    target_has_building = target in building_systems
    must_dirt = not source_has_building or not target_has_building

    best_result = None
    best_score = None

    for s, sg in s_candidates:
        for t, tg in t_candidates:
            if s == source:
                prefix_path = [source]
                prefix_len = 0
            else:
                try:
                    prefix_path = nx.shortest_path(G, source, s, weight="weight")
                    prefix_len = len(prefix_path) - 1
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

            if t == target:
                suffix_path = [target]
                suffix_len = 0
            else:
                try:
                    suffix_path = nx.shortest_path(G, t, target, weight="weight")
                    suffix_len = len(suffix_path) - 1
                except (nx.NetworkXNoPath, nx.NodeNotFound):
                    continue

            if prefix_len > max_gate or suffix_len > max_gate:
                continue

            try:
                mid_path = nx.shortest_path(DG, s, t, weight="weight")
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue

            full_path = prefix_path[:-1] + mid_path + suffix_path[1:]

            if len(full_path) != len(set(full_path)):
                continue

            result = _build_route_result(
                full_path, G, DG, jump_range, prefix_len, suffix_len, is_dirt_road=True
            )

            capital_jumps = result["capital_jumps"]
            total_jumps = result["actual_jumps"]
            total_ly = result["total_ly"]

            if direct_has_error:
                accept = True
            elif must_dirt:
                accept = True
            elif capital_jumps < direct_capital:
                accept = True
            else:
                accept = False

            if not accept:
                continue

            score = (capital_jumps, total_jumps, total_ly)
            if best_score is None or score < best_score:
                best_result = result
                best_score = score

    if best_result is None:
        direct.setdefault("dirt_road_jumps", 0)
        return direct

    return best_result


def route_capital_full(G, source: int, target: int, jump_range: float = 10.0,
                       character_id: int = 0, require_buildings: bool = False,
                       skip_buildings: bool = False, waypoints: list[int] = None,
                       avoid_systems: set[int] = None,
                       allow_dirt_road: bool = False) -> dict:
    """Capital jump routing with automatic player structure enrichment.
    If require_buildings=True, only systems with buildings are valid intermediate nodes.
    If skip_buildings=True, skip building queries entirely for fastest routing.
    If allow_dirt_road=True, allow up to 1 stargate detour at source/target
    when it saves capital jumps or when the endpoint lacks a building.
    waypoints: ordered list of system IDs the route must pass through.
    avoid_systems: set of system IDs to exclude from routing."""
    if waypoints is None:
        waypoints = []
    if avoid_systems is None:
        avoid_systems = set()

    needs_copy = avoid_systems or require_buildings
    if needs_copy:
        G = G.copy()

    if avoid_systems:
        avoid_in_graph = avoid_systems - {source, target} - set(waypoints)
        for nid in avoid_in_graph:
            if nid in G:
                G.remove_node(nid)

    if require_buildings:
        building_systems = get_all_building_systems()
        allowed = building_systems | {source, target} | set(waypoints)
        for nid in list(G.nodes()):
            if nid not in allowed:
                G.remove_node(nid)

    ordered_points = [source] + waypoints + [target]
    missing = [s for s in ordered_points if s not in G]
    if missing:
        names = [G.nodes.get(s, {}).get("name", str(s)) if s in G else str(s) for s in missing]
        return {"error": f"星系 {', '.join(names)} 不在图中或被避开", "path": []}

    segments = []
    cumulative = {
        "actual_jumps": 0, "stargate_jumps": 0,
        "capital_jumps": 0, "dirt_road_jumps": 0, "total_ly": 0,
    }

    for i in range(len(ordered_points) - 1):
        seg_src = ordered_points[i]
        seg_tgt = ordered_points[i + 1]
        if allow_dirt_road:
            seg = route_capital_dirt_road(G, seg_src, seg_tgt, jump_range)
        else:
            seg = route_capital_jump(G, seg_src, seg_tgt, jump_range)
        if seg.get("error"):
            return seg
        segments.append(seg)
        cumulative["actual_jumps"] += seg.get("actual_jumps", 0)
        cumulative["stargate_jumps"] += seg.get("stargate_jumps", 0)
        cumulative["capital_jumps"] += seg.get("capital_jumps", 0)
        cumulative["dirt_road_jumps"] += seg.get("dirt_road_jumps", 0)
        cumulative["total_ly"] += seg.get("total_ly", 0)

    combined_path = segments[0].get("path", [])
    for seg in segments[1:]:
        combined_path.extend(seg.get("path", [])[1:])

    mode_str = f"capital_dirt_{jump_range}ly" if cumulative["dirt_road_jumps"] > 0 else f"capital_{jump_range}ly"
    result = {
        "path": combined_path,
        "total_ly": round(cumulative["total_ly"], 2),
        "mode": mode_str,
        "actual_jumps": cumulative["actual_jumps"],
        "stargate_jumps": cumulative["stargate_jumps"],
        "capital_jumps": cumulative["capital_jumps"],
        "dirt_road_jumps": cumulative["dirt_road_jumps"],
        "jump_range": jump_range,
    }

    if skip_buildings:
        for node in result.get("path", []):
            node.setdefault("buildings", [])
    elif character_id:
        result = enrich_route_with_player_structures(result, character_id)
    else:
        _refresh_path_buildings(result.get("path", []))

    return result
