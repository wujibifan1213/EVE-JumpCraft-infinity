"""Build networkx graph from SQLite cache data, with building markers."""

import networkx as nx
from cache.storage import get_all_systems, get_all_stargates, get_all_building_systems
from graph.geometry import node_distance_ly


def build_graph() -> nx.Graph:
    systems = get_all_systems()
    stargates = get_all_stargates()
    building_systems = get_all_building_systems()

    if not systems:
        return nx.Graph()

    G = nx.Graph()

    for sys_data in systems:
        sid = sys_data["id"]
        G.add_node(sid, **{
            "name": sys_data["name"],
            "security": sys_data.get("security", 0.0) or 0.0,
            "x": sys_data.get("x", 0.0) or 0.0,
            "y": sys_data.get("y", 0.0) or 0.0,
            "z": sys_data.get("z", 0.0) or 0.0,
            "constellation_id": sys_data.get("constellation_id"),
            "region_id": sys_data.get("region_id"),
            "has_building": sid in building_systems,
        })

    for sg_data in stargates:
        if sg_data.get("from_system_id") and sg_data.get("to_system_id"):
            frm = sg_data["from_system_id"]
            to = sg_data["to_system_id"]
            if frm in G and to in G:
                dist = node_distance_ly(G, frm, to)
                G.add_edge(frm, to, weight=1.0, distance_ly=dist)

    return G


def mark_building_systems(G: nx.Graph):
    building_systems = get_all_building_systems()
    for sid in building_systems:
        if sid in G:
            G.nodes[sid]["has_building"] = True


def get_connected_components(G: nx.Graph) -> dict:
    if G.number_of_nodes() == 0:
        return {"largest_size": 0, "component_count": 0, "unreachable_count": 0}

    components = list(nx.connected_components(G))
    largest = max(components, key=len)
    largest_id = G.nodes[next(iter(largest))].get("name", str(largest))

    all_nodes = set(G.nodes())
    in_largest = set(largest)
    unreachable = all_nodes - in_largest

    return {
        "largest_size": len(largest),
        "largest_name": largest_id,
        "component_count": len(components),
        "unreachable_count": len(unreachable),
        "unreachable_ids": sorted(list(unreachable)),
    }
