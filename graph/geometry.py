"""Shared geometry utilities for EVE star map calculations."""

import math

LY_IN_METERS = 9.4607e15


def distance_ly(x1: float, y1: float, z1: float,
                x2: float, y2: float, z2: float) -> float:
    """Calculate light-year distance between two points in EVE coordinate space.

    EVE stores positions in meters, 1 light-year = 9.4607e15 meters.
    """
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2 + (z1 - z2) ** 2) / LY_IN_METERS


def system_distance_ly(a: dict, b: dict) -> float:
    """Calculate LY distance between two system dicts.

    Accepts systems with either flat coordinates (x/y/z) or nested
    position dicts (position.x/y/z), with fallback to 0.
    """
    def _coord(d, key):
        return (d.get(key) or d.get("position", {}).get(key, 0)) or 0

    return distance_ly(
        _coord(a, "x"), _coord(a, "y"), _coord(a, "z"),
        _coord(b, "x"), _coord(b, "y"), _coord(b, "z"),
    )


def node_distance_ly(G, u: int, v: int) -> float:
    """Calculate LY distance between two nodes in a networkx graph."""
    nu, nv = G.nodes[u], G.nodes[v]
    return distance_ly(
        nu.get("x", 0) or 0, nu.get("y", 0) or 0, nu.get("z", 0) or 0,
        nv.get("x", 0) or 0, nv.get("y", 0) or 0, nv.get("z", 0) or 0,
    )