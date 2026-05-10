"""FastAPI web application."""

import threading
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from pkg_utils import get_resource_dir

from graph.builder import build_graph, mark_building_systems, get_connected_components
from graph.routes import route_capital_full, clear_capital_cache
from graph.validator import get_unreachable_details, validate_node
from cache.storage import (
    init_db, search_systems, get_system, get_buildings_for_system,
    get_building_stats, set_meta, get_meta, get_all_systems,
    add_avoid_system, remove_avoid_system, get_all_avoid_systems, get_avoid_system_ids,
)
from esi.auth import (
    get_auth_session, exchange_code, get_valid_token,
    verify_token, logout, start_token_refresh_loop,
)
from services.sync import full_rebuild

app = FastAPI(title="EVE 星图跳跃路线计算")

_resource_dir = get_resource_dir()
_static_dir = _resource_dir / "web" / "static"
_templates_dir = _resource_dir / "web" / "templates"

if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

_graph_cache = None


def get_graph():
    global _graph_cache
    if _graph_cache is None:
        _graph_cache = build_graph()
    return _graph_cache


def refresh_graph():
    global _graph_cache
    mark_building_systems(get_graph())
    _graph_cache = build_graph()
    return _graph_cache


@app.on_event("startup")
async def startup():
    init_db()
    G = get_graph()
    from graph.routes import _precompute_capital_edges
    _precompute_capital_edges(G)
    start_token_refresh_loop(interval_seconds=900)


@app.get("/", response_class=HTMLResponse)
async def index():
    template_path = _templates_dir / "index.html"
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")
    return "<h1>Index template not found</h1>"


@app.get("/api/systems/search")
async def api_search_systems(q: str = "", limit: int = 20):
    if not q or len(q) < 1:
        return []
    results = search_systems(q, limit)
    return results


@app.get("/api/route")
async def api_route(
    from_: int = Query(..., alias="from"),
    to: int = Query(...),
    range_: float = Query(default=10.0, alias="range"),
    require_buildings: bool = Query(default=False),
    skip_buildings: bool = Query(default=False),
    waypoints: str = Query(default=""),
    allow_dirt_road: bool = Query(default=False),
):
    G = get_graph()
    character_id = 0

    if not skip_buildings:
        from esi.auth import get_valid_token, verify_token
        token = get_valid_token()
        if token:
            info = verify_token(token)
            if info and info.get("CharacterID"):
                character_id = info["CharacterID"]

    wp_list = []
    if waypoints:
        try:
            wp_list = [int(x.strip()) for x in waypoints.split(",") if x.strip()]
        except ValueError:
            wp_list = []

    avoid = get_avoid_system_ids()
    result = route_capital_full(G, from_, to, range_, character_id, require_buildings, skip_buildings, wp_list, avoid, allow_dirt_road)
    return result


@app.get("/api/systems/{system_id}")
async def api_system_detail(system_id: int):
    G = get_graph()
    sys_data = get_system(system_id)
    if not sys_data:
        return JSONResponse({"error": "System not found"}, status_code=404)

    buildings = get_buildings_for_system(system_id)
    validation = validate_node(G, system_id)

    neighbors = []
    if system_id in G:
        for nb in G.neighbors(system_id):
            n_data = G.nodes[nb]
            neighbors.append({
                "id": nb,
                "name": n_data.get("name", str(nb)),
                "security": n_data.get("security", 0) or 0,
                "has_building": n_data.get("has_building", False),
            })

    return {
        **sys_data,
        "buildings": [dict(b) for b in buildings],
        "validation": validation,
        "neighbors": neighbors,
    }


@app.get("/api/systems/{system_id}/buildings")
async def api_system_buildings(system_id: int):
    """Return all buildings (NPC + player) for a system, with type names."""
    sys_data = get_system(system_id)
    if not sys_data:
        return JSONResponse({"error": "System not found"}, status_code=404)

    from esi.universe import get_type_name, ensure_npc_stations_loaded, get_station_info

    ensure_npc_stations_loaded(system_id)

    buildings_raw = get_buildings_for_system(system_id)
    buildings = []
    for b in buildings_raw:
        bd = dict(b)
        type_id = bd.get("structure_type_id")

        if not type_id:
            if bd.get("building_type") == "npc_station":
                info = get_station_info(bd["building_id"])
                if info and info.get("type_id"):
                    type_id = info["type_id"]
            elif bd.get("building_type") == "player_structure":
                from esi.search import get_structure_info
                info = get_structure_info(bd["building_id"])
                if info and info.get("type_id"):
                    type_id = info["type_id"]

            if type_id:
                from cache.storage import upsert_building
                upsert_building(
                    system_id=system_id,
                    building_id=bd["building_id"],
                    building_name=bd.get("building_name"),
                    building_type=bd.get("building_type"),
                    owner_name=bd.get("owner_name"),
                    structure_type_id=type_id,
                )

        bd["type_name"] = get_type_name(type_id) if type_id else ""
        buildings.append(bd)

    return {
        "system_id": system_id,
        "system_name": sys_data["name"],
        "security": sys_data.get("security", 0),
        "buildings": buildings,
    }


@app.get("/api/stats")
async def api_stats():
    G = get_graph()
    components = get_connected_components(G)
    building_stats = get_building_stats()
    system_count = G.number_of_nodes()
    stargate_count = G.number_of_edges()

    return {
        "systems": system_count,
        "stargates": stargate_count,
        **components,
        **building_stats,
    }


@app.get("/api/unreachable")
async def api_unreachable():
    G = get_graph()
    details = get_unreachable_details(G)
    return {"count": len(details), "systems": details}


@app.post("/api/refresh")
async def api_refresh():
    global _graph_cache
    _graph_cache = None
    clear_capital_cache()
    G = get_graph()
    return {"status": "ok", "systems": G.number_of_nodes(), "stargates": G.number_of_edges()}


# ── Star map rebuild (full ESI pull + unreachable filter) ──────────────────

_rebuild_status: dict = {"running": False, "progress": "", "result": None}


@app.post("/api/rebuild")
async def api_rebuild():
    global _rebuild_status
    if _rebuild_status["running"]:
        return JSONResponse({"status": "error", "message": "重建已在进行中"}, status_code=409)

    _rebuild_status = {"running": True, "progress": "开始重建...", "result": None}

    def _run_rebuild():
        global _graph_cache, _rebuild_status
        from config import setup_logging
        _log = setup_logging("web.rebuild")
        try:
            def progress(msg):
                _rebuild_status["progress"] = msg

            G, result = full_rebuild(progress_callback=progress)

            _graph_cache = G
            _rebuild_status = {
                "running": False,
                "progress": "重建完成",
                "result": result,
            }
            _log.info("Rebuild complete: %d systems, %d stargates",
                      G.number_of_nodes(), G.number_of_edges())
        except Exception as e:
            _log.error("Rebuild failed: %s", e)
            _rebuild_status = {"running": False, "progress": f"重建失败: {e}", "result": None}

    threading.Thread(target=_run_rebuild, daemon=True).start()
    return {"status": "ok", "message": "重建已启动，请通过 /api/rebuild/status 查看进度"}


@app.get("/api/rebuild/status")
async def api_rebuild_status():
    return _rebuild_status


@app.post("/api/token")
async def api_set_token(request: Request):
    try:
        body = await request.json()
        token = body.get("token", "")
        import config
        config.ESI_TOKEN = token
        set_meta("esi_token", token)
        return {"status": "ok", "message": "Token updated (session only)"}
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)


@app.get("/api/buildings/stats")
async def api_building_stats():
    return get_building_stats()


@app.get("/api/avoid-systems")
async def api_list_avoid_systems():
    return get_all_avoid_systems()


@app.post("/api/avoid-systems")
async def api_add_avoid_system(request: Request):
    try:
        body = await request.json()
        system_id = body.get("system_id")
        system_name = body.get("system_name", "")
        security = body.get("security", 0.0)
        if not system_id:
            return JSONResponse({"error": "缺少 system_id"}, status_code=400)
        add_avoid_system(int(system_id), system_name, float(security))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.delete("/api/avoid-systems/{system_id}")
async def api_remove_avoid_system(system_id: int):
    remove_avoid_system(system_id)
    return {"status": "ok"}


# ── Authorization endpoints ──────────────────────────────────────────────────

@app.get("/auth", response_class=HTMLResponse)
async def auth_page():
    template_path = _templates_dir / "auth.html"
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")
    return "<h1>Auth template not found</h1>"


@app.get("/api/auth/status")
async def api_auth_status():
    token = get_valid_token()
    if not token:
        from esi.auth import _load_tokens
        stored = _load_tokens()
        if stored and stored.refresh_token:
            return {"logged_in": False, "has_refresh": True, "message": "Token 已过期，请重新登录"}
        return {"logged_in": False, "has_refresh": False, "message": "未登录"}

    info = verify_token(token)
    if info:
        return {
            "logged_in": True,
            "character_id": info.get("CharacterID"),
            "character_name": info.get("CharacterName"),
            "scopes": info.get("Scopes", ""),
            "expires_on": info.get("ExpiresOn", ""),
        }
    return {"logged_in": True, "message": "Token 有效但验证失败"}


@app.get("/api/auth/url")
async def api_auth_url():
    try:
        session = get_auth_session()
        return {"url": session["url"], "state": session["state"]}
    except ValueError as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.post("/api/auth/callback")
async def api_auth_callback(request: Request):
    try:
        body = await request.json()
        code = body.get("code", "").strip()
        if not code:
            return JSONResponse({"status": "error", "message": "未提供授权码"}, status_code=400)

        # Auto-extract code if user pasted full URL or query string
        import re
        m = re.search(r'[?&]code=([^&\s]+)', code)
        if m:
            code = m.group(1)

        tokens = exchange_code(code)
        if not tokens:
            return JSONResponse({"status": "error", "message": "Token 交换失败，请重新获取授权链接"}, status_code=400)

        info = verify_token(tokens.access_token)
        return {
            "status": "ok",
            "character_id": info.get("CharacterID") if info else None,
            "character_name": info.get("CharacterName") if info else None,
        }
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=400)


@app.post("/api/auth/logout")
async def api_auth_logout():
    logout()
    return {"status": "ok", "message": "已退出登录"}
