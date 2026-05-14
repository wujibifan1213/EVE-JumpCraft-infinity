"""Entry point: initialize data and start the web server.

Usage:
    python main.py                # start server with existing cache
    python main.py --refresh      # force full data refresh from ESI
"""

import os
import sys
import argparse
import threading
import webbrowser

import uvicorn
from dotenv import load_dotenv

load_dotenv()

from config import setup_logging
from services.sync import full_rebuild

_log = setup_logging("main")


def _open_browser(host: str, port: int, delay: float = 2.0):
    def _do():
        import time
        time.sleep(delay)
        webbrowser.open(f"http://localhost:{port}")
    threading.Thread(target=_do, daemon=True).start()


def _init_data(force_refresh: bool):
    from cache.storage import init_db, get_all_systems
    from pkg_utils import get_data_dir

    os.makedirs(str(get_data_dir()), exist_ok=True)
    init_db()

    existing_systems = get_all_systems()

    if not existing_systems or force_refresh:
        if force_refresh:
            _log.info("Forcing full refresh from ESI...")
        else:
            _log.info("No cache found, pulling full map from ESI (may take 10-15 minutes)...")
        G, result = full_rebuild()
    else:
        _log.info("Using cached data: %d systems.", len(existing_systems))
        from graph.builder import build_graph
        G = build_graph()

    _log.info("Graph: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
    from graph.builder import get_connected_components
    comp = get_connected_components(G)
    _log.info("Largest component: %d systems, %d components, %d unreachable",
              comp['largest_size'], comp['component_count'], comp['unreachable_count'])


def main():
    parser = argparse.ArgumentParser(description="EVE Star Map Jump Route Calculator")
    parser.add_argument("--refresh", action="store_true", help="Force full data refresh from ESI")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--no-init", action="store_true", help="Skip data initialization")
    parser.add_argument("--login", action="store_true", help="Login via EVE SSO and store tokens")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open browser")
    args = parser.parse_args()

    if args.login:
        from esi.auth import interactive_login
        token = interactive_login()
        if token:
            _log.info("Login successful. Token will auto-refresh when needed.")
        return

    if not args.no_init:
        _init_data(force_refresh=args.refresh)

    if not args.no_browser:
        _open_browser(args.host, args.port)

    from web.app import app

    print(f"\n[Server] Starting at http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
