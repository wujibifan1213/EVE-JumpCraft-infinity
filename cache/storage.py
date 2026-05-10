"""SQLite storage for systems, stargates, buildings, and meta."""

import json
import sqlite3
import time
from typing import Optional
from contextlib import contextmanager

from config import DB_PATH

_connection = None


@contextmanager
def _conn():
    """Context manager yielding a reusable SQLite connection with WAL mode."""
    global _connection
    if _connection is None:
        _connection = sqlite3.connect(DB_PATH)
        _connection.row_factory = sqlite3.Row
        _connection.execute("PRAGMA journal_mode=WAL")
        _connection.execute("PRAGMA synchronous=NORMAL")
    yield _connection


def init_db():
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS systems (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                security REAL,
                x REAL, y REAL, z REAL,
                constellation_id INTEGER,
                region_id INTEGER,
                raw_json TEXT
            );

            CREATE TABLE IF NOT EXISTS stargates (
                id INTEGER PRIMARY KEY,
                name TEXT,
                from_system_id INTEGER NOT NULL,
                to_system_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_buildings (
                system_id INTEGER NOT NULL,
                building_id INTEGER NOT NULL,
                building_name TEXT,
                building_type TEXT,
                owner_name TEXT,
                structure_type_id INTEGER,
                updated_at REAL,
                UNIQUE(system_id, building_id)
            );

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_systems_name ON systems(name);
            CREATE INDEX IF NOT EXISTS idx_systems_security ON systems(security);
            CREATE INDEX IF NOT EXISTS idx_stargates_from ON stargates(from_system_id);
            CREATE INDEX IF NOT EXISTS idx_stargates_to ON stargates(to_system_id);
            CREATE INDEX IF NOT EXISTS idx_buildings_system ON system_buildings(system_id);
            CREATE INDEX IF NOT EXISTS idx_buildings_type ON system_buildings(building_type);

            CREATE TABLE IF NOT EXISTS avoid_systems (
                system_id INTEGER PRIMARY KEY,
                system_name TEXT NOT NULL,
                security REAL
            );
        """)
        try:
            c.execute("ALTER TABLE system_buildings ADD COLUMN structure_type_id INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE system_buildings ADD COLUMN updated_at REAL")
        except sqlite3.OperationalError:
            pass
        c.commit()


def upsert_system(data: dict):
    with _conn() as c:
        c.execute("""
            INSERT OR REPLACE INTO systems (id, name, security, x, y, z, constellation_id, region_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["system_id"],
            data["name"],
            data.get("security_status"),
            data.get("position", {}).get("x"),
            data.get("position", {}).get("y"),
            data.get("position", {}).get("z"),
            data.get("constellation_id"),
            data.get("region_id"),
            json.dumps(data),
        ))
        c.commit()


def upsert_stargate(data: dict):
    with _conn() as c:
        c.execute("""
            INSERT OR REPLACE INTO stargates (id, name, from_system_id, to_system_id)
            VALUES (?, ?, ?, ?)
        """, (
            data["stargate_id"],
            data.get("name"),
            data["from_system_id"],
            data["to_system_id"],
        ))
        c.commit()


def upsert_building(system_id: int, building_id: int,
                    building_name: str = None, building_type: str = "npc_station",
                    owner_name: str = None, structure_type_id: int = None):
    with _conn() as c:
        c.execute("""
            INSERT OR REPLACE INTO system_buildings (system_id, building_id, building_name, building_type, owner_name, structure_type_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (system_id, building_id, building_name, building_type, owner_name, structure_type_id, time.time()))
        c.commit()


def batch_upsert_systems(systems: list[dict]):
    """Bulk insert systems in a single transaction."""
    with _conn() as c:
        c.executemany("""
            INSERT OR REPLACE INTO systems (id, name, security, x, y, z, constellation_id, region_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                d["system_id"], d["name"], d.get("security_status"),
                d.get("position", {}).get("x"), d.get("position", {}).get("y"),
                d.get("position", {}).get("z"), d.get("constellation_id"),
                d.get("region_id"), json.dumps(d),
            ) for d in systems
        ])
        c.commit()


def batch_upsert_stargates(stargates: list[dict]):
    """Bulk insert stargates in a single transaction."""
    with _conn() as c:
        c.executemany("""
            INSERT OR REPLACE INTO stargates (id, name, from_system_id, to_system_id)
            VALUES (?, ?, ?, ?)
        """, [
            (d["stargate_id"], d.get("name"), d["from_system_id"], d["to_system_id"])
            for d in stargates
        ])
        c.commit()


def get_all_systems() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM systems").fetchall()
    return [dict(r) for r in rows]


def get_system(system_id: int) -> Optional[dict]:
    with _conn() as c:
        row = c.execute("SELECT * FROM systems WHERE id = ?", (system_id,)).fetchone()
    return dict(row) if row else None


def search_systems(query: str, limit: int = 20) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, name, security FROM systems WHERE name LIKE ? LIMIT ?",
            (f"%{query}%", limit)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_stargates() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM stargates").fetchall()
    return [dict(r) for r in rows]


def get_buildings_for_system(system_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM system_buildings WHERE system_id = ?", (system_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_building_systems() -> set[int]:
    with _conn() as c:
        rows = c.execute("SELECT DISTINCT system_id FROM system_buildings").fetchall()
    return {r[0] for r in rows}


def get_building_stats() -> dict:
    with _conn() as c:
        npc = c.execute(
            "SELECT COUNT(DISTINCT system_id) FROM system_buildings WHERE building_type='npc_station'"
        ).fetchone()[0]
        player = c.execute(
            "SELECT COUNT(DISTINCT system_id) FROM system_buildings WHERE building_type='player_structure'"
        ).fetchone()[0]
        total_npc = c.execute(
            "SELECT COUNT(*) FROM system_buildings WHERE building_type='npc_station'"
        ).fetchone()[0]
        total_player = c.execute(
            "SELECT COUNT(*) FROM system_buildings WHERE building_type='player_structure'"
        ).fetchone()[0]
    return {
        "npc_systems": npc,
        "player_systems": player,
        "total_npc_buildings": total_npc,
        "total_player_buildings": total_player,
    }


def delete_player_structures_for_system(system_id: int) -> int:
    """Delete all player structures for a system (before re-query)."""
    with _conn() as c:
        cursor = c.execute(
            "DELETE FROM system_buildings WHERE system_id = ? AND building_type = 'player_structure'",
            (system_id,),
        )
        deleted = cursor.rowcount
        c.commit()
    return deleted


def clear_all_player_structures() -> int:
    """Delete all player structure cache entries."""
    with _conn() as c:
        cursor = c.execute(
            "DELETE FROM system_buildings WHERE building_type = 'player_structure'"
        )
        deleted = cursor.rowcount
        c.commit()
    return deleted


def set_meta(key: str, value: str):
    with _conn() as c:
        c.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        c.commit()


def get_meta(key: str) -> Optional[str]:
    with _conn() as c:
        row = c.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def clear_db():
    with _conn() as c:
        c.executescript("""
            DELETE FROM systems;
            DELETE FROM stargates;
            DELETE FROM system_buildings;
            DELETE FROM meta;
        """)
        c.commit()


def delete_systems(system_ids: list[int]) -> int:
    """Delete systems and their stargates from the database. Returns count affected."""
    if not system_ids:
        return 0
    with _conn() as c:
        placeholders = ",".join("?" * len(system_ids))
        c.execute(f"DELETE FROM stargates WHERE from_system_id IN ({placeholders}) OR to_system_id IN ({placeholders})", system_ids + system_ids)
        c.execute(f"DELETE FROM systems WHERE id IN ({placeholders})", system_ids)
        c.execute(f"DELETE FROM system_buildings WHERE system_id IN ({placeholders})", system_ids)
        deleted = c.total_changes
        c.commit()
    return deleted


def add_avoid_system(system_id: int, system_name: str, security: float = 0.0):
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO avoid_systems (system_id, system_name, security) VALUES (?, ?, ?)",
            (system_id, system_name, security),
        )
        c.commit()


def remove_avoid_system(system_id: int):
    with _conn() as c:
        c.execute("DELETE FROM avoid_systems WHERE system_id = ?", (system_id,))
        c.commit()


def get_all_avoid_systems() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT system_id, system_name, security FROM avoid_systems").fetchall()
    return [{"system_id": r[0], "system_name": r[1], "security": r[2]} for r in rows]


def get_avoid_system_ids() -> set[int]:
    with _conn() as c:
        rows = c.execute("SELECT system_id FROM avoid_systems").fetchall()
    return {r[0] for r in rows}


def clear_avoid_systems():
    with _conn() as c:
        c.execute("DELETE FROM avoid_systems")
        c.commit()