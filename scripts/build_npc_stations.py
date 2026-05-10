"""One-time script: extract NPC station list from EVE SDE staStations table.

Usage:
    python scripts/build_npc_stations.py path/to/sde.sqlite

Output:
    data/sde/npc_stations.json
"""

import json
import sqlite3
import sys
import os


def extract_stations(sde_path: str, output_path: str):
    if not os.path.exists(sde_path):
        print(f"SDE file not found: {sde_path}")
        sys.exit(1)

    conn = sqlite3.connect(sde_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT stationID, stationName, solarSystemID
        FROM staStations
        ORDER BY solarSystemID, stationID
    """)

    stations = []
    for row in cursor.fetchall():
        stations.append({
            "station_id": row[0],
            "station_name": row[1],
            "system_id": row[2],
        })

    conn.close()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stations, f, ensure_ascii=False, indent=2)

    print(f"Extracted {len(stations)} NPC stations to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/build_npc_stations.py <path_to_sde.sqlite>")
        sys.exit(1)

    sde_path = sys.argv[1]
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "data", "sde", "npc_stations.json"
    )
    extract_stations(sde_path, output_path)
