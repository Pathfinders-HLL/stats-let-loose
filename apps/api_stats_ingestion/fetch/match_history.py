"""
Read `all_matches.json`, call the per-map scoreboard API for each entry,
and store individual match result JSON files in `match_results/`.

Each output file is named `<id>-<mapid>.json`, where:
  - `id` is the numeric id of the entry in the `maps` array
  - `mapid` is the `id` field from the nested `map` object for that entry
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set

import requests

from apps.api_stats_ingestion.config import get_api_config
from libs.db.config import get_db_config
from libs.db.database import get_db_connection

ROOT_DIR = Path(__file__).parent
# Use environment variable for data directory, fallback to script directory
DATA_DIR = Path(os.getenv("API_INGESTION_DATA_DIR", str(ROOT_DIR)))
SOURCE_FILE = DATA_DIR / "all_matches.json"
OUTPUT_DIR = DATA_DIR / "match_results"


def load_maps() -> List[Dict[str, Any]]:
    """Load the `maps` array from `all_matches.json`."""
    data = json.loads(SOURCE_FILE.read_text(encoding="utf-8"))
    result = data.get("result") or {}
    maps = result.get("maps") or []
    if not isinstance(maps, list):
        raise ValueError("Expected `result.maps` to be a list")
    return maps


def get_existing_match_ids_from_db() -> Set[int]:
    """
    Query the database for existing match IDs.

    Returns a set of match IDs that have already been inserted into the database.
    """
    db_config = get_db_config()
    conn = get_db_connection(
        host=db_config.host,
        port=db_config.port,
        database=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT match_id FROM pathfinder_stats.match_history")
        existing_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return existing_ids
    finally:
        conn.close()


def fetch_match_scoreboard(match_id: int | str) -> Dict[str, Any]:
    """Fetch the scoreboard for a single match by id."""
    api_config = get_api_config()
    resp = requests.get(api_config.map_scoreboard_url, params={"map_id": match_id}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def save_match_result(
    match_id: int | str, map_id: str, payload: Dict[str, Any]
) -> None:
    """Save a single match result to `match_results/<id>-<mapid>.json`."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{match_id}-{map_id}.json"
    outfile = OUTPUT_DIR / filename
    outfile.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main(skip_existing: bool = False) -> None:
    """
    Main function to fetch match scoreboards.

    Args:
        skip_existing: If True, skip matches that already exist in the database.
                       If False, fetch all matches (normal route).
    """
    maps = load_maps()

    # If skip_existing is True, get the set of already-inserted match IDs from DB
    existing_match_ids: Set[int] = set()
    if skip_existing:
        existing_match_ids = get_existing_match_ids_from_db()
        print(f"Found {len(existing_match_ids)} existing matches in database")
        print(f"Skipping already-inserted matches...")

    seen_ids = set()
    fetched_count = 0
    skipped_count = 0

    for entry in maps:
        match_id = entry.get("id")
        if match_id is None:
            continue

        # Avoid duplicate calls if the same id appears multiple times.
        if match_id in seen_ids:
            continue
        seen_ids.add(match_id)

        map_info = entry.get("map") or {}
        map_id = map_info.get("id") or "unknown"
        # Extract pretty name from nested map structure
        map_map_info = map_info.get("map") or {}
        pretty_name = map_map_info.get("pretty_name") or map_id

        # Skip matches already in database if skip_existing is enabled
        if skip_existing and match_id in existing_match_ids:
            print(f"Skipping (in DB): id={match_id}, map={pretty_name}")
            skipped_count += 1
            continue

        payload = fetch_match_scoreboard(match_id)
        save_match_result(match_id, map_id, payload)
        fetched_count += 1
        print(f"Saved match result for id={match_id}, mapid={map_id}")

    print(
        f"\nSummary: Fetched {fetched_count} new matches, skipped {skipped_count} existing matches"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch match scoreboards from the API",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip matches that already exist in the database",
    )

    args = parser.parse_args()
    main(skip_existing=args.skip_existing)

