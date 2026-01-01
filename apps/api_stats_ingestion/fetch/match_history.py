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


def get_existing_match_ids() -> Set[int]:
    """
    Read existing match result files and extract match IDs.

    Returns a set of match IDs that have already been fetched.
    Match IDs are extracted from filenames in the format `<match_id>-<map_id>.json`.
    """
    existing_ids: Set[int] = set()

    if not OUTPUT_DIR.exists():
        return existing_ids

    for file_path in OUTPUT_DIR.glob("*.json"):
        # Extract match_id from filename (number before the first hyphen)
        filename = file_path.stem  # Gets filename without .json extension
        try:
            # Split on first hyphen and take the first part as match_id
            match_id_str = filename.split("-", 1)[0]
            match_id = int(match_id_str)
            existing_ids.add(match_id)
        except (ValueError, IndexError):
            # Skip files that don't match the expected naming pattern
            continue

    return existing_ids


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
        skip_existing: If True, skip matches that have already been fetched.
                       If False, fetch all matches (normal route).
    """
    maps = load_maps()

    # If skip_existing is True, get the set of already-fetched match IDs
    existing_match_ids: Set[int] = set()
    if skip_existing:
        existing_match_ids = get_existing_match_ids()
        print(f"Found {len(existing_match_ids)} existing match result files")
        print(f"Skipping already-fetched matches...")

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

        # Skip existing matches if skip_existing is enabled
        if skip_existing and match_id in existing_match_ids:
            print(f"Skipping existing match: id={match_id}, map={pretty_name}")
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
        help="Skip matches that have already been fetched and saved to disk",
    )

    args = parser.parse_args()
    main(skip_existing=args.skip_existing)

