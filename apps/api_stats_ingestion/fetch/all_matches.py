"""
Fetch and paginate through all matches from CRCON's get_scoreboard_maps endpoint.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os

import aiohttp

from pathlib import Path
from typing import Any, Dict, List

from apps.api_stats_ingestion.config import get_api_config

ROOT_DIR = Path(__file__).parent
DATA_DIR = Path(os.getenv("API_INGESTION_DATA_DIR", str(ROOT_DIR)))
OUTPUT_FILE = DATA_DIR / "all_matches.json"
DEFAULT_LIMIT = 500


async def fetch_page(session: aiohttp.ClientSession, page: int, limit: int) -> Dict[str, Any]:
    api_config = get_api_config()
    async with session.get(
        api_config.scoreboard_maps_url, params={"page": page, "limit": limit}, timeout=30
    ) as response:
        response.raise_for_status()
        return await response.json()


async def main(limit: int = DEFAULT_LIMIT) -> None:
    """Fetch all matches from the API, paginating through all pages."""
    page = 1
    total_pages = None
    all_maps: List[Dict[str, Any]] = []
    first_payload: Dict[str, Any] | None = None

    api_config = get_api_config()
    print(f"Fetching matches from {api_config.scoreboard_maps_url}...")
    print(f"Using page size: {limit}")

    async with aiohttp.ClientSession() as session:
        while True:
            payload = await fetch_page(session, page, limit)
            if first_payload is None:
                first_payload = payload

            result = payload.get("result") or {}
            current_page = result.get("page", page)
            page_size = result.get("page_size", limit)
            total = result.get("total")

            maps = result.get("maps") or []
            all_maps.extend(maps)
            print(f"Fetched page {current_page} with {len(maps)} matches (total so far: {len(all_maps)})")

            if total is not None and page_size:
                total_pages = math.ceil(total / page_size)
                if total_pages is not None:
                    print(f"  Total pages: {total_pages}, Total matches: {total}")

            # Stop if we've reached the final page we know about, or if no more items.
            if (total_pages is not None and current_page >= total_pages) or not maps:
                break

            page = current_page + 1

    # Build a combined payload similar to a single API response
    combined: Dict[str, Any] = {}
    if first_payload is not None:
        combined.update(first_payload)
        base_result = (first_payload.get("result") or {}).copy()
    else:
        base_result = {}

    # Update result metadata to reflect combined data
    base_result["page"] = 1
    base_result["page_size"] = len(all_maps)
    # Keep original total if present, otherwise use the actual count
    base_result.setdefault("total", len(all_maps))
    base_result["maps"] = all_maps
    combined["result"] = base_result

    # Save to file
    OUTPUT_FILE.write_text(json.dumps(combined, indent=2), encoding="utf-8")
    print(f"\n✓ Wrote combined results ({len(all_maps)} matches) to {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch all matches from get_scoreboard_maps API endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Note: The script will paginate through all pages automatically and save
      the combined results to all_matches.json.
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Number of items per page (default: {DEFAULT_LIMIT})",
    )

    args = parser.parse_args()
    asyncio.run(main(limit=args.limit))
