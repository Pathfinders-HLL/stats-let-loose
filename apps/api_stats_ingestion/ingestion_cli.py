"""
ETL pipeline orchestrator: fetch match data from CRCON API and load into PostgreSQL.
"""

from __future__ import annotations

import argparse
import sys

# Import functions from other modules
from apps.api_stats_ingestion.fetch.all_matches import main as fetch_all_matches_main
from apps.api_stats_ingestion.fetch.match_history import main as fetch_match_history_main
from apps.api_stats_ingestion.load.db_match_results import main as update_db_main


def run_pipeline(
    skip_all_matches_fetch: bool,
    skip_existing_fetch: bool,
    skip_duplicates_insert: bool,
    update_match_history: bool,
    update_player_stats: bool,
    skip_fetch: bool,
    skip_insert: bool,
) -> None:
    """Run the full ingestion pipeline with configurable steps."""
    print("=" * 70)
    print("MATCH RESULTS INGESTION PIPELINE")
    print("=" * 70)
    
    # Step 0: Fetch all matches list from API
    if not skip_all_matches_fetch:
        print("\n" + "=" * 70)
        print("STEP 0: FETCHING ALL MATCHES LIST FROM API")
        print("=" * 70)
        try:
            fetch_all_matches_main()
            print("\n✓ All matches fetch completed successfully")
        except Exception as e:
            print(f"\n✗ Error during all matches fetch: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\n" + "=" * 70)
        print("STEP 0: ALL MATCHES FETCH")
        print("=" * 70)
        print("Skipping all matches fetch (--skip-all-matches-fetch flag set)")
    
    # Step 1: Fetch match scoreboards from API
    if not skip_fetch:
        print("\n" + "=" * 70)
        print("STEP 1: FETCHING MATCH SCOREBOARDS FROM API")
        print("=" * 70)
        try:
            fetch_match_history_main(skip_existing=skip_existing_fetch)
            print("\n✓ Fetch step completed successfully")
        except Exception as e:
            print(f"\n✗ Error during fetch step: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping fetch step (using existing match_results/ directory)")
    
    # Step 2: Transform and insert data into database
    if not skip_insert:
        print("\n" + "=" * 70)
        print("STEP 2: TRANSFORMING AND INSERTING DATA INTO DATABASE")
        print("=" * 70)
        try:
            update_db_main(
                skip_duplicates=skip_duplicates_insert,
                update_match_history=update_match_history,
                update_player_stats=update_player_stats,
            )
            print("\n✓ Database update step completed successfully")
        except Exception as e:
            print(f"\n✗ Error during database update step: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping database insertion step")
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrate the match results ingestion pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Note: This script coordinates:
  1. fetch/all_matches.py - Fetches all matches list from get_scoreboard_maps API
  2. fetch/match_history.py - Fetches detailed match scoreboards from API
  3. load/db_match_results.py - Transforms and inserts data into PostgreSQL
        """,
    )
    
    # All matches fetch step options
    parser.add_argument(
        "--skip-all-matches-fetch",
        action="store_true",
        default=False,
        help="Skip fetching all matches list (assume all_matches.json already exists). Default: False (fetches all matches)."
    )
    
    # Fetch step options
    parser.add_argument(
        "--skip-existing-fetch",
        action="store_true",
        default=False,
        help="Skip matches that already exist in the database (for fetch step). Default: False (fetches all matches)."
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        default=False,
        help="Skip the fetch step entirely (assume match_results/ already exists). Default: False (performs fetch step)."
    )
    
    # Insert step options
    parser.add_argument(
        "--skip-insert",
        action="store_true",
        default=False,
        help="Skip the database insertion step. Default: False (performs database insertion)."
    )
    parser.add_argument(
        "--no-skip-duplicates",
        action="store_true",
        default=False,
        help="Fail on duplicate entries instead of skipping (for insert step). Default: False (skips duplicates)."
    )
    parser.add_argument(
        "--skip-match-history",
        action="store_true",
        default=False,
        help="Skip updating the match_history table. Default: False (updates match_history table)."
    )
    parser.add_argument(
        "--skip-player-stats",
        action="store_true",
        default=False,
        help="Skip updating the player_match_stats table. Default: False (updates player_match_stats table)."
    )
    
    args = parser.parse_args()
    
    # Determine which tables to update (default: both)
    # If both skip flags are set, update both (same behavior as old "both flags = update both")
    run_pipeline(
        skip_all_matches_fetch=args.skip_all_matches_fetch,
        skip_existing_fetch=args.skip_existing_fetch,
        skip_duplicates_insert=not args.no_skip_duplicates,
        update_match_history=True if (args.skip_match_history and args.skip_player_stats) else not args.skip_match_history,
        update_player_stats=True if (args.skip_match_history and args.skip_player_stats) else not args.skip_player_stats,
        skip_fetch=args.skip_fetch,
        skip_insert=args.skip_insert,
    )


if __name__ == "__main__":
    main()

