"""
ETL pipeline orchestrator: fetch match data from CRCON API and load into PostgreSQL.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Import async main functions from other modules
from apps.api_stats_ingestion.fetch.all_matches import main as fetch_all_matches_main
from apps.api_stats_ingestion.fetch.match_history import main as fetch_match_history_main
from apps.api_stats_ingestion.load.db_match_results import main as update_db_main
from apps.api_stats_ingestion.validate import check_all_integrity, delete_incomplete_match_data
from libs.db.config import get_db_config
from libs.db.database import get_db_connection

# Determine data directory (same pattern as other ingestion modules)
ROOT_DIR = Path(__file__).parent
DATA_DIR = Path(os.getenv("API_INGESTION_DATA_DIR", str(ROOT_DIR)))
# Marker file to track if initial validation has been run
VALIDATION_MARKER_FILE = DATA_DIR / ".initial_validation_complete"


def has_initial_validation_run() -> bool:
    """Check if the initial validation has already been completed."""
    return VALIDATION_MARKER_FILE.exists()


def mark_initial_validation_complete() -> None:
    """Mark that initial validation has been completed."""
    VALIDATION_MARKER_FILE.parent.mkdir(parents=True, exist_ok=True)
    VALIDATION_MARKER_FILE.touch()


async def run_initial_validation_if_needed(ingestion_successful: bool) -> None:
    """
    Run thorough validation after initial ingestion if it hasn't been run before.
    
    This runs automatically after the first successful ingestion job completes.
    Subsequent runs will skip this validation.
    
    Args:
        ingestion_successful: Whether the ingestion step completed successfully
    """
    # Only run if ingestion was successful and validation hasn't run before
    if not ingestion_successful:
        return
    
    if has_initial_validation_run():
        return
    
    print("\n" + "=" * 70)
    print("INITIAL POST-INGESTION VALIDATION (ONE-TIME)")
    print("=" * 70)
    print("Running thorough validation after initial ingestion...")
    print("This validation will only run once after the first successful ingestion.")
    
    try:
        db_config = get_db_config()
        conn = await get_db_connection(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
        
        # Run thorough validation with verbose output
        report = await check_all_integrity(conn, verbose=True, thorough=True)
        report.print_summary()
        
        # Print detailed issues if any found
        if report.has_issues:
            print("\n" + "-" * 70)
            print("DETAILED ISSUE LIST")
            print("-" * 70)
            for issue in report.issues:
                print(f"\n  [{issue.issue_type.value}]")
                print(f"    {issue.description}")
                if issue.player_ids and len(issue.player_ids) <= 10:
                    print(f"    Players: {issue.player_ids}")
                elif issue.player_ids:
                    print(f"    Players: {issue.player_ids[:5]} ... and {len(issue.player_ids) - 5} more")
            
            print("\n⚠ Data integrity issues detected after initial ingestion!")
            print("  This may indicate an interrupted pipeline run.")
            print("  Run validate_cli.py --repair to fix, or re-run this pipeline with --validate-before --auto-repair")
        else:
            print("\n✓ No data integrity issues found - all matches have complete data!")
        
        # Mark validation as complete
        mark_initial_validation_complete()
        print("\n✓ Initial validation complete. This check will not run again automatically.")
        print("  To run validation again, delete the marker file or use validate_cli.py")
        
        await conn.close()
    except Exception as e:
        print(f"\n✗ Initial validation error: {e}")
        import traceback
        traceback.print_exc()
        # Don't mark as complete if validation failed
        print("  Validation will be retried on the next ingestion run.")


async def run_pipeline(
    skip_all_matches_fetch: bool,
    skip_existing_fetch: bool,
    skip_duplicates_insert: bool,
    update_match_history: bool,
    update_player_stats: bool,
    skip_fetch: bool,
    skip_insert: bool,
    validate_before: bool = False,
    validate_after: bool = False,
    auto_repair: bool = False,
    thorough_validation: bool = False,
) -> None:
    """Run the full ingestion pipeline with configurable steps."""
    print("=" * 70)
    print("MATCH RESULTS INGESTION PIPELINE")
    print("=" * 70)
    
    # Pre-ingestion validation
    if validate_before:
        print("\n" + "=" * 70)
        print("PRE-INGESTION VALIDATION")
        print("=" * 70)
        try:
            db_config = get_db_config()
            conn = await get_db_connection(
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                user=db_config.user,
                password=db_config.password,
            )
            
            report = await check_all_integrity(conn, verbose=True, thorough=thorough_validation)
            report.print_summary()
            
            if report.has_issues and auto_repair:
                print("\nAuto-repairing detected issues...")
                affected_matches = sorted(report.affected_match_ids)
                await delete_incomplete_match_data(conn, affected_matches, dry_run=False)
                print(f"✓ Deleted incomplete data for {len(affected_matches)} matches")
                print("  These matches will be re-ingested during this pipeline run.")
            elif report.has_issues:
                print("\n⚠ Issues found but --auto-repair not set.")
                print("  Run with --auto-repair to fix issues, or use validate_cli.py for manual control.")
            
            await conn.close()
        except Exception as e:
            print(f"\n✗ Validation error: {e}")
            import traceback
            traceback.print_exc()
    
    # Step 0: Fetch all matches list from API
    if not skip_all_matches_fetch:
        print("\n" + "=" * 70)
        print("STEP 0: FETCHING ALL MATCHES LIST FROM API")
        print("=" * 70)
        try:
            await fetch_all_matches_main()
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
            await fetch_match_history_main(skip_existing=skip_existing_fetch)
            print("\n✓ Fetch step completed successfully")
        except Exception as e:
            print(f"\n✗ Error during fetch step: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping fetch step (using existing match_results/ directory)")
    
    # Step 2: Transform and insert data into database
    ingestion_successful = False
    if not skip_insert:
        print("\n" + "=" * 70)
        print("STEP 2: TRANSFORMING AND INSERTING DATA INTO DATABASE")
        print("=" * 70)
        try:
            await update_db_main(
                skip_duplicates=skip_duplicates_insert,
                update_match_history=update_match_history,
                update_player_stats=update_player_stats,
            )
            print("\n✓ Database update step completed successfully")
            ingestion_successful = True
        except Exception as e:
            print(f"\n✗ Error during database update step: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping database insertion step")
        # If skipping insert, don't run initial validation
        ingestion_successful = False
    
    # Run initial validation if needed (one-time, automatic, thorough)
    await run_initial_validation_if_needed(ingestion_successful)
    
    # Post-ingestion validation (manual, if requested)
    if validate_after:
        print("\n" + "=" * 70)
        print("POST-INGESTION VALIDATION")
        print("=" * 70)
        try:
            db_config = get_db_config()
            conn = await get_db_connection(
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                user=db_config.user,
                password=db_config.password,
            )
            
            report = await check_all_integrity(conn, verbose=True, thorough=thorough_validation)
            report.print_summary()
            
            if report.has_issues:
                print("\n⚠ Data integrity issues detected after ingestion!")
                print("  This may indicate an interrupted pipeline run.")
                print("  Run validate_cli.py --repair to fix, or re-run this pipeline with --validate-before --auto-repair")
            
            await conn.close()
        except Exception as e:
            print(f"\n✗ Validation error: {e}")
            import traceback
            traceback.print_exc()
    
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
    
    # Validation options
    parser.add_argument(
        "--validate-before",
        action="store_true",
        default=False,
        help="Run data integrity validation before ingestion to detect partial insertions."
    )
    parser.add_argument(
        "--validate-after",
        action="store_true",
        default=False,
        help="Run data integrity validation after ingestion to verify completeness."
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        default=False,
        help="Shorthand for --validate-before --validate-after (run validation before and after)."
    )
    parser.add_argument(
        "--auto-repair",
        action="store_true",
        default=False,
        help="Automatically delete incomplete match data detected during pre-validation (use with --validate-before)."
    )
    parser.add_argument(
        "--thorough-validation",
        action="store_true",
        default=False,
        help="Use thorough validation mode that checks every individual player record (slower but comprehensive)."
    )
    
    args = parser.parse_args()
    
    # Determine which tables to update (default: both)
    # If both skip flags are set, update both (same behavior as old "both flags = update both")
    
    # Handle --validate shorthand
    validate_before = args.validate_before or args.validate
    validate_after = args.validate_after or args.validate
    
    asyncio.run(run_pipeline(
        skip_all_matches_fetch=args.skip_all_matches_fetch,
        skip_existing_fetch=args.skip_existing_fetch,
        skip_duplicates_insert=not args.no_skip_duplicates,
        update_match_history=True if (args.skip_match_history and args.skip_player_stats) else not args.skip_match_history,
        update_player_stats=True if (args.skip_match_history and args.skip_player_stats) else not args.skip_player_stats,
        skip_fetch=args.skip_fetch,
        skip_insert=args.skip_insert,
        validate_before=validate_before,
        validate_after=validate_after,
        auto_repair=args.auto_repair,
        thorough_validation=args.thorough_validation,
    ))


if __name__ == "__main__":
    main()
