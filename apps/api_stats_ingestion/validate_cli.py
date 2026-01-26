"""
Data integrity validation CLI for the stats ingestion pipeline.

This script checks for partial match insertions and optionally repairs them by
deleting incomplete data so matches can be re-ingested.

Usage:
    # Check for issues (default - report only)
    python -m apps.api_stats_ingestion.validate_cli

    # Check and show detailed issue list
    python -m apps.api_stats_ingestion.validate_cli --verbose

    # Delete incomplete match data (dry run first)
    python -m apps.api_stats_ingestion.validate_cli --repair --dry-run

    # Actually delete incomplete match data
    python -m apps.api_stats_ingestion.validate_cli --repair

    # Delete specific match IDs
    python -m apps.api_stats_ingestion.validate_cli --delete-matches 123 456 789
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import List, Optional

from apps.api_stats_ingestion.validate import (
    IntegrityReport,
    check_all_integrity,
    delete_incomplete_match_data,
)
from libs.db.config import get_db_config
from libs.db.database import get_db_connection


async def run_validation(
    verbose: bool = False,
    repair: bool = False,
    dry_run: bool = True,
    delete_match_ids: Optional[List[int]] = None,
    thorough: bool = False,
) -> IntegrityReport:
    """
    Run data integrity validation and optionally repair issues.
    
    Args:
        verbose: If True, print detailed issue information
        repair: If True, delete data for matches with integrity issues
        dry_run: If True (with repair), only show what would be deleted
        delete_match_ids: Specific match IDs to delete (overrides auto-detection)
        thorough: If True, check every individual player record (slower but comprehensive)
    
    Returns:
        IntegrityReport with all issues found
    """
    print("=" * 60)
    print("DATA INTEGRITY VALIDATION")
    print("=" * 60)
    
    try:
        db_config = get_db_config()
    except (ValueError, TypeError) as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    
    try:
        conn = await get_db_connection(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
    except (ConnectionError, ValueError) as e:
        print(f"Database Error: {e}")
        sys.exit(1)
    
    try:
        # Run integrity checks
        report = await check_all_integrity(conn, verbose=True, thorough=thorough)
        
        # Print summary
        report.print_summary()
        
        # Print detailed issues if verbose
        if verbose and report.has_issues:
            print("\n" + "-" * 60)
            print("DETAILED ISSUE LIST")
            print("-" * 60)
            for issue in report.issues:
                print(f"\n  [{issue.issue_type.value}]")
                print(f"    {issue.description}")
                if issue.player_ids and len(issue.player_ids) <= 10:
                    print(f"    Players: {issue.player_ids}")
                elif issue.player_ids:
                    print(f"    Players: {issue.player_ids[:5]} ... and {len(issue.player_ids) - 5} more")
        
        # Handle deletion
        if delete_match_ids:
            # Delete specific matches
            print("\n" + "=" * 60)
            print("DELETING SPECIFIED MATCH DATA")
            print("=" * 60)
            print(f"\nMatches to delete: {delete_match_ids}")
            
            if dry_run:
                print("\n[DRY RUN MODE - No data will be deleted]")
            else:
                confirm = input("\nAre you sure you want to delete data for these matches? (yes/no): ")
                if confirm.lower() != "yes":
                    print("Deletion cancelled.")
                    return report
            
            await delete_incomplete_match_data(conn, delete_match_ids, dry_run=dry_run)
            
            if not dry_run:
                print("\n✓ Match data deleted. You can now re-run the ingestion pipeline to re-ingest these matches.")
        
        elif repair and report.has_issues:
            # Auto-repair detected issues
            print("\n" + "=" * 60)
            print("REPAIRING INTEGRITY ISSUES")
            print("=" * 60)
            
            affected_matches = sorted(report.affected_match_ids)
            print(f"\nMatches with issues: {affected_matches}")
            
            if dry_run:
                print("\n[DRY RUN MODE - No data will be deleted]")
                print("Run with --repair (without --dry-run) to actually delete data.")
            else:
                confirm = input(f"\nDelete all data for {len(affected_matches)} matches to allow re-ingestion? (yes/no): ")
                if confirm.lower() != "yes":
                    print("Repair cancelled.")
                    return report
            
            await delete_incomplete_match_data(conn, affected_matches, dry_run=dry_run)
            
            if not dry_run:
                print("\n✓ Incomplete match data deleted.")
                print("  Run the ingestion pipeline to re-ingest these matches:")
                print("  python -m apps.api_stats_ingestion.ingestion_cli --skip-all-matches-fetch --skip-existing-fetch")
        
        return report
    
    finally:
        await conn.close()
        print("\nDatabase connection closed")


async def update_player_counts_only() -> int:
    """
    Quick fix for matches with zero/null player_count but valid player stats.
    
    This doesn't require re-ingestion - just updates the count column.
    
    Returns:
        Number of matches updated
    """
    from apps.api_stats_ingestion.load.db.utils import update_match_player_counts
    
    print("=" * 60)
    print("UPDATING PLAYER COUNTS")
    print("=" * 60)
    
    try:
        db_config = get_db_config()
    except (ValueError, TypeError) as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    
    try:
        conn = await get_db_connection(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password,
        )
    except (ConnectionError, ValueError) as e:
        print(f"Database Error: {e}")
        sys.exit(1)
    
    try:
        updated = await update_match_player_counts(conn)
        print(f"\n✓ Updated player_count for {updated} matches")
        return updated
    
    finally:
        await conn.close()
        print("\nDatabase connection closed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate data integrity and detect partial match insertions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check for issues (report only)
  python -m apps.api_stats_ingestion.validate_cli

  # Check with detailed issue list
  python -m apps.api_stats_ingestion.validate_cli --verbose

  # Thorough check (verifies EVERY missing record, slower)
  python -m apps.api_stats_ingestion.validate_cli --thorough

  # Preview what would be deleted for detected issues
  python -m apps.api_stats_ingestion.validate_cli --repair --dry-run

  # Delete data for detected issues (requires confirmation)
  python -m apps.api_stats_ingestion.validate_cli --repair

  # Fix only the player_count column (quick fix, no re-ingestion needed)
  python -m apps.api_stats_ingestion.validate_cli --fix-player-counts

  # Delete specific matches (preview)
  python -m apps.api_stats_ingestion.validate_cli --delete-matches 123 456 --dry-run

  # Delete specific matches (actual deletion)
  python -m apps.api_stats_ingestion.validate_cli --delete-matches 123 456

Note:
  Standard mode (default) detects patterns of partial insertion:
    - Matches where ALL players are missing victim/nemesis stats
    - ANY players missing kill/death stats
  
  Thorough mode (--thorough) checks every individual player record:
    - Slower but finds ALL missing records, not just patterns
    - Recommended for verification after fixing issues
        """,
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed issue information",
    )
    
    parser.add_argument(
        "--thorough",
        action="store_true",
        help="Check every individual player record (slower but finds all missing records, not just patterns)",
    )
    
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Delete data for matches with integrity issues (allows re-ingestion)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without actually deleting (use with --repair or --delete-matches)",
    )
    
    parser.add_argument(
        "--delete-matches",
        nargs="+",
        type=int,
        metavar="MATCH_ID",
        help="Delete all data for specific match IDs",
    )
    
    parser.add_argument(
        "--fix-player-counts",
        action="store_true",
        help="Only fix matches with zero/null player_count (quick fix, no re-ingestion)",
    )
    
    args = parser.parse_args()
    
    # Handle quick fix for player counts
    if args.fix_player_counts:
        asyncio.run(update_player_counts_only())
        return
    
    # Run validation
    report = asyncio.run(run_validation(
        verbose=args.verbose,
        repair=args.repair,
        dry_run=args.dry_run,
        delete_match_ids=args.delete_matches,
        thorough=args.thorough,
    ))
    
    # Exit with appropriate code
    if report.has_issues and not args.repair:
        sys.exit(1)  # Issues found but not repaired
    sys.exit(0)


if __name__ == "__main__":
    main()
