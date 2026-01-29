"""
Update PostgreSQL database with transformed match results data.

This script orchestrates the ETL pipeline's load phase:
- Loads match history data into pathfinder_stats.match_history table
- Loads player statistics into pathfinder_stats.player_match_stats table
- Loads weapon statistics (kills/deaths) into player_kill_stats and player_death_stats tables
  (extracted from raw_info during initial ingestion)
- Loads opponent statistics (victims/nemesis) into player_victim and player_nemesis tables
- Updates player_count column in match_history for query optimization
- Handles duplicate entries gracefully (ON CONFLICT DO NOTHING)
- Provides progress feedback during insertion
- Supports graceful shutdown to complete current batch before exit

All data is inserted during the initial ingestion - no backfilling is required.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import sys

from apps.api_stats_ingestion.config import get_ingestion_config
from apps.api_stats_ingestion.graceful_shutdown import should_shutdown
from apps.api_stats_ingestion.load.db import (
    insert_match_history,
    insert_player_death_stats,
    insert_player_kill_stats,
    insert_player_nemesis_stats,
    insert_player_stats,
    insert_player_victim_stats,
    load_weapon_schemas,
    update_match_player_counts,
)
from apps.api_stats_ingestion.transform.match_results import (
    transform_match_history_data_batched,
    transform_player_stats_data_batched,
)
from libs.db.config import get_db_config
from libs.db.database import get_db_connection


async def main(
    skip_duplicates: bool = True,
    update_match_history: bool = True,
    update_player_stats: bool = True,
    update_weapon_stats: bool = True,
    update_opponent_stats: bool = True,
) -> None:
    """
    Main function to coordinate database loading operations.
    
    Args:
        skip_duplicates: If True, skip duplicate records (default: True)
        update_match_history: If True, update match_history table (default: True)
        update_player_stats: If True, update player_match_stats table (default: True)
        update_weapon_stats: If True, update player_kill_stats and player_death_stats tables (default: True)
        update_opponent_stats: If True, update player_victim and player_nemesis tables (default: True)
    """
    try:
        ingestion_config = get_ingestion_config()
        db_config = get_db_config()
    except (ValueError, TypeError) as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
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
        print(f"Error: {e}")
        return
    
    try:
        if update_match_history:
            print("\n" + "=" * 60)
            print("TRANSFORMING AND INSERTING MATCH HISTORY DATA")
            print("=" * 60)
            
            transform_batch_size = ingestion_config.match_history_batch_size * 10
            total_inserted = 0
            total_skipped = 0
            batch_count = 0
            shutdown_during_match_history = False
            
            for batch in transform_match_history_data_batched(batch_size=transform_batch_size):
                # Check for shutdown request between batches
                if should_shutdown():
                    print("Shutdown requested - stopping match history insertion after current batch")
                    shutdown_during_match_history = True
                    break
                
                batch_count += 1
                if batch:
                    # Wrap batch processing in a transaction to ensure atomicity per batch
                    async with conn.transaction():
                        inserted, skipped = await insert_match_history(
                            conn, batch, ingestion_config.match_history_batch_size, skip_duplicates=skip_duplicates
                        )
                        total_inserted += inserted
                        total_skipped += skipped
                    
                    # After batch transaction commits, check for shutdown
                    if should_shutdown():
                        print("Shutdown requested - stopping match history insertion after completing current batch")
                        shutdown_during_match_history = True
                        break
                    
                    gc.collect()
            
            print(f"\nMatch History Summary:")
            print(f"  Successfully inserted: {total_inserted}")
            print(f"  Skipped (duplicates/invalid): {total_skipped}")
            if batch_count == 0:
                print("No match history data to insert")
            
            # If shutdown was requested during match history, exit early
            if shutdown_during_match_history:
                print("\nGraceful shutdown: Exiting after match history phase")
                return
        
        weapon_schema_map = None
        if update_weapon_stats:
            try:
                print("\nLoading weapon schemas...")
                weapon_schema_map = load_weapon_schemas()
                print(f"Loaded {len(weapon_schema_map)} weapon name mappings")
            except Exception as e:
                print(f"Warning: Failed to load weapon schemas: {e}")
                print("Skipping weapon stats insertion")
                update_weapon_stats = False
        
        if update_player_stats or update_weapon_stats or update_opponent_stats:
            print("\n" + "=" * 60)
            print("TRANSFORMING AND INSERTING PLAYER STATISTICS DATA")
            print("=" * 60)
            
            # Fetch existing match IDs to skip already-processed files
            existing_match_ids = set()
            if skip_duplicates:
                try:
                    print("Fetching existing match IDs from database to skip already-processed files...")
                    existing_ids = await conn.fetch(
                        "SELECT DISTINCT match_id FROM pathfinder_stats.player_match_stats"
                    )
                    existing_match_ids = {row['match_id'] for row in existing_ids}
                    print(f"Found {len(existing_match_ids)} matches already in database")
                except Exception as e:
                    print(f"Warning: Could not fetch existing match IDs: {e}")
                    print("Will process all files (may lead to duplicate skipping during insertion)")
            
            transform_batch_size = ingestion_config.player_stats_batch_size * 10
            total_inserted = 0
            total_skipped = 0
            batch_count = 0
            shutdown_during_player_stats = False
            
            total_kill_stats_inserted = 0
            total_kill_stats_skipped = 0
            total_death_stats_inserted = 0
            total_death_stats_skipped = 0
            total_victim_stats_inserted = 0
            total_victim_stats_skipped = 0
            total_nemesis_stats_inserted = 0
            total_nemesis_stats_skipped = 0
            
            for batch in transform_player_stats_data_batched(
                batch_size=transform_batch_size,
                existing_match_ids=existing_match_ids if skip_duplicates else None
            ):
                # Check for shutdown request between batches
                if should_shutdown():
                    print("Shutdown requested - stopping player stats insertion after current batch")
                    shutdown_during_player_stats = True
                    break
                
                batch_count += 1
                if batch:
                    # Wrap batch processing in a transaction to ensure atomicity per batch
                    # This ensures that either all tables for a batch are inserted, or none are
                    # If shutdown occurs mid-batch, we complete the current batch (let it commit),
                    # then exit on the next iteration check
                    async with conn.transaction():
                        if update_weapon_stats and weapon_schema_map:
                            kill_inserted, kill_skipped = await insert_player_kill_stats(
                                conn, batch, weapon_schema_map, 
                                ingestion_config.player_stats_batch_size, 
                                skip_duplicates=skip_duplicates
                            )
                            total_kill_stats_inserted += kill_inserted
                            total_kill_stats_skipped += kill_skipped
                            
                            death_inserted, death_skipped = await insert_player_death_stats(
                                conn, batch, weapon_schema_map,
                                ingestion_config.player_stats_batch_size,
                                skip_duplicates=skip_duplicates
                            )
                            total_death_stats_inserted += death_inserted
                            total_death_stats_skipped += death_skipped
                        
                        if update_opponent_stats:
                            victim_inserted, victim_skipped = await insert_player_victim_stats(
                                conn, batch, ingestion_config.player_stats_batch_size,
                                skip_duplicates=skip_duplicates
                            )
                            total_victim_stats_inserted += victim_inserted
                            total_victim_stats_skipped += victim_skipped
                            
                            nemesis_inserted, nemesis_skipped = await insert_player_nemesis_stats(
                                conn, batch, ingestion_config.player_stats_batch_size,
                                skip_duplicates=skip_duplicates
                            )
                            total_nemesis_stats_inserted += nemesis_inserted
                            total_nemesis_stats_skipped += nemesis_skipped
                        
                        if update_player_stats:
                            inserted, skipped = await insert_player_stats(
                                conn, batch, ingestion_config.player_stats_batch_size, skip_duplicates=skip_duplicates
                            )
                            total_inserted += inserted
                            total_skipped += skipped
                    
                    # After batch transaction commits, check for shutdown
                    # This ensures the current batch completes before exiting
                    if should_shutdown():
                        print("Shutdown requested - stopping player stats insertion after completing current batch")
                        shutdown_during_player_stats = True
                        break
                    
                    gc.collect()
            
            if update_player_stats:
                print(f"\nPlayer Statistics Summary:")
                print(f"  Successfully inserted: {total_inserted}")
                print(f"  Skipped (duplicates/invalid): {total_skipped}")
                if batch_count == 0:
                    print("No player statistics data to insert")
            
            if update_weapon_stats:
                print(f"\nPlayer Kill Stats Summary:")
                print(f"  Successfully inserted: {total_kill_stats_inserted}")
                print(f"  Skipped (duplicates/invalid): {total_kill_stats_skipped}")
                
                print(f"\nPlayer Death Stats Summary:")
                print(f"  Successfully inserted: {total_death_stats_inserted}")
                print(f"  Skipped (duplicates/invalid): {total_death_stats_skipped}")
            
            if update_opponent_stats:
                print(f"\nPlayer Victim Stats Summary:")
                print(f"  Inserted: {total_victim_stats_inserted}, Skipped: {total_victim_stats_skipped}")
                
                print(f"\nPlayer Nemesis Stats Summary:")
                print(f"  Inserted: {total_nemesis_stats_inserted}, Skipped: {total_nemesis_stats_skipped}")
            
            # If shutdown was requested during player stats, exit early
            if shutdown_during_player_stats:
                print("\nGraceful shutdown: Exiting after player stats phase")
                return
            
            # Update player_count in match_history after inserting player stats
            print("\n" + "=" * 60)
            print("UPDATING MATCH PLAYER COUNTS")
            print("=" * 60)
            await update_match_player_counts(conn)
        
        print("\n" + "=" * 60)
        print("DATABASE UPDATE COMPLETE")
        print("=" * 60)
    
    except Exception as e:
        print(f"\nError during data processing: {e}")
        import traceback
        traceback.print_exc()
        return
    
    finally:
        await conn.close()
        print("\nDatabase connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update PostgreSQL database with transformed match results data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--no-skip-duplicates",
        action="store_true",
        help="Fail on duplicate entries instead of skipping",
    )
    parser.add_argument(
        "--only-match-history",
        action="store_true",
        help="Update only the match_history table",
    )
    parser.add_argument(
        "--only-player-stats",
        action="store_true",
        help="Update only the player_match_stats table",
    )
    
    args = parser.parse_args()
    
    update_match_history = not args.only_player_stats
    update_player_stats = not args.only_match_history
    
    if args.only_match_history and args.only_player_stats:
        update_match_history = True
        update_player_stats = True
    
    update_weapon_stats = update_player_stats
    update_opponent_stats = update_player_stats
    
    asyncio.run(main(
        skip_duplicates=not args.no_skip_duplicates,
        update_match_history=update_match_history,
        update_player_stats=update_player_stats,
        update_weapon_stats=update_weapon_stats,
        update_opponent_stats=update_opponent_stats,
    ))
