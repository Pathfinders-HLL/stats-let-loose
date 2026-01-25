"""Match history insertion operations."""

from typing import Any, Dict, List

import asyncpg

from apps.api_stats_ingestion.load.db.checks import check_existing_match_ids


async def insert_match_history(
    conn: asyncpg.Connection,
    matches: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert match history records into the database."""
    if not matches:
        return 0, 0
    
    if skip_duplicates:
        match_ids = [match.get("match_id") for match in matches if match.get("match_id") is not None]
        existing_match_ids = await check_existing_match_ids(conn, match_ids)
        
        matches_to_insert = [
            match for match in matches
            if match.get("match_id") not in existing_match_ids
        ]
        
        skipped_count = len(matches) - len(matches_to_insert)
        
        if not matches_to_insert:
            return 0, skipped_count
        
        matches = matches_to_insert
    else:
        skipped_count = 0
    
    columns = [
        "match_id", "map_id", "map_name", "map_short_name", "game_mode",
        "environment", "allies_score", "axis_score", "winning_team",
        "start_time", "end_time", "match_duration"
    ]
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.match_history ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.match_history ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(matches)
    print(f"Inserting {total_records} match history records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(matches), batch_size):
        batch = matches[i : i + batch_size]
        try:
            batch_data = [
                tuple(match.get(col) for col in columns)
                for match in batch
            ]
            await conn.executemany(insert_query, batch_data)
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count
