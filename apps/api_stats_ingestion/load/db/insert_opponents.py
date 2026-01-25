"""Opponent statistics insertion operations (victims and nemesis)."""

from typing import Any, Dict, List

import asyncpg

from apps.api_stats_ingestion.load.db.checks import (
    check_existing_player_nemesis_ids,
    check_existing_player_victim_ids,
)


async def check_existing_player_victim_stats(
    conn: asyncpg.Connection,
    player_match_keys: List[tuple[str, int, str]],
    batch_size: int = 10000,
) -> set[tuple[str, int, str]]:
    """Check which (player_id, match_id, victim_name) rows already exist in player_victim table."""
    if not player_match_keys:
        return set()
    
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        rows = await conn.fetch(
            """
            SELECT player_id, match_id, victim_name
            FROM pathfinder_stats.player_victim
            WHERE (player_id, match_id, victim_name) IN (
                SELECT * FROM unnest($1::text[], $2::int[], $3::text[])
            )
            """,
            [k[0] for k in batch],
            [k[1] for k in batch],
            [k[2] for k in batch]
        )
        existing.update({(row["player_id"], row["match_id"], row["victim_name"]) for row in rows})
    
    return existing


async def check_existing_player_nemesis_stats(
    conn: asyncpg.Connection,
    player_match_keys: List[tuple[str, int, str]],
    batch_size: int = 10000,
) -> set[tuple[str, int, str]]:
    """Check which (player_id, match_id, nemesis_name) rows already exist in player_nemesis table."""
    if not player_match_keys:
        return set()
    
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        rows = await conn.fetch(
            """
            SELECT player_id, match_id, nemesis_name
            FROM pathfinder_stats.player_nemesis
            WHERE (player_id, match_id, nemesis_name) IN (
                SELECT * FROM unnest($1::text[], $2::int[], $3::text[])
            )
            """,
            [k[0] for k in batch],
            [k[1] for k in batch],
            [k[2] for k in batch]
        )
        existing.update({(row["player_id"], row["match_id"], row["nemesis_name"]) for row in rows})
    
    return existing


async def insert_player_victim_stats(
    conn: asyncpg.Connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player victim statistics into player_victim table."""
    if not player_stats:
        return 0, 0
    
    processed_records = []
    
    for stat in player_stats:
        player_id = stat.get("player_id")
        match_id = stat.get("match_id")
        player_name = stat.get("player_name")
        team = stat.get("team")
        
        if not all([player_id, match_id, player_name]):
            continue
        
        raw_info = stat.get("raw_info")
        if raw_info and isinstance(raw_info, dict):
            most_killed = raw_info.get("most_killed", {})
        else:
            most_killed = stat.get("most_killed", {})
        
        if not isinstance(most_killed, dict) or not most_killed:
            continue
        
        for victim_name, count in most_killed.items():
            if not victim_name or not isinstance(count, (int, float)):
                continue
            
            processed_records.append({
                "player_id": player_id,
                "match_id": match_id,
                "player_name": player_name,
                "team": team,
                "victim_name": str(victim_name),
                "kill_count": int(count),
            })
    
    if not processed_records:
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"], record["victim_name"])
            for record in processed_records
        ]
        existing_keys = await check_existing_player_victim_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"], record["victim_name"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    columns = ["player_id", "match_id", "player_name", "team", "victim_name", "kill_count"]
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_victim ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (player_id, match_id, victim_name) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_victim ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player victim records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            batch_data = [
                tuple(record.get(col) for col in columns)
                for record in batch
            ]
            await conn.executemany(insert_query, batch_data)
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting victim batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count


async def insert_player_nemesis_stats(
    conn: asyncpg.Connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player nemesis statistics into player_nemesis table."""
    if not player_stats:
        return 0, 0
    
    processed_records = []
    
    for stat in player_stats:
        player_id = stat.get("player_id")
        match_id = stat.get("match_id")
        player_name = stat.get("player_name")
        team = stat.get("team")
        
        if not all([player_id, match_id, player_name]):
            continue
        
        raw_info = stat.get("raw_info")
        if raw_info and isinstance(raw_info, dict):
            death_by = raw_info.get("death_by", {})
        else:
            death_by = stat.get("death_by", {})
        
        if not isinstance(death_by, dict) or not death_by:
            continue
        
        for nemesis_name, count in death_by.items():
            if not nemesis_name or not isinstance(count, (int, float)):
                continue
            
            processed_records.append({
                "player_id": player_id,
                "match_id": match_id,
                "player_name": player_name,
                "team": team,
                "nemesis_name": str(nemesis_name),
                "death_count": int(count),
            })
    
    if not processed_records:
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"], record["nemesis_name"])
            for record in processed_records
        ]
        existing_keys = await check_existing_player_nemesis_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"], record["nemesis_name"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    columns = ["player_id", "match_id", "player_name", "team", "nemesis_name", "death_count"]
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_nemesis ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (player_id, match_id, nemesis_name) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_nemesis ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player nemesis records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            batch_data = [
                tuple(record.get(col) for col in columns)
                for record in batch
            ]
            await conn.executemany(insert_query, batch_data)
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting nemesis batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count
