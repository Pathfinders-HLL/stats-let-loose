"""Weapon statistics insertion operations (kills and deaths)."""

import asyncpg

from typing import Any, Dict, List

from apps.api_stats_ingestion.load.db.checks import (
    check_existing_player_death_ids,
    check_existing_player_kill_ids,
)
from apps.api_stats_ingestion.load.db.db_utils import map_weapon_to_column


async def insert_player_kill_stats(
    conn: asyncpg.Connection,
    player_stats: List[Dict[str, Any]],
    weapon_schema_map: Dict[str, str],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player kill statistics by weapon type into player_kill_stats table."""
    if not player_stats:
        return 0, 0
    
    all_columns = set(weapon_schema_map.values())
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
            weapons = raw_info.get("weapons", {})
        else:
            weapons = stat.get("weapons", {})
        
        if not isinstance(weapons, dict):
            continue
        
        kill_counts = {col: 0 for col in all_columns}
        unmapped_weapons = set()
        
        for weapon_name, count in weapons.items():
            if not isinstance(count, (int, float)):
                continue
            
            column_name = map_weapon_to_column(weapon_name, weapon_schema_map)
            if column_name:
                kill_counts[column_name] = kill_counts.get(column_name, 0) + int(count)
            else:
                unmapped_weapons.add(weapon_name)
        
        if unmapped_weapons:
            for weapon in sorted(unmapped_weapons):
                print(f"ERROR: Unmapped weapon in kill stats - Player: {player_name} (ID: {player_id}), Match: {match_id}, Weapon: '{weapon}'")
        
        record = {
            "player_id": player_id,
            "match_id": match_id,
            "player_name": player_name,
            "team": team,
            **kill_counts
        }
        processed_records.append(record)
    
    if not processed_records:
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"])
            for record in processed_records
        ]
        existing_keys = await check_existing_player_kill_ids(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    weapon_columns = sorted(all_columns)
    all_columns_list = ["player_id", "match_id", "player_name", "team"] + weapon_columns
    
    # Build the INSERT query with ON CONFLICT
    columns_str = ", ".join(all_columns_list)
    placeholders = ", ".join([f"${i+1}" for i in range(len(all_columns_list))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_kill_stats ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_kill_stats ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player kill stats records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            # Prepare batch data as list of tuples
            batch_data = [
                tuple(record.get(col) for col in all_columns_list)
                for record in batch
            ]
            result = await conn.executemany(insert_query, batch_data)
            # executemany doesn't return row counts easily, count by batch size
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting kill stats batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count


async def insert_player_death_stats(
    conn: asyncpg.Connection,
    player_stats: List[Dict[str, Any]],
    weapon_schema_map: Dict[str, str],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player death statistics by weapon type into player_death_stats table."""
    if not player_stats:
        return 0, 0
    
    all_columns = set(weapon_schema_map.values())
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
            death_by_weapons = raw_info.get("death_by_weapons", {})
        else:
            death_by_weapons = stat.get("death_by_weapons", {})
        
        if not isinstance(death_by_weapons, dict):
            continue
        
        death_counts = {col: 0 for col in all_columns}
        unmapped_weapons = set()
        
        for weapon_name, count in death_by_weapons.items():
            if not isinstance(count, (int, float)):
                continue
            
            column_name = map_weapon_to_column(weapon_name, weapon_schema_map)
            if column_name:
                death_counts[column_name] = death_counts.get(column_name, 0) + int(count)
            else:
                unmapped_weapons.add(weapon_name)
        
        if unmapped_weapons:
            for weapon in sorted(unmapped_weapons):
                print(f"ERROR: Unmapped weapon in death stats - Player: {player_name} (ID: {player_id}), Match: {match_id}, Weapon: '{weapon}'")
        
        record = {
            "player_id": player_id,
            "match_id": match_id,
            "player_name": player_name,
            "team": team,
            **death_counts
        }
        processed_records.append(record)
    
    if not processed_records:
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"])
            for record in processed_records
        ]
        existing_keys = await check_existing_player_death_ids(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    weapon_columns = sorted(all_columns)
    all_columns_list = ["player_id", "match_id", "player_name", "team"] + weapon_columns
    
    columns_str = ", ".join(all_columns_list)
    placeholders = ", ".join([f"${i+1}" for i in range(len(all_columns_list))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_death_stats ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_death_stats ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player death stats records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            batch_data = [
                tuple(record.get(col) for col in all_columns_list)
                for record in batch
            ]
            await conn.executemany(insert_query, batch_data)
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting death stats batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count
