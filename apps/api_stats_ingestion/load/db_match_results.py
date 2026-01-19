"""
Update PostgreSQL database with transformed match results data.

This script:
- Calls transform_match_history_data() to get match history data
- Calls transform_player_stats_data() to get player statistics data
- Inserts data into pathfinder_stats.match_history table
- Inserts data into pathfinder_stats.player_match_stats table
- Inserts data into pathfinder_stats.player_victim table
- Inserts data into pathfinder_stats.player_nemesis table
- Handles duplicate entries (ON CONFLICT DO NOTHING)
- Provides progress feedback during insertion
"""

from __future__ import annotations

import argparse
import csv
import gc
import sys
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_batch, Json

from apps.api_stats_ingestion.config import get_ingestion_config
from apps.api_stats_ingestion.transform.match_results import (
    transform_match_history_data_batched,
    transform_player_stats_data_batched,
)
from libs.db.config import get_db_config
from libs.db.database import get_db_connection
from libs.hll_data import WEAPON_SCHEMAS_PATH


def load_weapon_schemas() -> Dict[str, str]:
    """
    Load weapon schemas from CSV file and create a mapping from weapon names to column names.
    
    The CSV has columns: WeaponType, ValidNames, FriendlyName
    ValidNames is semicolon-separated list of weapon names (case-insensitive matching)
    
    Returns:
        Dictionary mapping weapon name (lowercase) to database column name (lowercase)
    """
    if not WEAPON_SCHEMAS_PATH.exists():
        raise FileNotFoundError(f"Weapon schemas file not found: {WEAPON_SCHEMAS_PATH}")
    
    weapon_name_to_column: Dict[str, str] = {}
    
    with open(WEAPON_SCHEMAS_PATH, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
        reader = csv.DictReader(f)
        for row in reader:
            # Handle BOM in column names by stripping it
            weapon_type = row.get('WeaponType', '').strip() or row.get('\ufeffWeaponType', '').strip()
            valid_names_str = row.get('ValidNames', '').strip()
            
            if not weapon_type or not valid_names_str:
                continue
            
            # Convert weapon type to column name (lowercase, matching database schema)
            column_name = weapon_type.lower()
            
            # Split ValidNames by semicolon and map each name (case-insensitive) to the column
            valid_names = [name.strip() for name in valid_names_str.split(';') if name.strip()]
            for name in valid_names:
                # Use lowercase for case-insensitive matching
                weapon_name_to_column[name.lower()] = column_name
    
    return weapon_name_to_column


def map_weapon_to_column(weapon_name: str, weapon_schema_map: Dict[str, str]) -> Optional[str]:
    """
    Map a weapon name to its corresponding database column name.
    
    Args:
        weapon_name: The weapon name from the JSON data
        weapon_schema_map: Dictionary mapping weapon names (lowercase) to column names
    
    Returns:
        Column name if found, None otherwise
    """
    return weapon_schema_map.get(weapon_name.lower())


def check_existing_match_ids(
    conn: psycopg2.extensions.connection,
    match_ids: List[int],
) -> set[int]:
    """Check which match IDs already exist in the match_history table."""
    if not match_ids:
        return set()
    
    cursor = conn.cursor()
    query = """
        SELECT match_id
        FROM pathfinder_stats.match_history
        WHERE match_id = ANY(%s)
    """
    cursor.execute(query, (match_ids,))
    existing = {row[0] for row in cursor.fetchall()}
    cursor.close()
    return existing


def check_existing_player_match_ids(
    conn: psycopg2.extensions.connection,
    player_match_keys: List[tuple[str, int]],
    batch_size: int = 10000,
) -> set[tuple[str, int]]:
    """Check which (player_id, match_id) pairs already exist in player_match_stats table."""
    if not player_match_keys:
        return set()
    
    cursor = conn.cursor()
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        values_list = ",".join(["(%s, %s)"] * len(batch))
        query = f"""
            SELECT player_id, match_id
            FROM pathfinder_stats.player_match_stats
            WHERE (player_id, match_id) IN (VALUES {values_list})
        """
        params = [item for pair in batch for item in pair]
        cursor.execute(query, params)
        existing.update({(row[0], row[1]) for row in cursor.fetchall()})
    
    cursor.close()
    return existing


def check_existing_player_kill_stats(
    conn: psycopg2.extensions.connection,
    player_match_keys: List[tuple[str, int]],
    batch_size: int = 10000,
) -> set[tuple[str, int]]:
    """Check which (player_id, match_id) pairs already exist in player_kill_stats table."""
    if not player_match_keys:
        return set()
    
    cursor = conn.cursor()
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        values_list = ",".join(["(%s, %s)"] * len(batch))
        query = f"""
            SELECT player_id, match_id
            FROM pathfinder_stats.player_kill_stats
            WHERE (player_id, match_id) IN (VALUES {values_list})
        """
        params = [item for pair in batch for item in pair]
        cursor.execute(query, params)
        existing.update({(row[0], row[1]) for row in cursor.fetchall()})
    
    cursor.close()
    return existing


def check_existing_player_death_stats(
    conn: psycopg2.extensions.connection,
    player_match_keys: List[tuple[str, int]],
    batch_size: int = 10000,
) -> set[tuple[str, int]]:
    """Check which (player_id, match_id) pairs already exist in player_death_stats table."""
    if not player_match_keys:
        return set()
    
    cursor = conn.cursor()
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        values_list = ",".join(["(%s, %s)"] * len(batch))
        query = f"""
            SELECT player_id, match_id
            FROM pathfinder_stats.player_death_stats
            WHERE (player_id, match_id) IN (VALUES {values_list})
        """
        params = [item for pair in batch for item in pair]
        cursor.execute(query, params)
        existing.update({(row[0], row[1]) for row in cursor.fetchall()})
    
    cursor.close()
    return existing


def check_existing_player_victim_stats(
    conn: psycopg2.extensions.connection,
    player_match_keys: List[tuple[str, int, str]],
    batch_size: int = 10000,
) -> set[tuple[str, int, str]]:
    """Check which (player_id, match_id, victim_name) rows already exist in player_victim table."""
    if not player_match_keys:
        return set()
    
    cursor = conn.cursor()
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        values_list = ",".join(["(%s, %s, %s)"] * len(batch))
        query = f"""
            SELECT player_id, match_id, victim_name
            FROM pathfinder_stats.player_victim
            WHERE (player_id, match_id, victim_name) IN (VALUES {values_list})
        """
        params = [item for triplet in batch for item in triplet]
        cursor.execute(query, params)
        existing.update({(row[0], row[1], row[2]) for row in cursor.fetchall()})
    
    cursor.close()
    return existing


def check_existing_player_nemesis_stats(
    conn: psycopg2.extensions.connection,
    player_match_keys: List[tuple[str, int, str]],
    batch_size: int = 10000,
) -> set[tuple[str, int, str]]:
    """Check which (player_id, match_id, nemesis_name) rows already exist in player_nemesis table."""
    if not player_match_keys:
        return set()
    
    cursor = conn.cursor()
    existing = set()
    
    for i in range(0, len(player_match_keys), batch_size):
        batch = player_match_keys[i : i + batch_size]
        values_list = ",".join(["(%s, %s, %s)"] * len(batch))
        query = f"""
            SELECT player_id, match_id, nemesis_name
            FROM pathfinder_stats.player_nemesis
            WHERE (player_id, match_id, nemesis_name) IN (VALUES {values_list})
        """
        params = [item for triplet in batch for item in triplet]
        cursor.execute(query, params)
        existing.update({(row[0], row[1], row[2]) for row in cursor.fetchall()})
    
    cursor.close()
    return existing


def backfill_weapon_stats_from_db(
    conn: psycopg2.extensions.connection,
    weapon_schema_map: Dict[str, str],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int, int, int]:
    """Backfill weapon stats from existing player_match_stats records."""
    cursor = conn.cursor()
    
    query = """
        SELECT 
            pms.player_id,
            pms.match_id,
            pms.player_name,
            pms.team,
            pms.raw_info
        FROM pathfinder_stats.player_match_stats pms
        WHERE pms.raw_info IS NOT NULL
        AND (
            pms.raw_info ? 'weapons' OR 
            pms.raw_info ? 'death_by_weapons'
        )
        AND (
            NOT EXISTS (
                SELECT 1 FROM pathfinder_stats.player_kill_stats pks
                WHERE pks.player_id = pms.player_id AND pks.match_id = pms.match_id
            )
            OR NOT EXISTS (
                SELECT 1 FROM pathfinder_stats.player_death_stats pds
                WHERE pds.player_id = pms.player_id AND pds.match_id = pms.match_id
            )
        )
        ORDER BY pms.match_id, pms.player_id
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        cursor.close()
        return 0, 0, 0, 0
    
    print(f"\nFound {len(rows)} existing player_match_stats records to backfill weapon stats for...")
    
    player_stats = []
    for row in rows:
        player_id, match_id, player_name, team, raw_info = row
        player_stats.append({
            "player_id": player_id,
            "match_id": match_id,
            "player_name": player_name,
            "team": team,
            "raw_info": raw_info
        })
    
    cursor.close()
    
    kill_inserted, kill_skipped = insert_player_kill_stats(
        conn, player_stats, weapon_schema_map, batch_size, skip_duplicates
    )
    death_inserted, death_skipped = insert_player_death_stats(
        conn, player_stats, weapon_schema_map, batch_size, skip_duplicates
    )
    
    return kill_inserted, kill_skipped, death_inserted, death_skipped


def insert_player_kill_stats(
    conn: psycopg2.extensions.connection,
    player_stats: List[Dict[str, Any]],
    weapon_schema_map: Dict[str, str],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player kill statistics by weapon type into player_kill_stats table."""
    if not player_stats:
        return 0, 0
    
    cursor = conn.cursor()
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
        cursor.close()
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"])
            for record in processed_records
        ]
        existing_keys = check_existing_player_kill_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            cursor.close()
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    weapon_columns = sorted(all_columns)
    all_columns_list = ["player_id", "match_id", "player_name", "team"] + weapon_columns
    columns_str = ", ".join(all_columns_list)
    placeholders_str = ", ".join([f"%({col})s" for col in all_columns_list])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_kill_stats ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_kill_stats ({columns_str})
            VALUES ({placeholders_str})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player kill stats records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting kill stats batch {i//batch_size + 1}: {e}")
    
    cursor.close()
    return inserted_count, skipped_count


def insert_player_death_stats(
    conn: psycopg2.extensions.connection,
    player_stats: List[Dict[str, Any]],
    weapon_schema_map: Dict[str, str],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player death statistics by weapon type into player_death_stats table."""
    if not player_stats:
        return 0, 0
    
    cursor = conn.cursor()
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
        cursor.close()
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"])
            for record in processed_records
        ]
        existing_keys = check_existing_player_death_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            cursor.close()
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    weapon_columns = sorted(all_columns)
    all_columns_list = ["player_id", "match_id", "player_name", "team"] + weapon_columns
    columns_str = ", ".join(all_columns_list)
    placeholders_str = ", ".join([f"%({col})s" for col in all_columns_list])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_death_stats ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_death_stats ({columns_str})
            VALUES ({placeholders_str})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player death stats records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting death stats batch {i//batch_size + 1}: {e}")
    
    cursor.close()
    return inserted_count, skipped_count


def insert_player_victim_stats(
    conn: psycopg2.extensions.connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player victim statistics into player_victim table."""
    if not player_stats:
        return 0, 0
    
    cursor = conn.cursor()
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
        cursor.close()
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"], record["victim_name"])
            for record in processed_records
        ]
        existing_keys = check_existing_player_victim_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"], record["victim_name"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            cursor.close()
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    columns = ["player_id", "match_id", "player_name", "team", "victim_name", "kill_count"]
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f"%({col})s" for col in columns])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_victim ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (player_id, match_id, victim_name) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_victim ({columns_str})
            VALUES ({placeholders_str})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player victim records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting victim batch {i//batch_size + 1}: {e}")
    
    cursor.close()
    return inserted_count, skipped_count


def insert_player_nemesis_stats(
    conn: psycopg2.extensions.connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player nemesis statistics into player_nemesis table."""
    if not player_stats:
        return 0, 0
    
    cursor = conn.cursor()
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
        cursor.close()
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (record["player_id"], record["match_id"], record["nemesis_name"])
            for record in processed_records
        ]
        existing_keys = check_existing_player_nemesis_stats(conn, player_match_keys)
        
        records_to_insert = [
            record for record in processed_records
            if (record["player_id"], record["match_id"], record["nemesis_name"]) not in existing_keys
        ]
        
        skipped_count = len(processed_records) - len(records_to_insert)
        
        if not records_to_insert:
            cursor.close()
            return 0, skipped_count
        
        processed_records = records_to_insert
    else:
        skipped_count = 0
    
    columns = ["player_id", "match_id", "player_name", "team", "nemesis_name", "death_count"]
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f"%({col})s" for col in columns])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_nemesis ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (player_id, match_id, nemesis_name) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_nemesis ({columns_str})
            VALUES ({placeholders_str})
        """
    
    inserted_count = 0
    total_records = len(processed_records)
    print(f"Inserting {total_records} player nemesis records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_records), batch_size):
        batch = processed_records[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting nemesis batch {i//batch_size + 1}: {e}")
    
    cursor.close()
    return inserted_count, skipped_count


def insert_match_history(
    conn: psycopg2.extensions.connection,
    matches: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert match history records into the database."""
    if not matches:
        return 0, 0
    
    if skip_duplicates:
        match_ids = [match.get("match_id") for match in matches if match.get("match_id") is not None]
        existing_match_ids = check_existing_match_ids(conn, match_ids)
        
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
    
    cursor = conn.cursor()
    
    if skip_duplicates:
        insert_query = """
            INSERT INTO pathfinder_stats.match_history (
                match_id, map_id, map_name, map_short_name, game_mode,
                environment, allies_score, axis_score, winning_team,
                start_time, end_time, match_duration
            )
            VALUES (
                %(match_id)s, %(map_id)s, %(map_name)s, %(map_short_name)s, %(game_mode)s,
                %(environment)s, %(allies_score)s, %(axis_score)s, %(winning_team)s,
                %(start_time)s, %(end_time)s, %(match_duration)s
            )
            ON CONFLICT (match_id) DO NOTHING
        """
    else:
        insert_query = """
            INSERT INTO pathfinder_stats.match_history (
                match_id, map_id, map_name, map_short_name, game_mode,
                environment, allies_score, axis_score, winning_team,
                start_time, end_time, match_duration
            )
            VALUES (
                %(match_id)s, %(map_id)s, %(map_name)s, %(map_short_name)s, %(game_mode)s,
                %(environment)s, %(allies_score)s, %(axis_score)s, %(winning_team)s,
                %(start_time)s, %(end_time)s, %(match_duration)s
            )
        """
    
    inserted_count = 0
    total_records = len(matches)
    print(f"Inserting {total_records} match history records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(matches), batch_size):
        batch = matches[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting batch {i//batch_size + 1}: {e}")
    
    cursor.close()
    return inserted_count, skipped_count


def insert_player_stats(
    conn: psycopg2.extensions.connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player statistics records into the database."""
    if not player_stats:
        return 0, 0
    
    cursor = conn.cursor()
    
    if skip_duplicates:
        player_match_keys = [
            (stat.get("player_id"), stat.get("match_id"))
            for stat in player_stats
            if stat.get("player_id") and stat.get("match_id")
        ]
        existing_keys = check_existing_player_match_ids(conn, player_match_keys)
        
        stats_to_process = [
            stat for stat in player_stats
            if (stat.get("player_id"), stat.get("match_id")) not in existing_keys
        ]
        
        skipped_count = len(player_stats) - len(stats_to_process)
        
        if not stats_to_process:
            return 0, len(player_stats)
        
        player_stats = stats_to_process
    else:
        skipped_count = 0
    
    processed_stats = []
    for stat in player_stats:
        processed_stat = {
            "player_id": stat.get("player_id"),
            "match_id": stat.get("match_id"),
            "player_name": stat.get("player_name"),
            "team": stat.get("team"),
            "total_kills": stat.get("total_kills"),
            "total_deaths": stat.get("total_deaths"),
            "kill_streak": stat.get("kill_streak"),
            "death_streak": stat.get("death_streak"),
            "kills_per_minute": stat.get("kills_per_minute"),
            "deaths_per_minute": stat.get("deaths_per_minute"),
            "kill_death_ratio": stat.get("kill_death_ratio"),
            "combat_score": stat.get("combat_score"),
            "offense_score": stat.get("offense_score"),
            "defense_score": stat.get("defense_score"),
            "support_score": stat.get("support_score"),
            "shortest_life": stat.get("shortest_life"),
            "longest_life": stat.get("longest_life"),
            "time_played": stat.get("time_played"),
            "teamkills": stat.get("teamkills"),
            "infantry_kills": stat.get("infantry_kills"),
            "grenade_kills": stat.get("grenade_kills"),
            "machine_gun_kills": stat.get("machine_gun_kills"),
            "sniper_kills": stat.get("sniper_kills"),
            "artillery_kills": stat.get("artillery_kills"),
            "bazooka_kills": stat.get("bazooka_kills"),
            "mine_kills": stat.get("mine_kills"),
            "satchel_kills": stat.get("satchel_kills"),
            "commander_kills": stat.get("commander_kills"),
            "armor_kills": stat.get("armor_kills"),
            "pak_kills": stat.get("pak_kills"),
            "spa_kills": stat.get("spa_kills"),
            "infantry_deaths": stat.get("infantry_deaths"),
            "grenade_deaths": stat.get("grenade_deaths"),
            "machine_gun_deaths": stat.get("machine_gun_deaths"),
            "sniper_deaths": stat.get("sniper_deaths"),
            "artillery_deaths": stat.get("artillery_deaths"),
            "bazooka_deaths": stat.get("bazooka_deaths"),
            "mine_deaths": stat.get("mine_deaths"),
            "satchel_deaths": stat.get("satchel_deaths"),
            "commander_deaths": stat.get("commander_deaths"),
            "armor_deaths": stat.get("armor_deaths"),
            "pak_deaths": stat.get("pak_deaths"),
            "spa_deaths": stat.get("spa_deaths"),
        }
        
        if "raw_info" in stat and stat["raw_info"] is not None:
            processed_stat["raw_info"] = Json(stat["raw_info"])
        else:
            processed_stat["raw_info"] = None
            
        processed_stats.append(processed_stat)
    
    if skip_duplicates:
        insert_query = """
            INSERT INTO pathfinder_stats.player_match_stats (
                player_id, match_id, player_name, team,
                total_kills, total_deaths, kill_streak, death_streak,
                kills_per_minute, deaths_per_minute, kill_death_ratio, time_played,
                combat_score, offense_score, defense_score, support_score,
                shortest_life, longest_life, teamkills,
                infantry_kills, grenade_kills, machine_gun_kills, sniper_kills,
                artillery_kills, bazooka_kills, mine_kills, satchel_kills,
                commander_kills, armor_kills, pak_kills, spa_kills,
                infantry_deaths, grenade_deaths, machine_gun_deaths, sniper_deaths,
                artillery_deaths, bazooka_deaths, mine_deaths, satchel_deaths,
                commander_deaths, armor_deaths, pak_deaths, spa_deaths,
                raw_info
            )
            VALUES (
                %(player_id)s, %(match_id)s, %(player_name)s, %(team)s,
                %(total_kills)s, %(total_deaths)s, %(kill_streak)s, %(death_streak)s,
                %(kills_per_minute)s, %(deaths_per_minute)s, %(kill_death_ratio)s, %(time_played)s,
                %(combat_score)s, %(offense_score)s, %(defense_score)s, %(support_score)s,
                %(shortest_life)s, %(longest_life)s, %(teamkills)s,
                %(infantry_kills)s, %(grenade_kills)s, %(machine_gun_kills)s, %(sniper_kills)s,
                %(artillery_kills)s, %(bazooka_kills)s, %(mine_kills)s, %(satchel_kills)s,
                %(commander_kills)s, %(armor_kills)s, %(pak_kills)s, %(spa_kills)s,
                %(infantry_deaths)s, %(grenade_deaths)s, %(machine_gun_deaths)s, %(sniper_deaths)s,
                %(artillery_deaths)s, %(bazooka_deaths)s, %(mine_deaths)s, %(satchel_deaths)s,
                %(commander_deaths)s, %(armor_deaths)s, %(pak_deaths)s, %(spa_deaths)s,
                %(raw_info)s::jsonb
            )
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = """
            INSERT INTO pathfinder_stats.player_match_stats (
                player_id, match_id, player_name, team,
                total_kills, total_deaths, kill_streak, death_streak,
                kills_per_minute, deaths_per_minute, kill_death_ratio, time_played,
                combat_score, offense_score, defense_score, support_score,
                shortest_life, longest_life, teamkills,
                infantry_kills, grenade_kills, machine_gun_kills, sniper_kills,
                artillery_kills, bazooka_kills, mine_kills, satchel_kills,
                commander_kills, armor_kills, pak_kills, spa_kills,
                infantry_deaths, grenade_deaths, machine_gun_deaths, sniper_deaths,
                artillery_deaths, bazooka_deaths, mine_deaths, satchel_deaths,
                commander_deaths, armor_deaths, pak_deaths, spa_deaths,
                raw_info
            )
            VALUES (
                %(player_id)s, %(match_id)s, %(player_name)s, %(team)s,
                %(total_kills)s, %(total_deaths)s, %(kill_streak)s, %(death_streak)s,
                %(kills_per_minute)s, %(deaths_per_minute)s, %(kill_death_ratio)s, %(time_played)s,
                %(combat_score)s, %(offense_score)s, %(defense_score)s, %(support_score)s,
                %(shortest_life)s, %(longest_life)s, %(teamkills)s,
                %(infantry_kills)s, %(grenade_kills)s, %(machine_gun_kills)s, %(sniper_kills)s,
                %(artillery_kills)s, %(bazooka_kills)s, %(mine_kills)s, %(satchel_kills)s,
                %(commander_kills)s, %(armor_kills)s, %(pak_kills)s, %(spa_kills)s,
                %(infantry_deaths)s, %(grenade_deaths)s, %(machine_gun_deaths)s, %(sniper_deaths)s,
                %(artillery_deaths)s, %(bazooka_deaths)s, %(mine_deaths)s, %(satchel_deaths)s,
                %(commander_deaths)s, %(armor_deaths)s, %(pak_deaths)s, %(spa_deaths)s,
                %(raw_info)s::jsonb
            )
        """
    
    inserted_count = 0
    total_records = len(player_stats)
    print(f"Inserting {total_records} player statistics records (batch size: {batch_size})...")
    
    del player_stats
    gc.collect()
    
    for i in range(0, len(processed_stats), batch_size):
        batch = processed_stats[i : i + batch_size]
        try:
            execute_batch(cursor, insert_query, batch, page_size=batch_size)
            conn.commit()
            batch_inserted = cursor.rowcount
            inserted_count += batch_inserted
            skipped_count += len(batch) - batch_inserted
            del batch
            if (i + batch_size) % (batch_size * 10) == 0:
                gc.collect()
        except psycopg2.Error as e:
            conn.rollback()
            print(f"Error inserting batch {i//batch_size + 1}: {e}")
            del batch
    
    if len(processed_stats) > 0:
        gc.collect()
    
    del processed_stats
    gc.collect()
    
    cursor.close()
    return inserted_count, skipped_count


def main(
    skip_duplicates: bool = True,
    update_match_history: bool = True,
    update_player_stats: bool = True,
    update_weapon_stats: bool = True,
    update_opponent_stats: bool = True,
) -> None:
    """Main function to transform and insert match results data."""
    try:
        db_config = get_db_config()
        
        missing_vars = []
        if not db_config.host:
            missing_vars.append("POSTGRES_HOST")
        if not db_config.port:
            missing_vars.append("POSTGRES_PORT")
        if not db_config.database:
            missing_vars.append("POSTGRES_DB")
        if not db_config.user:
            missing_vars.append("POSTGRES_USER")
        if not db_config.password:
            missing_vars.append("POSTGRES_PASSWORD")
        
        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )
        
        ingestion_config = get_ingestion_config()
        
    except (ValueError, TypeError) as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Configuration Error: {e}")
        sys.exit(1)
    
    try:
        conn = get_db_connection(
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
            
            for batch in transform_match_history_data_batched(batch_size=transform_batch_size):
                batch_count += 1
                if batch:
                    inserted, skipped = insert_match_history(
                        conn, batch, ingestion_config.match_history_batch_size, skip_duplicates=skip_duplicates
                    )
                    total_inserted += inserted
                    total_skipped += skipped
                    gc.collect()
            
            print(f"\nMatch History Summary:")
            print(f"  Successfully inserted: {total_inserted}")
            print(f"  Skipped (duplicates/invalid): {total_skipped}")
            if batch_count == 0:
                print("No match history data to insert")
        
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
            
            transform_batch_size = ingestion_config.player_stats_batch_size * 10
            total_inserted = 0
            total_skipped = 0
            batch_count = 0
            
            total_kill_stats_inserted = 0
            total_kill_stats_skipped = 0
            total_death_stats_inserted = 0
            total_death_stats_skipped = 0
            total_victim_stats_inserted = 0
            total_victim_stats_skipped = 0
            total_nemesis_stats_inserted = 0
            total_nemesis_stats_skipped = 0
            
            for batch in transform_player_stats_data_batched(batch_size=transform_batch_size):
                batch_count += 1
                if batch:
                    if update_weapon_stats and weapon_schema_map:
                        kill_inserted, kill_skipped = insert_player_kill_stats(
                            conn, batch, weapon_schema_map, 
                            ingestion_config.player_stats_batch_size, 
                            skip_duplicates=skip_duplicates
                        )
                        total_kill_stats_inserted += kill_inserted
                        total_kill_stats_skipped += kill_skipped
                        
                        death_inserted, death_skipped = insert_player_death_stats(
                            conn, batch, weapon_schema_map,
                            ingestion_config.player_stats_batch_size,
                            skip_duplicates=skip_duplicates
                        )
                        total_death_stats_inserted += death_inserted
                        total_death_stats_skipped += death_skipped
                    
                    if update_opponent_stats:
                        victim_inserted, victim_skipped = insert_player_victim_stats(
                            conn, batch, ingestion_config.player_stats_batch_size,
                            skip_duplicates=skip_duplicates
                        )
                        total_victim_stats_inserted += victim_inserted
                        total_victim_stats_skipped += victim_skipped
                        
                        nemesis_inserted, nemesis_skipped = insert_player_nemesis_stats(
                            conn, batch, ingestion_config.player_stats_batch_size,
                            skip_duplicates=skip_duplicates
                        )
                        total_nemesis_stats_inserted += nemesis_inserted
                        total_nemesis_stats_skipped += nemesis_skipped
                    
                    if update_player_stats:
                        inserted, skipped = insert_player_stats(
                            conn, batch, ingestion_config.player_stats_batch_size, skip_duplicates=skip_duplicates
                        )
                        total_inserted += inserted
                        total_skipped += skipped
                    
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
                
                print("\n" + "=" * 60)
                print("BACKFILLING WEAPON STATS FROM EXISTING PLAYER_MATCH_STATS")
                print("=" * 60)
                
                backfill_kill_inserted, backfill_kill_skipped, backfill_death_inserted, backfill_death_skipped = backfill_weapon_stats_from_db(
                    conn, weapon_schema_map, ingestion_config.player_stats_batch_size, skip_duplicates=skip_duplicates
                )
                
                total_kill_stats_inserted += backfill_kill_inserted
                total_kill_stats_skipped += backfill_kill_skipped
                total_death_stats_inserted += backfill_death_inserted
                total_death_stats_skipped += backfill_death_skipped
                
                print(f"\nBackfill Summary:")
                print(f"  Kill stats inserted: {backfill_kill_inserted}, skipped: {backfill_kill_skipped}")
                print(f"  Death stats inserted: {backfill_death_inserted}, skipped: {backfill_death_skipped}")
                
                print(f"\nTotal Weapon Stats Summary:")
                print(f"  Kill stats - Inserted: {total_kill_stats_inserted}, Skipped: {total_kill_stats_skipped}")
                print(f"  Death stats - Inserted: {total_death_stats_inserted}, Skipped: {total_death_stats_skipped}")
            
            if update_opponent_stats:
                print(f"\nPlayer Victim Stats Summary:")
                print(f"  Inserted: {total_victim_stats_inserted}, Skipped: {total_victim_stats_skipped}")
                
                print(f"\nPlayer Nemesis Stats Summary:")
                print(f"  Inserted: {total_nemesis_stats_inserted}, Skipped: {total_nemesis_stats_skipped}")
        
        print("\n" + "=" * 60)
        print("DATABASE UPDATE COMPLETE")
        print("=" * 60)
    
    except Exception as e:
        print(f"\nError during data processing: {e}")
        import traceback
        traceback.print_exc()
        return
    
    finally:
        conn.close()
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
    
    main(
        skip_duplicates=not args.no_skip_duplicates,
        update_match_history=update_match_history,
        update_player_stats=update_player_stats,
        update_weapon_stats=update_weapon_stats,
        update_opponent_stats=update_opponent_stats,
    )

