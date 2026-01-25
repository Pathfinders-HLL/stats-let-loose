"""Utility functions for database operations."""

import csv
from pathlib import Path
from typing import Dict, Optional

import asyncpg

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


async def update_match_player_counts(conn: asyncpg.Connection) -> int:
    """
    Update player_count column in match_history based on player_match_stats.
    
    This should be called after inserting player_match_stats to ensure
    the player_count column reflects actual participant counts.
    
    Returns:
        Number of match_history records updated
    """
    print("Updating match player counts...")
    
    # Update player_count for all matches that have NULL or outdated counts
    result = await conn.execute("""
        UPDATE pathfinder_stats.match_history mh
        SET player_count = subq.cnt
        FROM (
            SELECT match_id, COUNT(*) as cnt
            FROM pathfinder_stats.player_match_stats
            GROUP BY match_id
        ) subq
        WHERE mh.match_id = subq.match_id
          AND (mh.player_count IS NULL OR mh.player_count != subq.cnt)
    """)
    
    # Parse the result to get update count (format: "UPDATE N")
    updated_count = 0
    if result:
        parts = result.split()
        if len(parts) >= 2 and parts[0] == "UPDATE":
            try:
                updated_count = int(parts[1])
            except ValueError:
                pass
    
    if updated_count > 0:
        print(f"  Updated player_count for {updated_count} matches")
    else:
        print("  All player counts are already up to date")
    
    return updated_count
