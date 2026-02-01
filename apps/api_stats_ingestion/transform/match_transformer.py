"""
Transform match results data from JSON files into structured data for database insertion.

This script provides two main transformation methods:
1. transform_match_history_data() - Transforms data from all_matches.json for match_history table
2. transform_player_stats_data() - Transforms data from match_results/*.json for player_match_stats table

These methods analyze JSON files and return data structures ready for insertion into PostgreSQL.
"""

from __future__ import annotations

import gc
import os

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

try:
    import orjson as json
    # orjson.loads returns dict directly, but we need to handle bytes
    _original_loads = json.loads
    json.loads = lambda s: _original_loads(s.encode() if isinstance(s, str) else s)
except ImportError:
    import json

from apps.api_stats_ingestion.transform.transform_utils import (
    calculate_duration,
    calculate_winning_team,
    parse_timestamp,
)

ROOT_DIR = Path(__file__).parent
# Use environment variable for data directory, fallback to script directory
DATA_DIR = Path(os.getenv("API_INGESTION_DATA_DIR", str(ROOT_DIR)))
SOURCE_FILE = DATA_DIR / "all_matches.json"
MATCH_RESULTS_DIR = DATA_DIR / "match_results"


def transform_match_history_data() -> List[Dict[str, Any]]:
    """
    Transform match history data from all_matches.json for insertion into match_history table.
    
    Reads from all_matches.json, extracts match data from the result.maps array,
    and returns a list of dictionaries with keys matching the pathfinder_stats.match_history table columns.
    
    Returns:
        List of dictionaries, each containing:
        - match_id (int)
        - map_id (str)
        - map_name (str)
        - map_short_name (str)
        - game_mode (str)
        - environment (str)
        - allies_score (int)
        - axis_score (int)
        - winning_team (str): "Allies", "Axis", or "Tie"
        - start_time (datetime)
        - end_time (datetime)
        - match_duration (int): duration in seconds
    """
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    # Read and parse JSON
    file_content = SOURCE_FILE.read_text(encoding="utf-8")
    data = json.loads(file_content)
    result = data.get("result") or {}
    maps = result.get("maps") or []
    
    if not isinstance(maps, list):
        raise ValueError("Expected `result.maps` to be a list")
    
    total_maps = len(maps)
    print(f"Transforming match history data from {SOURCE_FILE.name}...")
    print(f"Found {total_maps} match entries to process")
    
    # Release file content and data to help GC
    del file_content
    del data
    
    transformed_matches = []
    skipped_count = 0
    PROGRESS_INTERVAL = 5000  # Print progress every N entries
    
    for idx, entry in enumerate(maps, 1):
        if idx % PROGRESS_INTERVAL == 0:
            print(f"  Processed {idx}/{total_maps} entries ({len(transformed_matches)} transformed, {skipped_count} skipped)")
        
        # Extract match ID
        match_id = entry.get("id")
        if match_id is None:
            skipped_count += 1
            continue
        
        # Extract map information
        map_info = entry.get("map") or {}
        map_id = map_info.get("id")
        if map_id is None:
            skipped_count += 1
            continue
        
        map_map_info = map_info.get("map") or {}
        map_name = map_map_info.get("pretty_name")
        map_short_name = map_map_info.get("shortname")
        game_mode = map_info.get("game_mode")
        environment = map_info.get("environment")
        
        # Extract result information
        result_info = entry.get("result") or {}
        allies_score = result_info.get("allied", 0)
        axis_score = result_info.get("axis", 0)
        
        # Extract timestamps
        start_str = entry.get("start")
        end_str = entry.get("end")
        
        if not all([map_name, map_short_name, game_mode, environment, start_str, end_str]):
            skipped_count += 1
            continue
        
        try:
            start_time = parse_timestamp(start_str)
            end_time = parse_timestamp(end_str)
        except (ValueError, TypeError):
            skipped_count += 1
            continue
        
        # Calculate derived fields
        winning_team = calculate_winning_team(allies_score, axis_score)
        match_duration = calculate_duration(start_time, end_time)
        
        transformed_matches.append({
            "match_id": match_id,
            "map_id": map_id,
            "map_name": map_name,
            "map_short_name": map_short_name,
            "game_mode": game_mode,
            "environment": environment,
            "allies_score": allies_score,
            "axis_score": axis_score,
            "winning_team": winning_team,
            "start_time": start_time,
            "end_time": end_time,
            "match_duration": match_duration
        })
        
        # Periodic garbage collection for large datasets
        if idx % 10000 == 0:
            gc.collect()
    
    # Clear maps list reference
    del maps
    gc.collect()
    
    print(f"\n✓ Match history transformation complete: {len(transformed_matches)} matches transformed, {skipped_count} skipped")
    return transformed_matches


def transform_match_history_data_batched(
    batch_size: int = 1000
) -> Iterator[List[Dict[str, Any]]]:
    """
    Transform match history data in batches to reduce memory usage.
    
    This is a generator that yields batches of transformed match data.
    Use this instead of transform_match_history_data() for memory efficiency.
    
    Args:
        batch_size: Number of matches to process per batch
    
    Yields:
        Batches of transformed match dictionaries
    """
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")
    
    # Read and parse JSON
    file_content = SOURCE_FILE.read_text(encoding="utf-8")
    data = json.loads(file_content)
    result = data.get("result") or {}
    maps = result.get("maps") or []
    
    if not isinstance(maps, list):
        raise ValueError("Expected `result.maps` to be a list")
    
    total_maps = len(maps)
    print(f"Transforming match history data from {SOURCE_FILE.name} in batches of {batch_size}...")
    print(f"Found {total_maps} match entries to process")
    
    # Release file content and data to help GC
    del file_content
    del data
    
    batch = []
    skipped_count = 0
    processed_count = 0
    
    for idx, entry in enumerate(maps, 1):
        # Extract match ID
        match_id = entry.get("id")
        if match_id is None:
            skipped_count += 1
            continue
        
        # Extract map information
        map_info = entry.get("map") or {}
        map_id = map_info.get("id")
        if map_id is None:
            skipped_count += 1
            continue
        
        map_map_info = map_info.get("map") or {}
        map_name = map_map_info.get("pretty_name")
        map_short_name = map_map_info.get("shortname")
        game_mode = map_info.get("game_mode")
        environment = map_info.get("environment")
        
        # Extract result information
        result_info = entry.get("result") or {}
        allies_score = result_info.get("allied", 0)
        axis_score = result_info.get("axis", 0)
        
        # Extract timestamps
        start_str = entry.get("start")
        end_str = entry.get("end")
        
        if not all([map_name, map_short_name, game_mode, environment, start_str, end_str]):
            skipped_count += 1
            continue
        
        try:
            start_time = parse_timestamp(start_str)
            end_time = parse_timestamp(end_str)
        except (ValueError, TypeError):
            skipped_count += 1
            continue
        
        # Calculate derived fields
        winning_team = calculate_winning_team(allies_score, axis_score)
        match_duration = calculate_duration(start_time, end_time)
        
        batch.append({
            "match_id": match_id,
            "map_id": map_id,
            "map_name": map_name,
            "map_short_name": map_short_name,
            "game_mode": game_mode,
            "environment": environment,
            "allies_score": allies_score,
            "axis_score": axis_score,
            "winning_team": winning_team,
            "start_time": start_time,
            "end_time": end_time,
            "match_duration": match_duration
        })
        
        processed_count += 1
        
        # Yield batch when it reaches batch_size
        if len(batch) >= batch_size:
            yield batch
            batch = []
            gc.collect()
        
        # Progress reporting
        if idx % 5000 == 0:
            print(f"  Processed {idx}/{total_maps} entries ({processed_count} transformed, {skipped_count} skipped)")
    
    # Yield remaining batch
    if batch:
        yield batch
    
    # Clear maps list reference
    del maps
    gc.collect()
    
    print(f"\n✓ Match history transformation complete: {processed_count} matches transformed, {skipped_count} skipped")


def transform_player_stats_data() -> List[Dict[str, Any]]:
    """
    Transform player statistics data from match_results/*.json files for insertion into player_match_stats table.
    
    Reads JSON files from the match_results directory, extracts player statistics from
    each file's result.player_stats array, and returns a list of dictionaries with keys
    matching the player_match_stats table columns.
    
    Returns:
        List of dictionaries with player stats data.
    """
    if not MATCH_RESULTS_DIR.exists():
        raise FileNotFoundError(f"Match results directory not found: {MATCH_RESULTS_DIR}")
    
    print(f"Transforming player stats data from {MATCH_RESULTS_DIR}...")
    print("Scanning for match result files...")
    
    # Use iterator for memory efficiency - don't load all file paths at once
    # Count files while collecting paths (only for reasonable sizes to avoid memory issues)
    file_count = 0
    file_paths = []
    for file_path in MATCH_RESULTS_DIR.glob("*.json"):
        file_count += 1
        # Only store paths for small to medium datasets to avoid memory issues
        if file_count <= 50000:  # Reasonable limit
            file_paths.append(file_path)
    
    total_files = file_count
    print(f"Found {total_files} match result files to process")
    
    # Use list if we collected paths, otherwise re-iterate (slower but memory efficient)
    if file_count <= 50000:
        json_files = file_paths
    else:
        # For very large datasets, re-scan (slower but memory efficient)
        json_files = MATCH_RESULTS_DIR.glob("*.json")
    
    transformed_player_stats = []
    processed_count = 0
    skipped_files = 0
    PROGRESS_INTERVAL = 100  # Print progress every N files
    
    for file_idx, file_path in enumerate(json_files, 1):
        if file_idx % PROGRESS_INTERVAL == 0:
            print(f"  Processed {file_idx}/{total_files} files ({len(transformed_player_stats)} player stats extracted)")
        
        try:
            # Read file content
            file_content = file_path.read_text(encoding="utf-8")
            data = json.loads(file_content)
            match_result = data.get("result")
            
            # Release file content immediately after parsing
            del file_content
            
            if match_result is None:
                del data
                continue
            
            match_id = match_result.get("id")
            if match_id is None:
                del data, match_result
                continue
            
            player_stats = match_result.get("player_stats") or []
            
            for player_stat in player_stats:
                transformed_stat = _extract_player_stat_data(player_stat, match_id)
                if transformed_stat:
                    transformed_player_stats.append(transformed_stat)
            
            # Release data references
            del data, match_result
            
            processed_count += 1
            
            # Periodic garbage collection for large datasets
            if file_idx % 1000 == 0:
                gc.collect()
        
        except (json.JSONDecodeError, KeyError, IOError):
            # Skip files that can't be parsed
            skipped_files += 1
            continue
    
    # Final garbage collection
    gc.collect()
    
    print(f"\n✓ Player stats transformation complete: {len(transformed_player_stats)} player stats from {processed_count} matches ({skipped_files} files skipped)")
    return transformed_player_stats


def transform_player_stats_data_batched(
    batch_size: int = 1000,
    existing_match_ids: set = None
) -> Iterator[List[Dict[str, Any]]]:
    """
    Transform player statistics data in batches to reduce memory usage.
    
    This is a generator that yields batches of transformed player stats.
    Use this instead of transform_player_stats_data() for memory efficiency.
    
    Args:
        batch_size: Number of player stats records to accumulate per batch
        existing_match_ids: Set of match IDs already in database (to skip processing)
    
    Yields:
        Batches of transformed player stat dictionaries
    """
    if not MATCH_RESULTS_DIR.exists():
        raise FileNotFoundError(f"Match results directory not found: {MATCH_RESULTS_DIR}")
    
    print(f"Transforming player stats data from {MATCH_RESULTS_DIR} in batches...")
    print("Scanning for match result files...")
    
    # Use iterator for memory efficiency
    file_count = 0
    file_paths = []
    for file_path in MATCH_RESULTS_DIR.glob("*.json"):
        file_count += 1
        if file_count <= 50000:  # Reasonable limit
            file_paths.append(file_path)
    
    total_files = file_count
    print(f"Found {total_files} match result files to process")
    
    if existing_match_ids:
        print(f"Skipping {len(existing_match_ids)} matches already in database")
    
    if file_count <= 50000:
        json_files = file_paths
    else:
        json_files = MATCH_RESULTS_DIR.glob("*.json")
    
    batch = []
    processed_count = 0
    skipped_files = 0
    skipped_existing = 0
    total_stats = 0
    PROGRESS_INTERVAL = 100  # Print progress every N files
    
    for file_idx, file_path in enumerate(json_files, 1):
        if file_idx % PROGRESS_INTERVAL == 0:
            print(f"  Processed {file_idx}/{total_files} files ({total_stats} player stats extracted, {skipped_existing} already in DB)")
        
        try:
            file_content = file_path.read_text(encoding="utf-8")
            data = json.loads(file_content)
            match_result = data.get("result")
            
            if match_result is None:
                continue
            
            match_id = match_result.get("id")
            if match_id is None:
                continue
            
            # Skip if match already processed
            if existing_match_ids and match_id in existing_match_ids:
                skipped_existing += 1
                continue
            
            player_stats = match_result.get("player_stats") or []
            
            for player_stat in player_stats:
                transformed_stat = _extract_player_stat_data(player_stat, match_id)
                if transformed_stat:
                    batch.append(transformed_stat)
                    total_stats += 1
                    
                    # Yield batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
            
            processed_count += 1
            
            # Less frequent garbage collection (every 500 files instead of 1000)
            if file_idx % 500 == 0:
                gc.collect()
        
        except (json.JSONDecodeError, KeyError, IOError):
            skipped_files += 1
            continue
    
    # Yield remaining batch
    if batch:
        yield batch
    
    gc.collect()
    
    print(f"\n✓ Player stats transformation complete: {total_stats} player stats from {processed_count} matches ({skipped_files} files skipped, {skipped_existing} already in DB)")


def _extract_player_stat_data(
    player_stat: Dict[str, Any],
    match_id: int
) -> Optional[Dict[str, Any]]:
    """
    Extract player statistics data from a single entry in player_stats array.
    
    Returns a dictionary with keys matching the player_match_stats table columns,
    or None if required fields are missing.
    """
    # Extract required fields
    player_id = player_stat.get("player_id")
    player_name = player_stat.get("player")
    
    if not player_id or not player_name:
        return None
    
    # Extract team information
    team_info = player_stat.get("team") or {}
    team = team_info.get("side")
    if team == "unknown":
        team = None
    
    # Extract basic statistics
    total_kills = player_stat.get("kills", 0)
    total_deaths = player_stat.get("deaths", 0)
    kill_streak = player_stat.get("kills_streak", 0)
    death_streak = player_stat.get("deaths_without_kill_streak", 0)
    kills_per_minute = float(player_stat.get("kills_per_minute", 0.0))
    deaths_per_minute = float(player_stat.get("deaths_per_minute", 0.0))
    kill_death_ratio = float(player_stat.get("kill_death_ratio", 0.0))
    
    # Extract score statistics
    combat_score = player_stat.get("combat", 0)
    offense_score = player_stat.get("offense", 0)
    defense_score = player_stat.get("defense", 0)
    support_score = player_stat.get("support", 0)
    
    # Extract life statistics
    shortest_life = player_stat.get("shortest_life_secs", 0)
    longest_life = player_stat.get("longest_life_secs", 0)
    time_played = player_stat.get("time_seconds", 0)
    
    # Extract teamkill statistics
    teamkills = player_stat.get("teamkills", 0)
    
    # Extract kills by type
    kills_by_type = player_stat.get("kills_by_type") or {}
    infantry_kills = kills_by_type.get("infantry", 0)
    grenade_kills = kills_by_type.get("grenade", 0)
    machine_gun_kills = kills_by_type.get("machine_gun", 0)
    sniper_kills = kills_by_type.get("sniper", 0)
    artillery_kills = kills_by_type.get("artillery", 0)
    bazooka_kills = kills_by_type.get("bazooka", 0)
    mine_kills = kills_by_type.get("mine", 0)
    satchel_kills = kills_by_type.get("satchel", 0)
    commander_kills = kills_by_type.get("commander", 0)
    armor_kills = kills_by_type.get("armor", 0)
    pak_kills = kills_by_type.get("pak", 0)
    spa_kills = kills_by_type.get("self_propelled_artillery", 0)
    
    # Extract deaths by type
    deaths_by_type = player_stat.get("deaths_by_type") or {}
    infantry_deaths = deaths_by_type.get("infantry", 0)
    grenade_deaths = deaths_by_type.get("grenade", 0)
    machine_gun_deaths = deaths_by_type.get("machine_gun", 0)
    sniper_deaths = deaths_by_type.get("sniper", 0)
    artillery_deaths = deaths_by_type.get("artillery", 0)
    bazooka_deaths = deaths_by_type.get("bazooka", 0)
    mine_deaths = deaths_by_type.get("mine", 0)
    satchel_deaths = deaths_by_type.get("satchel", 0)
    commander_deaths = deaths_by_type.get("commander", 0)
    armor_deaths = deaths_by_type.get("armor", 0)
    pak_deaths = deaths_by_type.get("pak", 0)
    spa_deaths = deaths_by_type.get("self_propelled_artillery", 0)
    
    return {
        "player_id": player_id,
        "match_id": match_id,
        "player_name": player_name,
        "team": team,
        "total_kills": total_kills,
        "total_deaths": total_deaths,
        "kill_streak": kill_streak,
        "death_streak": death_streak,
        "kills_per_minute": kills_per_minute,
        "deaths_per_minute": deaths_per_minute,
        "kill_death_ratio": kill_death_ratio,
        "combat_score": combat_score,
        "offense_score": offense_score,
        "defense_score": defense_score,
        "support_score": support_score,
        "shortest_life": shortest_life,
        "longest_life": longest_life,
        "time_played": time_played,
        "teamkills": teamkills,
        "infantry_kills": infantry_kills,
        "grenade_kills": grenade_kills,
        "machine_gun_kills": machine_gun_kills,
        "sniper_kills": sniper_kills,
        "artillery_kills": artillery_kills,
        "bazooka_kills": bazooka_kills,
        "mine_kills": mine_kills,
        "satchel_kills": satchel_kills,
        "commander_kills": commander_kills,
        "armor_kills": armor_kills,
        "pak_kills": pak_kills,
        "spa_kills": spa_kills,
        "infantry_deaths": infantry_deaths,
        "grenade_deaths": grenade_deaths,
        "machine_gun_deaths": machine_gun_deaths,
        "sniper_deaths": sniper_deaths,
        "artillery_deaths": artillery_deaths,
        "bazooka_deaths": bazooka_deaths,
        "mine_deaths": mine_deaths,
        "satchel_deaths": satchel_deaths,
        "commander_deaths": commander_deaths,
        "armor_deaths": armor_deaths,
        "pak_deaths": pak_deaths,
        "spa_deaths": spa_deaths,
        "raw_info": player_stat  # Store as dict for JSONB - database driver will handle conversion
    }
