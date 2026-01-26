"""Player statistics insertion operations."""

import json

import asyncpg

from typing import Any, Dict, List

from apps.api_stats_ingestion.load.db.checks import check_existing_player_match_ids


async def insert_player_stats(
    conn: asyncpg.Connection,
    player_stats: List[Dict[str, Any]],
    batch_size: int,
    skip_duplicates: bool = True,
) -> tuple[int, int]:
    """Insert player statistics records into the database."""
    if not player_stats:
        return 0, 0
    
    if skip_duplicates:
        player_match_keys = [
            (stat.get("player_id"), stat.get("match_id"))
            for stat in player_stats
            if stat.get("player_id") and stat.get("match_id")
        ]
        existing_keys = await check_existing_player_match_ids(conn, player_match_keys)
        
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
    
    columns = [
        "player_id", "match_id", "player_name", "team",
        "total_kills", "total_deaths", "kill_streak", "death_streak",
        "kills_per_minute", "deaths_per_minute", "kill_death_ratio", "time_played",
        "combat_score", "offense_score", "defense_score", "support_score",
        "shortest_life", "longest_life", "teamkills",
        "infantry_kills", "grenade_kills", "machine_gun_kills", "sniper_kills",
        "artillery_kills", "bazooka_kills", "mine_kills", "satchel_kills",
        "commander_kills", "armor_kills", "pak_kills", "spa_kills",
        "infantry_deaths", "grenade_deaths", "machine_gun_deaths", "sniper_deaths",
        "artillery_deaths", "bazooka_deaths", "mine_deaths", "satchel_deaths",
        "commander_deaths", "armor_deaths", "pak_deaths", "spa_deaths",
        "raw_info"
    ]
    
    processed_stats = []
    for stat in player_stats:
        # Convert raw_info to JSON string for asyncpg
        raw_info = stat.get("raw_info")
        if raw_info is not None:
            raw_info_json = json.dumps(raw_info)
        else:
            raw_info_json = None
        
        processed_stat = (
            stat.get("player_id"),
            stat.get("match_id"),
            stat.get("player_name"),
            stat.get("team"),
            stat.get("total_kills"),
            stat.get("total_deaths"),
            stat.get("kill_streak"),
            stat.get("death_streak"),
            stat.get("kills_per_minute"),
            stat.get("deaths_per_minute"),
            stat.get("kill_death_ratio"),
            stat.get("time_played"),
            stat.get("combat_score"),
            stat.get("offense_score"),
            stat.get("defense_score"),
            stat.get("support_score"),
            stat.get("shortest_life"),
            stat.get("longest_life"),
            stat.get("teamkills"),
            stat.get("infantry_kills"),
            stat.get("grenade_kills"),
            stat.get("machine_gun_kills"),
            stat.get("sniper_kills"),
            stat.get("artillery_kills"),
            stat.get("bazooka_kills"),
            stat.get("mine_kills"),
            stat.get("satchel_kills"),
            stat.get("commander_kills"),
            stat.get("armor_kills"),
            stat.get("pak_kills"),
            stat.get("spa_kills"),
            stat.get("infantry_deaths"),
            stat.get("grenade_deaths"),
            stat.get("machine_gun_deaths"),
            stat.get("sniper_deaths"),
            stat.get("artillery_deaths"),
            stat.get("bazooka_deaths"),
            stat.get("mine_deaths"),
            stat.get("satchel_deaths"),
            stat.get("commander_deaths"),
            stat.get("armor_deaths"),
            stat.get("pak_deaths"),
            stat.get("spa_deaths"),
            raw_info_json,
        )
        processed_stats.append(processed_stat)
    
    columns_str = ", ".join(columns)
    placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
    
    if skip_duplicates:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_match_stats ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT (player_id, match_id) DO NOTHING
        """
    else:
        insert_query = f"""
            INSERT INTO pathfinder_stats.player_match_stats ({columns_str})
            VALUES ({placeholders})
        """
    
    inserted_count = 0
    total_records = len(processed_stats)
    print(f"Inserting {total_records} player statistics records (batch size: {batch_size})...")
    if skipped_count > 0:
        print(f"  Skipped {skipped_count} existing records (checked before insert)")
    
    for i in range(0, len(processed_stats), batch_size):
        batch = processed_stats[i : i + batch_size]
        try:
            await conn.executemany(insert_query, batch)
            inserted_count += len(batch)
        except asyncpg.PostgresError as e:
            print(f"Error inserting batch {i//batch_size + 1}: {e}")
    
    return inserted_count, skipped_count
