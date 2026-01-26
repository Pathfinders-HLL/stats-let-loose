"""Database check functions to verify existing records before insertion."""

import asyncpg

from typing import List, Tuple


async def check_existing_match_ids(
    conn: asyncpg.Connection,
    match_ids: List[int],
) -> set[int]:
    """Check which match IDs already exist in the match_history table."""
    if not match_ids:
        return set()
    
    rows = await conn.fetch(
        "SELECT match_id FROM pathfinder_stats.match_history WHERE match_id = ANY($1)",
        match_ids
    )
    return {row["match_id"] for row in rows}


async def check_existing_player_match_ids(
    conn: asyncpg.Connection,
    player_match_keys: List[Tuple[str, int]],
) -> set[Tuple[str, int]]:
    """Check which (player_id, match_id) combinations already exist in player_match_stats."""
    if not player_match_keys:
        return set()
    
    player_ids = [key[0] for key in player_match_keys]
    match_ids = [key[1] for key in player_match_keys]
    
    rows = await conn.fetch("""
        SELECT player_id, match_id 
        FROM pathfinder_stats.player_match_stats 
        WHERE player_id = ANY($1::text[]) AND match_id = ANY($2::int[])
    """, player_ids, match_ids)
    
    return {(row["player_id"], row["match_id"]) for row in rows}


async def check_existing_player_kill_ids(
    conn: asyncpg.Connection,
    player_match_keys: List[Tuple[str, int]],
) -> set[Tuple[str, int]]:
    """Check which (player_id, match_id) combinations already exist in player_kill_stats."""
    if not player_match_keys:
        return set()
    
    player_ids = [key[0] for key in player_match_keys]
    match_ids = [key[1] for key in player_match_keys]
    
    rows = await conn.fetch("""
        SELECT player_id, match_id 
        FROM pathfinder_stats.player_kill_stats 
        WHERE player_id = ANY($1::text[]) AND match_id = ANY($2::int[])
    """, player_ids, match_ids)
    
    return {(row["player_id"], row["match_id"]) for row in rows}


async def check_existing_player_death_ids(
    conn: asyncpg.Connection,
    player_match_keys: List[Tuple[str, int]],
) -> set[Tuple[str, int]]:
    """Check which (player_id, match_id) combinations already exist in player_death_stats."""
    if not player_match_keys:
        return set()
    
    player_ids = [key[0] for key in player_match_keys]
    match_ids = [key[1] for key in player_match_keys]
    
    rows = await conn.fetch("""
        SELECT player_id, match_id 
        FROM pathfinder_stats.player_death_stats 
        WHERE player_id = ANY($1::text[]) AND match_id = ANY($2::int[])
    """, player_ids, match_ids)
    
    return {(row["player_id"], row["match_id"]) for row in rows}


async def check_existing_player_victim_ids(
    conn: asyncpg.Connection,
    player_match_keys: List[Tuple[str, int]],
) -> set[Tuple[str, int]]:
    """Check which (player_id, match_id) combinations already exist in player_victim."""
    if not player_match_keys:
        return set()
    
    player_ids = [key[0] for key in player_match_keys]
    match_ids = [key[1] for key in player_match_keys]
    
    rows = await conn.fetch("""
        SELECT player_id, match_id 
        FROM pathfinder_stats.player_victim 
        WHERE player_id = ANY($1::text[]) AND match_id = ANY($2::int[])
    """, player_ids, match_ids)
    
    return {(row["player_id"], row["match_id"]) for row in rows}


async def check_existing_player_nemesis_ids(
    conn: asyncpg.Connection,
    player_match_keys: List[Tuple[str, int]],
) -> set[Tuple[str, int]]:
    """Check which (player_id, match_id) combinations already exist in player_nemesis."""
    if not player_match_keys:
        return set()
    
    player_ids = [key[0] for key in player_match_keys]
    match_ids = [key[1] for key in player_match_keys]
    
    rows = await conn.fetch("""
        SELECT player_id, match_id 
        FROM pathfinder_stats.player_nemesis 
        WHERE player_id = ANY($1::text[]) AND match_id = ANY($2::int[])
    """, player_ids, match_ids)
    
    return {(row["player_id"], row["match_id"]) for row in rows}
