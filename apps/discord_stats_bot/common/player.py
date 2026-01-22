"""
Player lookup utilities and pathfinder ID management.
"""

import logging
from pathlib import Path
from typing import Optional, Set, Tuple

import asyncpg

logger = logging.getLogger(__name__)

# Cache for pathfinder player IDs loaded from file
_pathfinder_player_ids: Optional[Set[str]] = None


async def find_player_by_id_or_name(
    conn: asyncpg.Connection, 
    player: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Look up a player by ID or name. 
    
    Returns:
        Tuple of (player_id, player_name) or (None, None) if not found.
    """
    player = str(player).strip()
    
    # First, check if the input is a player_id
    check_query = """
        SELECT 1 FROM pathfinder_stats.player_match_stats WHERE player_id = $1
        UNION ALL
        SELECT 1 FROM pathfinder_stats.player_kill_stats WHERE player_id = $1
        UNION ALL
        SELECT 1 FROM pathfinder_stats.player_death_stats WHERE player_id = $1
        LIMIT 1
    """
    player_exists = await conn.fetchval(check_query, player)
    
    if player_exists:
        # Get most recent player name
        name_query = """
            SELECT DISTINCT ON (pms.player_id) pms.player_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE pms.player_id = $1
            ORDER BY pms.player_id, mh.start_time DESC
            LIMIT 1
        """
        found_player_name = await conn.fetchval(name_query, player)
        return (player, found_player_name if found_player_name else player)
    
    # If no results from player_id, try player_name
    find_player_query = """
        SELECT DISTINCT player_id
        FROM (
            SELECT player_id FROM pathfinder_stats.player_match_stats
            WHERE player_name ILIKE $1 OR LOWER(player_name) = LOWER($1)
            UNION
            SELECT player_id FROM pathfinder_stats.player_kill_stats
            WHERE player_name ILIKE $1 OR LOWER(player_name) = LOWER($1)
            UNION
            SELECT player_id FROM pathfinder_stats.player_death_stats
            WHERE player_name ILIKE $1 OR LOWER(player_name) = LOWER($1)
        ) combined_results
        LIMIT 1
    """
    found_player_id = await conn.fetchval(find_player_query, player)
    
    if found_player_id:
        # Get most recent player name
        name_query = """
            SELECT DISTINCT ON (pms.player_id) pms.player_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE pms.player_id = $1
            ORDER BY pms.player_id, mh.start_time DESC
            LIMIT 1
        """
        found_player_name = await conn.fetchval(name_query, found_player_id)
        return (found_player_id, found_player_name if found_player_name else player)
    
    return (None, None)


def get_pathfinder_player_ids() -> Set[str]:
    """Load player IDs from pathfinder_player_ids.txt (cached after first load)."""
    global _pathfinder_player_ids
    
    if _pathfinder_player_ids is not None:
        return _pathfinder_player_ids
    
    common_dir = Path(__file__).parent
    file_path = common_dir / "pathfinder_player_ids.txt"
    player_ids: Set[str] = set()
    
    if not file_path.exists():
        logger.info(f"Player IDs file not found at {file_path}")
        _pathfinder_player_ids = player_ids
        return player_ids
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    player_ids.add(line)
        
        _pathfinder_player_ids = player_ids
        logger.info(f"Loaded {len(player_ids)} player IDs")
        return player_ids
    except Exception as e:
        logger.error(f"Error loading player IDs: {e}", exc_info=True)
        _pathfinder_player_ids = set()
        return _pathfinder_player_ids
