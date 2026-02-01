"""
Player lookup utilities and pathfinder ID management.

Provides:
- Player ID resolution from cache or user input
- Database player lookup by ID or name
- Pathfinder player ID management from S3
"""

import logging

import asyncpg
import boto3

from dataclasses import dataclass
from typing import Optional, Set, Tuple

from apps.discord_stats_bot.common.cache import get_player_id

logger = logging.getLogger(__name__)


# =============================================================================
# Player Resolution and Lookup
# =============================================================================

# Error messages for player-related operations
PLAYER_NOT_SET_ERROR = (
    "❌ No player ID provided and you haven't set one! "
    "Either provide a player ID/name, or use `/profile setid` to set a default."
)


def player_not_found_error(player_input: str) -> str:
    """Generate error message for player not found."""
    return f"❌ Could not find user: `{player_input}`. Try using a player ID or exact player name."


@dataclass
class PlayerLookupResult:
    """
    Result of a player lookup operation.
    
    Attributes:
        player_id: The resolved player ID, or None if not found
        player_name: The most recent player name, or None if not found
        original_input: The original input used for lookup
        is_found: Whether the player was found in the database
    """
    player_id: Optional[str]
    player_name: Optional[str]
    original_input: str
    
    @property
    def is_found(self) -> bool:
        """Returns True if the player was found."""
        return self.player_id is not None
    
    @property
    def display_name(self) -> str:
        """Returns the best display name available."""
        return self.player_name or self.original_input


async def resolve_player_input(
    discord_user_id: int,
    player_input: Optional[str]
) -> Optional[str]:
    """
    Resolve player input, falling back to stored player ID if not provided.
    
    Args:
        discord_user_id: The Discord user's ID for cache lookup
        player_input: Optional player ID or name provided by user
        
    Returns:
        Resolved player string, or None if no player could be determined
    """
    if player_input:
        return player_input
    
    stored_player_id = await get_player_id(discord_user_id)
    return stored_player_id


async def lookup_player(
    conn: asyncpg.Connection,
    discord_user_id: int,
    player_input: Optional[str]
) -> Tuple[Optional[PlayerLookupResult], Optional[str]]:
    """
    Resolve player input and look up in database.
    
    This is a convenience function that combines resolve_player_input and
    find_player_by_id_or_name, returning structured results.
    
    Args:
        conn: Database connection
        discord_user_id: The Discord user's ID for cache lookup
        player_input: Optional player ID or name provided by user
        
    Returns:
        Tuple of (PlayerLookupResult, error_message):
        - If player input couldn't be resolved: (None, PLAYER_NOT_SET_ERROR)
        - If player not found in DB: (PlayerLookupResult with is_found=False, error_message)
        - If player found: (PlayerLookupResult with data, None)
    """
    # Resolve the player input (from param or cache)
    resolved_input = await resolve_player_input(discord_user_id, player_input)
    
    if not resolved_input:
        return None, PLAYER_NOT_SET_ERROR
    
    # Look up in database
    player_id, player_name = await find_player_by_id_or_name(conn, resolved_input)
    
    result = PlayerLookupResult(
        player_id=player_id,
        player_name=player_name,
        original_input=resolved_input
    )
    
    if not result.is_found:
        return result, player_not_found_error(resolved_input)
    
    return result, None

# S3 configuration
S3_BUCKET_NAME = "stats-let-loose"
S3_KEY = "pathfinder_player_ids.txt"

# Cache for pathfinder player IDs loaded from S3
_pathfinder_player_ids: Optional[Set[str]] = None
_pathfinder_player_ids_initialized: bool = False


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


async def load_pathfinder_player_ids_from_s3() -> None:
    """
    Load pathfinder player IDs from S3 bucket.
    
    This function must be called during bot startup before the bot is ready.
    It loads the file from S3 bucket 'stats-let-loose' with key 'pathfinder_player_ids.txt'.
    """
    global _pathfinder_player_ids, _pathfinder_player_ids_initialized
    
    if _pathfinder_player_ids_initialized:
        logger.info("Pathfinder player IDs already initialized")
        return
    
    logger.info(f"Loading pathfinder player IDs from S3: s3://{S3_BUCKET_NAME}/{S3_KEY}")
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_KEY)
        content = response['Body'].read().decode('utf-8')
        
        player_ids: Set[str] = set()
        for line in content.splitlines():
            line = line.strip()
            if line and not line.startswith('#'):
                player_ids.add(line)
        
        _pathfinder_player_ids = player_ids
        _pathfinder_player_ids_initialized = True
        logger.info(f"Successfully loaded {len(player_ids)} pathfinder player IDs from S3")
    except Exception as e:
        logger.error(f"Unexpected error loading player IDs from S3: {e}", exc_info=True)
        _pathfinder_player_ids = set()
        _pathfinder_player_ids_initialized = True
        raise


def get_pathfinder_player_ids() -> Set[str]:
    """
    Get cached pathfinder player IDs.
    
    Note: load_pathfinder_player_ids_from_s3() must be called during bot startup
    before this function is used.
    
    Returns:
        Set of pathfinder player IDs, or empty set if not yet loaded.
    """
    global _pathfinder_player_ids
    
    if _pathfinder_player_ids is None:
        logger.warning(
            "get_pathfinder_player_ids() called before initialization. "
            "Call load_pathfinder_player_ids_from_s3() during bot startup."
        )
        return set()
    
    return _pathfinder_player_ids
