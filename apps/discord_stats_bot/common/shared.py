"""
Shared utilities for Discord bot commands: database access, logging, validation.
"""

import inspect
import logging
import os
import time
from functools import wraps
from pathlib import Path
from typing import Optional, Tuple, Callable, Any, Set

import asyncpg
import discord
from discord import app_commands

from libs.db.config import get_db_config

logger = logging.getLogger(__name__)

# Connection pool for async database operations
_db_pool: Optional[asyncpg.Pool] = None

# Cache for pathfinder player IDs loaded from file
_pathfinder_player_ids: Optional[Set[str]] = None


async def get_readonly_db_pool() -> asyncpg.Pool:
    """
    Get or create the async PostgreSQL connection pool (read-only user).
    This pool is created on first call and reused for subsequent calls.
    """
    global _db_pool
    
    if _db_pool is not None:
        return _db_pool
    
    db_config = get_db_config()
    ro_user = os.getenv("POSTGRES_RO_USER")
    ro_password = os.getenv("POSTGRES_RO_PASSWORD")
    
    if not all([db_config.host, db_config.port, db_config.database, ro_user]):
        missing = []
        if not db_config.host:
            missing.append("POSTGRES_HOST")
        if not db_config.port:
            missing.append("POSTGRES_PORT")
        if not db_config.database:
            missing.append("POSTGRES_DB")
        if not ro_user:
            missing.append("POSTGRES_RO_USER")
        raise ValueError(f"Missing database config: {', '.join(missing)}")
    
    try:
        _db_pool = await asyncpg.create_pool(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=ro_user,
            password=ro_password,
            min_size=2,  # Minimum connections in pool
            max_size=10,  # Maximum connections in pool
            command_timeout=60,  # 60 second timeout for queries
        )
        logger.info("Created async database connection pool")
        return _db_pool
    except Exception as e:
        raise ConnectionError(f"Failed to create database connection pool: {e}") from e


async def close_db_pool() -> None:
    """Close the database connection pool if open."""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
        logger.info("Closed database connection pool")


def escape_sql_identifier(identifier: str) -> str:
    """Escape a SQL identifier with double quotes for PostgreSQL."""
    return f'"{identifier}"'


async def find_player_by_id_or_name(conn: asyncpg.Connection, player: str) -> Tuple[Optional[str], Optional[str]]:
    """Look up a player by ID or name. Returns (player_id, player_name) or (None, None)."""
    player = player.strip()
    check_query = "SELECT 1 FROM pathfinder_stats.player_match_stats WHERE player_id = $1 LIMIT 1"
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
        FROM pathfinder_stats.player_match_stats
        WHERE LOWER(player_name) = LOWER($1)
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


def log_command_data(interaction: discord.Interaction, command_name: str, **kwargs) -> None:
    """Log command invocation with user, channel, and parameters."""
    user = interaction.user
    user_info = f"{user.name}#{user.discriminator} ({user.id})"
    channel_info = f"#{interaction.channel.name if interaction.channel else 'DM'} ({interaction.channel_id})"
    
    # Build parameter string
    params = ", ".join([f"{k}={v}" for k, v in kwargs.items() if v is not None])
    params_str = f" | Params: {params}" if params else ""
    
    logger.info(
        f"Command: {command_name} | User: {user_info} | Channel: {channel_info}{params_str}"
    )


def get_command_latency_ms(start_time: float) -> float:
    """Calculate elapsed time in milliseconds since start_time."""
    return (time.time() - start_time) * 1000


def log_command_completion(
    command_name: str,
    start_time: float,
    success: bool = True,
    interaction: Optional[discord.Interaction] = None,
    kwargs: Optional[dict] = None
) -> None:
    """Log command completion status with latency and user info."""
    status = "SUCCESS" if success else "FAILED"
    latency_ms = get_command_latency_ms(start_time)
    
    user_info = ""
    if interaction and interaction.user:
        user = interaction.user
        user_info = f" | User: {user.name}#{user.discriminator} ({user.id})"
    
    params_str = ""
    if kwargs:
        # Filter out None values and internal parameters
        filtered_kwargs = {k: v for k, v in kwargs.items() 
                          if v is not None and k not in ('command_start_time', 'interaction')}
        if filtered_kwargs:
            params = ", ".join([f"{k}={v}" for k, v in filtered_kwargs.items()])
            params_str = f" | Params: {params}"
    
    logger.info(
        f"Command: {command_name} | Status: {status} | Latency: {latency_ms:.2f}ms{user_info}{params_str}"
    )


async def handle_command_errors(
    interaction: discord.Interaction,
    command_name: str,
    start_time: float,
    error: Exception,
    use_ephemeral: bool = False,
    kwargs: Optional[dict] = None
) -> None:
    """Handle errors with appropriate logging and user-facing messages."""
    # Preserve the exception traceback explicitly to ensure no information is lost
    exc_info = (type(error), error, error.__traceback__)
    
    if isinstance(error, ValueError):
        logger.error(f"Configuration error in {command_name}: {error}", exc_info=exc_info)
        log_command_completion(command_name, start_time, success=False, interaction=interaction, kwargs=kwargs)
        error_msg = "❌ Configuration error. Please check database connection settings. Go ask Gordon Bombay to fix this."
    elif isinstance(error, ConnectionError):
        logger.error(f"Database connection error in {command_name}: {error}", exc_info=exc_info)
        log_command_completion(command_name, start_time, success=False, interaction=interaction, kwargs=kwargs)
        error_msg = "❌ Failed to connect to database. Go ask Gordon Bombay to fix this."
    elif isinstance(error, asyncpg.PostgresError):
        logger.error(f"Database query error in {command_name}: {error}", exc_info=exc_info)
        log_command_completion(command_name, start_time, success=False, interaction=interaction, kwargs=kwargs)
        error_msg = "❌ Database error. Go ask Gordon Bombay to fix this."
    else:
        logger.error(f"Unexpected error in {command_name}: {error}", exc_info=exc_info)
        log_command_completion(command_name, start_time, success=False, interaction=interaction, kwargs=kwargs)
        error_msg = "❌ An unexpected error occurred. Go ask Gordon Bombay to fix this."

    if not interaction.response.is_done():
        await interaction.response.send_message(error_msg, ephemeral=use_ephemeral)
    else:
        await interaction.followup.send(error_msg, ephemeral=use_ephemeral)


def validate_over_last_days(over_last_days: int) -> None:
    """Raise ValueError if days is negative."""
    if over_last_days < 0:
        raise ValueError(f"Invalid number of days: {over_last_days}. Must be a number greater than or equal to zero.")


def validate_choice_parameter(
    parameter_name: str,
    value: str,
    valid_choices: set,
    display_choices: list = None
) -> str:
    """Validate and normalize a choice parameter, raising ValueError if invalid."""
    normalized_value = value.lower().strip()
    if normalized_value not in valid_choices:
        display_list = display_choices or list(valid_choices)
        raise ValueError(f"Invalid {parameter_name}: {value}. Valid types: {', '.join(display_list)}")
    return normalized_value


def get_pathfinder_player_ids() -> Set[str]:
    """Load player IDs from pathfinder_player_ids.txt (cached after first load)."""
    global _pathfinder_player_ids
    
    # Return cached value if already loaded
    if _pathfinder_player_ids is not None:
        return _pathfinder_player_ids
    
    common_dir = Path(__file__).parent
    file_path = common_dir / "pathfinder_player_ids.txt"
    player_ids = set[str]()
    
    if not file_path.exists():
        logger.debug(f"Player IDs file not found at {file_path}")
        _pathfinder_player_ids = player_ids
        return player_ids
    
    # Load player IDs from file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    player_ids.add(line)
        
        _pathfinder_player_ids = player_ids
        logger.info(f"Loaded {len(player_ids)} player IDs")
        return player_ids
    except Exception as e:
        logger.error(f"Error loading player IDs: {e}", exc_info=True)
        _pathfinder_player_ids = set[str]()
        return _pathfinder_player_ids


def create_time_filter_params(over_last_days: int) -> Tuple[str, list, str]:
    """Build time filter SQL clause, params list, and display text."""
    if over_last_days > 0:
        from datetime import datetime, timedelta
        time_threshold = datetime.utcnow() - timedelta(days=over_last_days)
        time_filter = "AND mh.start_time >= $1"
        query_params = [time_threshold]
        time_period_text = f"  over the last {over_last_days} day{'s' if over_last_days != 1 else ''}"
    else:
        time_filter = ""
        query_params = []
        time_period_text = " (All Time)"

    return time_filter, query_params, time_period_text


def command_wrapper(
    command_name: str,
    channel_check: Optional[Callable[[discord.Interaction], bool]] = None,
    log_params: Optional[dict] = None
):
    """Decorator that handles channel checks, logging, error handling, and response deferral."""
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            command_start_time = time.time()

            log_kwargs = log_params or {}
            log_kwargs.update(kwargs)
            log_command_data(interaction, command_name, **log_kwargs)

            try:
                if channel_check and not channel_check(interaction):
                    await interaction.response.send_message(
                        "❌ This bot can only be used in the designated channel.",
                        ephemeral=True
                    )
                    log_command_completion(command_name, command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                    return

                # Defer response
                await interaction.response.defer()
                
                # Execute the actual command logic
                result = await func(interaction, *args, **kwargs)
                return result

            except Exception as e:
                await handle_command_errors(interaction, command_name, command_start_time, e, kwargs=log_kwargs)
                return
        
        wrapper.__signature__ = inspect.signature(func)
        return wrapper
    return decorator
