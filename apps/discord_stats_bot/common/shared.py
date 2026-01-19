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
    player = str(player).strip()
    
    # First, check if the input is a player_id by searching any of the tables
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
        # Get most recent player name from player_match_stats (most reliable source)
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
    
    # If no results from player_id, try player_name across all three tables
    # Using UNION DISTINCT to get unique player_ids efficiently
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
        # Get most recent player name from player_match_stats (most reliable source)
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
        logger.info(f"Player IDs file not found at {file_path}")
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
        from datetime import datetime, timedelta, timezone
        time_threshold = datetime.now(timezone.utc) - timedelta(days=over_last_days)
        time_filter = "AND mh.start_time >= $1"
        query_params = [time_threshold]
        time_period_text = f"  over the last {over_last_days} day{'s' if over_last_days != 1 else ''}"
    else:
        time_filter = ""
        query_params = []
        time_period_text = " (All Time)"

    return time_filter, query_params, time_period_text


# =============================================================================
# SQL Query Building Helpers
# =============================================================================

def build_pathfinder_filter(
    table_alias: str,
    param_start: int,
    pathfinder_ids: list,
    use_and: bool = True
) -> Tuple[str, list, int]:
    """
    Build a pathfinder filter WHERE/AND clause.
    
    Args:
        table_alias: Table alias to use (e.g., 'pms', 'pks')
        param_start: Starting parameter number (e.g., 1 for $1)
        pathfinder_ids: List of pathfinder player IDs from file
        use_and: If True, prefix with AND; if False, prefix with WHERE
        
    Returns:
        Tuple of (sql_clause, params_to_add, next_param_num)
    """
    prefix = "AND" if use_and else "WHERE"
    
    if pathfinder_ids:
        clause = (
            f"{prefix} ({table_alias}.player_name ILIKE ${param_start} "
            f"OR {table_alias}.player_name ILIKE ${param_start + 1} "
            f"OR {table_alias}.player_id = ANY(${param_start + 2}::text[]))"
        )
        params = ["PFr |%", "PF |%", pathfinder_ids]
        return clause, params, param_start + 3
    else:
        clause = (
            f"{prefix} ({table_alias}.player_name ILIKE ${param_start} "
            f"OR {table_alias}.player_name ILIKE ${param_start + 1})"
        )
        params = ["PFr |%", "PF |%"]
        return clause, params, param_start + 2


def build_lateral_name_lookup(
    player_id_ref: str,
    extra_where: str = ""
) -> str:
    """
    Build a LATERAL JOIN subquery to get the most recent player name.
    
    Args:
        player_id_ref: Reference to player_id column (e.g., 'tp.player_id')
        extra_where: Additional WHERE clauses (should start with AND if provided)
        
    Returns:
        SQL string for the LATERAL JOIN
    """
    return f"""LEFT JOIN LATERAL (
            SELECT pms.player_name
            FROM pathfinder_stats.player_match_stats pms
            INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
            WHERE pms.player_id = {player_id_ref}
                {extra_where}
            ORDER BY mh.start_time DESC
            LIMIT 1
        ) rn ON TRUE"""


def build_from_clause_with_time_filter(
    table: str,
    table_alias: str,
    has_time_filter: bool
) -> Tuple[str, str]:
    """
    Build FROM clause with optional JOIN to match_history for time filtering.
    
    Args:
        table: Full table name (e.g., 'pathfinder_stats.player_match_stats')
        table_alias: Alias for the table (e.g., 'pms')
        has_time_filter: Whether time filtering is needed
        
    Returns:
        Tuple of (from_clause, time_column_prefix)
        - from_clause: SQL FROM clause
        - time_column_prefix: 'mh.' if joined, empty string otherwise
    """
    if has_time_filter:
        from_clause = f"""FROM {table} {table_alias}
                INNER JOIN pathfinder_stats.match_history mh
                    ON {table_alias}.match_id = mh.match_id"""
        return from_clause, "mh."
    else:
        return f"FROM {table} {table_alias}", ""


def build_where_clause(
    *clauses: str,
    base_filter: str = ""
) -> str:
    """
    Combine multiple WHERE clause fragments into a single WHERE clause.
    
    Args:
        *clauses: Variable number of clause fragments (can be empty strings)
        base_filter: A filter to always include (e.g., 'pms.column > 0')
        
    Returns:
        Combined WHERE clause string
    """
    # Filter out empty clauses
    active_clauses = [c.strip() for c in clauses if c and c.strip()]
    
    if not active_clauses and not base_filter:
        return ""
    
    result_parts = []
    has_where = False
    
    for clause in active_clauses:
        if clause.upper().startswith("WHERE "):
            if has_where:
                # Convert WHERE to AND
                clause = "AND " + clause[6:]
            else:
                has_where = True
        result_parts.append(clause)
    
    # Add base filter
    if base_filter:
        if result_parts:
            result_parts.append(f"AND {base_filter}")
        else:
            result_parts.append(f"WHERE {base_filter}")
    
    return " ".join(result_parts)


def format_sql_query_with_params(query: str, params: list) -> str:
    """
    Format a SQL query with PostgreSQL-style parameters ($1, $2, etc.) by substituting
    the actual parameter values. This is for logging purposes only.
    
    Args:
        query: SQL query string with PostgreSQL placeholders ($1, $2, etc.)
        params: List of parameter values to substitute
        
    Returns:
        Formatted SQL query string with parameter values substituted
    """
    import re
    from datetime import datetime
    
    formatted_query = query
    
    # Find all parameter placeholders ($1, $2, etc.) in reverse order to avoid index issues
    param_pattern = r'\$(\d+)'
    matches = list(re.finditer(param_pattern, formatted_query))
    
    # Process in reverse order to maintain correct indices
    for match in reversed(matches):
        param_index = int(match.group(1)) - 1  # Convert to 0-based index
        
        if param_index < len(params):
            param_value = params[param_index]
            
            # Format the parameter value based on its type
            if param_value is None:
                formatted_value = "NULL"
            elif isinstance(param_value, str):
                # Escape single quotes and wrap in quotes
                escaped = param_value.replace("'", "''")
                formatted_value = f"'{escaped}'"
            elif isinstance(param_value, (int, float)):
                formatted_value = str(param_value)
            elif isinstance(param_value, datetime):
                # Format datetime as ISO string
                formatted_value = f"'{param_value.isoformat()}'"
            elif isinstance(param_value, list):
                # Handle array parameters (e.g., text[])
                if all(isinstance(x, str) for x in param_value):
                    escaped_items = [item.replace("'", "''") for item in param_value]
                    quoted_items = [f"'{item}'" for item in escaped_items]
                    formatted_value = f"ARRAY[{', '.join(quoted_items)}]"
                else:
                    formatted_value = f"ARRAY[{', '.join(str(x) for x in param_value)}]"
            else:
                # Fallback: convert to string and escape
                escaped = str(param_value).replace("'", "''")
                formatted_value = f"'{escaped}'"
            
            # Replace the placeholder with the formatted value
            formatted_query = formatted_query[:match.start()] + formatted_value + formatted_query[match.end():]
    
    return formatted_query


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
