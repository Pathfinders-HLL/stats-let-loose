"""
Scheduled task to post comprehensive Pathfinder leaderboard statistics.

Posts top 50 players for multiple stat categories every 30 minutes,
with interactive dropdowns to view different timeframes and stats,
plus pagination buttons for navigating through player rankings.
"""

import asyncio
import json
import logging
import os

import discord

from asyncpg import exceptions as asyncpg_exceptions
from datetime import datetime, timezone
from discord.ext import tasks
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    get_pathfinder_player_ids,
    escape_sql_identifier,
    create_time_filter_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    format_sql_query_with_params,
    get_weapon_mapping,
    build_compact_leaderboard_embed,
    PATHFINDER_COLOR,
)
from apps.discord_stats_bot.config import get_bot_config

logger = logging.getLogger(__name__)

_bot_instance: Optional[discord.Client] = None

# Persistent storage for leaderboard message ID
CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))
LEADERBOARD_STATE_FILE = CACHE_DIR / "leaderboard_state.json"
SQL_LOG_FILE = CACHE_DIR / "sql_queries.log"
_leaderboard_state_lock = asyncio.Lock()
_stored_message_id: Optional[int] = None
_stored_channel_id: Optional[int] = None

# Match quality thresholds
MIN_MATCH_DURATION_SECONDS = 2700  # 45 minutes
MIN_PLAYERS_PER_MATCH = 60
MIN_MATCHES_FOR_AGGREGATE = 5  # Minimum matches for stats #1, #2, #5

# Pagination settings
TOP_PLAYERS_LIMIT = 50  # Top 50 players per stat
PLAYERS_PER_PAGE = 10   # 10 players per page = 5 pages

# Timeframe options
TIMEFRAME_OPTIONS = {
    "1d": {"days": 1, "label": "Last 24 Hours"},
    "7d": {"days": 7, "label": "Last 7 Days"},
    "30d": {"days": 30, "label": "Last 30 Days"},
    "all": {"days": 0, "label": "All Time"},
}

# Global cache for pre-computed leaderboard results
# Structure: {timeframe_key: {"stats": {}, "embeds": [], "timestamp": datetime}}
_leaderboard_cache: Dict[str, Dict[str, Any]] = {}

# Track which SQL queries have been logged to avoid duplicate logging
_logged_queries: set = set()
# Accumulate SQL queries in memory to write all at once
_sql_query_logs: List[str] = []


def _log_sql_query_once(query_name: str, query: str, query_params: List[Any]) -> None:
    """Log SQL query only once per query name to avoid duplicate logs on message edits."""
    if query_name not in _logged_queries:
        formatted_query = format_sql_query_with_params(query, query_params)
        log_entry = f"SQL Query [{query_name}]: {formatted_query}\n"
        _sql_query_logs.append(log_entry)
        _logged_queries.add(query_name)


def _write_sql_logs_to_file() -> None:
    """Write accumulated SQL queries to file, overwriting previous contents."""
    if not _sql_query_logs:
        return
    
    try:
        # Ensure cache directory exists
        SQL_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Overwrite file (mode 'w') to prevent file size growth
        with open(SQL_LOG_FILE, 'w', encoding='utf-8') as f:
            f.writelines(_sql_query_logs)
    except Exception as e:
        logger.warning(f"Failed to write SQL queries to file: {e}", exc_info=True)


def _clear_sql_logs() -> None:
    """Clear accumulated SQL query logs and reset tracking."""
    global _logged_queries, _sql_query_logs
    _logged_queries.clear()
    _sql_query_logs.clear()


async def _fetch_with_timeout_logging(conn, query_name: str, query: str, query_params: List[Any]):
    """Execute a query and log it if the statement times out."""
    try:
        return await conn.fetch(query, *query_params)
    except asyncpg_exceptions.QueryCanceledError:
        formatted_query = format_sql_query_with_params(query, query_params)
        logger.error(f"SQL query timed out [{query_name}]: {formatted_query}", exc_info=True)
        raise


async def _load_leaderboard_state() -> None:
    """Load persisted leaderboard message ID from disk."""
    global _stored_message_id, _stored_channel_id
    
    if not LEADERBOARD_STATE_FILE.exists():
        logger.info(f"Leaderboard state file not found at {LEADERBOARD_STATE_FILE}, starting fresh")
        return
    
    try:
        with open(LEADERBOARD_STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        _stored_message_id = data.get("message_id")
        _stored_channel_id = data.get("channel_id")
        
        if _stored_message_id and _stored_channel_id:
            logger.info(f"Loaded leaderboard state: message_id={_stored_message_id}, channel_id={_stored_channel_id}")
        else:
            logger.info("Leaderboard state file exists but contains no valid IDs")
            
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load leaderboard state from {LEADERBOARD_STATE_FILE}: {e}. Starting fresh.")


async def _save_leaderboard_state(message_id: int, channel_id: int) -> None:
    """Save leaderboard message ID to disk."""
    global _stored_message_id, _stored_channel_id
    
    try:
        LEADERBOARD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "message_id": message_id,
            "channel_id": channel_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        temp_file = LEADERBOARD_STATE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(LEADERBOARD_STATE_FILE)
        
        async with _leaderboard_state_lock:
            _stored_message_id = message_id
            _stored_channel_id = channel_id
        
        logger.info(f"Saved leaderboard state: message_id={message_id}, channel_id={channel_id}")
        
    except IOError as e:
        logger.error(f"Failed to save leaderboard state to {LEADERBOARD_STATE_FILE}: {e}", exc_info=True)


async def _set_query_timeout(conn, over_last_days: int, default_timeout: int = 60, all_time_timeout: int = 300) -> None:
    """
    Set statement timeout for queries based on time period.
    
    Args:
        conn: Database connection
        over_last_days: Number of days (0 for all-time)
        default_timeout: Timeout in seconds for filtered queries (default: 60)
        all_time_timeout: Timeout in seconds for all-time queries (default: 300 = 5 minutes)
    """
    if over_last_days == 0:
        # All-time queries need more time
        await conn.execute(f"SET statement_timeout = '{all_time_timeout}s'")
    else:
        # Use default timeout for filtered queries
        await conn.execute(f"SET statement_timeout = '{default_timeout}s'")


def _build_quality_match_subquery(time_where_clause: str = "") -> str:
    """
    Build subquery to filter matches with 60+ players.
    
    Uses the player_count column in match_history for efficient filtering
    instead of recalculating player counts via GROUP BY.
    
    Args:
        time_where_clause: Optional WHERE clause for time filtering (e.g., "WHERE mh.start_time >= $1")
    """
    # Use player_count column for efficient filtering (no expensive GROUP BY)
    base_filter = f"player_count >= {MIN_PLAYERS_PER_MATCH}"
    
    if time_where_clause:
        # Replace WHERE with AND since we're adding to existing WHERE clause
        time_condition = time_where_clause.replace("WHERE ", "AND ")
        return f"""
            SELECT match_id 
            FROM pathfinder_stats.match_history mh
            WHERE {base_filter}
            {time_condition}
        """
    else:
        # No time filter - just filter by player_count
        return f"""
            SELECT match_id 
            FROM pathfinder_stats.match_history
            WHERE {base_filter}
        """


async def _get_most_infantry_kills(
    pool, 
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #1: Most infantry kills over the time period.
    Requires minimum 5 matches for qualification.
    """
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration filter
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}"
        ]
        
        # Combine WHERE clauses
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build LATERAL join pathfinder filter
        lateral_where = ""
        if pathfinder_ids:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    SUM(pms.infantry_kills) as total_infantry_kills,
                    COUNT(*) as match_count
                {from_clause}
                INNER JOIN qualified_matches qm ON pms.match_id = qm.match_id
                {player_stats_where}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= {MIN_MATCHES_FOR_AGGREGATE}
            ),
            top_players AS (
                SELECT player_id, total_infantry_kills, match_count
                FROM player_stats
                ORDER BY total_infantry_kills DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tp.player_id,
                tp.total_infantry_kills as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            {lateral_join}
            ORDER BY tp.total_infantry_kills DESC
        """
        
        _log_sql_query_once("infantry_kills", query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "infantry_kills", query, query_params)
        return [dict(row) for row in results]


async def _get_average_kd(
    pool,
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #2: Average K/D ratio over the time period.
    Requires minimum 5 matches for qualification.
    """
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration filter
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}",
            f"pms.time_played >= {MIN_MATCH_DURATION_SECONDS}"
        ]
        
        # Combine WHERE clauses
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build LATERAL join pathfinder filter
        lateral_where = ""
        if pathfinder_ids:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    AVG(pms.kill_death_ratio) as avg_kd,
                    COUNT(*) as match_count
                {from_clause}
                INNER JOIN qualified_matches qm ON pms.match_id = qm.match_id
                {player_stats_where}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= {MIN_MATCHES_FOR_AGGREGATE}
            ),
            top_players AS (
                SELECT player_id, avg_kd, match_count
                FROM player_stats
                ORDER BY avg_kd DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tp.player_id,
                ROUND(tp.avg_kd::numeric, 2) as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            {lateral_join}
            ORDER BY tp.avg_kd DESC
        """
        
        _log_sql_query_once("average_kd", query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "average_kd", query, query_params)
        return [dict(row) for row in results]


async def _get_most_kills_single_match(
    pool,
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #3: Most kills in a single match.
    No minimum matches required.
    Excludes matches with high artillery/SPA kills.
    """
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration and mh.map_name
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}",
            "pms.artillery_kills <= 5 AND pms.spa_kills <= 5"
        ]
        
        # Combine WHERE clauses
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            )
            SELECT DISTINCT ON (pms.player_id)
                pms.player_id,
                pms.player_name,
                pms.total_kills as value,
                mh.map_name
            {from_clause}
            INNER JOIN qualified_matches qm ON pms.match_id = qm.match_id
            {player_stats_where}
            ORDER BY pms.player_id, pms.total_kills DESC
        """
        
        # First get best match per player, then sort by kills
        wrapper_query = f"""
            WITH best_matches AS ({query})
            SELECT player_id, player_name, value, map_name
            FROM best_matches
            ORDER BY value DESC
            LIMIT {TOP_PLAYERS_LIMIT}
        """
        
        _log_sql_query_once("single_match_kills", wrapper_query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "single_match_kills", wrapper_query, query_params)
        return [dict(row) for row in results]


async def _get_best_kd_single_match(
    pool,
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #4: Best K/D ratio in a single match.
    No minimum matches required.
    """
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration and mh.map_name
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}",
            f"pms.time_played >= {MIN_MATCH_DURATION_SECONDS}"
        ]
        
        # Combine WHERE clauses
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            )
            SELECT DISTINCT ON (pms.player_id)
                pms.player_id,
                pms.player_name,
                pms.kill_death_ratio as value,
                mh.map_name
            {from_clause}
            INNER JOIN qualified_matches qm ON pms.match_id = qm.match_id
            {player_stats_where}
            ORDER BY pms.player_id, pms.kill_death_ratio DESC
        """
        
        wrapper_query = f"""
            WITH best_matches AS ({query})
            SELECT player_id, player_name, ROUND(value::numeric, 2) as value, map_name
            FROM best_matches
            ORDER BY value DESC
            LIMIT {TOP_PLAYERS_LIMIT}
        """
        
        _log_sql_query_once("single_match_kd", wrapper_query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "single_match_kd", wrapper_query, query_params)
        return [dict(row) for row in results]


async def _get_most_k98_kills(
    pool,
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #5: Most Karabiner 98k kills over the time period.
    No minimum matches required.
    """
    weapon_mapping = get_weapon_mapping()
    column_name = weapon_mapping.get("karabiner 98k", "karabiner_98k")
    escaped_column = escape_sql_identifier(column_name)
    
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration filter
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_kill_stats", "pks", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter (uses pks alias for player_kill_stats)
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pks", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}"
        ]
        
        # Combine WHERE clauses
        kill_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build LATERAL join pathfinder filter (uses pms alias for player_match_stats)
        lateral_where = ""
        if pathfinder_ids:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            ),
            player_stats AS (
                SELECT 
                    pks.player_id,
                    SUM(pks.{escaped_column}) as total_k98_kills,
                    COUNT(*) as match_count
                {from_clause}
                INNER JOIN qualified_matches qm ON pks.match_id = qm.match_id
                {kill_stats_where}
                GROUP BY pks.player_id
                HAVING SUM(pks.{escaped_column}) > 0
            ),
            top_players AS (
                SELECT player_id, total_k98_kills, match_count
                FROM player_stats
                ORDER BY total_k98_kills DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tp.player_id,
                tp.total_k98_kills as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            {lateral_join}
            ORDER BY tp.total_k98_kills DESC
        """
        
        _log_sql_query_once("k98_kills", query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "k98_kills", query, query_params)
        return [dict(row) for row in results]


async def _get_avg_objective_efficiency(
    pool,
    over_last_days: int,
    pathfinder_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Stat #6: Average objective efficiency ((offense_score + defense_score) / time_played).
    Calculated per minute for readability.
    """
    # Calculate time period filter
    _, base_query_params, _ = create_time_filter_params(over_last_days)
    
    async with pool.acquire() as conn:
        # Set timeout based on query type (all-time queries get longer timeout)
        await _set_query_timeout(conn, over_last_days)
        
        param_num = 1
        query_params = []
        
        # Build FROM clause - always include match_history JOIN for mh.match_duration filter
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", True
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if pathfinder_ids:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Build quality match filters
        quality_filters = [
            f"mh.match_duration >= {MIN_MATCH_DURATION_SECONDS}",
            f"pms.time_played >= {MIN_MATCH_DURATION_SECONDS}"
        ]
        
        # Combine WHERE clauses
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(quality_filters)
        )
        
        # Build LATERAL join pathfinder filter
        lateral_where = ""
        if pathfinder_ids:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        # Build qualified_matches CTE with time filter applied
        qualified_matches_cte = _build_quality_match_subquery(time_where)
        
        # Calculate efficiency per minute: (offense + defense) / (time_played / 60)
        query = f"""
            WITH qualified_matches AS (
                {qualified_matches_cte}
            ),
            player_stats AS (
                SELECT 
                    pms.player_id,
                    AVG(
                        CASE WHEN pms.time_played > 0 
                        THEN (pms.offense_score + pms.defense_score)::float / (pms.time_played / 60.0)
                        ELSE 0 
                        END
                    ) as avg_obj_efficiency,
                    COUNT(*) as match_count
                {from_clause}
                INNER JOIN qualified_matches qm ON pms.match_id = qm.match_id
                {player_stats_where}
                GROUP BY pms.player_id
                HAVING COUNT(*) >= 3
            ),
            top_players AS (
                SELECT player_id, avg_obj_efficiency, match_count
                FROM player_stats
                ORDER BY avg_obj_efficiency DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tp.player_id,
                ROUND(tp.avg_obj_efficiency::numeric, 2) as value,
                tp.match_count,
                COALESCE(rn.player_name, tp.player_id) as player_name
            FROM top_players tp
            {lateral_join}
            ORDER BY tp.avg_obj_efficiency DESC
        """
        
        _log_sql_query_once("obj_efficiency", query, query_params)
        
        results = await _fetch_with_timeout_logging(conn, "obj_efficiency", query, query_params)
        return [dict(row) for row in results]


async def fetch_all_leaderboard_stats(
    days: int = 7
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch all leaderboard statistics for the given time period.
    
    Args:
        days: Number of days to look back (0 for all-time)
    
    Returns:
        Dictionary with stat category keys and result lists
    """
    pool = await get_readonly_db_pool()
    pathfinder_ids = list(get_pathfinder_player_ids())
    
    stats = {
        "infantry_kills": await _get_most_infantry_kills(pool, days, pathfinder_ids),
        "avg_kd": await _get_average_kd(pool, days, pathfinder_ids),
        "single_match_kills": await _get_most_kills_single_match(pool, days, pathfinder_ids),
        "single_match_kd": await _get_best_kd_single_match(pool, days, pathfinder_ids),
        "k98_kills": await _get_most_k98_kills(pool, days, pathfinder_ids),
        "obj_efficiency": await _get_avg_objective_efficiency(pool, days, pathfinder_ids),
    }
    
    return stats


# Stat configuration for building embeds
STAT_CONFIGS = [
    {
        "key": "infantry_kills",
        "title": "🎯 Most Infantry Kills",
        "compact_title": "🎯 Highest Kills",
        "value_label": "Kills",
        "value_abbrev": "Tot",  # 3-char abbreviation for compact view
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": f"Min {MIN_MATCHES_FOR_AGGREGATE} matches required"
    },
    {
        "key": "avg_kd",
        "title": "📊 Highest Average K/D",
        "compact_title": "📊 Top Average K/D",
        "value_label": "Avg K/D",
        "value_abbrev": "K/D",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": f"Min {MIN_MATCHES_FOR_AGGREGATE} matches, 45+ min each"
    },
    {
        "key": "single_match_kills",
        "title": "💥 Most Kills in Single Match",
        "compact_title": "💥 Most Single Match Kills",
        "value_label": "Kills",
        "value_abbrev": "Kil",
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": "Best single match performance"
    },
    {
        "key": "single_match_kd",
        "title": "⚔️ Best K/D in Single Match",
        "compact_title": "⚔️ Highest Single Match KDR",
        "value_label": "K/D",
        "value_abbrev": "K/D",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": "Best single match K/D ratio"
    },
    {
        "key": "k98_kills",
        "title": "🔫 Most Karabiner 98k Kills",
        "compact_title": "🔫 Most K98 Kills",
        "value_label": "K98 Kills",
        "value_abbrev": "K98",
        "color": PATHFINDER_COLOR,
        "value_format": "int",
        "footer_note": "Total kills with Karabiner 98k"
    },
    {
        "key": "obj_efficiency",
        "title": "🏆 Highest Objective Efficiency",
        "compact_title": "🏆 Objective Efficiency",
        "value_label": "Pts/Min",
        "value_abbrev": "Pts",
        "color": PATHFINDER_COLOR,
        "value_format": "float",
        "footer_note": "(Offense + Defense) / Time Played per minute"
    },
]

# Number of players to show in compact view
COMPACT_VIEW_PLAYERS = 10


def _build_stat_embed_page(
    results: List[Dict[str, Any]],
    stat_config: Dict[str, Any],
    page: int,
    total_pages: int,
    timeframe_label: str,
    updated_timestamp: datetime
) -> discord.Embed:
    """Build a single page of a stat category embed with 3 columns."""
    title = f"{stat_config['title']} ({timeframe_label})"
    color = stat_config["color"]
    value_label = stat_config["value_label"]
    value_format = stat_config["value_format"]
    
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        # Still show footer with page info
        unix_ts = int(updated_timestamp.timestamp())
        embed.set_footer(text=f"Page {page}/{total_pages} • {stat_config['title'].split(' ', 1)[1]} • {timeframe_label} • Updated <t:{unix_ts}:R>")
        return embed
    
    # Calculate which results to show for this page
    start_idx = (page - 1) * PLAYERS_PER_PAGE
    end_idx = start_idx + PLAYERS_PER_PAGE
    page_results = results[start_idx:end_idx]
    
    ranks = []
    players = []
    values = []
    
    for rank, row in enumerate(page_results, start_idx + 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        
        if value_format == "int":
            values.append(f"{int(value):,}")
        elif value_format == "float":
            values.append(f"{float(value):.2f}")
        else:
            values.append(str(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    # Build footer: "Page 2/5 • Most Infantry Kills • Last 7 Days • Updated <timestamp>"
    stat_name = stat_config['title'].split(' ', 1)[1]  # Remove emoji
    unix_ts = int(updated_timestamp.timestamp())
    footer_text = f"Page {page}/{total_pages} • {stat_name} • {timeframe_label} • Updated <t:{unix_ts}:R>"
    embed.set_footer(text=footer_text)
    
    return embed


def _get_total_pages(results: List[Dict[str, Any]]) -> int:
    """Calculate total pages for results."""
    if not results:
        return 1
    return max(1, (len(results) + PLAYERS_PER_PAGE - 1) // PLAYERS_PER_PAGE)


def _build_stat_embed(
    title: str,
    results: List[Dict[str, Any]],
    value_label: str,
    color: discord.Color,
    value_format: str = "int",
    footer_note: str = ""
) -> discord.Embed:
    """Build a single stat category embed with 3 columns (for first page overview)."""
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        return embed
    
    # Only show first page (10 players) for overview
    page_results = results[:PLAYERS_PER_PAGE]
    
    ranks = []
    players = []
    values = []
    
    for rank, row in enumerate(page_results, 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        
        if value_format == "int":
            values.append(f"{int(value):,}")
        elif value_format == "float":
            values.append(f"{float(value):.2f}")
        else:
            values.append(str(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    if footer_note:
        embed.set_footer(text=footer_note)
    
    return embed


def build_leaderboard_embeds(
    stats: Dict[str, List[Dict[str, Any]]],
    timeframe_label: str
) -> List[discord.Embed]:
    """Build all leaderboard embeds from stats data (first page overview for each stat)."""
    embeds = []
    
    for config in STAT_CONFIGS:
        embed = _build_stat_embed(
            f"{config['title']} ({timeframe_label})",
            stats.get(config["key"], []),
            config["value_label"],
            config["color"],
            value_format=config["value_format"],
            footer_note=config["footer_note"]
        )
        embeds.append(embed)
    
    return embeds


class StatSelect(discord.ui.Select):
    """Dropdown select for choosing which stat to view."""
    
    def __init__(self, current_stat_idx: int = 0):
        options = []
        for idx, config in enumerate(STAT_CONFIGS):
            emoji = config["title"].split(" ")[0]
            stat_name = config["title"].split(" ", 1)[1]
            options.append(discord.SelectOption(
                label=stat_name,
                value=str(idx),
                emoji=emoji,
                default=(idx == current_stat_idx)
            ))
        
        super().__init__(
            custom_id="pathfinder_stat_select",
            placeholder="Select a stat...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle stat selection - update the view's stat index."""
        view: PaginatedLeaderboardView = self.view
        view.current_stat_idx = int(self.values[0])
        view.current_page = 1  # Reset to first page when changing stats
        await view.update_message(interaction)


class TimeframeSelect(discord.ui.Select):
    """Dropdown select for choosing leaderboard timeframe."""
    
    def __init__(self, current_timeframe: str = "7d"):
        options = [
            discord.SelectOption(
                label="Last 24 Hours",
                value="1d",
                description="View stats from the past day",
                emoji="📅",
                default=(current_timeframe == "1d")
            ),
            discord.SelectOption(
                label="Last 7 Days",
                value="7d",
                description="View stats from the past week",
                emoji="📆",
                default=(current_timeframe == "7d")
            ),
            discord.SelectOption(
                label="Last 30 Days",
                value="30d",
                description="View stats from the past month",
                emoji="🗓️",
                default=(current_timeframe == "30d")
            ),
            discord.SelectOption(
                label="All Time",
                value="all",
                description="View all-time stats",
                emoji="♾️",
                default=(current_timeframe == "all")
            ),
        ]
        super().__init__(
            custom_id="pathfinder_leaderboard_timeframe",
            placeholder="Select a timeframe...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle timeframe selection - update the view's timeframe."""
        view: PaginatedLeaderboardView = self.view
        view.current_timeframe = self.values[0]
        view.current_page = 1  # Reset to first page when changing timeframe
        await view.update_message(interaction)


class PaginatedLeaderboardView(discord.ui.View):
    """Persistent view with pagination, stat selection, and timeframe selector."""
    
    def __init__(
        self,
        current_stat_idx: int = 0,
        current_page: int = 1,
        current_timeframe: str = "7d"
    ):
        # Set timeout to None for persistent view
        super().__init__(timeout=None)
        
        self.current_stat_idx = current_stat_idx
        self.current_page = current_page
        self.current_timeframe = current_timeframe
        
        # Add stat selector (row 0)
        self.add_item(StatSelect(current_stat_idx))
        
        # Add timeframe selector (row 1)
        timeframe_select = TimeframeSelect(current_timeframe)
        timeframe_select.row = 1
        self.add_item(timeframe_select)
    
    def _get_cached_data(self) -> Tuple[Optional[Dict[str, Any]], datetime]:
        """Get cached data for current timeframe."""
        cached = _leaderboard_cache.get(self.current_timeframe)
        if cached and cached.get("stats"):
            return cached["stats"], cached["timestamp"]
        return None, datetime.now(timezone.utc)
    
    def _get_current_results(self) -> Tuple[List[Dict[str, Any]], datetime]:
        """Get results for the current stat from cache."""
        stats, timestamp = self._get_cached_data()
        if stats is None:
            return [], timestamp
        
        stat_key = STAT_CONFIGS[self.current_stat_idx]["key"]
        return stats.get(stat_key, []), timestamp
    
    def _get_total_pages(self) -> int:
        """Get total pages for current stat."""
        results, _ = self._get_current_results()
        return _get_total_pages(results)
    
    def build_embed(self) -> discord.Embed:
        """Build the current page embed."""
        results, timestamp = self._get_current_results()
        stat_config = STAT_CONFIGS[self.current_stat_idx]
        timeframe_config = TIMEFRAME_OPTIONS.get(self.current_timeframe, TIMEFRAME_OPTIONS["7d"])
        total_pages = self._get_total_pages()
        
        return _build_stat_embed_page(
            results=results,
            stat_config=stat_config,
            page=self.current_page,
            total_pages=total_pages,
            timeframe_label=timeframe_config["label"],
            updated_timestamp=timestamp
        )
    
    async def update_message(self, interaction: discord.Interaction):
        """Update the message with current state."""
        embed = self.build_embed()
        
        # Rebuild the view to update button states
        new_view = PaginatedLeaderboardView(
            current_stat_idx=self.current_stat_idx,
            current_page=self.current_page,
            current_timeframe=self.current_timeframe
        )
        
        await interaction.response.edit_message(embed=embed, view=new_view)
    
    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary, custom_id="first_page", row=2)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to first page."""
        self.current_page = 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary, custom_id="prev_page", row=2)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        if self.current_page > 1:
            self.current_page -= 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary, custom_id="next_page", row=2)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        total_pages = self._get_total_pages()
        if self.current_page < total_pages:
            self.current_page += 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, custom_id="last_page", row=2)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to last page."""
        self.current_page = self._get_total_pages()
        await self.update_message(interaction)


class LeaderboardView(discord.ui.View):
    """Simple persistent view with a Browse Details button for the main post."""
    
    def __init__(self):
        # Set timeout to None for persistent view
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="Advanced View",
        emoji="🔍",
        style=discord.ButtonStyle.primary,
        custom_id="pathfinder_browse_details"
    )
    async def browse_details(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open the paginated leaderboard browser."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Create paginated view starting at first stat, first page, 7-day timeframe
            view = PaginatedLeaderboardView(
                current_stat_idx=0,
                current_page=1,
                current_timeframe="7d"
            )
            
            embed = view.build_embed()
            
            await interaction.followup.send(
                embed=embed,
                view=view,
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Error opening browse details: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while fetching the leaderboards.",
                ephemeral=True
            )


@tasks.loop(minutes=20)
async def refresh_leaderboard_cache():
    """
    Pre-compute and cache leaderboard data for all timeframes.
    Runs every 20 minutes to keep cache fresh.
    """
    global _leaderboard_cache
    
    try:
        logger.info("Starting leaderboard cache refresh...")
        
        # Clear SQL logs at the start of each refresh cycle
        _clear_sql_logs()
        
        now_utc = datetime.now(timezone.utc)
        
        # Pre-compute stats for all timeframes
        cached_timeframes = 0
        for timeframe_key, config in TIMEFRAME_OPTIONS.items():
            try:
                days = config["days"]
                label = config["label"]
                
                # Fetch stats and build embeds
                stats = await fetch_all_leaderboard_stats(days)
                embeds = build_leaderboard_embeds(stats, label)
                
                # Store in cache
                _leaderboard_cache[timeframe_key] = {
                    "stats": stats,
                    "embeds": embeds,
                    "timestamp": now_utc,
                    "label": label
                }
                
                logger.info(f"Cached leaderboard data for {timeframe_key} ({label})")
                cached_timeframes += 1
                
            except Exception as e:
                logger.error(f"Error caching leaderboard data for {timeframe_key}: {e}", exc_info=True)
        
        # Write accumulated SQL queries to file at the end of refresh cycle
        _write_sql_logs_to_file()
        
        not_cached = len(TIMEFRAME_OPTIONS) - cached_timeframes
        logger.info(
            f"Leaderboard cache refresh complete. Cached {cached_timeframes}/{len(TIMEFRAME_OPTIONS)} "
            f"timeframes ({not_cached} not cached)."
        )
        
    except Exception as e:
        logger.error(f"Error in refresh_leaderboard_cache task: {e}", exc_info=True)


@tasks.loop(minutes=30)
async def post_pathfinder_leaderboards():
    """Post comprehensive Pathfinder leaderboards, editing the previous message if possible."""
    global _bot_instance
    
    if not _bot_instance:
        logger.error("Bot instance not set for pathfinder leaderboards")
        return
    
    try:
        bot_config = get_bot_config()
        stats_channel_id = bot_config.stats_channel_id
        
        if not stats_channel_id:
            logger.warning("DISCORD_STATS_CHANNEL_ID not configured, skipping leaderboard posting")
            return
        
        channel = _bot_instance.get_channel(stats_channel_id)
        if not channel:
            logger.error(f"Channel {stats_channel_id} not found")
            return
        
        # Use cached data for default 7-day period
        cached_7d = _leaderboard_cache.get("7d")
        now_utc = datetime.now(timezone.utc)
        
        if cached_7d and cached_7d.get("stats"):
            stats = cached_7d["stats"]
            cache_timestamp = cached_7d.get("timestamp", now_utc)
            logger.info("Using cached stats for compact leaderboard posting")
        else:
            # Fallback: compute on-demand if cache is empty
            logger.warning("Cache empty, computing 7d leaderboard stats on-demand")
            # Clear logs before fetching to start fresh for this on-demand fetch
            _clear_sql_logs()
            stats = await fetch_all_leaderboard_stats(days=7)
            # Write logs after on-demand fetch
            _write_sql_logs_to_file()
            cache_timestamp = now_utc
        
        # Build compact embed with 2 stats per row
        compact_embed = build_compact_leaderboard_embed(
            stats, STAT_CONFIGS, "Last 7 Days", cache_timestamp, COMPACT_VIEW_PLAYERS
        )
        embeds = [compact_embed]
        
        # Create the view with Advanced View button
        view = LeaderboardView()
        
        # Get current timestamp for the update
        unix_timestamp = int(now_utc.timestamp())
        discord_time = f"<t:{unix_timestamp}:F>"
        
        header_content = (
            f"*Last updated: {discord_time}*\n"
            "*Click the button below for advanced filtering and pagination*"
        )
        
        # Try to edit the stored message ID first
        async with _leaderboard_state_lock:
            stored_msg_id = _stored_message_id
            stored_chan_id = _stored_channel_id
        
        if stored_msg_id and stored_chan_id == stats_channel_id:
            try:
                logger.info(f"Attempting to edit stored leaderboard message: {stored_msg_id}")
                stored_message = await channel.fetch_message(stored_msg_id)
                
                if stored_message and stored_message.author == _bot_instance.user:
                    await stored_message.edit(
                        content=header_content,
                        embeds=embeds,
                        view=view
                    )
                    logger.info(f"Successfully edited stored leaderboard message {stored_msg_id}")
                    return
                else:
                    logger.warning(f"Stored message {stored_msg_id} not found or not owned by bot, will create new")
                    
            except discord.NotFound:
                logger.info(f"Stored message {stored_msg_id} not found (may have been deleted), will create new")
            except discord.Forbidden:
                logger.warning(f"No permission to edit stored message {stored_msg_id}, will create new")
            except Exception as e:
                logger.warning(f"Error editing stored message {stored_msg_id}: {e}, will create new")
        
        # Fallback: Try to find and edit the last bot message in channel history
        try:
            logger.info("Looking for existing leaderboard message in channel history...")
            last_message = None
            
            # Look through recent messages to find our leaderboard post
            async for message in channel.history(limit=20):
                if message.author == _bot_instance.user:
                    # Check for new format (embed with Pathfinder title)
                    has_leaderboard_embed = any(
                        embed.title and "Pathfinder Leaderboards" in embed.title
                        for embed in message.embeds
                    )
                    # Check for old format (header content)
                    has_leaderboard_content = (
                        message.content and 
                        "# 🏅 Pathfinder Leaderboards" in message.content
                    )
                    if has_leaderboard_embed or has_leaderboard_content:
                        last_message = message
                        break
            
            if last_message:
                logger.info(f"Found existing leaderboard message: {last_message.id}, editing...")
                await last_message.edit(
                    content=header_content,
                    embeds=embeds,
                    view=view
                )
                # Save the found message ID for future edits
                await _save_leaderboard_state(last_message.id, stats_channel_id)
                logger.info(f"Successfully edited leaderboard message {last_message.id} and saved state")
                return
            else:
                logger.info("No existing leaderboard message found in channel history")
                
        except discord.Forbidden as e:
            logger.error(f"Permission error accessing channel history: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error finding/editing leaderboard message: {e}", exc_info=True)
        
        # Send new message if editing failed
        logger.info("Sending new leaderboard message...")
        new_message = await channel.send(
            content=header_content,
            embeds=embeds,
            view=view
        )
        # Save the new message ID for future edits
        await _save_leaderboard_state(new_message.id, stats_channel_id)
        logger.info(f"Posted new leaderboard message {new_message.id} and saved state")
        
    except Exception as e:
        logger.error(f"Error in post_pathfinder_leaderboards task: {e}", exc_info=True)


def setup_pathfinder_leaderboards_task(bot: discord.Client) -> None:
    """Start the scheduled leaderboards posting and cache refresh tasks."""
    global _bot_instance
    _bot_instance = bot
    
    @refresh_leaderboard_cache.before_loop
    async def before_cache_refresh():
        await bot.wait_until_ready()
        # Populate cache immediately on startup
        logger.info("Performing initial leaderboard cache population...")
        try:
            await refresh_leaderboard_cache.coro()
        except Exception as e:
            logger.error(f"Error in initial cache population: {e}", exc_info=True)
    
    @post_pathfinder_leaderboards.before_loop
    async def before_leaderboards():
        await bot.wait_until_ready()
        # Load persisted message ID state
        await _load_leaderboard_state()
        # Ensure cache is populated before first post
        if not _leaderboard_cache:
            logger.info("Waiting for cache to populate before posting leaderboards...")
            await refresh_leaderboard_cache.coro()
    
    # Start cache refresh task (every 20 min)
    if not refresh_leaderboard_cache.is_running():
        refresh_leaderboard_cache.start()
        logger.info("Started leaderboard cache refresh task (every 20 min)")
    else:
        logger.warning("Leaderboard cache refresh task already running")
    
    # Start posting task (every 30 min)
    if not post_pathfinder_leaderboards.is_running():
        post_pathfinder_leaderboards.start()
        logger.info("Started Pathfinder leaderboards posting task (every 30 min)")
    else:
        logger.warning("Pathfinder leaderboards posting task already running")
