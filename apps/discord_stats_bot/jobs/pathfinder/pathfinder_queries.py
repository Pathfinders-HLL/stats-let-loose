"""
Database query functions for Pathfinder leaderboard statistics.

Contains all SQL queries for fetching leaderboard data including:
- Infantry kills aggregates
- K/D ratio calculations
- Single match performance records
- Weapon-specific kills (K98)
- Objective efficiency metrics
"""

import logging

from asyncpg import exceptions as asyncpg_exceptions
from typing import List, Dict, Any

from apps.discord_stats_bot.common import (
    get_pathfinder_leaderboard_pool,
    escape_sql_identifier,
    create_time_filter_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    format_sql_query_with_params,
    get_weapon_mapping,
)
from apps.discord_stats_bot.common.constants import (
    MIN_MATCH_DURATION_SECONDS,
    MIN_PLAYERS_PER_MATCH,
    MIN_MATCHES_FOR_AGGREGATE,
    TOP_PLAYERS_LIMIT,
)

logger = logging.getLogger(__name__)

# Import SQL logging function from cache module (will be set by cache module)
_log_sql_query_once = None


def set_sql_logger(log_func):
    """Set the SQL logging function (called by cache module on import)."""
    global _log_sql_query_once
    _log_sql_query_once = log_func


def _log_sql(query_name: str, query: str, query_params: List[Any]) -> None:
    """Log SQL query if logger is available."""
    if _log_sql_query_once:
        _log_sql_query_once(query_name, query, query_params)


async def _fetch_with_timeout_logging(conn, query_name: str, query: str, query_params: List[Any]):
    """Execute a query and log it if the statement times out."""
    try:
        return await conn.fetch(query, *query_params)
    except asyncpg_exceptions.QueryCanceledError:
        formatted_query = format_sql_query_with_params(query, query_params)
        logger.error(f"SQL query timed out [{query_name}]: {formatted_query}", exc_info=True)
        raise


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
        
        _log_sql("infantry_kills", query, query_params)
        
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
        
        _log_sql("average_kd", query, query_params)
        
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
        
        _log_sql("single_match_kills", wrapper_query, query_params)
        
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
        
        _log_sql("single_match_kd", wrapper_query, query_params)
        
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
        
        _log_sql("k98_kills", query, query_params)
        
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
        
        _log_sql("obj_efficiency", query, query_params)
        
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
    pool = await get_pathfinder_leaderboard_pool()
    # Uncomment and use this to get the pathfinder player ids
    #pathfinder_ids = list(get_pathfinder_player_ids())

    # Use empty list to include all players (no Pathfinder-only filter)
    pathfinder_ids: List[str] = []

    stats = {
        "infantry_kills": await _get_most_infantry_kills(pool, days, pathfinder_ids),
        "avg_kd": await _get_average_kd(pool, days, pathfinder_ids),
        "single_match_kills": await _get_most_kills_single_match(pool, days, pathfinder_ids),
        "single_match_kd": await _get_best_kd_single_match(pool, days, pathfinder_ids),
        "k98_kills": await _get_most_k98_kills(pool, days, pathfinder_ids),
        "obj_efficiency": await _get_avg_objective_efficiency(pool, days, pathfinder_ids),
    }
    
    return stats
