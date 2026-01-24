"""
Leaderboard performance subcommand - Get top players by performance metrics.
"""

import logging
import time
from typing import Any, Dict, List

import discord
from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_choice_parameter,
    create_time_filter_params,
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    stat_type_autocomplete,
    STAT_TYPE_CONFIG,
    STAT_TYPE_VALID_VALUES,
    STAT_TYPE_DISPLAY_LIST,
    PATHFINDER_COLOR,
)
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
    TIMEFRAME_OPTIONS,
)

logger = logging.getLogger(__name__)


async def fetch_performance_leaderboard(
    stat_type_lower: str,
    only_pathfinders: bool,
    over_last_days: int
) -> List[Dict[str, Any]]:
    """Fetch performance leaderboard data."""
    config = STAT_TYPE_CONFIG.get(stat_type_lower)
    if not config:
        return []
    
    column_name = config["column"]
    is_streak_stat = config["is_streak"]
    
    time_filter, base_query_params, _ = create_time_filter_params(over_last_days)
    
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        escaped_column = escape_sql_identifier(column_name)
        
        # For streaks, use MAX instead of AVG
        aggregate_function = "MAX" if is_streak_stat else "AVG"
        having_clause = "" if is_streak_stat else "HAVING COUNT(*) >= 10"
        
        pathfinder_ids_list = list(get_pathfinder_player_ids()) if only_pathfinders else []
        
        param_num = 1
        query_params = []
        
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", bool(base_query_params)
        )
        
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        pathfinder_where = ""
        if only_pathfinders:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        extra_filters = []
        if is_streak_stat:
            extra_filters.append("pms.artillery_kills <= 5 AND pms.spa_kills <= 5")
        else:
            extra_filters.append("pms.time_played >= 2700")
        
        extra_filters.append("""pms.match_id IN (
            SELECT match_id 
            FROM pathfinder_stats.player_match_stats 
            GROUP BY match_id 
            HAVING COUNT(*) >= 60
        )""")
        
        player_stats_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(extra_filters) if extra_filters else ""
        )
        
        lateral_extra_where = "" if is_streak_stat else "AND pms.time_played >= 2700"
        lateral_pathfinder_filter = ""
        if only_pathfinders:
            lateral_pathfinder_filter, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=True
            )
            query_params.extend(lateral_params)
        
        lateral_join = build_lateral_name_lookup(
            "tps.player_id",
            f"{lateral_extra_where} {lateral_pathfinder_filter}".strip()
        )
        
        query = f"""
            WITH player_stats AS (
                SELECT 
                    pms.player_id,
                    {aggregate_function}(pms.{escaped_column}) as avg_stat
                {from_clause}
                {player_stats_where}
                GROUP BY pms.player_id
                {having_clause}
            ),
            top_player_stats AS (
                SELECT 
                    ps.player_id,
                    ps.avg_stat
                FROM player_stats ps
                ORDER BY ps.avg_stat DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tps.player_id,
                COALESCE(rn.player_name, tps.player_id) as player_name,
                tps.avg_stat
            FROM top_player_stats tps
            {lateral_join}
            ORDER BY tps.avg_stat DESC
        """
        
        logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
        results = await conn.fetch(query, *query_params)
        
        return [dict(row) for row in results]


def register_performance_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """Register the performance subcommand."""
    
    @leaderboard_group.command(
        name="performance", 
        description="Get top players by average KDR, KPM, DPM, kill/death streaks"
    )
    @app_commands.describe(
        stat_type="The stat type to rank by",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(stat_type=stat_type_autocomplete)
    @command_wrapper("leaderboard performance", channel_check=channel_check)
    async def leaderboard_performance(
        interaction: discord.Interaction, 
        stat_type: str, 
        only_pathfinders: bool = False
    ):
        """Get top players by performance metrics."""
        command_start_time = time.time()
        log_kwargs = {"stat_type": stat_type, "only_pathfinders": only_pathfinders}
        
        try:
            stat_type_lower = validate_choice_parameter(
                "stat type", stat_type, STAT_TYPE_VALID_VALUES, STAT_TYPE_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("leaderboard performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        config = STAT_TYPE_CONFIG[stat_type_lower]
        display_name = config["display_name"]
        format_str = config["format"]
        is_streak_stat = config["is_streak"]
        
        default_timeframe = "30d"
        default_days = TIMEFRAME_OPTIONS[default_timeframe]["days"]
        
        logger.info(f"Querying top average {stat_type_lower}")
        
        results = await fetch_performance_leaderboard(
            stat_type_lower, only_pathfinders, default_days
        )
        
        if not results:
            await interaction.followup.send(
                f"❌ No data found for `{display_name}`.",
                ephemeral=True
            )
            log_command_completion("leaderboard performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        async def fetch_data(days: int) -> List[Dict[str, Any]]:
            return await fetch_performance_leaderboard(
                stat_type_lower, only_pathfinders, days
            )
        
        def format_value(value):
            if is_streak_stat:
                return format_str.format(value)
            if abs(value - round(value)) < 0.001:
                return f"{int(round(value))}"
            return format_str.format(value)
        
        stat_label = "Highest" if is_streak_stat else "Average"
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - {stat_label} {display_name}{filter_text}"
        
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key="avg_stat",
            value_label=display_name,
            color=PATHFINDER_COLOR,
            format_value=format_value,
            current_timeframe=default_timeframe,
            fetch_data_func=fetch_data,
            show_timeframe_in_title=True
        )
        log_command_completion("leaderboard performance", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
