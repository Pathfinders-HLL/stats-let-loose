"""
Leaderboard contributions subcommand - Get top players by average or sum of scores.
"""

import logging
import time

import discord

from typing import Any, Dict, List
from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_choice_parameter,
    create_time_filter_params,
    command_wrapper,
    get_pathfinder_player_ids,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    score_type_autocomplete,
    aggregate_by_autocomplete,
    SCORE_TYPE_CONFIG,
    SCORE_TYPE_VALID_VALUES,
    SCORE_TYPE_DISPLAY_LIST,
    AGGREGATE_BY_VALID_VALUES,
    AGGREGATE_BY_DISPLAY_LIST,
    PATHFINDER_COLOR,
)
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
    TIMEFRAME_OPTIONS,
)

logger = logging.getLogger(__name__)


async def fetch_contributions_leaderboard(
    score_type_lower: str,
    aggregate_by_lower: str,
    only_pathfinders: bool,
    over_last_days: int
) -> List[Dict[str, Any]]:
    """Fetch contributions leaderboard data."""
    config = SCORE_TYPE_CONFIG.get(score_type_lower)
    if not config:
        return []
    
    score_column = config["column"]
    is_average = aggregate_by_lower == "average"
    aggregate_func = "AVG" if is_average else "SUM"
    value_column_name = "avg_score" if is_average else "total_score"

    time_filter, base_query_params, _ = create_time_filter_params(over_last_days)
        
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        escaped_column = escape_sql_identifier(score_column)
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
        
        quality_match_filters = []
        if is_average:
            quality_match_filters.append("pms.time_played >= 2700")
            quality_match_filters.append("""pms.match_id IN (
                SELECT match_id 
                FROM pathfinder_stats.player_match_stats 
                GROUP BY match_id 
                HAVING COUNT(*) >= 60
            )""")
        
        base_filters = [f"pms.{escaped_column} > 0"] + quality_match_filters
        ranked_matches_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(base_filters)
        )
        
        lateral_where = ""
        if only_pathfinders:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=True
            )
            query_params.extend(lateral_params)
        
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        query = f"""
            WITH player_stats AS (
                SELECT 
                    pms.player_id,
                    {aggregate_func}(pms.{escaped_column}) as {value_column_name}
                {from_clause}
                {ranked_matches_where}
                GROUP BY pms.player_id
            ),
            top_players AS (
                SELECT 
                    ps.player_id,
                    ps.{value_column_name}
                FROM player_stats ps
                ORDER BY ps.{value_column_name} DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tp.player_id,
                COALESCE(rn.player_name, tp.player_id) as player_name,
                tp.{value_column_name}
            FROM top_players tp
            {lateral_join}
            ORDER BY tp.{value_column_name} DESC
        """
        
        logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
        results = await conn.fetch(query, *query_params)
        
        formatted_results = []
        for row in results:
            result_dict = dict(row)
            if is_average and value_column_name in result_dict:
                result_dict[value_column_name] = round(result_dict[value_column_name], 2)
            formatted_results.append(result_dict)
        
        return formatted_results


def register_contributions_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """Register the contributions subcommand with the leaderboard group."""
    
    @leaderboard_group.command(
        name="contributions", 
        description="Get top players by average or sum of scores from all matches"
    )
    @app_commands.describe(
        score_type="The score type to filter by",
        aggregate_by="(Optional) Whether to use average or sum (default: average)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(score_type=score_type_autocomplete)
    @app_commands.autocomplete(aggregate_by=aggregate_by_autocomplete)
    @command_wrapper("leaderboard contributions", channel_check=channel_check)
    async def leaderboard_contributions(
        interaction: discord.Interaction, 
        score_type: str, 
        aggregate_by: str = "average", 
        only_pathfinders: bool = False
    ):
        """Get top players by average or sum of scores from all matches."""
        command_start_time = time.time()
        log_kwargs = {"score_type": score_type, "aggregate_by": aggregate_by, "only_pathfinders": only_pathfinders}

        try:
            score_type_lower = validate_choice_parameter(
                "score type", score_type, SCORE_TYPE_VALID_VALUES, SCORE_TYPE_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        try:
            aggregate_by_lower = validate_choice_parameter(
                "aggregate by", aggregate_by, AGGREGATE_BY_VALID_VALUES, AGGREGATE_BY_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        config = SCORE_TYPE_CONFIG[score_type_lower]
        display_name = config["display_name"]
        
        is_average = aggregate_by_lower == "average"
        aggregate_label = "Average" if is_average else "Sum"
        value_column_name = "avg_score" if is_average else "total_score"
        
        default_timeframe = "30d"
        default_days = TIMEFRAME_OPTIONS[default_timeframe]["days"]
        
        logger.info(f"Querying top players by {aggregate_label.lower()} of {display_name}")
        
        results = await fetch_contributions_leaderboard(
            score_type_lower, aggregate_by_lower, only_pathfinders, default_days
        )
    
        if not results:
            await interaction.followup.send(
                f"❌ No data found for `{display_name}` from all matches.",
                ephemeral=True
            )
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        async def fetch_data(days: int) -> List[Dict[str, Any]]:
            return await fetch_contributions_leaderboard(
                score_type_lower, aggregate_by_lower, only_pathfinders, days
            )
        
        def format_value(value):
            if is_average:
                if abs(value - round(value)) < 0.001:
                    return f"{int(round(value)):,}"
                return f"{value:.2f}"
            return f"{int(value):,}"
        
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - {aggregate_label} of {display_name}{filter_text}"
        
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key=value_column_name,
            value_label=display_name,
            color=PATHFINDER_COLOR,
            format_value=format_value,
            current_timeframe=default_timeframe,
            fetch_data_func=fetch_data,
            show_timeframe_in_title=True
        )
        log_command_completion("leaderboard contributions", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
