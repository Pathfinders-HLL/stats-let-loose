"""
Leaderboard contributions subcommand - Get top players by average or sum of scores from all matches.
"""

import logging
import time
from typing import List, Dict, Any

import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
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
)
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
    TIMEFRAME_OPTIONS,
)

logger = logging.getLogger(__name__)


# Score type choices for autocomplete
SCORE_TYPE_CHOICES = [
    app_commands.Choice(name="Support Score", value="support"),
    app_commands.Choice(name="Attack Score", value="attack"),
    app_commands.Choice(name="Defense Score", value="defense"),
    app_commands.Choice(name="Combat Score", value="combat"),
]

# Aggregate by choices for autocomplete
AGGREGATE_BY_CHOICES = [
    app_commands.Choice(name="Average", value="average"),
    app_commands.Choice(name="Sum", value="sum"),
]

# Score type configuration
SCORE_TYPE_CONFIG = {
    "support": {
        "column": "support_score",
        "display_name": "Support Score"
    },
    "attack": {
        "column": "offense_score",
        "display_name": "Attack Score"
    },
    "defense": {
        "column": "defense_score",
        "display_name": "Defense Score"
    },
    "combat": {
        "column": "combat_score",
        "display_name": "Combat Score"
    }
}


async def score_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for score_type parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in SCORE_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


async def aggregate_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for aggregate_by parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in AGGREGATE_BY_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


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
    
    # Determine aggregation function
    is_average = aggregate_by_lower == "average"
    aggregate_func = "AVG" if is_average else "SUM"
    value_column_name = "avg_score" if is_average else "total_score"

    # Calculate time period filter
    time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
        
    # Connect to database and query
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        escaped_column = escape_sql_identifier(score_column)
        
        # Get pathfinder player IDs from file if needed
        pathfinder_ids_list = list(get_pathfinder_player_ids()) if only_pathfinders else []
        
        # Build query components
        param_num = 1
        query_params = []
        
        # Build FROM clause with optional time filter JOIN
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_match_stats", "pms", bool(base_query_params)
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter
        pathfinder_where = ""
        if only_pathfinders:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Add match quality filters for average mode (45+ min, 60+ players)
        quality_match_filters = []
        if is_average:
            quality_match_filters.append("pms.time_played >= 2700")
            quality_match_filters.append("""pms.match_id IN (
                SELECT match_id 
                FROM pathfinder_stats.player_match_stats 
                GROUP BY match_id 
                HAVING COUNT(*) >= 60
            )""")
        
        # Combine WHERE clauses with score column filter and match quality filters
        base_filters = [f"pms.{escaped_column} > 0"] + quality_match_filters
        ranked_matches_where = build_where_clause(
            time_where, pathfinder_where,
            base_filter=" AND ".join(base_filters)
        )
        
        # Build LATERAL join pathfinder filter
        lateral_where = ""
        if only_pathfinders:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tp.player_id", lateral_where)
        
        # Build query to get top players by average or sum of scores from all matches
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
        
        # Convert to list of dicts and format values
        formatted_results = []
        for row in results:
            result_dict = dict(row)
            if is_average and value_column_name in result_dict:
                result_dict[value_column_name] = round(result_dict[value_column_name], 2)
            formatted_results.append(result_dict)
        
        return formatted_results


def register_contributions_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the contributions subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="contributions", description="Get top players by average or sum of scores from all matches")
    @app_commands.describe(
        score_type="The score type to filter by (Support Score, Attack Score, Defense Score, Combat Score)",
        aggregate_by="(Optional) Whether to use average or sum (default: average)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(score_type=score_type_autocomplete)
    @app_commands.autocomplete(aggregate_by=aggregate_by_autocomplete)
    @command_wrapper("leaderboard contributions", channel_check=channel_check)
    async def leaderboard_contributions(interaction: discord.Interaction, score_type: str, aggregate_by: str = "average", only_pathfinders: bool = False):
        """Get top players by average or sum of scores from all matches."""
        command_start_time = time.time()

        # Validate score_type
        try:
            score_type_lower = validate_choice_parameter(
                "score type", score_type, {"support", "attack", "defense", "combat"},
                ["Support Score", "Attack Score", "Defense Score", "Combat Score"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "only_pathfinders": only_pathfinders})
            return
        
        # Validate aggregate_by
        try:
            aggregate_by_lower = validate_choice_parameter(
                "aggregate by", aggregate_by, {"average", "sum"},
                ["Average", "Sum"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "only_pathfinders": only_pathfinders})
            return
        
        config = SCORE_TYPE_CONFIG[score_type_lower]
        display_name = config["display_name"]
        
        # Determine value column and labels
        is_average = aggregate_by_lower == "average"
        aggregate_label = "Average" if is_average else "Sum"
        value_column_name = "avg_score" if is_average else "total_score"
        
        # Default timeframe
        default_timeframe = "30d"
        default_days = TIMEFRAME_OPTIONS[default_timeframe]["days"]
        
        logger.info(f"Querying top players by {aggregate_label.lower()} of {display_name} from all matches")
        
        # Fetch initial data
        results = await fetch_contributions_leaderboard(
            score_type_lower, aggregate_by_lower, only_pathfinders, default_days
        )
    
        if not results:
            await interaction.followup.send(
                f"❌ No data found for `{display_name}` from all matches.",
                ephemeral=True
            )
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "only_pathfinders": only_pathfinders})
            return

        # Create fetch function for timeframe changes
        async def fetch_data(days: int) -> List[Dict[str, Any]]:
            return await fetch_contributions_leaderboard(
                score_type_lower, aggregate_by_lower, only_pathfinders, days
            )
        
        # Format value function
        def format_value(value):
            if is_average:
                # Only show decimals if they're non-zero
                if abs(value - round(value)) < 0.001:
                    return f"{int(round(value)):,}"
                return f"{value:.2f}"
            return f"{int(value):,}"
        
        # Build title
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - {aggregate_label} of {display_name}{filter_text}"
        
        # Send paginated leaderboard using user's format preference
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key=value_column_name,
            value_label=display_name,
            color=discord.Color.from_rgb(16, 74, 0),
            format_value=format_value,
            current_timeframe=default_timeframe,
            fetch_data_func=fetch_data,
            show_timeframe_in_title=True
        )
        log_command_completion("leaderboard contributions", command_start_time, success=True, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "only_pathfinders": only_pathfinders})
