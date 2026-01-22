"""
Leaderboard contributions subcommand - Get top players by average or sum of scores from all matches.
"""

import logging
import time
from typing import List

import asyncpg
import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
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
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(score_type=score_type_autocomplete)
    @app_commands.autocomplete(aggregate_by=aggregate_by_autocomplete)
    @command_wrapper("leaderboard contributions", channel_check=channel_check)
    async def leaderboard_contributions(interaction: discord.Interaction, score_type: str, aggregate_by: str = "average", over_last_days: int = 30, only_pathfinders: bool = False):
        """Get top players by average or sum of scores from all matches."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return

        # Validate score_type
        try:
            score_type_lower = validate_choice_parameter(
                "score type", score_type, {"support", "attack", "defense", "combat"},
                ["Support Score", "Attack Score", "Defense Score", "Combat Score"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
        
        # Validate aggregate_by
        try:
            aggregate_by_lower = validate_choice_parameter(
                "aggregate by", aggregate_by, {"average", "sum"},
                ["Average", "Sum"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
        
        # Map score type to column name and display name
        score_type_config = {
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
        
        config = score_type_config[score_type_lower]
        score_column = config["column"]
        display_name = config["display_name"]
        
        # Determine aggregation function and labels
        is_average = aggregate_by_lower == "average"
        aggregate_func = "AVG" if is_average else "SUM"
        aggregate_label = "Average" if is_average else "Sum"
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
                    LIMIT 25
                )
                SELECT 
                    tp.player_id,
                    COALESCE(rn.player_name, tp.player_id) as player_name,
                    tp.{value_column_name}
                FROM top_players tp
                {lateral_join}
                ORDER BY tp.{value_column_name} DESC
            """
            
            logger.info(f"Querying top players by {aggregate_label.lower()} of {display_name} from all matches{time_period_text}")
            
            # Log SQL query with parameters substituted
            logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
            
            results = await conn.fetch(query, *query_params)
    
        if not results:
            await interaction.followup.send(
                f"❌ No data found for `{display_name}` from all matches{time_period_text}."
            )
            log_command_completion("leaderboard contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
                
        # Format results as Discord embed with three column fields
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        embed = discord.Embed(
            title=f"Top Players - {aggregate_label} of {display_name} from All Matches{time_period_text}{filter_text}",
            color=discord.Color.from_rgb(16, 74, 0)
        )
        
        # Build three columns: rank, player name, score
        rank_values = []
        player_values = []
        score_values = []
        
        for rank, row in enumerate(results, 1):
            # Use player_name if available, otherwise use player_id
            display_player_name = row['player_name'] if row['player_name'] else row['player_id']
            score_value = row[value_column_name]
            rank_values.append(f"#{rank}")
            player_values.append(display_player_name)
            if is_average:
                score_values.append(f"{score_value:.2f}")
            else:
                score_values.append(f"{score_value:,}")
        
        # Add the three columns as inline fields (side-by-side)
        embed.add_field(
            name="Rank",
            value="\n".join(rank_values),
            inline=True
        )
        embed.add_field(
            name="Player",
            value="\n".join(player_values),
            inline=True
        )
        embed.add_field(
            name=display_name,
            value="\n".join(score_values),
            inline=True
        )
        
        await interaction.followup.send(embed=embed)
        log_command_completion("leaderboard contributions", command_start_time, success=True, interaction=interaction, kwargs={"score_type": score_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
