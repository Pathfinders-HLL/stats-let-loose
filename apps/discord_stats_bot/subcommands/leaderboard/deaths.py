"""
Leaderboard topdeaths subcommand - Get top players by average or sum of deaths from all matches (with death type filtering).
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


# Death type choices for autocomplete
DEATH_TYPE_CHOICES = [
    app_commands.Choice(name="All Deaths", value="all"),
    app_commands.Choice(name="Infantry Deaths", value="infantry"),
    app_commands.Choice(name="Armor Deaths", value="armor"),
    app_commands.Choice(name="Artillery Deaths", value="artillery"),
]

# Aggregate by choices for autocomplete
AGGREGATE_BY_CHOICES = [
    app_commands.Choice(name="Average", value="average"),
    app_commands.Choice(name="Sum", value="sum"),
]


async def death_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for death_type parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in DEATH_TYPE_CHOICES
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


def register_deaths_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the topdeaths subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="deaths", description="Get top players by average or sum of deaths from all matches")
    @app_commands.describe(
        death_type="(Optional) The death type to filter by (All Deaths, Infantry Deaths, Armor Deaths, Artillery Deaths)",
        aggregate_by="(Optional) Whether to use average or sum (default: average)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(death_type=death_type_autocomplete)
    @app_commands.autocomplete(aggregate_by=aggregate_by_autocomplete)
    @command_wrapper("leaderboard deaths", channel_check=channel_check)
    async def leaderboard_topdeaths(interaction: discord.Interaction, death_type: str = "all", aggregate_by: str = "average", over_last_days: int = 30, only_pathfinders: bool = False):
        """Get top players by average or sum of deaths from all matches."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard deaths", command_start_time, success=False, interaction=interaction, kwargs={"death_type": death_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return

        # Validate death_type
        try:
            death_type_lower = validate_choice_parameter(
                "death type", death_type, {"all", "infantry", "armor", "artillery"},
                ["All Deaths", "Infantry Deaths", "Armor Deaths", "Artillery Deaths"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard deaths", command_start_time, success=False, interaction=interaction, kwargs={"death_type": death_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
        
        # Validate aggregate_by
        try:
            aggregate_by_lower = validate_choice_parameter(
                "aggregate by", aggregate_by, {"average", "sum"},
                ["Average", "Sum"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard deaths", command_start_time, success=False, interaction=interaction, kwargs={"death_type": death_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
            
        # Map death type to column name and display name
        death_type_config = {
            "all": {
                "column": "total_deaths",
                "display_name": "All Deaths"
            },
            "infantry": {
                "column": "infantry_deaths",
                "display_name": "Infantry Deaths"
            },
            "armor": {
                "column": "armor_deaths",
                "display_name": "Armor Deaths"
            },
            "artillery": {
                "column": "artillery_deaths",
                "display_name": "Artillery Deaths"
            }
        }

        config = death_type_config[death_type_lower]
        death_column = config["column"]
        display_name = config["display_name"]
        
        # Determine aggregation function and labels
        is_average = aggregate_by_lower == "average"
        aggregate_func = "AVG" if is_average else "SUM"
        aggregate_label = "Average" if is_average else "Sum"
        value_column_name = "avg_deaths" if is_average else "total_deaths"

        # Calculate time period filter
        time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            escaped_column = escape_sql_identifier(death_column)
            
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
            
            # Combine WHERE clauses with death column filter and match quality filters
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
            
            # Build query to get top players by average or sum of deaths from all matches
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
            log_command_completion("leaderboard deaths", command_start_time, success=False, interaction=interaction, kwargs={"death_type": death_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return

        # Format results as Discord embed with three column fields
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        embed = discord.Embed(
            title=f"Top Players - {aggregate_label} of {display_name} from All Matches{time_period_text}{filter_text}",
            color=discord.Color.from_rgb(16, 74, 0)
        )

        # Build three columns: rank, player name, deaths
        rank_values = []
        player_values = []
        deaths_values = []

        for rank, row in enumerate(results, 1):
            # Use player_name if available, otherwise use player_id
            display_player_name = row['player_name'] if row['player_name'] else row['player_id']
            death_value = row[value_column_name]
            rank_values.append(f"#{rank}")
            player_values.append(display_player_name)
            if is_average:
                deaths_values.append(f"{death_value:.2f}")
            else:
                deaths_values.append(f"{death_value:,}")

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
            value="\n".join(deaths_values),
            inline=True
        )

        await interaction.followup.send(embed=embed)
        log_command_completion("leaderboard deaths", command_start_time, success=True, interaction=interaction, kwargs={"death_type": death_type, "aggregate_by": aggregate_by, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})

