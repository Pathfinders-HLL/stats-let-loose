"""
Leaderboard subcommand for top players by performance metrics (KDR, KPM, DPM, streaks).
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
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
)

logger = logging.getLogger(__name__)


STAT_TYPE_CHOICES = [
    app_commands.Choice(name="KDR (Kill/Death Ratio)", value="kdr"),
    app_commands.Choice(name="KPM (Kills per Minute)", value="kpm"),
    app_commands.Choice(name="DPM (Deaths per Minute)", value="dpm"),
    app_commands.Choice(name="Kill Streak", value="kill_streak"),
    app_commands.Choice(name="Death Streak", value="death_streak"),
]


async def stat_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    current_lower = current.lower()
    matching = [
        choice for choice in STAT_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_performance_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """Register the performance subcommand."""
    @leaderboard_group.command(name="performance", description="Get top players by average KDR, KPM, DPM, kill/death streaks (players must have played 45+ minutes)")
    @app_commands.describe(
        stat_type="The stat type to rank by (KDR, KPM, DPM, Kill Streak, or Death Streak)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time). Only includes matches where the player played 45+ minutes.",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(stat_type=stat_type_autocomplete)
    @command_wrapper("leaderboard performance", channel_check=channel_check)
    async def leaderboard_performance(interaction: discord.Interaction, stat_type: str, over_last_days: int = 30, only_pathfinders: bool = False):
        command_start_time = time.time()
        
        # Validate stat_type
        try:
            stat_type_lower = validate_choice_parameter(
                "stat type", stat_type, {"kdr", "kpm", "dpm", "kill_streak", "death_streak"},
                ["KDR", "KPM", "DPM", "Kill Streak", "Death Streak"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
        
        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
            return
        
        # Map stat type to column name and display name
        stat_config = {
            "kdr": {
                "column": "kill_death_ratio",
                "display_name": "KDR (Kill/Death Ratio)",
                "format": "{:.2f}"
            },
            "kpm": {
                "column": "kills_per_minute",
                "display_name": "KPM (Kills per Minute)",
                "format": "{:.2f}"
            },
            "dpm": {
                "column": "deaths_per_minute",
                "display_name": "DPM (Deaths per Minute)",
                "format": "{:.2f}"
            },
            "kill_streak": {
                "column": "kill_streak",
                "display_name": "Kill Streak",
                "format": "{:.0f}"
            },
            "death_streak": {
                "column": "death_streak",
                "display_name": "Death Streak",
                "format": "{:.0f}"
            }
        }
        
        config = stat_config[stat_type_lower]
        column_name = config["column"]
        display_name = config["display_name"]
        format_str = config["format"]
        
        # Calculate time period filter
        time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
        
        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Build query to get top players by average stat (or MAX for streaks)
            escaped_column = escape_sql_identifier(column_name)
            
            # For streaks, use MAX instead of AVG (show highest streak achieved)
            # For other stats, use AVG (show average performance)
            is_streak_stat = stat_type_lower in {"kill_streak", "death_streak"}
            aggregate_function = "MAX" if is_streak_stat else "AVG"
            
            # Build HAVING clause - require at least 10 matches for non-streak stats
            having_clause = "" if is_streak_stat else "HAVING COUNT(*) >= 10"
            
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
            
            # Build extra filters based on stat type
            extra_filters = []
            if is_streak_stat:
                # For streak stats, filter out artillery/SPA heavy matches
                extra_filters.append("pms.artillery_kills <= 5 AND pms.spa_kills <= 5")
            else:
                # For non-streak stats, require minimum time played
                extra_filters.append("pms.time_played >= 2700")
            
            # Combine WHERE clauses
            player_stats_where = build_where_clause(
                time_where, pathfinder_where,
                base_filter=" AND ".join(extra_filters) if extra_filters else ""
            )
            
            # Build LATERAL join filters
            lateral_extra_where = "" if is_streak_stat else "AND pms.time_played >= 2700"
            lateral_pathfinder_filter = ""
            if only_pathfinders:
                lateral_pathfinder_filter, lateral_params, param_num = build_pathfinder_filter(
                    "pms", param_num, pathfinder_ids_list, use_and=True
                )
                query_params.extend(lateral_params)
            
            # Build LATERAL JOIN for player name lookup
            lateral_join = build_lateral_name_lookup(
                "tps.player_id",
                f"{lateral_extra_where} {lateral_pathfinder_filter}".strip()
            )
            
            # Build the query
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
                        LIMIT 25
                    )
                    SELECT 
                        tps.player_id,
                        COALESCE(rn.player_name, tps.player_id) as player_name,
                        tps.avg_stat
                    FROM top_player_stats tps
                    {lateral_join}
                    ORDER BY tps.avg_stat DESC
                """
            
            # Build log message
            log_msg = f"Querying top average {stat_type_lower}"
            if base_query_params:
                log_msg += f" for last {over_last_days} days"
            else:
                log_msg += " (All Time)"
            if only_pathfinders:
                if pathfinder_ids_list:
                    log_msg += f" (Pathfinders only, {len(pathfinder_ids_list)} IDs from file)"
                else:
                    log_msg += " (Pathfinders only)"
            logger.info(log_msg)
            
            # Log SQL query with parameters substituted
            logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
            
            results = await conn.fetch(query, *query_params)
        
            if not results:
                await interaction.followup.send(
                    f"❌ No data found for `{display_name}`{time_period_text}."
                )
                log_command_completion("leaderboard performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
                return
            
            # Format results as Discord embed with inline fields
            stat_label = "Highest" if stat_type_lower in {"kill_streak", "death_streak"} else "Average"
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            embed = discord.Embed(
                title=f"Top Players - {stat_label} {display_name}{time_period_text}{filter_text}",
                color=discord.Color.blue()
            )
            
            for rank, row in enumerate(results, 1):
                # Use player_name if available, otherwise use player_id
                display_player_name = row['player_name'] if row['player_name'] else row['player_id']
                # Format the average stat
                formatted_stat = format_str.format(row['avg_stat'])
                embed.add_field(
                    name=f"#{rank} {display_player_name}",
                    value=formatted_stat,
                    inline=True
                )
        
            await interaction.followup.send(embed=embed)
            log_command_completion("leaderboard performance", command_start_time, success=True, interaction=interaction, kwargs={"stat_type": stat_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
