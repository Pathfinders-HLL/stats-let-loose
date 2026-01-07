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
    format_sql_query_with_params
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
    @leaderboard_group.command(name="performance", description="Get top players by average KDR, KPM, DPM, kill/death streaks (players must have played 60+ minutes)")
    @app_commands.describe(
        stat_type="The stat type to rank by (KDR, KPM, DPM, Kill Streak, or Death Streak)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time). Only includes matches where the player played 60+ minutes.",
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
            if stat_type_lower in {"kill_streak", "death_streak"}:
                aggregate_function = "MAX"
                stat_label = "Highest"
            else:
                aggregate_function = "AVG"
                stat_label = "Average"
            
            # Build HAVING clause - require at least 10 matches for non-streak stats
            # For streak stats, only exclude players with artillery/SPA kills (no minimum match requirement)
            if stat_type_lower in {"kill_streak", "death_streak"}:
                having_clause = ""
            else:
                having_clause = "HAVING COUNT(*) >= 10"
            
            # Get pathfinder player IDs from file if needed
            pathfinder_ids = get_pathfinder_player_ids() if only_pathfinders else set()
            pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
            
            # Build query components conditionally
            param_num = 1
            query_params = []
            
            # Build FROM clause - include JOIN if time filtering is needed
            if base_query_params:
                from_clause = """FROM pathfinder_stats.player_match_stats pms
                        INNER JOIN pathfinder_stats.match_history mh
                            ON pms.match_id = mh.match_id"""
                time_where = f"WHERE mh.start_time >= ${param_num}"
                query_params.extend(base_query_params)
                param_num += len(base_query_params)
            else:
                from_clause = "FROM pathfinder_stats.player_match_stats pms"
                time_where = ""
            
            # Build pathfinder filter WHERE clause
            pathfinder_where = ""
            if only_pathfinders:
                if pathfinder_ids:
                    if time_where:
                        pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    else:
                        pathfinder_where = f"WHERE (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                    param_num += 3  # Increment param_num after adding parameters
                else:
                    if time_where:
                        pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    else:
                        pathfinder_where = f"WHERE (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
                    param_num += 2  # Increment param_num after adding parameters
            
            # Combine WHERE clauses properly - handle the time_played filter
            where_clauses = []
            if time_where:
                where_clauses.append(time_where)
            if pathfinder_where:
                # pathfinder_where already contains WHERE or AND as needed
                # But if we already have a WHERE clause, ensure it starts with AND
                if where_clauses and pathfinder_where.strip().startswith("WHERE"):
                    pathfinder_where = pathfinder_where.replace("WHERE", "AND", 1)
                where_clauses.append(pathfinder_where)
            
            # Add the time_played filter
            time_played_filter = "pms.time_played >= 3600"
            if where_clauses:
                where_clauses.append(f"AND {time_played_filter}")
            else:
                where_clauses.append(f"WHERE {time_played_filter}")
            
            # Add artillery/SPA kills filter per match (for streak stats only)so we filter matches instead of players
            # so a player with a high streak in one match won't be excluded if they have other matchs with high artillery/SPA kill
            if stat_type_lower in {"kill_streak", "death_streak"}:
                artillery_spa_filter = "AND pms.artillery_kills <= 5 AND pms.spa_kills <= 5"
                where_clauses.append(artillery_spa_filter)
            
            player_stats_where = " ".join(where_clauses)
            
            # Build pathfinder filter for LATERAL JOIN (to ensure we get pathfinder names)
            # This ensures the most recent player name also matches pathfinder criteria
            lateral_pathfinder_filter = ""
            lateral_param_num = param_num  # Track parameter numbers for lateral join (after main query params)
            if only_pathfinders:
                if pathfinder_ids:
                    lateral_pathfinder_filter = f"AND (pms.player_name ILIKE ${lateral_param_num} OR pms.player_name ILIKE ${lateral_param_num + 1} OR pms.player_id = ANY(${lateral_param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                    param_num += 3  # Update param_num after adding parameters
                else:
                    lateral_pathfinder_filter = f"AND (pms.player_name ILIKE ${lateral_param_num} OR pms.player_name ILIKE ${lateral_param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
                    param_num += 2  # Update param_num after adding parameters
            
            # Build the query using conditional components
            query = f"""
                    WITH player_stats AS (
                        SELECT 
                            pms.player_id,
                            {aggregate_function}(pms.{escaped_column}) as avg_stat,
                            COUNT(*) as match_count
                        {from_clause}
                        {player_stats_where}
                        GROUP BY pms.player_id
                        {having_clause}
                    ),
                    top_player_stats AS (
                        SELECT 
                            ps.player_id,
                            ps.avg_stat,
                            ps.match_count
                        FROM player_stats ps
                        ORDER BY ps.avg_stat DESC
                        LIMIT 25
                    )
                    SELECT 
                        tps.player_id,
                        COALESCE(rn.player_name, tps.player_id) as player_name,
                        tps.avg_stat,
                        tps.match_count
                    FROM top_player_stats tps
                    LEFT JOIN LATERAL (
                        SELECT pms.player_name
                        FROM pathfinder_stats.player_match_stats pms
                        INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                        WHERE pms.player_id = tps.player_id
                            AND pms.time_played >= 3600
                            {lateral_pathfinder_filter}
                        ORDER BY mh.start_time DESC
                        LIMIT 1
                    ) rn ON TRUE
                    ORDER BY tps.avg_stat DESC
                """
            
            # Build log message
            log_msg = f"Querying top average {stat_type_lower}"
            if base_query_params:
                log_msg += f" for last {over_last_days} days"
            else:
                log_msg += " (All Time)"
            if only_pathfinders:
                if pathfinder_ids:
                    log_msg += f" (Pathfinders only, {len(pathfinder_ids)} IDs from file)"
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
            
            # Format results
            leaderboard_lines = []
            stat_label = "Highest" if stat_type_lower in {"kill_streak", "death_streak"} else "Average"
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            leaderboard_lines.append(f"## Top Players - {stat_label} {display_name}{time_period_text}{filter_text}\n")
            
            for rank, row in enumerate(results, 1):
                # Use player_name if available, otherwise use player_id
                display_player_name = row['player_name'] if row['player_name'] else row['player_id']
                # Format the average stat
                formatted_stat = format_str.format(row['avg_stat'])
                match_count = row['match_count']
                leaderboard_lines.append(
                    f"{rank}. **{display_player_name}** - {formatted_stat} ({match_count} match{'es' if match_count != 1 else ''})"
                )
        
            # Discord message limit is 2000 characters
            message = "\n".join(leaderboard_lines)
            if len(message) > 2000:
                # Truncate if needed
                message = message[:1997] + "..."
            
            await interaction.followup.send(message)
            log_command_completion("leaderboard performance", command_start_time, success=True, interaction=interaction, kwargs={"stat_type": stat_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})
