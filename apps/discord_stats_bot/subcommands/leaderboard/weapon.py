"""
Leaderboard weapon subcommand - Get top players by weapon kills in the last 30 days.
"""

import logging
import time

import asyncpg
import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    create_time_filter_params,
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
)
from apps.discord_stats_bot.common.weapon_autocomplete import weapon_category_autocomplete, get_weapon_mapping

logger = logging.getLogger(__name__)

# Load weapon mapping at module level
WEAPON_MAPPING = get_weapon_mapping()


def register_weapon_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the weapon subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="weapon", description="Get top players by weapon kills over a time period")
    @app_commands.describe(
        weapon_category="The weapon category (e.g., 'M1 Garand', 'Thompson', 'Sniper')",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(weapon_category=weapon_category_autocomplete)
    @command_wrapper("leaderboard weapon", channel_check=channel_check)
    async def leaderboard_weapon(interaction: discord.Interaction, weapon_category: str, only_pathfinders: bool = False, over_last_days: int = 30):
        """Get top players by weapon kills over a time period."""
        command_start_time = time.time()
        
        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders, "over_last_days": over_last_days})
            return
        
        # Map friendly name to database column name
        weapon_category_lower = weapon_category.lower().strip()
        column_name = WEAPON_MAPPING.get(weapon_category_lower)
        
        if not column_name:
            # List available weapon categories
            available_categories = sorted(set(WEAPON_MAPPING.keys()))
            await interaction.followup.send(f"❌ Unknown weapon category: `{weapon_category}`. Available categories: {', '.join(sorted(available_categories))}")
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders, "over_last_days": over_last_days})
            return
        
        # Calculate time period filter
        time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
        
        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Build query with safe identifier escaping
            escaped_column = escape_sql_identifier(column_name)
            
            # Get pathfinder player IDs from file if needed
            pathfinder_ids_list = list(get_pathfinder_player_ids()) if only_pathfinders else []
            
            # Build query components
            param_num = 1
            query_params = []
            
            # Build FROM clause with optional time filter JOIN
            from_clause, _ = build_from_clause_with_time_filter(
                "pathfinder_stats.player_kill_stats", "pks", bool(base_query_params)
            )
            
            # Build time filter WHERE clause
            time_where = ""
            if base_query_params:
                time_where = f"WHERE mh.start_time >= ${param_num}"
                query_params.extend(base_query_params)
                param_num += len(base_query_params)
            
            # Build pathfinder filter (uses pks alias for player_kill_stats)
            pathfinder_where = ""
            if only_pathfinders:
                pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                    "pks", param_num, pathfinder_ids_list, use_and=bool(time_where)
                )
                query_params.extend(pf_params)
            
            # Combine WHERE clauses
            kill_stats_where = build_where_clause(time_where, pathfinder_where)
            
            # Build LATERAL join pathfinder filter (uses pms alias for player_match_stats)
            lateral_where = ""
            if only_pathfinders:
                lateral_where, lateral_params, param_num = build_pathfinder_filter(
                    "pms", param_num, pathfinder_ids_list, use_and=True
                )
                query_params.extend(lateral_params)
            
            # Build LATERAL JOIN for player name lookup
            lateral_join = build_lateral_name_lookup("tks.player_id", lateral_where)
            
            # Build the query using conditional components
            query = f"""
                WITH kill_stats AS (
                    SELECT 
                        pks.player_id,
                        SUM(pks.{escaped_column}) as total_kills
                    {from_clause}
                    {kill_stats_where}
                    GROUP BY pks.player_id
                    HAVING SUM(pks.{escaped_column}) > 0
                ),
                top_kill_stats AS (
                    SELECT 
                        ks.player_id,
                        ks.total_kills
                    FROM kill_stats ks
                    ORDER BY ks.total_kills DESC
                    LIMIT 25
                )
                SELECT 
                    tks.player_id,
                    COALESCE(rn.player_name, tks.player_id) as player_name,
                    tks.total_kills
                FROM top_kill_stats tks
                {lateral_join}
                ORDER BY tks.total_kills DESC
            """
            
            # Build log message
            log_msg = f"Querying top kills for weapon: {weapon_category_lower} (column: {column_name})"
            if base_query_params:
                log_msg += f" in last {over_last_days} days"
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
                    f"❌ No kills found for `{weapon_category}`{time_period_text}."
                )
                log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders, "over_last_days": over_last_days})
                return
            
            # Format results as Discord embed with three column fields
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            embed = discord.Embed(
                title=f"Top Players - {weapon_category}{time_period_text}{filter_text}",
                color=discord.Color.blue()
            )
            
            # Build three columns: rank, player name, kills
            rank_values = []
            player_values = []
            kills_values = []
            
            for rank, row in enumerate(results, 1):
                # Use player_name if available, otherwise use player_id
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                rank_values.append(f"#{rank}")
                player_values.append(display_name)
                kills_values.append(f"{row['total_kills']:,}")
            
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
                name="Kills",
                value="\n".join(kills_values),
                inline=True
            )
        
            await interaction.followup.send(embed=embed)
            log_command_completion("leaderboard weapon", command_start_time, success=True, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders, "over_last_days": over_last_days})
