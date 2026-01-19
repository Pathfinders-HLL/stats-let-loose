"""
Leaderboard alltime subcommand - Get top players by weapon kills of all time.
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
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
)
from apps.discord_stats_bot.common.weapon_autocomplete import weapon_category_autocomplete, get_weapon_mapping

logger = logging.getLogger(__name__)

# Load weapon mapping at module level
WEAPON_MAPPING = get_weapon_mapping()


def register_alltime_weapons_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the alltime subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="alltime", description="Get top players by weapon kills of all time")
    @app_commands.describe(
        weapon_category="The weapon category (e.g., 'M1 Garand', 'Thompson', 'Sniper')",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(weapon_category=weapon_category_autocomplete)
    @command_wrapper("leaderboard alltime", channel_check=channel_check)
    async def leaderboard_alltime(interaction: discord.Interaction, weapon_category: str, only_pathfinders: bool = False):
        """Get top players by weapon kills of all time."""
        command_start_time = time.time()
        
        # Map friendly name to database column name
        weapon_category_lower = weapon_category.lower().strip()
        column_name = WEAPON_MAPPING.get(weapon_category_lower)
        
        if not column_name:
            # List available weapon categories
            available_categories = sorted(set(WEAPON_MAPPING.keys()))
            await interaction.followup.send(f"❌ Unknown weapon category: `{weapon_category}`. Available categories: {', '.join(sorted(available_categories))}")
            log_command_completion("leaderboard alltime", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
            return
        
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
            
            # Build pathfinder filter for kill_stats CTE
            kill_stats_where = ""
            if only_pathfinders:
                kill_stats_where, pf_params, param_num = build_pathfinder_filter(
                    "pks", param_num, pathfinder_ids_list, use_and=False
                )
                query_params.extend(pf_params)
            
            # Build LATERAL join pathfinder filter
            lateral_where = ""
            if only_pathfinders:
                lateral_where, lateral_params, param_num = build_pathfinder_filter(
                    "pms", param_num, pathfinder_ids_list, use_and=True
                )
                query_params.extend(lateral_params)
            
            # Build LATERAL JOIN for player name lookup
            lateral_join = build_lateral_name_lookup("tks.player_id", lateral_where)
            
            # Build the query
            query = f"""
                WITH kill_stats AS (
                    SELECT 
                        pks.player_id,
                        SUM(pks.{escaped_column}) as total_kills
                    FROM pathfinder_stats.player_kill_stats pks
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
            log_msg = f"Querying all-time top kills for weapon: {weapon_category_lower} (column: {column_name})"
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
                    f"❌ No kills found for `{weapon_category}`."
                )
                log_command_completion("leaderboard alltime", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
                return
            
            # Format results as Discord embed with three column fields
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            embed = discord.Embed(
                title=f"Top Players - {weapon_category} (All Time{filter_text})",
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
            log_command_completion("leaderboard alltime", command_start_time, success=True, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})

