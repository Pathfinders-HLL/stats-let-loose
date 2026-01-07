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
    format_sql_query_with_params
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
            pathfinder_ids = get_pathfinder_player_ids() if only_pathfinders else set()
            pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
            
            # Build WHERE clause for kill_stats CTE
            param_num = 1
            kill_stats_where = ""
            query_params = []
            
            if only_pathfinders:
                if pathfinder_ids:
                    kill_stats_where = f"WHERE (pks.player_name ILIKE ${param_num} OR pks.player_name ILIKE ${param_num + 1} OR pks.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                    param_num += 3
                else:
                    kill_stats_where = f"WHERE (pks.player_name ILIKE ${param_num} OR pks.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
                    param_num += 2
            
            # Build WHERE clause for LATERAL join
            lateral_where = ""
            if only_pathfinders:
                if pathfinder_ids:
                    lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                else:
                    lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
            
            # Build the query using conditional WHERE clauses
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
                LEFT JOIN LATERAL (
                    SELECT pms.player_name
                    FROM pathfinder_stats.player_match_stats pms
                    INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                    WHERE pms.player_id = tks.player_id
                        {lateral_where}
                    ORDER BY mh.start_time DESC
                    LIMIT 1
                ) rn ON TRUE
                ORDER BY tks.total_kills DESC
            """
            
            # Build log message
            log_msg = f"Querying all-time top kills for weapon: {weapon_category_lower} (column: {column_name})"
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
                    f"❌ No kills found for `{weapon_category}`."
                )
                log_command_completion("leaderboard alltime", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
                return
            
            # Format results
            leaderboard_lines = []
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            leaderboard_lines.append(f"## Top Players - {weapon_category} (All Time{filter_text})\n")
            
            for rank, row in enumerate(results, 1):
                # Use player_name if available, otherwise use player_id
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                leaderboard_lines.append(f"{rank}. **{display_name}** - {row['total_kills']:,} kills")
        
            # Discord message limit is 2000 characters
            message = "\n".join(leaderboard_lines)
            if len(message) > 2000:
                # Truncate if needed
                message = message[:1997] + "..."
            
            await interaction.followup.send(message)
            log_command_completion("leaderboard alltime", command_start_time, success=True, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})

