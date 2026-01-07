"""
Leaderboard 100killgames subcommand - Get top players with the most 100+ kill games.
"""

import logging
import time

import asyncpg
import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    log_command_completion,
    get_pathfinder_player_ids,
    command_wrapper
)

logger = logging.getLogger(__name__)


def register_100killgames_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the 100killgames subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="100killgames", description="Get top players with the most 100+ kill games")
    @app_commands.describe(
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @command_wrapper("leaderboard 100killgames", channel_check=channel_check)
    async def leaderboard_100killgames(interaction: discord.Interaction, only_pathfinders: bool = False):
        """Get top players with the most 100+ kill games."""
        command_start_time = time.time()
        
        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Get pathfinder player IDs from file if needed
            pathfinder_ids = get_pathfinder_player_ids() if only_pathfinders else set()
            pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
            
            # Build WHERE clause conditionally
            param_num = 1
            query_params = []
            pathfinder_where = ""
            
            if only_pathfinders:
                if pathfinder_ids:
                    pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                else:
                    pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
            
            # Build query to find players with most 100+ kill games
            query = f"""
                WITH hundred_kill_games AS (
                    SELECT 
                        pms.player_id,
                        COUNT(*) as game_count
                    FROM pathfinder_stats.player_match_stats pms
                    WHERE pms.total_kills >= 100
                        {pathfinder_where}
                    GROUP BY pms.player_id
                ),
                top_hundred_kill_games AS (
                    SELECT 
                        hkg.player_id,
                        hkg.game_count
                    FROM hundred_kill_games hkg
                    ORDER BY hkg.game_count DESC
                    LIMIT 25
                )
                SELECT 
                    thkg.player_id,
                    COALESCE(rn.player_name, thkg.player_id) as player_name,
                    thkg.game_count
                FROM top_hundred_kill_games thkg
                LEFT JOIN LATERAL (
                    SELECT pms.player_name
                    FROM pathfinder_stats.player_match_stats pms
                    INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                    WHERE pms.player_id = thkg.player_id
                    ORDER BY mh.start_time DESC
                    LIMIT 1
                ) rn ON TRUE
                ORDER BY thkg.game_count DESC
            """
            
            logger.info(f"Querying top players with most 100+ kill games{' (Pathfinders only)' if only_pathfinders else ''}")
            results = await conn.fetch(query, *query_params)
        
            if not results:
                await interaction.followup.send(
                    f"❌ No players found with 100+ kill games."
                )
                log_command_completion("leaderboard 100killgames", command_start_time, success=False, interaction=interaction, kwargs={"only_pathfinders": only_pathfinders})
                return
            
            # Format results
            leaderboard_lines = []
            filter_text = " (Pathfinders Only)" if only_pathfinders else ""
            leaderboard_lines.append(f"## Top Players - Most 100+ Kill Games{filter_text}\n")
            
            for rank, row in enumerate(results, 1):
                # Use player_name if available, otherwise use player_id
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                game_count = row['game_count']
                leaderboard_lines.append(f"{rank}. **{display_name}** - {game_count:,} game{'s' if game_count != 1 else ''}")
        
            # Discord message limit is 2000 characters
            message = "\n".join(leaderboard_lines)
            if len(message) > 2000:
                # Truncate if needed
                message = message[:1997] + "..."
            
            await interaction.followup.send(message)
            log_command_completion("leaderboard 100killgames", command_start_time, success=True, interaction=interaction, kwargs={"only_pathfinders": only_pathfinders})

