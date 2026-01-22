"""
Leaderboard 100killgames subcommand - Get top players with the most 100+ kill games.
"""

import logging
import time
from typing import List, Dict, Any

import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    log_command_completion,
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params
)
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
)

logger = logging.getLogger(__name__)


async def fetch_100killgames_leaderboard(
    only_pathfinders: bool
) -> List[Dict[str, Any]]:
    """Fetch 100+ kill games leaderboard data."""
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
                param_num += 3
            else:
                pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                query_params.extend(["PFr |%", "PF |%"])
                param_num += 2
        
        # Build LATERAL join pathfinder filter (for getting player names)
        lateral_where = ""
        if only_pathfinders:
            if pathfinder_ids:
                lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
            else:
                lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
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
                LIMIT {TOP_PLAYERS_LIMIT}
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
                    {lateral_where}
                ORDER BY mh.start_time DESC
                LIMIT 1
            ) rn ON TRUE
            ORDER BY thkg.game_count DESC
        """
        
        logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
        
        results = await conn.fetch(query, *query_params)
        
        # Convert to list of dicts
        return [dict(row) for row in results]


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
        
        logger.info(f"Querying top players with most 100+ kill games{' (Pathfinders only)' if only_pathfinders else ''}")
        
        # Fetch data
        results = await fetch_100killgames_leaderboard(only_pathfinders)
        
        if not results:
            await interaction.followup.send(
                f"❌ No players found with 100+ kill games.",
                ephemeral=True
            )
            log_command_completion("leaderboard 100killgames", command_start_time, success=False, interaction=interaction, kwargs={"only_pathfinders": only_pathfinders})
            return
        
        # Format value function
        def format_value(value):
            return f"{int(value):,}"
        
        # Build title
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - Most 100+ Kill Games{filter_text}"
        
        # Send paginated leaderboard using user's format preference
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key="game_count",
            value_label="Games",
            color=discord.Color.from_rgb(16, 74, 0),
            format_value=format_value,
            current_timeframe="all",
            fetch_data_func=None,  # No timeframe changes for this command
            show_timeframe_in_title=False
        )
        log_command_completion("leaderboard 100killgames", command_start_time, success=True, interaction=interaction, kwargs={"only_pathfinders": only_pathfinders})
