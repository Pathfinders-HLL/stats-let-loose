"""
Player victim subcommand - Get top 25 players you killed the most.
"""

import logging
import time

import discord
from discord import app_commands
from tabulate import tabulate

from apps.discord_stats_bot.common.player_id_cache import get_player_id
from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    validate_over_last_days,
    create_time_filter_params,
    command_wrapper
)

logger = logging.getLogger(__name__)


def register_victim_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the victim subcommand with the player group.
    
    Args:
        player_group: The player command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @player_group.command(name="victim", description="Get top 25 players you killed the most")
    @app_commands.describe(
        player="(Optional) The player ID or player name (optional if you've set one with /profile setid)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @command_wrapper("player victim", channel_check=channel_check)
    async def player_victim(interaction: discord.Interaction, player: str = None, over_last_days: int = 30):
        """Get top 25 players you killed the most."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player victim", command_start_time, success=False, interaction=interaction, kwargs={"player": player, "over_last_days": over_last_days})
            return

        # If player not provided, try to get stored one from cache
        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                return

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                log_command_completion("player victim", command_start_time, success=False, interaction=interaction, kwargs={"player": player, "over_last_days": over_last_days})
                return

            # Calculate time period filter
            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            # Adjust parameter number in time_filter if we have base params
            # Since player_id is $1, time_threshold needs to be $2
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            # Build query to get top 25 victims (players you killed the most)
            # Aggregate kill_count by victim_name across all matches
            query = f"""
                SELECT
                    pv.victim_name,
                    SUM(pv.kill_count) as total_kills,
                    COUNT(DISTINCT pv.match_id) as matches_encountered
                FROM pathfinder_stats.player_victim pv
                INNER JOIN pathfinder_stats.match_history mh
                    ON pv.match_id = mh.match_id
                WHERE pv.player_id = $1
                    {time_filter}
                GROUP BY pv.victim_name
                HAVING SUM(pv.kill_count) > 0
                ORDER BY total_kills DESC
                LIMIT 25
            """

            logger.info(f"Querying top 25 victims for player {player_id}")
            results = await conn.fetch(query, *query_params)

            if not results:
                await interaction.followup.send(
                    f"❌ No victim data found for player `{found_player_name or player}`{time_period_text}."
                )
                log_command_completion("player victim", command_start_time, success=False, interaction=interaction, kwargs={"player": player, "over_last_days": over_last_days})
                return

            # Format results as a table
            display_player_name = found_player_name if found_player_name else player
            
            # Prepare data for table formatting
            table_data = []
            for rank, row in enumerate(results, 1):
                victim_name = row['victim_name']
                total_kills = int(row['total_kills'])
                matches_encountered = int(row['matches_encountered'])

                table_data.append([
                    rank,
                    victim_name[:20] + "..." if len(victim_name) > 20 else victim_name,
                    total_kills,
                    matches_encountered
                ])

            # Headers for the table
            headers = ["#", "Victim", "Kills", "Matches"]
            
            # Build message, removing rows if needed to fit Discord's 2000 character limit
            message_prefix_lines = [f"## Top 25 Victims - {display_player_name}{time_period_text}"]
            message_prefix_lines.append("*Players you killed the most*")
            
            # Try with all rows first
            for num_rows in range(len(table_data), 0, -1):
                table_str = tabulate(
                    table_data[:num_rows],
                    headers=headers,
                    tablefmt="github"
                )
                
                message_lines = message_prefix_lines.copy()
                message_lines.append("```")
                message_lines.append(table_str)
                message_lines.append("```")
                
                if num_rows < len(table_data):
                    message_lines.append(f"\n*Showing {num_rows} of {len(table_data)} victims (message length limit)*")
                
                message = "\n".join(message_lines)
                
                if len(message) <= 2000:
                    break

            await interaction.followup.send(message)
            log_command_completion("player victim", command_start_time, success=True, interaction=interaction, kwargs={"player": player, "over_last_days": over_last_days})
