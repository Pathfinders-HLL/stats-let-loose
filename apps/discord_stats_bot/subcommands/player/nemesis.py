"""
Player nemesis subcommand - Get top 25 players who killed you the most.
"""

import logging
import time

import discord
from discord import app_commands
from tabulate import tabulate

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    validate_over_last_days,
    create_time_filter_params,
    command_wrapper,
    get_player_id,
)

logger = logging.getLogger(__name__)


def register_nemesis_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the nemesis subcommand with the player group."""
    
    @player_group.command(
        name="nemesis", 
        description="Get top 25 players who killed you the most"
    )
    @app_commands.describe(
        player="(Optional) The player ID or player name",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @command_wrapper("player nemesis", channel_check=channel_check)
    async def player_nemesis(
        interaction: discord.Interaction, 
        player: str = None, 
        over_last_days: int = 30
    ):
        """Get top 25 players who killed you the most."""
        command_start_time = time.time()
        log_kwargs = {"player": player, "over_last_days": over_last_days}

        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player nemesis", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send(
                    "❌ No player ID provided and you haven't set one! "
                    "Either provide a player ID/name, or use `/profile setid` to set a default.", 
                    ephemeral=True
                )
                return

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(
                    f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.",
                    ephemeral=True
                )
                log_command_completion("player nemesis", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return

            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            query = f"""
                SELECT
                    pn.nemesis_name,
                    SUM(pn.death_count) as total_deaths,
                    COUNT(DISTINCT pn.match_id) as matches_encountered
                FROM pathfinder_stats.player_nemesis pn
                INNER JOIN pathfinder_stats.match_history mh
                    ON pn.match_id = mh.match_id
                WHERE pn.player_id = $1
                    {time_filter}
                GROUP BY pn.nemesis_name
                HAVING SUM(pn.death_count) > 0
                ORDER BY total_deaths DESC
                LIMIT 25
            """

            logger.info(f"Querying top 25 nemeses for player {player_id}")
            results = await conn.fetch(query, *query_params)

            if not results:
                await interaction.followup.send(
                    f"❌ No nemesis data found for player `{found_player_name or player}`{time_period_text}.",
                    ephemeral=True
                )
                log_command_completion("player nemesis", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return

            display_player_name = found_player_name if found_player_name else player
            
            table_data = []
            for rank, row in enumerate(results, 1):
                nemesis_name = row['nemesis_name']
                total_deaths = int(row['total_deaths'])
                matches_encountered = int(row['matches_encountered'])

                table_data.append([
                    rank,
                    nemesis_name[:20] + "..." if len(nemesis_name) > 20 else nemesis_name,
                    total_deaths,
                    matches_encountered
                ])

            headers = ["#", "Nemesis", "Deaths", "Matches"]
            
            message_prefix_lines = [
                f"## Top 25 Nemeses - {display_player_name}{time_period_text}",
                "*Players who killed you the most*"
            ]
            
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
                    message_lines.append(f"\n*Showing {num_rows} of {len(table_data)} nemeses (message length limit)*")
                
                message = "\n".join(message_lines)
                
                if len(message) <= 2000:
                    break

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player nemesis", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
