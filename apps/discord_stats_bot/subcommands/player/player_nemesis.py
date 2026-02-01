"""
Player nemesis subcommand - Get top 25 players who killed you the most.
"""

import logging
import time

import discord

from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    validate_over_last_days,
    build_player_time_query_params,
    command_wrapper,
    build_table_message,
    lookup_player,
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

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_result, error = await lookup_player(conn, interaction.user.id, player)
            if error:
                await interaction.followup.send(error, ephemeral=True)
                log_command_completion("player nemesis", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            player_id = player_result.player_id
            time_filter, query_params, time_period_text = build_player_time_query_params(player_id, over_last_days)
                    
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
                    f"❌ No nemesis data found for player `{player_result.display_name}`{time_period_text}.",
                    ephemeral=True
                )
                log_command_completion("player nemesis", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
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
                f"## Top 25 Nemeses - {player_result.display_name}{time_period_text}",
                "*Players who killed you the most*"
            ]
            
            message = build_table_message(
                table_data=table_data,
                headers=headers,
                message_prefix_lines=message_prefix_lines,
                item_name="nemeses"
            )

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player nemesis", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
