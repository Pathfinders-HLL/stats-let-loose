"""
Player kills subcommand - Get top 25 matches for a player by total kills.
"""

import logging
import time

import discord

from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    validate_choice_parameter,
    build_player_time_query_params,
    command_wrapper,
    kill_type_autocomplete,
    format_date,
    build_table_message,
    lookup_player,
    KILL_TYPE_CONFIG,
    KILL_TYPE_VALID_VALUES,
    KILL_TYPE_DISPLAY_LIST,
)

logger = logging.getLogger(__name__)


def register_kills_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the kills subcommand with the player group."""
    
    @player_group.command(
        name="kills", 
        description="Get top 25 matches for a player by total kills"
    )
    @app_commands.describe(
        kill_type="The kill type to filter by",
        player="(Optional) The player ID or player name",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(kill_type=kill_type_autocomplete)
    @command_wrapper("player kills", channel_check=channel_check)
    async def player_kills(
        interaction: discord.Interaction, 
        kill_type: str = "all", 
        player: str = None, 
        over_last_days: int = 30
    ):
        """Get top 25 matches for a player by total kills."""
        command_start_time = time.time()
        log_kwargs = {"kill_type": kill_type, "player": player, "over_last_days": over_last_days}

        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        try:
            kill_type_lower = validate_choice_parameter(
                "kill type", kill_type, KILL_TYPE_VALID_VALUES, KILL_TYPE_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
            
        config = KILL_TYPE_CONFIG[kill_type_lower]
        kill_column = config["column"]
        display_name = config["display_name"]

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_result, error = await lookup_player(conn, interaction.user.id, player)
            if error:
                await interaction.followup.send(error, ephemeral=True)
                log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            player_id = player_result.player_id
            time_filter, query_params, time_period_text = build_player_time_query_params(player_id, over_last_days)
                    
            escaped_column = escape_sql_identifier(kill_column)
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column} as kill_count,
                    pms.total_kills,
                    pms.total_deaths,
                    pms.kill_death_ratio as kdr
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh
                    ON pms.match_id = mh.match_id
                WHERE pms.player_id = $1
                    {time_filter}
                    AND pms.{escaped_column} > 0
                ORDER BY pms.{escaped_column} DESC
                LIMIT 25
            """

            logger.info(f"Querying top 25 matches for player {player_id} by {display_name}")
            results = await conn.fetch(query, *query_params)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{player_result.display_name}` "
                    f"with {display_name.lower()}{time_period_text}.",
                    ephemeral=True
                )
                log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            table_data = []
            for rank, row in enumerate(results, 1):
                kills = int(row['kill_count'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])

                start_time_str = format_date(row['start_time'])

                table_data.append([
                    rank,
                    row['map_name'],
                    kills,
                    deaths,
                    f"{kdr:.2f}",
                    start_time_str
                ])

            headers = ["#", "Map Name", "Kills", "Deaths", "K/D", "Date"]
            message_prefix_lines = [f"## Top 25 Matches - {player_result.display_name} ({display_name}){time_period_text}"]
            
            message = build_table_message(
                table_data=table_data,
                headers=headers,
                message_prefix_lines=message_prefix_lines,
                item_name="matches"
            )

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player kills", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
