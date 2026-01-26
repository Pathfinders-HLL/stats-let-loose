"""
Player performance subcommand - Get top matches for a player by performance stats.
"""

import logging
import time

import discord

from datetime import datetime
from typing import List
from discord import app_commands
from tabulate import tabulate

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    validate_choice_parameter,
    create_time_filter_params,
    command_wrapper,
    get_player_id,
    stat_type_autocomplete,
    STAT_TYPE_CONFIG,
    STAT_TYPE_VALID_VALUES,
    STAT_TYPE_DISPLAY_LIST,
)

logger = logging.getLogger(__name__)


def register_performance_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the performance subcommand with the player group."""
    
    @player_group.command(
        name="performance", 
        description="Get top matches for a player by performance stat"
    )
    @app_commands.describe(
        stat_type="The performance stat to rank by",
        player="(Optional) The player ID or player name",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(stat_type=stat_type_autocomplete)
    @command_wrapper("player performance", channel_check=channel_check)
    async def player_performance(
        interaction: discord.Interaction, 
        stat_type: str, 
        player: str = None, 
        over_last_days: int = 30
    ):
        """Get top matches for a player by performance stat."""
        command_start_time = time.time()
        log_kwargs = {"stat_type": stat_type, "player": player, "over_last_days": over_last_days}

        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
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

        try:
            stat_type_lower = validate_choice_parameter(
                "stat type", stat_type, STAT_TYPE_VALID_VALUES, STAT_TYPE_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
            
        config = STAT_TYPE_CONFIG[stat_type_lower]
        stat_column = config["column"]
        display_name = config["display_name"]
        format_str = config["format"]

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(
                    f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.",
                    ephemeral=True
                )
                log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return

            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            escaped_column = escape_sql_identifier(stat_column)
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column} as stat_value,
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

            logger.info(f"Querying top matches for player {player_id} by {display_name}")
            results = await conn.fetch(query, *query_params)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{found_player_name or player}` "
                    f"with {display_name.lower()}{time_period_text}.",
                    ephemeral=True
                )
                log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return

            display_player_name = found_player_name if found_player_name else player
            
            table_data = []
            for rank, row in enumerate(results, 1):
                stat_value = row['stat_value']
                kills = int(row['total_kills'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])

                start_time_val = row['start_time']
                if isinstance(start_time_val, datetime):
                    start_time_str = start_time_val.strftime("%Y-%m-%d")
                else:
                    start_time_str = str(start_time_val)

                table_data.append([
                    rank,
                    row['map_name'],
                    format_str.format(stat_value),
                    kills,
                    deaths,
                    f"{kdr:.2f}",
                    start_time_str
                ])

            headers = ["#", "Map", display_name, "Kills", "Deaths", "K/D", "Date"]
            message_prefix_lines = [f"## Top 25 Matches - {display_player_name} ({display_name}){time_period_text}"]
            
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
                    message_lines.append(f"\n*Showing {num_rows} of {len(table_data)} matches (message length limit)*")
                
                message = "\n".join(message_lines)
                
                if len(message) <= 2000:
                    break

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player performance", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
