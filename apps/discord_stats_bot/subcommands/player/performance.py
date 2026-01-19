"""
Player subcommand for top matches by performance metrics.
"""

import logging
import time
from datetime import datetime
from typing import List

import asyncpg
import discord
from discord import app_commands
from tabulate import tabulate

from apps.discord_stats_bot.common.player_id_cache import get_player_id
from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    escape_sql_identifier,
    validate_choice_parameter,
    command_wrapper
)

logger = logging.getLogger(__name__)


STAT_TYPE_CHOICES = [
    app_commands.Choice(name="KPM (Kills Per Minute)", value="kpm"),
    app_commands.Choice(name="KDR (Kill-Death Ratio)", value="kdr"),
    app_commands.Choice(name="DPM (Deaths Per Minute)", value="dpm"),
    app_commands.Choice(name="Kill Streak", value="kill_streak"),
    app_commands.Choice(name="Death Streak", value="death_streak"),
    app_commands.Choice(name="Most Kills", value="kills"),
]


async def stat_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    current_lower = current.lower()
    matching = [
        choice for choice in STAT_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_performance_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the performance subcommand."""
    @player_group.command(name="performance", description="Get top matches for a player (kills, KPM, KDR, DPM, or kill/death streaks (only 45+ minute matches)")
    @app_commands.describe(
        stat_type="The stat type to rank by (KPM, KDR, DPM, Kill Streak, Death Streak, or Most Kills)",
        player="The player ID or player name (optional if you've set one with /profile setid)"
    )
    @app_commands.autocomplete(stat_type=stat_type_autocomplete)
    @command_wrapper("player performance", channel_check=channel_check)
    async def player_performance(interaction: discord.Interaction, stat_type: str, player: str = None):
        command_start_time = time.time()

        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                return

        # Validate stat_type
        try:
            stat_type_lower = validate_choice_parameter(
                "stat type", stat_type, {"kpm", "kdr", "dpm", "kill_streak", "death_streak", "kills"},
                ["KPM", "KDR", "DPM", "Kill Streak", "Death Streak", "Most Kills"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "player": player})
            return
            
        # Map stat type to column name and display name
        stat_config = {
            "kpm": {
                "column": "kills_per_minute",
                "display_name": "KPM (Kills Per Minute)",
                "format": "{:.2f}"
            },
            "kdr": {
                "column": "kill_death_ratio",
                "display_name": "KDR (Kill-Death Ratio)",
                "format": "{:.2f}"
            },
            "dpm": {
                "column": "deaths_per_minute",
                "display_name": "DPM (Deaths Per Minute)",
                "format": "{:.2f}"
            },
            "kill_streak": {
                "column": "kill_streak",
                "display_name": "Kill Streak",
                "format": "{:.0f}"
            },
            "death_streak": {
                "column": "death_streak",
                "display_name": "Death Streak",
                "format": "{:.0f}"
            },
            "kills": {
                "column": "total_kills",
                "display_name": "Most Kills",
                "format": "{:.0f}"
            }
        }

        config = stat_config[stat_type_lower]
        stat_column = config["column"]
        display_name = config["display_name"]
        format_str = config["format"]

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "player": player})
                return
                    
            # Build query to get top 25 matches by stat
            escaped_column = escape_sql_identifier(stat_column)
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column},
                    pms.total_kills,
                    pms.total_deaths
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh
                    ON pms.match_id = mh.match_id
                WHERE pms.player_id = $1
                    AND pms.time_played >= 2700
                ORDER BY pms.{escaped_column} DESC
                LIMIT 25
            """

            logger.info(f"Querying top 25 matches for player {player_id} by {display_name}")
            results = await conn.fetch(query, player_id)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{found_player_name or player}` where they played 45+ minutes."
                )
                log_command_completion("player performance", command_start_time, success=False, interaction=interaction, kwargs={"stat_type": stat_type, "player": player})
                return
                    
        # Format results as a table
        display_player_name = found_player_name if found_player_name else player
        
        # Prepare data for table formatting
        table_data = []
        for row in results:
            stat_value = row[stat_column]
            formatted_stat = format_str.format(stat_value)
            kills = int(row['total_kills'])
            deaths = int(row['total_deaths'])
            
            # Format start_time (timestamp to readable date)
            start_time_val = row['start_time']
            if isinstance(start_time_val, datetime):
                start_time_str = start_time_val.strftime("%Y-%m-%d")
            else:
                start_time_str = str(start_time_val)

            table_data.append([
                row['map_name'],
                formatted_stat,
                kills,
                deaths,
                start_time_str
            ])

        # Headers
        headers = ["Map", display_name, "Kills", "Deaths", "Date"]
        
        # Build table using tabulate with github format
        table_str = tabulate(
            table_data,
            headers=headers,
            tablefmt="github"
        )
        
        # Format as standard message with code block
        message_lines = []
        message_lines.append(f"## Top 25 Matches - {display_player_name} ({display_name})")
        message_lines.append("*Player must have played 45+ minutes*\n")
        message_lines.append("```")
        message_lines.append(table_str)
        message_lines.append("```")
        
        message = "\n".join(message_lines)
        
        # Discord message limit is 2000 characters
        if len(message) > 2000:
            message = message[:1997] + "..."

        await interaction.followup.send(message)
        log_command_completion("player performance", command_start_time, success=True, interaction=interaction, kwargs={"stat_type": stat_type, "player": player})

