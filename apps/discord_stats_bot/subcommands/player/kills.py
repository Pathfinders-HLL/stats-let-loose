"""
Player top25matches subcommand - Get top 25 matches for a player by total kills (with kill type filtering).
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
    validate_over_last_days,
    validate_choice_parameter,
    create_time_filter_params,
    command_wrapper
)

logger = logging.getLogger(__name__)


# Kill type choices for autocomplete
KILL_TYPE_CHOICES = [
    app_commands.Choice(name="All Kills", value="all"),
    app_commands.Choice(name="Infantry Kills", value="infantry"),
    app_commands.Choice(name="Armor Kills", value="armor"),
    app_commands.Choice(name="Artillery Kills", value="artillery"),
]


async def kill_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for kill_type parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in KILL_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_kills_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the topkills subcommand with the player group.
    
    Args:
        player_group: The player command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @player_group.command(name="kills", description="Get top 25 matches for a player by total kills (with kill type filtering)")
    @app_commands.describe(
        kill_type="The kill type to filter by (All Kills, Infantry Kills, Armor Kills, Artillery Kills)",
        player="The player ID or player name (optional if you've set one with /profile setid)",
        over_last_days="Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(kill_type=kill_type_autocomplete)
    @command_wrapper("player kills", channel_check=channel_check)
    async def player_topkills(interaction: discord.Interaction, kill_type: str = "all", player: str = None, over_last_days: int = 30):
        """Get top 25 matches for a player by total kills with optional kill type filtering."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs={"kill_type": kill_type, "player": player, "over_last_days": over_last_days})
            return

        # If player not provided, try to get stored one from cache
        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                return

        # Validate kill_type
        try:
            kill_type_lower = validate_choice_parameter(
                "kill type", kill_type, {"all", "infantry", "armor", "artillery"},
                ["All Kills", "Infantry Kills", "Armor Kills", "Artillery Kills"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs={"kill_type": kill_type, "player": player, "over_last_days": over_last_days})
            return
            
        # Map kill type to column name and display name
        kill_type_config = {
            "all": {
                "column": "total_kills",
                "display_name": "All Kills"
            },
            "infantry": {
                "column": "infantry_kills",
                "display_name": "Infantry Kills"
            },
            "armor": {
                "column": "armor_kills",
                "display_name": "Armor Kills"
            },
            "artillery": {
                "column": "artillery_kills",
                "display_name": "Artillery Kills"
            }
        }

        config = kill_type_config[kill_type_lower]
        kill_column = config["column"]
        display_name = config["display_name"]

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs={"kill_type": kill_type, "player": player, "over_last_days": over_last_days})
                return

            # Calculate time period filter
            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            # Adjust parameter number in time_filter if we have base params
            # Since player_id is $1, time_threshold needs to be $2
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            # Build query to get top 25 matches by kill type
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
                    f"❌ No matches found for player `{found_player_name or player}` with {display_name.lower()}{time_period_text}."
                )
                log_command_completion("player kills", command_start_time, success=False, interaction=interaction, kwargs={"kill_type": kill_type, "player": player, "over_last_days": over_last_days})
                return

            # Format results as a table
            display_player_name = found_player_name if found_player_name else player
            
            # Prepare data for table formatting
            table_data = []
            for rank, row in enumerate(results, 1):
                kills = int(row['kill_count'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])

                # Format start_time (timestamp to readable date)
                start_time_val = row['start_time']
                if isinstance(start_time_val, datetime):
                    start_time_str = start_time_val.strftime("%Y-%m-%d")
                else:
                    start_time_str = str(start_time_val)

                table_data.append([
                    rank,
                    row['map_name'],
                    kills,
                    deaths,
                    f"{kdr:.2f}",
                    start_time_str
                ])

            # Headers for the table
            headers = ["#", "Map Name", "Kills", "Deaths", "K/D", "Date"]
            
            # Build table using tabulate with github format (single-space padding, auto-expanding columns)
            table_str = tabulate(
                table_data,
                headers=headers,
                tablefmt="github"
            )
            
            # Format as standard message with code block
            message_lines = []
            message_lines.append(f"## Top 25 Matches - {display_player_name} ({display_name}){time_period_text}")
            message_lines.append("```")
            message_lines.append(table_str)
            message_lines.append("```")
            
            message = "\n".join(message_lines)
            
            # Discord message limit is 2000 characters
            if len(message) > 2000:
                message = message[:1997] + "..."

            await interaction.followup.send(message)
            log_command_completion("player kills", command_start_time, success=True, interaction=interaction, kwargs={"kill_type": kill_type, "player": player, "over_last_days": over_last_days})

