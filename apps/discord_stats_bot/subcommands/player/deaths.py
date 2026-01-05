"""
Player topdeaths subcommand - Get top 25 matches for a player by total deaths (with death type filtering).
"""

import logging
import time
from datetime import datetime
from typing import List

import asyncpg
import discord
from discord import app_commands

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


# Death type choices for autocomplete
DEATH_TYPE_CHOICES = [
    app_commands.Choice(name="All Deaths", value="all"),
    app_commands.Choice(name="Infantry Deaths", value="infantry"),
    app_commands.Choice(name="Armor Deaths", value="armor"),
    app_commands.Choice(name="Artillery Deaths", value="artillery"),
]


async def death_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for death_type parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in DEATH_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_deaths_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the topdeaths subcommand with the player group.
    
    Args:
        player_group: The player command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @player_group.command(name="deaths", description="Get top 25 matches for a player by total deaths (with death type filtering)")
    @app_commands.describe(
        death_type="The death type to filter by (All Deaths, Infantry Deaths, Armor Deaths, Artillery Deaths)",
        player="The player ID or player name (optional if you've set one with /profile setid)",
        over_last_days="Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(death_type=death_type_autocomplete)
    @command_wrapper("player deaths", channel_check=channel_check)
    async def player_topdeaths(interaction: discord.Interaction, death_type: str = "all", player: str = None, over_last_days: int = 30):
        """Get top 25 matches for a player by total deaths with optional death type filtering."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            return

        # If player not provided, try to get stored one from cache
        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                return

        # Validate death_type
        try:
            death_type_lower = validate_choice_parameter(
                "death type", death_type, {"all", "infantry", "armor", "artillery"},
                ["All Deaths", "Infantry Deaths", "Armor Deaths", "Artillery Deaths"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            return
            
        # Map death type to column name and display name
        death_type_config = {
            "all": {
                "column": "total_deaths",
                "display_name": "All Deaths"
            },
            "infantry": {
                "column": "infantry_deaths",
                "display_name": "Infantry Deaths"
            },
            "armor": {
                "column": "armor_deaths",
                "display_name": "Armor Deaths"
            },
            "artillery": {
                "column": "artillery_deaths",
                "display_name": "Artillery Deaths"
            }
        }

        config = death_type_config[death_type_lower]
        death_column = config["column"]
        display_name = config["display_name"]

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                return

            # Calculate time period filter
            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            # Adjust parameter number in time_filter if we have base params
            # Since player_id is $1, time_threshold needs to be $2
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            # Build query to get top 25 matches by death type
            escaped_column = escape_sql_identifier(death_column)
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column} as death_count,
                    pms.total_kills,
                    pms.total_deaths,
                    mh.match_duration
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
                return

            # Calculate total deaths from top 25 matches
            total_deaths_top25 = sum(row['death_count'] for row in results)

            # Format results
            summary_lines = []
            display_player_name = found_player_name if found_player_name else player
            summary_lines.append(f"## Top 25 Matches - {display_player_name} ({display_name}){time_period_text}\n")
            summary_lines.append(f"**Total {display_name.lower()} in top 25 matches: {total_deaths_top25:,}**\n")

            for rank, row in enumerate(results, 1):
                death_count = row['death_count']
                # Format duration (seconds to minutes)
                duration_min = row['match_duration'] // 60 if row['match_duration'] else 0
                # Format start_time (timestamp to readable date)
                start_time_val = row['start_time']
                if isinstance(start_time_val, datetime):
                    start_time_str = start_time_val.strftime("%Y-%m-%d")
                else:
                    start_time_str = str(start_time_val)

                summary_lines.append(
                    f"{rank}. **{row['map_name']}** - {death_count:,} {display_name.lower()} "
                    f"({row['total_kills']}K/{row['total_deaths']}D, {duration_min}min) - {start_time_str}"
                )

            # Discord message limit is 2000 characters
            message = "\n".join(summary_lines)
            if len(message) > 2000:
                message = message[:1997] + "..."

            await interaction.followup.send(message)
            log_command_completion("player deaths", command_start_time, success=True, interaction=interaction, kwargs={"death_type": death_type, "player": player, "over_last_days": over_last_days})

