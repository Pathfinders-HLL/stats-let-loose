"""
Player maps subcommand - Get a player's best match stats for a specific map.
"""

import logging
import time
from datetime import datetime
from typing import List

import discord
from discord import app_commands

from apps.discord_stats_bot.common.player_id_cache import get_player_id
from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    escape_sql_identifier,
    validate_choice_parameter,
    command_wrapper
)
from apps.discord_stats_bot.common.map_autocomplete import (
    map_name_autocomplete,
    find_map_name_case_insensitive,
    get_map_names
)

logger = logging.getLogger(__name__)


# Order by choices for autocomplete
ORDER_BY_CHOICES = [
    app_commands.Choice(name="Kills", value="kills"),
    app_commands.Choice(name="KDR (Kill-Death Ratio)", value="kdr"),
    app_commands.Choice(name="KPM (Kills Per Minute)", value="kpm"),
]


async def order_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for order_by parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in ORDER_BY_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_maps_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the maps subcommand with the player group.
    
    Args:
        player_group: The player command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @player_group.command(name="maps", description="Get a player's best match stats for a specific map")
    @app_commands.describe(
        map_name="The map name (e.g., 'Carentan', 'Stalingrad', 'Omaha Beach')",
        order_by="How to order results (Kills, KDR, or KPM)",
        player="The player ID or player name (optional if you've set one with /profile setid)"
    )
    @app_commands.autocomplete(map_name=map_name_autocomplete, order_by=order_by_autocomplete)
    @command_wrapper("player maps", channel_check=channel_check)
    async def player_maps(interaction: discord.Interaction, map_name: str, order_by: str = "kills", player: str = None):
        """Get a player's best match stats for a specific map."""
        command_start_time = time.time()

        # If player not provided, try to get stored one from cache
        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
                return

        # Validate map_name
        map_name = map_name.strip()
        if not map_name:
            await interaction.followup.send("❌ Please provide a map name.")
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
            return
        
        # Find the properly cased map name
        proper_map_name = find_map_name_case_insensitive(map_name)
        available_maps = get_map_names()
        
        # Check if the map exists (case-insensitive)
        if proper_map_name.lower() not in [m.lower() for m in available_maps]:
            # Show a helpful error with some suggestions
            suggestions = [m for m in available_maps if map_name.lower() in m.lower()][:5]
            suggestion_text = ""
            if suggestions:
                suggestion_text = f"\n\nDid you mean: {', '.join(suggestions)}?"
            await interaction.followup.send(f"❌ Unknown map: `{map_name}`.{suggestion_text}")
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
            return

        # Validate order_by
        try:
            order_by_lower = validate_choice_parameter(
                "order by", order_by, {"kills", "kdr", "kpm"},
                ["Kills", "KDR (Kill-Death Ratio)", "KPM (Kills Per Minute)"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
            return

        # Map order_by to column name and display name
        order_by_config = {
            "kills": {
                "column": "total_kills",
                "display_name": "Kills",
                "format": "{:.0f}"
            },
            "kdr": {
                "column": "kill_death_ratio",
                "display_name": "KDR",
                "format": "{:.2f}"
            },
            "kpm": {
                "column": "kills_per_minute",
                "display_name": "KPM",
                "format": "{:.2f}"
            }
        }

        config = order_by_config[order_by_lower]
        order_column = config["column"]
        order_display_name = config["display_name"]
        format_str = config["format"]

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
                return

            # Build query to get best matches for the specific map
            escaped_order_column = escape_sql_identifier(order_column)
            query = f"""
                SELECT
                    mh.map_name,
                    mh.match_id,
                    pms.total_kills,
                    pms.total_deaths,
                    pms.kill_death_ratio as kdr,
                    pms.kills_per_minute as kpm,
                    pms.combat_score,
                    pms.offense_score,
                    pms.defense_score,
                    pms.support_score,
                    pms.{escaped_order_column} as order_value,
                    mh.start_time,
                    mh.match_duration
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh
                    ON pms.match_id = mh.match_id
                WHERE pms.player_id = $1
                    AND LOWER(mh.map_name) = LOWER($2)
                    AND pms.{escaped_order_column} > 0
                ORDER BY pms.{escaped_order_column} DESC
                LIMIT 25
            """

            logger.info(f"Querying best matches for player {player_id} on map {proper_map_name} ordered by {order_display_name}")
            results = await conn.fetch(query, player_id, proper_map_name)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{found_player_name or player}` on map `{proper_map_name}`."
                )
                log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
                return

            # Format results as a table
            display_player_name = found_player_name if found_player_name else player
            
            # Create Discord embed with pseudo-columns
            embed = discord.Embed(
                title=f"Best Matches on {proper_map_name}",
                description=f"**Player:** {display_player_name}\n**Ordered by:** {order_display_name}",
                color=0x00ff00  # Green color
            )

            # Prepare data for each column
            kills_list = []
            deaths_list = []
            kdr_list = []
            kpm_list = []

            for row in results:
                kills = int(row['total_kills'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])
                kpm = float(row['kpm'])

                kills_list.append(f"`{kills}`")
                deaths_list.append(f"`{deaths}`")
                kdr_list.append(f"`{kdr:.2f}`")
                kpm_list.append(f"`{kpm:.2f}`")

            # Add columns as inline fields
            embed.add_field(
                name="Kills",
                value="\n".join(kills_list),
                inline=True
            )

            embed.add_field(
                name="Deaths",
                value="\n".join(deaths_list),
                inline=True
            )

            embed.add_field(
                name="K/D",
                value="\n".join(kdr_list),
                inline=True
            )

            embed.add_field(
                name="KPM",
                value="\n".join(kpm_list),
                inline=True
            )

            await interaction.followup.send(embed=embed)
            log_command_completion("player maps", command_start_time, success=True, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
