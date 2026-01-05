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
    app_commands.Choice(name="Combat Score", value="combat"),
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
        order_by="How to order results (Kills, KDR, KPM, or Combat Score)",
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
                return

        # Validate map_name
        map_name = map_name.strip()
        if not map_name:
            await interaction.followup.send("❌ Please provide a map name.")
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
            return

        # Validate order_by
        try:
            order_by_lower = validate_choice_parameter(
                "order by", order_by, {"kills", "kdr", "kpm", "combat"},
                ["Kills", "KDR (Kill-Death Ratio)", "KPM (Kills Per Minute)", "Combat Score"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
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
            },
            "combat": {
                "column": "combat_score",
                "display_name": "Combat Score",
                "format": "{:.0f}"
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
                return

            # Format results
            summary_lines = []
            display_player_name = found_player_name if found_player_name else player
            summary_lines.append(f"## Best Matches on {proper_map_name} - {display_player_name}\n")
            summary_lines.append(f"*Top matches ordered by {order_display_name} (best to worst)*\n\n")

            for rank, row in enumerate(results, 1):
                kills = row['total_kills']
                deaths = row['total_deaths']
                kdr = row['kdr']
                kpm = row['kpm']
                combat_score = row['combat_score']
                offense_score = row['offense_score']
                defense_score = row['defense_score']
                support_score = row['support_score']
                
                # Format the ordering stat value
                order_value = row['order_value']
                formatted_order_value = format_str.format(order_value)
                
                # Format duration (seconds to minutes)
                duration_min = row['match_duration'] // 60 if row['match_duration'] else 0
                
                # Format start_time (timestamp to readable date)
                start_time_val = row['start_time']
                if isinstance(start_time_val, datetime):
                    start_time_str = start_time_val.strftime("%Y-%m-%d")
                else:
                    start_time_str = str(start_time_val)

                # Build stats line with key information
                summary_lines.append(
                    f"{rank}. **{formatted_order_value} {order_display_name.lower()}** - "
                    f"{kills}K/{deaths}D (KDR: {kdr:.2f}, KPM: {kpm:.2f}) | "
                    f"Combat: {combat_score:,} | Off: {offense_score:,} | Def: {defense_score:,} | Sup: {support_score:,} | "
                    f"{duration_min}min - {start_time_str}"
                )

            # Discord message limit is 2000 characters
            message = "\n".join(summary_lines)
            if len(message) > 2000:
                message = message[:1997] + "..."

            await interaction.followup.send(message)
            log_command_completion("player maps", command_start_time, success=True, interaction=interaction, kwargs={"map_name": map_name, "order_by": order_by, "player": player})
