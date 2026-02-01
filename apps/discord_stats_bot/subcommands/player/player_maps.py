"""
Player maps subcommand - Get a player's best match stats for a specific map.
"""

import logging
import time

import discord

from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_choice_parameter,
    command_wrapper,
    map_name_autocomplete,
    find_map_name_case_insensitive,
    get_map_names,
    order_by_autocomplete,
    format_date,
    build_table_message,
    lookup_player,
    ORDER_BY_CONFIG,
    ORDER_BY_VALID_VALUES,
    ORDER_BY_DISPLAY_LIST,
)

logger = logging.getLogger(__name__)


def register_maps_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the maps subcommand with the player group."""
    
    @player_group.command(
        name="maps", 
        description="Get a player's best match stats for a specific map"
    )
    @app_commands.describe(
        map_name="The map name (e.g., 'Carentan', 'Stalingrad', 'Omaha Beach')",
        order_by="How to order results (Kills, KDR, or KPM)",
        player="(Optional) The player ID or player name"
    )
    @app_commands.autocomplete(map_name=map_name_autocomplete, order_by=order_by_autocomplete)
    @command_wrapper("player maps", channel_check=channel_check)
    async def player_maps(
        interaction: discord.Interaction, 
        map_name: str, 
        order_by: str = "kills", 
        player: str = None
    ):
        """Get a player's best match stats for a specific map."""
        command_start_time = time.time()
        log_kwargs = {"map_name": map_name, "order_by": order_by, "player": player}

        map_name = map_name.strip()
        if not map_name:
            await interaction.followup.send("❌ Please provide a map name.", ephemeral=True)
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        proper_map_name = find_map_name_case_insensitive(map_name)
        available_maps = get_map_names()
        
        if proper_map_name.lower() not in [m.lower() for m in available_maps]:
            suggestions = [m for m in available_maps if map_name.lower() in m.lower()][:5]
            suggestion_text = ""
            if suggestions:
                suggestion_text = f"\n\nDid you mean: {', '.join(suggestions)}?"
            await interaction.followup.send(f"❌ Unknown map: `{map_name}`.{suggestion_text}", ephemeral=True)
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        try:
            order_by_lower = validate_choice_parameter(
                "order by", order_by, ORDER_BY_VALID_VALUES, ORDER_BY_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        config = ORDER_BY_CONFIG[order_by_lower]
        order_column = config["column"]
        order_display_name = config["display_name"]

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_result, error = await lookup_player(conn, interaction.user.id, player)
            if error:
                await interaction.followup.send(error, ephemeral=True)
                log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            player_id = player_result.player_id

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
                    mh.start_time
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh
                    ON pms.match_id = mh.match_id
                WHERE pms.player_id = $1
                    AND LOWER(mh.map_name) = LOWER($2)
                    AND pms.{escaped_order_column} > 0
                ORDER BY pms.{escaped_order_column} DESC
                LIMIT 25
            """

            logger.info(f"Querying best matches for player {player_id} on map {proper_map_name}")
            results = await conn.fetch(query, player_id, proper_map_name)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{player_result.display_name}` on map `{proper_map_name}`.",
                    ephemeral=True
                )
                log_command_completion("player maps", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            table_data = []
            for row in results:
                kills = int(row['total_kills'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])
                kpm = float(row['kpm'])

                start_time_str = format_date(row['start_time'])

                table_data.append([
                    kills,
                    deaths,
                    f"{kdr:.2f}",
                    f"{kpm:.2f}",
                    start_time_str
                ])

            headers = ["Kills", "Deaths", "K/D", "KPM", "Date"]
            
            message_prefix_lines = [
                f"## Best Matches on {proper_map_name}",
                f"**Player:** {player_result.display_name}",
                f"**Ordered by:** {order_display_name}\n"
            ]
            
            message = build_table_message(
                table_data=table_data,
                headers=headers,
                message_prefix_lines=message_prefix_lines,
                item_name="matches"
            )

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player maps", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
