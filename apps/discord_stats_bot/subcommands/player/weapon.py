"""
Player weapon subcommand - Get total kills for a player by weapon category.
"""

import json
import logging
import time
from typing import List

import discord
from discord import app_commands
from tabulate import tabulate

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    create_time_filter_params,
    command_wrapper,
    get_player_id,
    weapon_category_autocomplete,
    get_weapon_mapping,
    get_weapon_names,
)

logger = logging.getLogger(__name__)

WEAPON_MAPPING = get_weapon_mapping()
ALL_WEAPONS_VALUE = "ALL_WEAPONS_SPECIAL_VALUE"


async def weapon_category_autocomplete_with_all(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function that includes 'All Weapons' option."""
    choices = []
    
    if not current or "all weapons" in current.lower():
        choices.append(app_commands.Choice(name="All Weapons", value=ALL_WEAPONS_VALUE))
    
    regular_choices = await weapon_category_autocomplete(interaction, current)
    choices.extend(regular_choices)
    
    return choices[:25]


def register_weapon_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the weapon subcommand with the player group."""
    
    @player_group.command(
        name="weapon", 
        description="Get total kills for a player by weapon category"
    )
    @app_commands.describe(
        weapon_category="The weapon category (defaults to all weapons if not specified)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)",
        player="(Optional) The player ID or player name"
    )
    @app_commands.autocomplete(weapon_category=weapon_category_autocomplete_with_all)
    @command_wrapper("player weapon", channel_check=channel_check)
    async def player_weapon(
        interaction: discord.Interaction, 
        weapon_category: str = None, 
        player: str = None, 
        over_last_days: int = 30
    ):
        """Get the total kills for a player by weapon category."""
        command_start_time = time.time()
        log_kwargs = {"weapon_category": weapon_category, "player": player, "over_last_days": over_last_days}

        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
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
                log_command_completion("player weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return

        if not weapon_category or weapon_category == ALL_WEAPONS_VALUE:
            await _handle_all_weapons(interaction, player, over_last_days, command_start_time)
            return

        weapon_category_lower = weapon_category.lower().strip()
        column_name = WEAPON_MAPPING.get(weapon_category_lower)

        if not column_name:
            available_categories = sorted(set(WEAPON_MAPPING.keys()))
            await interaction.followup.send(
                f"❌ Unknown weapon category: `{weapon_category}` "
                f"Available categories: {', '.join(sorted(available_categories))}",
                ephemeral=True
            )
            log_command_completion("player weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        friendly_category_name = None
        weapon_names = get_weapon_names()
        for friendly_name in weapon_names:
            friendly_name_lower = friendly_name.lower().strip()
            if friendly_name_lower == weapon_category_lower:
                friendly_category_name = friendly_name
                break
        
        if friendly_category_name is None:
            for friendly_name in weapon_names:
                friendly_name_lower = friendly_name.lower().strip()
                if WEAPON_MAPPING.get(friendly_name_lower) == column_name:
                    friendly_category_name = friendly_name
                    break
        
        if friendly_category_name is None:
            friendly_category_name = weapon_category
            
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(
                    f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.",
                    ephemeral=True
                )
                log_command_completion("player weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
                    
            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
                        
            escaped_column = escape_sql_identifier(column_name)
            query1 = f"""
                SELECT COALESCE(SUM(pks.{escaped_column}), 0) as total_kills
                FROM pathfinder_stats.player_kill_stats pks
                INNER JOIN pathfinder_stats.match_history mh
                    ON pks.match_id = mh.match_id
                WHERE pks.player_id = $1
                    {time_filter}
            """

            query_params = [player_id] + base_query_params
            total_kills = await conn.fetchval(query1, *query_params) or 0
            
            if base_query_params:
                query2 = f"""
                    WITH player_totals AS (
                        SELECT
                            pks.player_id,
                            COALESCE(SUM(pks.{escaped_column}), 0) as total_kills
                        FROM pathfinder_stats.player_kill_stats pks
                        INNER JOIN pathfinder_stats.match_history mh
                            ON pks.match_id = mh.match_id
                        WHERE mh.start_time >= $1
                        GROUP BY pks.player_id
                        HAVING COALESCE(SUM(pks.{escaped_column}), 0) > 0
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE total_kills > $2) + 1 as rank,
                        COUNT(*) as total_players
                    FROM player_totals
                """
                result2 = await conn.fetchrow(query2, base_query_params[0], total_kills)
            else:
                query2 = f"""
                    WITH player_totals AS (
                        SELECT
                            player_id,
                            COALESCE(SUM({escaped_column}), 0) as total_kills
                        FROM pathfinder_stats.player_kill_stats
                        GROUP BY player_id
                        HAVING COALESCE(SUM({escaped_column}), 0) > 0
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE total_kills > $1) + 1 as rank,
                        COUNT(*) as total_players
                    FROM player_totals
                """
                result2 = await conn.fetchrow(query2, total_kills)

            if result2:
                rank = result2['rank'] if result2['rank'] is not None else 0
                total_players = result2['total_players'] if result2['total_players'] is not None else 0
            else:
                rank = 0
                total_players = 0

            display_name = found_player_name if found_player_name else player
            if total_kills == 0:
                await interaction.followup.send(
                    f"Player `{display_name}` has **0** total kills with `{friendly_category_name}`{time_period_text}",
                    ephemeral=True
                )
            else:
                rank_text = f"Rank **#{rank}**"
                if total_players > 0:
                    rank_text += f" out of **{total_players}** players"
                await interaction.followup.send(
                    f"Player `{display_name}` has **{total_kills:,}** total kills "
                    f"with `{friendly_category_name}`{time_period_text} ({rank_text})",
                    ephemeral=True
                )

            log_command_completion("player weapon", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)


async def _handle_all_weapons(
    interaction: discord.Interaction, 
    player: str, 
    over_last_days: int, 
    command_start_time: float = None
) -> None:
    """Handle the 'All Weapons' case."""
    if command_start_time is None:
        command_start_time = time.time()
    
    log_kwargs = {"weapon_category": "All Weapons", "player": player, "over_last_days": over_last_days}
    
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        player_id, found_player_name = await find_player_by_id_or_name(conn, player)

        if not player_id:
            await interaction.followup.send(
                f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.",
                ephemeral=True
            )
            log_command_completion("player weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
        
        all_column_names = sorted(set(WEAPON_MAPPING.values()))
        
        weapon_names = get_weapon_names()
        column_to_friendly = {}
        for friendly_name in weapon_names:
            friendly_name_lower = friendly_name.lower().strip()
            col_name = WEAPON_MAPPING.get(friendly_name_lower)
            if col_name and col_name not in column_to_friendly:
                column_to_friendly[col_name] = friendly_name
        
        escaped_columns = [escape_sql_identifier(col) for col in all_column_names]
        json_keys = [f"'{col}'" for col in all_column_names]
        json_values = [f"COALESCE(SUM(pks.{esc}), 0)" for esc in escaped_columns]
        
        json_pairs = ", ".join([f"{key}, {val}" for key, val in zip(json_keys, json_values)])
        
        adjusted_time_filter = time_filter
        if base_query_params:
            adjusted_time_filter = time_filter.replace("$1", "$2")
        
        query_params = [player_id] + base_query_params
        if base_query_params:
            query = f"""
                SELECT json_build_object({json_pairs}) as weapon_totals
                FROM pathfinder_stats.player_kill_stats pks
                INNER JOIN pathfinder_stats.match_history mh
                    ON pks.match_id = mh.match_id
                WHERE pks.player_id = $1
                    {adjusted_time_filter}
            """
        else:
            query = f"""
                SELECT json_build_object({json_pairs}) as weapon_totals
                FROM pathfinder_stats.player_kill_stats pks
                WHERE pks.player_id = $1
            """
            query_params = [player_id]
        
        result = await conn.fetchrow(query, *query_params)
        weapon_totals_json = result['weapon_totals'] if result else {}
        if isinstance(weapon_totals_json, str):
            weapon_totals_json = json.loads(weapon_totals_json)
        
        weapon_stats = []
        
        for column_name in all_column_names:
            total_kills = weapon_totals_json.get(column_name, 0) if weapon_totals_json else 0
            
            if total_kills == 0:
                continue
            
            escaped_column = escape_sql_identifier(column_name)
            
            if base_query_params:
                rank_query = f"""
                    WITH player_totals AS (
                        SELECT
                            pks.player_id,
                            COALESCE(SUM(pks.{escaped_column}), 0) as total_kills
                        FROM pathfinder_stats.player_kill_stats pks
                        INNER JOIN pathfinder_stats.match_history mh
                            ON pks.match_id = mh.match_id
                        WHERE mh.start_time >= $1
                        GROUP BY pks.player_id
                        HAVING COALESCE(SUM(pks.{escaped_column}), 0) > 0
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE total_kills > $2) + 1 as rank,
                        COUNT(*) as total_players
                    FROM player_totals
                """
                rank_result = await conn.fetchrow(rank_query, base_query_params[0], total_kills)
            else:
                rank_query = f"""
                    WITH player_totals AS (
                        SELECT
                            player_id,
                            COALESCE(SUM({escaped_column}), 0) as total_kills
                        FROM pathfinder_stats.player_kill_stats
                        GROUP BY player_id
                        HAVING COALESCE(SUM({escaped_column}), 0) > 0
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE total_kills > $1) + 1 as rank,
                        COUNT(*) as total_players
                    FROM player_totals
                """
                rank_result = await conn.fetchrow(rank_query, total_kills)
            
            rank = rank_result['rank'] if rank_result and rank_result['rank'] is not None else 0
            total_players = rank_result['total_players'] if rank_result and rank_result['total_players'] is not None else 0
            
            friendly_name = column_to_friendly.get(column_name, column_name.replace('_', ' ').title())
            
            weapon_stats.append({
                'weapon': friendly_name,
                'kills': total_kills,
                'rank': rank,
                'total_players': total_players
            })
        
        weapon_stats.sort(key=lambda x: x['kills'], reverse=True)
        
        if not weapon_stats:
            display_name = found_player_name if found_player_name else player
            await interaction.followup.send(
                f"Player `{display_name}` has **0** kills with any weapon{time_period_text}",
                ephemeral=True
            )
            return
        
        display_name = found_player_name if found_player_name else player
        
        table_data = []
        for weapon_stat in weapon_stats:
            rank_text = f"#{weapon_stat['rank']}"
            if weapon_stat['total_players'] > 0:
                rank_text += f"/{weapon_stat['total_players']}"
            
            table_data.append([
                weapon_stat['weapon'],
                weapon_stat['kills'],
                rank_text
            ])

        headers = ["Weapon", "Kills", "Rank"]
        
        message_prefix_lines = [
            f"## All Weapons - {display_name}{time_period_text}",
            "*Sorted by total kills (highest to lowest)*\n"
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
                message_lines.append(f"\n*Showing {num_rows} of {len(table_data)} weapons (message length limit)*")
            
            message = "\n".join(message_lines)
            
            if len(message) <= 2000:
                break
        
        await interaction.followup.send(message, ephemeral=True)
        log_command_completion("player weapon", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
