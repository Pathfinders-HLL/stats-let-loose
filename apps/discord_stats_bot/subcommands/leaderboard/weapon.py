"""
Leaderboard weapon subcommand - Get top players by weapon kills over a time period.
"""

import logging
import time

import discord

from typing import Any, Dict, List
from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    create_time_filter_params,
    get_pathfinder_player_ids,
    command_wrapper,
    format_sql_query_with_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    weapon_category_autocomplete,
    get_weapon_mapping,
    PATHFINDER_COLOR,
)
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
    TIMEFRAME_OPTIONS,
)

logger = logging.getLogger(__name__)

WEAPON_MAPPING = get_weapon_mapping()


async def fetch_weapon_leaderboard(
    weapon_category_lower: str,
    only_pathfinders: bool,
    over_last_days: int
) -> List[Dict[str, Any]]:
    """Fetch weapon leaderboard data."""
    column_name = WEAPON_MAPPING.get(weapon_category_lower)
    if not column_name:
        return []
    
    time_filter, base_query_params, _ = create_time_filter_params(over_last_days)
    
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        escaped_column = escape_sql_identifier(column_name)
        pathfinder_ids_list = list(get_pathfinder_player_ids()) if only_pathfinders else []
        
        param_num = 1
        query_params = []
        
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_kill_stats", "pks", bool(base_query_params)
        )
        
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        pathfinder_where = ""
        if only_pathfinders:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pks", param_num, pathfinder_ids_list, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        kill_stats_where = build_where_clause(time_where, pathfinder_where)
        
        lateral_where = ""
        if only_pathfinders:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=True
            )
            query_params.extend(lateral_params)
        
        lateral_join = build_lateral_name_lookup("tks.player_id", lateral_where)
        
        query = f"""
            WITH kill_stats AS (
                SELECT 
                    pks.player_id,
                    SUM(pks.{escaped_column}) as total_kills
                {from_clause}
                {kill_stats_where}
                GROUP BY pks.player_id
                HAVING SUM(pks.{escaped_column}) > 0
            ),
            top_kill_stats AS (
                SELECT 
                    ks.player_id,
                    ks.total_kills
                FROM kill_stats ks
                ORDER BY ks.total_kills DESC
                LIMIT {TOP_PLAYERS_LIMIT}
            )
            SELECT 
                tks.player_id,
                COALESCE(rn.player_name, tks.player_id) as player_name,
                tks.total_kills
            FROM top_kill_stats tks
            {lateral_join}
            ORDER BY tks.total_kills DESC
        """
        
        logger.info(f"SQL Query: {format_sql_query_with_params(query, query_params)}")
        results = await conn.fetch(query, *query_params)
        
        return [dict(row) for row in results]


def register_weapon_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """Register the weapon subcommand with the leaderboard group."""
    
    @leaderboard_group.command(
        name="weapon", 
        description="Get top players by weapon kills over a time period"
    )
    @app_commands.describe(
        weapon_category="The weapon category (e.g., 'M1 Garand', 'Thompson', 'Sniper')",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(weapon_category=weapon_category_autocomplete)
    @command_wrapper("leaderboard weapon", channel_check=channel_check)
    async def leaderboard_weapon(
        interaction: discord.Interaction, 
        weapon_category: str, 
        only_pathfinders: bool = False
    ):
        """Get top players by weapon kills over a time period."""
        command_start_time = time.time()
        log_kwargs = {"weapon_category": weapon_category, "only_pathfinders": only_pathfinders}
        
        weapon_category_lower = weapon_category.lower().strip()
        column_name = WEAPON_MAPPING.get(weapon_category_lower)
        
        if not column_name:
            available_categories = sorted(set(WEAPON_MAPPING.keys()))
            await interaction.followup.send(
                f"❌ Unknown weapon category: `{weapon_category}`. Available categories: {', '.join(sorted(available_categories))}",
                ephemeral=True
            )
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
        
        default_timeframe = "30d"
        default_days = TIMEFRAME_OPTIONS[default_timeframe]["days"]
        
        logger.info(f"Querying top kills for weapon: {weapon_category_lower}")
        
        results = await fetch_weapon_leaderboard(
            weapon_category_lower, only_pathfinders, default_days
        )
        
        if not results:
            await interaction.followup.send(
                f"❌ No kills found for `{weapon_category}`.",
                ephemeral=True
            )
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        async def fetch_data(days: int) -> List[Dict[str, Any]]:
            return await fetch_weapon_leaderboard(
                weapon_category_lower, only_pathfinders, days
            )
        
        def format_value(value):
            return f"{int(value):,}"
        
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - {weapon_category}{filter_text}"
        
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key="total_kills",
            value_label="Kills",
            color=PATHFINDER_COLOR,
            format_value=format_value,
            current_timeframe=default_timeframe,
            fetch_data_func=fetch_data,
            show_timeframe_in_title=True
        )
        log_command_completion("leaderboard weapon", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
