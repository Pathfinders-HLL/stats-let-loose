"""
Leaderboard weapon subcommand - Get top players by weapon kills over a time period.
"""

import logging
import time
from typing import List, Dict, Any

import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
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
)
from apps.discord_stats_bot.common.weapon_autocomplete import weapon_category_autocomplete, get_weapon_mapping
from apps.discord_stats_bot.common.leaderboard_pagination import (
    send_paginated_leaderboard,
    TOP_PLAYERS_LIMIT,
    TIMEFRAME_OPTIONS,
)

logger = logging.getLogger(__name__)

# Load weapon mapping at module level
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
    
    # Calculate time period filter
    time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
    
    # Connect to database and query
    pool = await get_readonly_db_pool()
    async with pool.acquire() as conn:
        # Build query with safe identifier escaping
        escaped_column = escape_sql_identifier(column_name)
        
        # Get pathfinder player IDs from file if needed
        pathfinder_ids_list = list(get_pathfinder_player_ids()) if only_pathfinders else []
        
        # Build query components
        param_num = 1
        query_params = []
        
        # Build FROM clause with optional time filter JOIN
        from_clause, _ = build_from_clause_with_time_filter(
            "pathfinder_stats.player_kill_stats", "pks", bool(base_query_params)
        )
        
        # Build time filter WHERE clause
        time_where = ""
        if base_query_params:
            time_where = f"WHERE mh.start_time >= ${param_num}"
            query_params.extend(base_query_params)
            param_num += len(base_query_params)
        
        # Build pathfinder filter (uses pks alias for player_kill_stats)
        pathfinder_where = ""
        if only_pathfinders:
            pathfinder_where, pf_params, param_num = build_pathfinder_filter(
                "pks", param_num, pathfinder_ids_list, use_and=bool(time_where)
            )
            query_params.extend(pf_params)
        
        # Combine WHERE clauses
        kill_stats_where = build_where_clause(time_where, pathfinder_where)
        
        # Build LATERAL join pathfinder filter (uses pms alias for player_match_stats)
        lateral_where = ""
        if only_pathfinders:
            lateral_where, lateral_params, param_num = build_pathfinder_filter(
                "pms", param_num, pathfinder_ids_list, use_and=True
            )
            query_params.extend(lateral_params)
        
        # Build LATERAL JOIN for player name lookup
        lateral_join = build_lateral_name_lookup("tks.player_id", lateral_where)
        
        # Build the query using conditional components
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
        
        # Convert to list of dicts
        return [dict(row) for row in results]


def register_weapon_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the weapon subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="weapon", description="Get top players by weapon kills over a time period")
    @app_commands.describe(
        weapon_category="The weapon category (e.g., 'M1 Garand', 'Thompson', 'Sniper')",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(weapon_category=weapon_category_autocomplete)
    @command_wrapper("leaderboard weapon", channel_check=channel_check)
    async def leaderboard_weapon(interaction: discord.Interaction, weapon_category: str, only_pathfinders: bool = False):
        """Get top players by weapon kills over a time period."""
        command_start_time = time.time()
        
        # Map friendly name to database column name
        weapon_category_lower = weapon_category.lower().strip()
        column_name = WEAPON_MAPPING.get(weapon_category_lower)
        
        if not column_name:
            # List available weapon categories
            available_categories = sorted(set(WEAPON_MAPPING.keys()))
            await interaction.followup.send(
                f"❌ Unknown weapon category: `{weapon_category}`. Available categories: {', '.join(sorted(available_categories))}",
                ephemeral=True
            )
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
            return
        
        # Default timeframe
        default_timeframe = "30d"
        default_days = TIMEFRAME_OPTIONS[default_timeframe]["days"]
        
        logger.info(f"Querying top kills for weapon: {weapon_category_lower} (column: {column_name})")
        
        # Fetch initial data
        results = await fetch_weapon_leaderboard(
            weapon_category_lower, only_pathfinders, default_days
        )
        
        if not results:
            await interaction.followup.send(
                f"❌ No kills found for `{weapon_category}`.",
                ephemeral=True
            )
            log_command_completion("leaderboard weapon", command_start_time, success=False, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
            return

        # Create fetch function for timeframe changes
        async def fetch_data(days: int) -> List[Dict[str, Any]]:
            return await fetch_weapon_leaderboard(
                weapon_category_lower, only_pathfinders, days
            )
        
        # Format value function
        def format_value(value):
            return f"{int(value):,}"
        
        # Build title
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        title = f"Top Players - {weapon_category}{filter_text}"
        
        # Send paginated leaderboard using user's format preference
        await send_paginated_leaderboard(
            interaction=interaction,
            results=results,
            title_template=title,
            value_key="total_kills",
            value_label="Kills",
            color=discord.Color.from_rgb(16, 74, 0),
            format_value=format_value,
            current_timeframe=default_timeframe,
            fetch_data_func=fetch_data,
            show_timeframe_in_title=True
        )
        log_command_completion("leaderboard weapon", command_start_time, success=True, interaction=interaction, kwargs={"weapon_category": weapon_category, "only_pathfinders": only_pathfinders})
