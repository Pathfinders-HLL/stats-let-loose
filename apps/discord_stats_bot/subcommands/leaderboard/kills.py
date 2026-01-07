"""
Leaderboard topkills subcommand - Get top players by sum of kills from their top 25 matches (with kill type filtering).
"""

import logging
import time
from typing import List

import asyncpg
import discord
from discord import app_commands

from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    validate_choice_parameter,
    create_time_filter_params,
    command_wrapper,
    get_pathfinder_player_ids
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


def register_kills_subcommand(leaderboard_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the topkills subcommand with the leaderboard group.
    
    Args:
        leaderboard_group: The leaderboard command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @leaderboard_group.command(name="kills", description="Get top players by sum of kills from their top 25 matches (with kill type filtering)")
    @app_commands.describe(
        kill_type="(Optional) The kill type to filter by (All Kills, Infantry Kills, Armor Kills, Artillery Kills)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)",
        only_pathfinders="(Optional) If true, only show Pathfinder players (default: false)"
    )
    @app_commands.autocomplete(kill_type=kill_type_autocomplete)
    @command_wrapper("leaderboard kills", channel_check=channel_check)
    async def leaderboard_topkills(interaction: discord.Interaction, kill_type: str = "all", over_last_days: int = 30, only_pathfinders: bool = False):
        """Get top players by sum of kills from their top 25 matches with optional kill type filtering."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            return

        # Validate kill_type
        try:
            kill_type_lower = validate_choice_parameter(
                "kill type", kill_type, {"all", "infantry", "armor", "artillery"},
                ["All Kills", "Infantry Kills", "Armor Kills", "Artillery Kills"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
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

        # Calculate time period filter
        time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            escaped_column = escape_sql_identifier(kill_column)
            
            # Get pathfinder player IDs from file if needed
            pathfinder_ids = get_pathfinder_player_ids() if only_pathfinders else set()
            pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
            
            # Build query components conditionally
            param_num = 1
            query_params = []
            
            # Build FROM clause - include JOIN if time filtering is needed
            if base_query_params:
                from_clause = """FROM pathfinder_stats.player_match_stats pms
                        INNER JOIN pathfinder_stats.match_history mh
                            ON pms.match_id = mh.match_id"""
                time_where = f"WHERE mh.start_time >= ${param_num}"
                query_params.extend(base_query_params)
                param_num += len(base_query_params)
            else:
                from_clause = "FROM pathfinder_stats.player_match_stats pms"
                time_where = ""
            
            # Build pathfinder filter WHERE clause
            pathfinder_where = ""
            if only_pathfinders:
                if pathfinder_ids:
                    if time_where:
                        pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    else:
                        pathfinder_where = f"WHERE (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                    param_num += 3
                else:
                    if time_where:
                        pathfinder_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    else:
                        pathfinder_where = f"WHERE (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
                    param_num += 2
            
            # Combine WHERE clauses
            # Build the WHERE clause properly - start with WHERE if no conditions exist yet
            where_clauses = []
            if time_where:
                where_clauses.append(time_where)
            if pathfinder_where:
                # pathfinder_where already contains WHERE or AND as needed
                # But if we already have a WHERE clause, ensure it starts with AND
                if where_clauses and pathfinder_where.strip().startswith("WHERE"):
                    pathfinder_where = pathfinder_where.replace("WHERE", "AND", 1)
                where_clauses.append(pathfinder_where)
            
            # Add the kill column filter
            kill_filter = f"pms.{escaped_column} > 0"
            if where_clauses:
                where_clauses.append(f"AND {kill_filter}")
            else:
                where_clauses.append(f"WHERE {kill_filter}")
            
            ranked_matches_where = " ".join(where_clauses)
            
            # Build LATERAL join pathfinder filter (for getting player names)
            lateral_where = ""
            if only_pathfinders:
                if pathfinder_ids:
                    lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1} OR pms.player_id = ANY(${param_num + 2}::text[]))"
                    query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                else:
                    lateral_where = f"AND (pms.player_name ILIKE ${param_num} OR pms.player_name ILIKE ${param_num + 1})"
                    query_params.extend(["PFr |%", "PF |%"])
            
            # Build query to get top players by sum of kills from their top 25 matches
            # This uses a window function to rank matches per player, then sums the top 25
            query = f"""
                    WITH ranked_matches AS (
                        SELECT
                            pms.player_id,
                            pms.{escaped_column} as kill_count,
                            ROW_NUMBER() OVER (PARTITION BY pms.player_id ORDER BY pms.{escaped_column} DESC) as match_rank
                        {from_clause}
                        {ranked_matches_where}
                    ),
                    top25_per_player AS (
                        SELECT
                            player_id,
                            SUM(kill_count) as total_kills_top25
                        FROM ranked_matches
                        WHERE match_rank <= 25
                        GROUP BY player_id
                    ),
                    top_players AS (
                        SELECT
                            tpp.player_id,
                            tpp.total_kills_top25
                        FROM top25_per_player tpp
                        ORDER BY tpp.total_kills_top25 DESC
                        LIMIT 25
                    )
                    SELECT
                        tp.player_id,
                        COALESCE(rn.player_name, tp.player_id) as player_name,
                        tp.total_kills_top25
                    FROM top_players tp
                    LEFT JOIN LATERAL (
                        SELECT pms.player_name
                        FROM pathfinder_stats.player_match_stats pms
                        INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                        WHERE pms.player_id = tp.player_id
                            {lateral_where}
                        ORDER BY mh.start_time DESC
                        LIMIT 1
                    ) rn ON TRUE
                    ORDER BY tp.total_kills_top25 DESC
                """
            
            logger.info(f"Querying top players by sum of {display_name} from top 25 matches{time_period_text}")
            
            # Log SQL query and parameters for debugging
            logger.info(f"SQL Query: {query}")
            logger.info(f"SQL Parameters: {query_params}")
            
            results = await conn.fetch(query, *query_params)
                    
        if not results:
            await interaction.followup.send(
                f"❌ No data found for `{display_name}` from top 25 matches{time_period_text}."
            )
            return

        # Format results
        leaderboard_lines = []
        filter_text = " (Pathfinders Only)" if only_pathfinders else ""
        leaderboard_lines.append(f"## Top Players - Sum of {display_name} from Top 25 Matches{time_period_text}{filter_text}\n")

        for rank, row in enumerate(results, 1):
            # Use player_name if available, otherwise use player_id
            display_player_name = row['player_name'] if row['player_name'] else row['player_id']
            total_kills = row['total_kills_top25']
            leaderboard_lines.append(
                f"{rank}. **{display_player_name}** - {total_kills:,} {display_name.lower()}"
            )

        # Discord message limit is 2000 characters
        message = "\n".join(leaderboard_lines)
        if len(message) > 2000:
            # Truncate if needed
            message = message[:1997] + "..."

        await interaction.followup.send(message)
        log_command_completion("leaderboard kills", command_start_time, success=True, interaction=interaction, kwargs={"kill_type": kill_type, "over_last_days": over_last_days, "only_pathfinders": only_pathfinders})

