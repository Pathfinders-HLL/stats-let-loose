"""
Player contributions subcommand - Get top 25 matches for a player by score.
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


# Score type choices for autocomplete
SCORE_TYPE_CHOICES = [
    app_commands.Choice(name="Support Score", value="support"),
    app_commands.Choice(name="Attack Score", value="attack"),
    app_commands.Choice(name="Defense Score", value="defense"),
    app_commands.Choice(name="Combat Score", value="combat"),
]


async def score_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete function for score_type parameter."""
    current_lower = current.lower()
    matching = [
        choice for choice in SCORE_TYPE_CHOICES
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]  # Discord limit


def register_contributions_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """
    Register the contributions subcommand with the player group.
    
    Args:
        player_group: The player command group to register the subcommand with
        channel_check: Optional function to check if the channel is allowed
    """
    @player_group.command(name="contributions", description="Get top 25 matches for a player by score")
    @app_commands.describe(
        score_type="The score type to filter by (Support Score, Attack Score, Defense Score, Combat Score)",
        player="(Optional) The player ID or player name (optional if you've set one with /profile setid)",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(score_type=score_type_autocomplete)
    @command_wrapper("player contributions", channel_check=channel_check)
    async def player_contributions(interaction: discord.Interaction, score_type: str, player: str = None, over_last_days: int = 30):
        """Get top 25 matches for a player by score with optional score type filtering."""
        command_start_time = time.time()

        # Validate over_last_days (allow 0 for all-time, but otherwise 1-180)
        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "player": player, "over_last_days": over_last_days})
            return

        # If player not provided, try to get stored one from cache
        if not player:
            stored_player_id = await get_player_id(interaction.user.id)
            if stored_player_id:
                player = stored_player_id
            else:
                await interaction.followup.send("❌ No player ID provided and you haven't set one! Either provide a player ID/name, or use `/profile setid` to set a default.", ephemeral=True)
                return

        # Validate score_type
        try:
            score_type_lower = validate_choice_parameter(
                "score type", score_type, {"support", "attack", "defense", "combat"},
                ["Support Score", "Attack Score", "Defense Score", "Combat Score"]
            )
        except ValueError as e:
            await interaction.followup.send(str(e))
            log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "player": player, "over_last_days": over_last_days})
            return
            
        # Map score type to column name and display name
        score_type_config = {
            "support": {
                "column": "support_score",
                "display_name": "Support Score"
            },
            "attack": {
                "column": "offense_score",
                "display_name": "Attack Score"
            },
            "defense": {
                "column": "defense_score",
                "display_name": "Defense Score"
            },
            "combat": {
                "column": "combat_score",
                "display_name": "Combat Score"
            }
        }

        config = score_type_config[score_type_lower]
        score_column = config["column"]
        display_name = config["display_name"]

        # Connect to database and query
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            # Find player by ID or name
            player_id, found_player_name = await find_player_by_id_or_name(conn, player)

            if not player_id:
                await interaction.followup.send(f"❌ Could not find user: `{player}`. Try using a player ID or exact player name.")
                log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "player": player, "over_last_days": over_last_days})
                return

            # Calculate time period filter
            time_filter, base_query_params, time_period_text = create_time_filter_params(over_last_days)
            
            # Adjust parameter number in time_filter if we have base params
            # Since player_id is $1, time_threshold needs to be $2
            if base_query_params:
                time_filter = time_filter.replace("$1", "$2")
            
            query_params = [player_id] + base_query_params
                    
            # Build query to get top 25 matches by score type
            escaped_column = escape_sql_identifier(score_column)
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column} as score_value,
                    pms.total_kills,
                    pms.total_deaths
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
                log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs={"score_type": score_type, "player": player, "over_last_days": over_last_days})
                return
                    
        # Calculate total score from top 25 matches
        total_score_top25 = sum(row['score_value'] for row in results)

        # Format results as a table
        display_player_name = found_player_name if found_player_name else player
        
        # Prepare data for table formatting
        table_data = []
        for row in results:
            score_value = int(row['score_value'])
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
                score_value,
                kills,
                deaths,
                start_time_str
            ])

        # Headers
        headers = ["Map", display_name, "Kills", "Deaths", "Date"]
        
        # Build message, removing rows if needed to fit Discord's 2000 character limit
        message_prefix_lines = [
            f"## Top 25 Matches - {display_player_name} ({display_name}){time_period_text}\n"
        ]
        
        # Try with all rows first
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
                message_lines.append(f"\n*Showing {num_rows} of {len(table_data)} matches (message length limit)*")
            
            message = "\n".join(message_lines)
            
            if len(message) <= 2000:
                break

        await interaction.followup.send(message)
        log_command_completion("player contributions", command_start_time, success=True, interaction=interaction, kwargs={"score_type": score_type, "player": player, "over_last_days": over_last_days})

