"""
Player contributions subcommand - Get top 25 matches for a player by score.
"""

import logging
import time

import discord

from discord import app_commands

from apps.discord_stats_bot.common import (
    get_readonly_db_pool,
    log_command_completion,
    escape_sql_identifier,
    validate_over_last_days,
    validate_choice_parameter,
    build_player_time_query_params,
    command_wrapper,
    score_type_autocomplete,
    format_time_seconds,
    format_date,
    build_table_message,
    lookup_player,
    SCORE_TYPE_CONFIG,
    SCORE_TYPE_VALID_VALUES,
    SCORE_TYPE_DISPLAY_LIST,
)

logger = logging.getLogger(__name__)


def register_contributions_subcommand(player_group: app_commands.Group, channel_check=None) -> None:
    """Register the contributions subcommand with the player group."""
    
    @player_group.command(
        name="contributions", 
        description="Get top 25 matches for a player by score"
    )
    @app_commands.describe(
        score_type="The score type to filter by",
        player="(Optional) The player ID or player name",
        over_last_days="(Optional) Number of days to look back (default: 30, use 0 for all-time)"
    )
    @app_commands.autocomplete(score_type=score_type_autocomplete)
    @command_wrapper("player contributions", channel_check=channel_check)
    async def player_contributions(
        interaction: discord.Interaction, 
        score_type: str, 
        player: str = None, 
        over_last_days: int = 30
    ):
        """Get top 25 matches for a player by score."""
        command_start_time = time.time()
        log_kwargs = {"score_type": score_type, "player": player, "over_last_days": over_last_days}

        try:
            validate_over_last_days(over_last_days)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return

        try:
            score_type_lower = validate_choice_parameter(
                "score type", score_type, SCORE_TYPE_VALID_VALUES, SCORE_TYPE_DISPLAY_LIST
            )
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
            return
            
        config = SCORE_TYPE_CONFIG[score_type_lower]
        score_column = config["column"]
        display_name = config["display_name"]

        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            player_result, error = await lookup_player(conn, interaction.user.id, player)
            if error:
                await interaction.followup.send(error, ephemeral=True)
                log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            player_id = player_result.player_id
            time_filter, query_params, time_period_text = build_player_time_query_params(player_id, over_last_days)
                    
            escaped_column = escape_sql_identifier(score_column)
            
            # Special handling for seeding: filter by player count (2-59 players)
            seeding_filter = ""
            if score_type_lower == "seeding":
                seeding_filter = "AND mh.player_count > 1 AND mh.player_count < 60"
            
            query = f"""
                SELECT
                    pms.match_id,
                    mh.map_name,
                    mh.start_time,
                    pms.{escaped_column} as score,
                    pms.total_kills,
                    pms.total_deaths,
                    pms.kill_death_ratio as kdr
                FROM pathfinder_stats.player_match_stats pms
                INNER JOIN pathfinder_stats.match_history mh
                    ON pms.match_id = mh.match_id
                WHERE pms.player_id = $1
                    {time_filter}
                    {seeding_filter}
                    AND pms.{escaped_column} > 0
                ORDER BY pms.{escaped_column} DESC
                LIMIT 25
            """

            logger.info(f"Querying top 25 matches for player {player_id} by {display_name}")
            results = await conn.fetch(query, *query_params)

            if not results:
                await interaction.followup.send(
                    f"❌ No matches found for player `{player_result.display_name}` "
                    f"with {display_name.lower()}{time_period_text}.",
                    ephemeral=True
                )
                log_command_completion("player contributions", command_start_time, success=False, interaction=interaction, kwargs=log_kwargs)
                return
            
            # Format score differently for seeding (time) vs other scores
            format_score = format_time_seconds if score_type_lower == "seeding" else lambda x: str(int(x))
            
            table_data = []
            for rank, row in enumerate(results, 1):
                score = int(row['score'])
                kills = int(row['total_kills'])
                deaths = int(row['total_deaths'])
                kdr = float(row['kdr'])

                start_time_str = format_date(row['start_time'])

                table_data.append([
                    rank,
                    row['map_name'],
                    format_score(score),
                    kills,
                    deaths,
                    f"{kdr:.2f}",
                    start_time_str
                ])

            headers = ["#", "Map", "Score", "Kills", "Deaths", "K/D", "Date"]
            message_prefix_lines = [f"## Top 25 Matches - {player_result.display_name} ({display_name}){time_period_text}"]
            
            message = build_table_message(
                table_data=table_data,
                headers=headers,
                message_prefix_lines=message_prefix_lines,
                item_name="matches"
            )

            await interaction.followup.send(message, ephemeral=True)
            log_command_completion("player contributions", command_start_time, success=True, interaction=interaction, kwargs=log_kwargs)
