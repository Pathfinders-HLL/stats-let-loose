"""
Scheduled task to post top Karabiner 98k kills leaderboard.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord
from discord.ext import tasks

from apps.discord_stats_bot.common.shared import get_readonly_db_pool, escape_sql_identifier, get_pathfinder_player_ids
from apps.discord_stats_bot.common.weapon_autocomplete import get_weapon_mapping
from apps.discord_stats_bot.config import get_bot_config

logger = logging.getLogger(__name__)

_bot_instance: Optional[discord.Client] = None


@tasks.loop(minutes=30)
async def post_karabiner_stats():
    """Post top Karabiner 98k kills leaderboard, editing the previous message if possible."""
    global _bot_instance
    
    if not _bot_instance:
        logger.error("Bot instance not set")
        return
    
    try:
        # Get stats channel ID from config
        bot_config = get_bot_config()
        stats_channel_id = bot_config.stats_channel_id
        
        if not stats_channel_id:
            logger.warning("DISCORD_STATS_CHANNEL_ID not configured, skipping stats posting")
            return
        
        # Get the channel
        channel = _bot_instance.get_channel(stats_channel_id)
        if not channel:
            logger.error(f"Channel {stats_channel_id} not found")
            return
        
        # Get weapon mapping to find the column name
        weapon_mapping = get_weapon_mapping()
        column_name = weapon_mapping.get("karabiner 98k")
        
        if not column_name:
            logger.error("Could not find Karabiner 98k in weapon mapping")
            return
        
        # Calculate 7 days ago and 24 hours ago
        # Use naive datetimes for PostgreSQL TIMESTAMP columns
        now_utc_aware = datetime.now(timezone.utc)
        now_utc = now_utc_aware.replace(tzinfo=None)
        seven_days_ago = now_utc - timedelta(days=7)
        twenty_four_hours_ago = now_utc - timedelta(hours=24)
        
        # Get pathfinder player IDs (always filter to pathfinders only)
        pathfinder_ids = get_pathfinder_player_ids()
        pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
        
        # Query database for top players
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            escaped_column = escape_sql_identifier(column_name)
            
            # Build pathfinder filter WHERE clause for 7-day query
            query_params_7day = [seven_days_ago]
            
            if pathfinder_ids:
                pathfinder_where = f"AND (pks.player_name LIKE $2 OR pks.player_name LIKE $3 OR pks.player_id = ANY($4::text[]))"
                query_params_7day.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                recent_names_where = f"AND (pms.player_name LIKE $5 OR pms.player_name LIKE $6 OR pms.player_id = ANY($7::text[]))"
                query_params_7day.extend(["PFr |%", "PF |%", pathfinder_ids_list])
            else:
                pathfinder_where = f"AND (pks.player_name LIKE $2 OR pks.player_name LIKE $3)"
                query_params_7day.extend(["PFr |%", "PF |%"])
                recent_names_where = f"AND (pms.player_name LIKE $4 OR pms.player_name LIKE $5)"
                query_params_7day.extend(["PFr |%", "PF |%"])
            
            # Query for 7-day aggregated stats
            query_7day = f"""
                WITH kill_stats AS (
                    SELECT 
                        pks.player_id,
                        SUM(pks.{escaped_column}) as total_kills
                    FROM pathfinder_stats.player_kill_stats pks
                    INNER JOIN pathfinder_stats.match_history mh
                        ON pks.match_id = mh.match_id
                    WHERE mh.start_time >= $1
                        {pathfinder_where}
                    GROUP BY pks.player_id
                    HAVING SUM(pks.{escaped_column}) > 0
                ),
                recent_names AS (
                    SELECT DISTINCT ON (pms.player_id)
                        pms.player_id,
                        pms.player_name
                    FROM pathfinder_stats.player_match_stats pms
                    INNER JOIN pathfinder_stats.match_history mh ON pms.match_id = mh.match_id
                    INNER JOIN kill_stats ks ON pms.player_id = ks.player_id
                    WHERE mh.start_time >= $1
                        {recent_names_where}
                    ORDER BY pms.player_id, mh.start_time DESC
                )
                SELECT 
                    ks.player_id,
                    COALESCE(rn.player_name, ks.player_id) as player_name,
                    ks.total_kills
                FROM kill_stats ks
                LEFT JOIN recent_names rn ON ks.player_id = rn.player_id
                ORDER BY ks.total_kills DESC
                LIMIT 10
            """
            
            results_7day = await conn.fetch(query_7day, *query_params_7day)
            
            # Query for top 10 players from past 24 hours with match details
            query_params_daily = [twenty_four_hours_ago]
            
            if pathfinder_ids:
                daily_pathfinder_where = f"AND (pks.player_name LIKE $2 OR pks.player_name LIKE $3 OR pks.player_id = ANY($4::text[]))"
                query_params_daily.extend(["PFr |%", "PF |%", pathfinder_ids_list])
            else:
                daily_pathfinder_where = f"AND (pks.player_name LIKE $2 OR pks.player_name LIKE $3)"
                query_params_daily.extend(["PFr |%", "PF |%"])
            
            query_daily = f"""
                WITH daily_kills AS (
                    SELECT 
                        pks.player_id,
                        pks.match_id,
                        pks.{escaped_column} as karabiner_kills
                    FROM pathfinder_stats.player_kill_stats pks
                    INNER JOIN pathfinder_stats.match_history mh
                        ON pks.match_id = mh.match_id
                    WHERE mh.start_time >= $1
                        AND pks.{escaped_column} > 0
                        {daily_pathfinder_where}
                ),
                best_match_per_player AS (
                    SELECT DISTINCT ON (dk.player_id)
                        dk.player_id,
                        dk.match_id,
                        dk.karabiner_kills
                    FROM daily_kills dk
                    ORDER BY dk.player_id, dk.karabiner_kills DESC
                ),
                top_10_players AS (
                    SELECT 
                        bmp.player_id,
                        bmp.match_id,
                        bmp.karabiner_kills
                    FROM best_match_per_player bmp
                    ORDER BY bmp.karabiner_kills DESC
                    LIMIT 10
                )
                SELECT 
                    t10.player_id,
                    COALESCE(pms.player_name, t10.player_id) as player_name,
                    pms.total_kills,
                    pms.total_deaths,
                    pms.kills_per_minute as kpm,
                    pms.kill_death_ratio as kdr,
                    mh.map_name,
                    t10.karabiner_kills
                FROM top_10_players t10
                INNER JOIN pathfinder_stats.player_match_stats pms 
                    ON t10.player_id = pms.player_id AND t10.match_id = pms.match_id
                INNER JOIN pathfinder_stats.match_history mh ON t10.match_id = mh.match_id
                ORDER BY t10.karabiner_kills DESC
            """
            
            results_daily = await conn.fetch(query_daily, *query_params_daily)

        # Get current timestamp for the update (UTC, Discord format)
        unix_timestamp = int(now_utc_aware.timestamp())
        discord_time = f"<t:{unix_timestamp}:F>"

        message_lines = []
        
        # Format 7-day aggregated stats
        if not results_7day:
            message_lines.append("### Top Pathfinder Karabiner 98k Kills (Last 7 Days)\n")
            message_lines.append("No kills found in the last 7 days.\n")
        else:
            message_lines.append("### Top Pathfinder Karabiner 98k Kills (Last 7 Days)\n")
            message_lines.append("```")
            message_lines.append(f"{'#':<4} {'Player':<30} {'Kills':>8}")
            message_lines.append("-" * 44)

            for rank, row in enumerate(results_7day, 1):
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                # Truncate name if too long
                if len(display_name) > 28:
                    display_name = display_name[:25] + "..."
                
                kills = row['total_kills']
                
                message_lines.append(
                    f"{rank:<4} {display_name:<30} {kills:>8,}"
                )

            message_lines.append("```")
        
        # Format daily top 10 with match details
        if results_daily:
            message_lines.append("\n### Top 10 Players Today\n")
            message_lines.append("```")
            message_lines.append(f"{'#':<4} {'Player':<25} {'Kills':>7} {'Deaths':>7} {'K/M':>7} {'K/D':>7} {'Map':<20}")
            message_lines.append("-" * 75)

            for rank, row in enumerate(results_daily, 1):
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                # Truncate name if too long
                if len(display_name) > 23:
                    display_name = display_name[:20] + "..."
                
                kills = row['total_kills']
                deaths = row['total_deaths']
                kpm = float(row['kpm']) if row['kpm'] else 0.0
                kdr = float(row['kdr']) if row['kdr'] else 0.0
                map_name = row['map_name'] if row['map_name'] else 'Unknown'
                # Truncate map name if too long
                if len(map_name) > 18:
                    map_name = map_name[:15] + "..."
                
                message_lines.append(
                    f"{rank:<4} {display_name:<25} {kills:>7} {deaths:>7} {kpm:>6.2f} {kdr:>6.2f} {map_name:<20}"
                )

            message_lines.append("```")
        else:
            message_lines.append("\n### Top 10 Players Today\n")
            message_lines.append("No matches found in the past 24 hours.\n")
        
        message_lines.append(f"\n*Last updated: {discord_time}*")
        message_content = "\n".join(message_lines)

        # Try to find and edit the last message sent by the bot in this channel
        try:
            logger.info("Attempting to read channel history to find last message...")
            last_message = None
            async for message in channel.history(limit=1):
                last_message = message
                break

            if last_message:
                logger.info(f"Found last message: ID={last_message.id}, Author={last_message.author}")
                # Check if the last message exists and was sent by this bot
                if last_message.author == _bot_instance.user:
                    logger.info("Last message is from this bot, attempting to edit...")
                    await last_message.edit(content=message_content)
                    logger.info(f"Successfully edited Karabiner 98k stats message {last_message.id}")
                    return  # Successfully edited, no need to send new message
                else:
                    logger.info(f"Last message is from {last_message.author}, will send new message instead")
            else:
                logger.info("No messages found in channel history")

        except discord.Forbidden as e:
            logger.error(f"Forbidden error when reading channel history or editing message: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error finding/editing last message: {e}", exc_info=True)

        # Send a new message if editing failed or wasn't possible
        logger.info("Sending new message to channel...")
        new_message = await channel.send(message_content)
        logger.info(
            f"Successfully posted Karabiner 98k stats to channel {stats_channel_id} "
            f"(message ID: {new_message.id})"
        )
        
    except Exception as e:
        logger.error(f"Error in post_karabiner_stats task: {e}", exc_info=True)


def setup_karabiner_stats_task(bot: discord.Client) -> None:
    """Start the scheduled stats posting task."""
    global _bot_instance
    _bot_instance = bot
    
    @post_karabiner_stats.before_loop
    async def before_karabiner_stats():
        await bot.wait_until_ready()
    
    if not post_karabiner_stats.is_running():
        post_karabiner_stats.start()
        logger.info("Started Karabiner 98k stats task (every 30 min)")
    else:
        logger.warning("Karabiner 98k stats task already running")