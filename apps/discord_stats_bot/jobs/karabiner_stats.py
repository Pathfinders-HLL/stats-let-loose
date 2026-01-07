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
        
        # Calculate 7 days ago
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        
        # Get pathfinder player IDs (always filter to pathfinders only)
        pathfinder_ids = get_pathfinder_player_ids()
        pathfinder_ids_list = list(pathfinder_ids) if pathfinder_ids else []
        
        # Query database for top players
        pool = await get_readonly_db_pool()
        async with pool.acquire() as conn:
            escaped_column = escape_sql_identifier(column_name)
            
            # Build pathfinder filter WHERE clause
            query_params = [seven_days_ago]
            
            if pathfinder_ids:
                pathfinder_where = f"AND (pks.player_name ILIKE $2 OR pks.player_name ILIKE $3 OR pks.player_id = ANY($4::text[]))"
                query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
                recent_names_where = f"AND (pms.player_name ILIKE $5 OR pms.player_name ILIKE $6 OR pms.player_id = ANY($7::text[]))"
                query_params.extend(["PFr |%", "PF |%", pathfinder_ids_list])
            else:
                pathfinder_where = f"AND (pks.player_name ILIKE $2 OR pks.player_name ILIKE $3)"
                query_params.extend(["PFr |%", "PF |%"])
                recent_names_where = f"AND (pms.player_name ILIKE $4 OR pms.player_name ILIKE $5)"
                query_params.extend(["PFr |%", "PF |%"])
            
            query = f"""
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
                LIMIT 25
            """
            
            results = await conn.fetch(query, *query_params)

        # Get current timestamp for the update (UTC, Discord format)
        now_utc = datetime.now(timezone.utc)
        unix_timestamp = int(now_utc.timestamp())
        discord_time = f"<t:{unix_timestamp}:F>"

        if not results:
            message_content = (
                "## Top Pathfinder Karabiner 98k Kills (Last 7 Days)\n"
                "No kills found in the last 7 days.\n"
                f"*Last updated: {discord_time}*"
            )
        else:
            # Format the message
            message_lines = ["## Top Pathfinder Karabiner 98k Kills (Last 7 Days)\n"]

            for rank, row in enumerate(results, 1):
                display_name = row['player_name'] if row['player_name'] else row['player_id']
                message_lines.append(
                    f"{rank}. **{display_name}** - {row['total_kills']:,} kills"
                )

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