"""
Discord message posting for Pathfinder leaderboards.

Handles:
- Posting new leaderboard messages
- Editing existing leaderboard messages
- Finding existing messages in channel history
"""

import logging

import discord

from datetime import datetime, timezone
from discord.ext import tasks
from typing import Optional

from apps.discord_stats_bot.common import build_compact_leaderboard_embed
from apps.discord_stats_bot.common.constants import (
    DEFAULT_COMPACT_VIEW_PLAYERS,
    LEADERBOARD_STAT_CONFIGS,
)
from apps.discord_stats_bot.bot_config import get_bot_config
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_cache import (
    get_leaderboard_cache,
    get_stored_message_state,
    _save_leaderboard_state,
    _clear_sql_logs,
    _write_sql_logs_to_file,
)
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_queries import fetch_all_leaderboard_stats
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_ui import LeaderboardView

logger = logging.getLogger(__name__)

# Bot instance reference (set by job.py)
_bot_instance: Optional[discord.Client] = None


def set_bot_instance(bot: discord.Client) -> None:
    """Set the bot instance for posting messages."""
    global _bot_instance
    _bot_instance = bot


def get_bot_instance() -> Optional[discord.Client]:
    """Get the bot instance."""
    return _bot_instance


@tasks.loop(minutes=30)
async def post_pathfinder_leaderboards():
    """Post comprehensive Pathfinder leaderboards, editing the previous message if possible."""
    global _bot_instance
    
    if not _bot_instance:
        logger.error("Bot instance not set for pathfinder leaderboards")
        return
    
    try:
        bot_config = get_bot_config()
        stats_channel_id = bot_config.stats_channel_id
        
        if not stats_channel_id:
            logger.warning("DISCORD_STATS_CHANNEL_ID not configured, skipping leaderboard posting")
            return
        
        channel = _bot_instance.get_channel(stats_channel_id)
        if not channel:
            logger.error(f"Channel {stats_channel_id} not found")
            return
        
        # Use cached data for default 7-day period
        leaderboard_cache = get_leaderboard_cache()
        cached_7d = leaderboard_cache.get("7d")
        now_utc = datetime.now(timezone.utc)
        
        if cached_7d and cached_7d.get("stats"):
            stats = cached_7d["stats"]
            cache_timestamp = cached_7d.get("timestamp", now_utc)
            logger.info("Using cached stats for compact leaderboard posting")
        else:
            # Fallback: compute on-demand if cache is empty
            logger.warning("Cache empty, computing 7d leaderboard stats on-demand")
            # Clear logs before fetching to start fresh for this on-demand fetch
            _clear_sql_logs()
            stats = await fetch_all_leaderboard_stats(days=7)
            # Write logs after on-demand fetch
            _write_sql_logs_to_file()
            cache_timestamp = now_utc
        
        # Build compact embed with 2 stats per row
        compact_embed = build_compact_leaderboard_embed(
            stats, LEADERBOARD_STAT_CONFIGS, "Last 7 Days", cache_timestamp, DEFAULT_COMPACT_VIEW_PLAYERS
        )
        embeds = [compact_embed]
        
        # Create the view with Advanced View button
        view = LeaderboardView()
        
        # Get current timestamp for the update
        unix_timestamp = int(now_utc.timestamp())
        discord_time = f"<t:{unix_timestamp}:F>"
        
        header_content = (
            f"*Last updated: {discord_time}*\n"
            "*Click the button below for advanced filtering and pagination*"
        )
        
        # Try to edit the stored message ID first
        stored_msg_id, stored_chan_id = await get_stored_message_state()
        
        if stored_msg_id and stored_chan_id == stats_channel_id:
            try:
                logger.info(f"Attempting to edit stored leaderboard message: {stored_msg_id}")
                stored_message = await channel.fetch_message(stored_msg_id)
                
                if stored_message and stored_message.author == _bot_instance.user:
                    await stored_message.edit(
                        content=header_content,
                        embeds=embeds,
                        view=view
                    )
                    logger.info(f"Successfully edited stored leaderboard message {stored_msg_id}")
                    return
                else:
                    logger.warning(f"Stored message {stored_msg_id} not found or not owned by bot, will create new")
                    
            except discord.NotFound:
                logger.info(f"Stored message {stored_msg_id} not found (may have been deleted), will create new")
            except discord.Forbidden:
                logger.warning(f"No permission to edit stored message {stored_msg_id}, will create new")
            except Exception as e:
                logger.warning(f"Error editing stored message {stored_msg_id}: {e}, will create new")
        
        # Fallback: Try to find and edit the last bot message in channel history
        try:
            logger.info("Looking for existing leaderboard message in channel history...")
            last_message = None
            
            # Look through recent messages to find our leaderboard post
            async for message in channel.history(limit=20):
                if message.author == _bot_instance.user:
                    # Check for new format (embed with Pathfinder title)
                    has_leaderboard_embed = any(
                        embed.title and "Pathfinder Leaderboards" in embed.title
                        for embed in message.embeds
                    )
                    # Check for old format (header content)
                    has_leaderboard_content = (
                        message.content and 
                        "# 🏅 Pathfinder Leaderboards" in message.content
                    )
                    if has_leaderboard_embed or has_leaderboard_content:
                        last_message = message
                        break
            
            if last_message:
                logger.info(f"Found existing leaderboard message: {last_message.id}, editing...")
                await last_message.edit(
                    content=header_content,
                    embeds=embeds,
                    view=view
                )
                # Save the found message ID for future edits
                await _save_leaderboard_state(last_message.id, stats_channel_id)
                logger.info(f"Successfully edited leaderboard message {last_message.id} and saved state")
                return
            else:
                logger.info("No existing leaderboard message found in channel history")
                
        except discord.Forbidden as e:
            logger.error(f"Permission error accessing channel history: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error finding/editing leaderboard message: {e}", exc_info=True)
        
        # Send new message if editing failed
        logger.info("Sending new leaderboard message...")
        new_message = await channel.send(
            content=header_content,
            embeds=embeds,
            view=view
        )
        # Save the new message ID for future edits
        await _save_leaderboard_state(new_message.id, stats_channel_id)
        logger.info(f"Posted new leaderboard message {new_message.id} and saved state")
        
    except Exception as e:
        logger.error(f"Error in post_pathfinder_leaderboards task: {e}", exc_info=True)
