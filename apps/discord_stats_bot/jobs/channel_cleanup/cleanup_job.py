"""
Scheduled task to clean up messages in the stats channel.

Runs every 5 minutes and deletes all messages EXCEPT:
- Messages posted by the bot itself
- Messages posted by users who have a role in the cleanup allowed roles list

This helps keep the stats channel clean and focused on bot-generated content.
"""

import logging
from typing import Optional

import discord
from discord.ext import tasks

from apps.discord_stats_bot.bot_config import get_bot_config

logger = logging.getLogger(__name__)

# Bot instance reference (set by setup function)
_bot_instance: Optional[discord.Client] = None


def set_bot_instance(bot: discord.Client) -> None:
    """Set the bot instance for cleanup operations."""
    global _bot_instance
    _bot_instance = bot


def get_bot_instance() -> Optional[discord.Client]:
    """Get the bot instance."""
    return _bot_instance


async def _is_message_protected(message: discord.Message, allowed_role_ids: set[int]) -> bool:
    """
    Check if a message should be protected from deletion.
    
    A message is protected if:
    - It was posted by the bot
    - The author has a role in the allowed roles list
    
    Args:
        message: The Discord message to check
        allowed_role_ids: Set of role IDs that are allowed to post
        
    Returns:
        True if the message should NOT be deleted, False otherwise
    """
    global _bot_instance
    
    if not _bot_instance:
        return True  # Protect message if we can't verify
    
    # Protect bot's own messages
    if message.author == _bot_instance.user:
        return True
    
    # If no allowed roles configured, only protect bot messages
    if not allowed_role_ids:
        return False
    
    # Check if author has any allowed role
    # message.author is a Member in guild context, User in DMs
    if isinstance(message.author, discord.Member):
        author_role_ids = {role.id for role in message.author.roles}
        if author_role_ids & allowed_role_ids:  # Check for any intersection
            return True
    
    return False


@tasks.loop(minutes=5)
async def cleanup_stats_channel():
    """Clean up non-protected messages from the stats channel."""
    global _bot_instance
    
    if not _bot_instance:
        logger.error("Bot instance not set for channel cleanup")
        return
    
    try:
        bot_config = get_bot_config()
        stats_channel_id = bot_config.stats_channel_id
        allowed_role_ids = bot_config.cleanup_allowed_role_ids
        
        if not stats_channel_id:
            logger.info("DISCORD_STATS_CHANNEL_ID not configured, skipping channel cleanup")
            return
        
        channel = _bot_instance.get_channel(stats_channel_id)
        if not channel:
            logger.error(f"Channel {stats_channel_id} not found for cleanup")
            return
        
        if not isinstance(channel, discord.TextChannel):
            logger.error(f"Channel {stats_channel_id} is not a text channel")
            return
        
        # Sync check for purge: True = delete this message. Mirrors _is_message_protected (protected = don't delete).
        def should_delete(message: discord.Message) -> bool:
            if _bot_instance is None:
                return False  # Protect all if we can't verify (match original safety)
            if message.author == _bot_instance.user:
                return False  # Never delete bot's own messages
            if not allowed_role_ids:
                return True  # No allow-list: delete all non-bot messages
            if isinstance(message.author, discord.Member):
                author_role_ids = {r.id for r in message.author.roles}
                if author_role_ids & allowed_role_ids:
                    return False  # Author has an allow-listed role: keep
            return True  # Not bot, no allow-listed role: delete

        logger.info(f"Scanning channel {channel.name} for messages to clean up...")

        # purge() with bulk=True uses Discord's bulk delete API (1 request per batch of up to 100
        # messages under 14 days old), avoiding per-channel rate limits (5 req/5 sec for single deletes)
        deleted = await channel.purge(limit=100, check=should_delete, bulk=True)
        if deleted:
            logger.info(f"Channel cleanup complete: {len(deleted)} message(s) deleted")
            
    except Exception as e:
        logger.error(f"Error in channel cleanup task: {e}", exc_info=True)


def setup_channel_cleanup_task(bot: discord.Client) -> None:
    """Start the scheduled channel cleanup task."""
    # Set bot instance
    set_bot_instance(bot)
    
    @cleanup_stats_channel.before_loop
    async def before_cleanup():
        await bot.wait_until_ready()
        logger.info("Channel cleanup task ready")
    
    # Start cleanup task (every 5 minutes)
    if not cleanup_stats_channel.is_running():
        cleanup_stats_channel.start()
        logger.info("Started channel cleanup task (every 5 min)")
    else:
        logger.warning("Channel cleanup task already running")
