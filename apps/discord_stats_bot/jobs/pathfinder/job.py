"""
Scheduled task to post comprehensive Pathfinder leaderboard statistics.

Posts top 50 players for multiple stat categories every 30 minutes,
with interactive dropdowns to view different timeframes and stats,
plus pagination buttons for navigating through player rankings.

This module serves as the main entry point for the pathfinder leaderboard
job, orchestrating the cache refresh and posting tasks.
"""

import logging

import discord

from apps.discord_stats_bot.jobs.pathfinder.cache import (
    refresh_leaderboard_cache,
    get_leaderboard_cache,
    _load_leaderboard_state,
)
from apps.discord_stats_bot.jobs.pathfinder.posting import (
    post_pathfinder_leaderboards,
    set_bot_instance,
)

logger = logging.getLogger(__name__)


def setup_pathfinder_leaderboards_task(bot: discord.Client) -> None:
    """Start the scheduled leaderboards posting and cache refresh tasks."""
    # Set bot instance for posting module
    set_bot_instance(bot)
    
    @refresh_leaderboard_cache.before_loop
    async def before_cache_refresh():
        await bot.wait_until_ready()
        # Populate cache immediately on startup
        logger.info("Performing initial leaderboard cache population...")
        try:
            await refresh_leaderboard_cache.coro()
        except Exception as e:
            logger.error(f"Error in initial cache population: {e}", exc_info=True)
    
    @post_pathfinder_leaderboards.before_loop
    async def before_leaderboards():
        await bot.wait_until_ready()
        # Load persisted message ID state
        await _load_leaderboard_state()
        # Ensure cache is populated before first post
        leaderboard_cache = get_leaderboard_cache()
        if not leaderboard_cache:
            logger.info("Waiting for cache to populate before posting leaderboards...")
            await refresh_leaderboard_cache.coro()
    
    # Start cache refresh task (every 20 min)
    if not refresh_leaderboard_cache.is_running():
        refresh_leaderboard_cache.start()
        logger.info("Started leaderboard cache refresh task (every 20 min)")
    else:
        logger.warning("Leaderboard cache refresh task already running")
    
    # Start posting task (every 30 min)
    if not post_pathfinder_leaderboards.is_running():
        post_pathfinder_leaderboards.start()
        logger.info("Started Pathfinder leaderboards posting task (every 30 min)")
    else:
        logger.warning("Pathfinder leaderboards posting task already running")
