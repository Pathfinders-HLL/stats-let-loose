"""
Jobs module for Discord bot.

Contains scheduled tasks that automatically post statistics
to Discord channels at regular intervals.
"""

from apps.discord_stats_bot.jobs.pathfinder import setup_pathfinder_leaderboards_task
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_ui import LeaderboardView
from apps.discord_stats_bot.jobs.channel_cleanup import setup_channel_cleanup_task

__all__ = [
    'setup_pathfinder_leaderboards_task',
    'LeaderboardView',
    'setup_channel_cleanup_task',
]
