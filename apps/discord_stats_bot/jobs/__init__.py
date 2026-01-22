"""
Jobs module for Discord bot.

Contains scheduled tasks that automatically post statistics
to Discord channels at regular intervals.
"""

from apps.discord_stats_bot.jobs.pathfinder_leaderboards import (
    setup_pathfinder_leaderboards_task,
    LeaderboardView,
)

__all__ = [
    'setup_pathfinder_leaderboards_task',
    'LeaderboardView',
]

