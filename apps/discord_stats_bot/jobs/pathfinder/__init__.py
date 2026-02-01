"""
Pathfinder leaderboard job package.

This package handles scheduled posting of comprehensive Pathfinder
leaderboard statistics with interactive Discord UI components.
"""

from apps.discord_stats_bot.jobs.pathfinder.pathfinder_job import setup_pathfinder_leaderboards_task

__all__ = ["setup_pathfinder_leaderboards_task"]
