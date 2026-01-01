"""
Jobs module for Discord bot.

Contains scheduled tasks that automatically post statistics
to Discord channels at regular intervals.
"""

from apps.discord_stats_bot.jobs.karabiner_stats import setup_karabiner_stats_task

__all__ = ['setup_karabiner_stats_task']

