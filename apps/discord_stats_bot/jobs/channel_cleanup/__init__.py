"""
Channel cleanup job module.

Contains the scheduled task that automatically cleans up messages
from the stats channel that are not from the bot or allowed roles.
"""

from apps.discord_stats_bot.jobs.channel_cleanup.cleanup_job import (
    setup_channel_cleanup_task,
)

__all__ = [
    'setup_channel_cleanup_task',
]
