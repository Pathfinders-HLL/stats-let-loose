"""
Commands module for Discord bot.

Contains the main command groups (player, leaderboard, profile).
"""

from apps.discord_stats_bot.commands.leaderboard import setup_leaderboard_command
from apps.discord_stats_bot.commands.management import setup_profile_command
from apps.discord_stats_bot.commands.player import setup_player_command

__all__ = [
    'setup_player_command',
    'setup_leaderboard_command',
    'setup_profile_command',
]

