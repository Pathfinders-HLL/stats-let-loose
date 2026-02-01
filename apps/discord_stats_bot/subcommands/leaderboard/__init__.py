"""
Leaderboard subcommands module.

Provides subcommand registration for the /leaderboard command group.
"""

from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_kills import register_kills_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_deaths import register_deaths_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_contributions import register_contributions_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_performance import register_performance_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_weapon import register_weapon_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_alltime_weapons import register_alltime_weapons_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.leaderboard_killgames100 import register_100killgames_subcommand

__all__ = [
    'register_kills_subcommand',
    'register_deaths_subcommand',
    'register_contributions_subcommand',
    'register_performance_subcommand',
    'register_weapon_subcommand',
    'register_alltime_weapons_subcommand',
    'register_100killgames_subcommand',
]
