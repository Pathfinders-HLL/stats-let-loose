"""Leaderboard subcommands for the HLL Stats Discord bot."""

from apps.discord_stats_bot.subcommands.leaderboard.alltime_weapons import register_alltime_weapons_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.contributions import register_contributions_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.deaths import register_deaths_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.killgames100 import register_100killgames_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.kills import register_kills_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.performance import register_performance_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.weapon import register_weapon_subcommand

__all__ = [
    'register_weapon_subcommand',
    'register_100killgames_subcommand',
    'register_alltime_weapons_subcommand',
    'register_performance_subcommand',
    'register_kills_subcommand',
    'register_deaths_subcommand',
    'register_contributions_subcommand',
]
