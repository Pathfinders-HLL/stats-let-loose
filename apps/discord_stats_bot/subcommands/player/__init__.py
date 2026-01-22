"""
Player subcommands module.

Provides subcommand registration for the /player command group.
"""

from apps.discord_stats_bot.subcommands.player.kills import register_kills_subcommand
from apps.discord_stats_bot.subcommands.player.deaths import register_deaths_subcommand
from apps.discord_stats_bot.subcommands.player.contributions import register_contributions_subcommand
from apps.discord_stats_bot.subcommands.player.performance import register_performance_subcommand
from apps.discord_stats_bot.subcommands.player.weapon import register_weapon_subcommand
from apps.discord_stats_bot.subcommands.player.maps import register_maps_subcommand
from apps.discord_stats_bot.subcommands.player.nemesis import register_nemesis_subcommand
from apps.discord_stats_bot.subcommands.player.victim import register_victim_subcommand

__all__ = [
    'register_kills_subcommand',
    'register_deaths_subcommand',
    'register_contributions_subcommand',
    'register_performance_subcommand',
    'register_weapon_subcommand',
    'register_maps_subcommand',
    'register_nemesis_subcommand',
    'register_victim_subcommand',
]
