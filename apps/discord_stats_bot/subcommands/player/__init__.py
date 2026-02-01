"""
Player subcommands module.

Provides subcommand registration for the /player command group.
"""

from apps.discord_stats_bot.subcommands.player.player_kills import register_kills_subcommand
from apps.discord_stats_bot.subcommands.player.player_deaths import register_deaths_subcommand
from apps.discord_stats_bot.subcommands.player.player_contributions import register_contributions_subcommand
from apps.discord_stats_bot.subcommands.player.player_performance import register_performance_subcommand
from apps.discord_stats_bot.subcommands.player.player_weapon import register_weapon_subcommand
from apps.discord_stats_bot.subcommands.player.player_maps import register_maps_subcommand
from apps.discord_stats_bot.subcommands.player.player_nemesis import register_nemesis_subcommand
from apps.discord_stats_bot.subcommands.player.player_victim import register_victim_subcommand

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
