"""Player subcommands for the HLL Stats Discord bot."""

from apps.discord_stats_bot.subcommands.player.contributions import register_contributions_subcommand
from apps.discord_stats_bot.subcommands.player.deaths import register_deaths_subcommand
from apps.discord_stats_bot.subcommands.player.kills import register_kills_subcommand
from apps.discord_stats_bot.subcommands.player.maps import register_maps_subcommand
from apps.discord_stats_bot.subcommands.player.performance import register_performance_subcommand
from apps.discord_stats_bot.subcommands.player.weapon import register_weapon_subcommand

__all__ = [
    'register_weapon_subcommand',
    'register_performance_subcommand',
    'register_kills_subcommand',
    'register_deaths_subcommand',
    'register_contributions_subcommand',
    'register_maps_subcommand',
]
