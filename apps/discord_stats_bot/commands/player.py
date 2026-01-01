"""
Player command group with subcommands for individual player statistics.
"""

import logging

from discord import app_commands

from apps.discord_stats_bot.subcommands.player.contributions import register_contributions_subcommand as register_player_contributions_subcommand
from apps.discord_stats_bot.subcommands.player.deaths import register_deaths_subcommand as register_player_deaths_subcommand
from apps.discord_stats_bot.subcommands.player.kills import register_kills_subcommand as register_player_kills_subcommand
from apps.discord_stats_bot.subcommands.player.maps import register_maps_subcommand as register_player_maps_subcommand
from apps.discord_stats_bot.subcommands.player.performance import register_performance_subcommand as register_player_performance_subcommand
from apps.discord_stats_bot.subcommands.player.weapon import register_weapon_subcommand as register_player_weapon_subcommand

logger = logging.getLogger(__name__)


def setup_player_command(tree: app_commands.CommandTree, channel_check=None) -> None:
    """Register the /player command group with all subcommands."""
    player_group = app_commands.Group(
        name="player",
        description="Get player statistics and information"
    )
    
    # Register all subcommands
    register_player_weapon_subcommand(player_group, channel_check)
    register_player_performance_subcommand(player_group, channel_check)
    register_player_kills_subcommand(player_group, channel_check)
    register_player_deaths_subcommand(player_group, channel_check)
    register_player_contributions_subcommand(player_group, channel_check)
    register_player_maps_subcommand(player_group, channel_check)
    
    # Add the group to the command tree
    tree.add_command(player_group)

