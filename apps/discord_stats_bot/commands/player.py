"""
Player command group with subcommands for individual player statistics.
"""

import logging

from discord import app_commands

from apps.discord_stats_bot.subcommands.player import (
    register_kills_subcommand,
    register_deaths_subcommand,
    register_contributions_subcommand,
    register_performance_subcommand,
    register_weapon_subcommand,
    register_maps_subcommand,
    register_nemesis_subcommand,
    register_victim_subcommand,
)

logger = logging.getLogger(__name__)


def setup_player_command(tree: app_commands.CommandTree, channel_check=None) -> None:
    """Register the /player command group with all subcommands."""
    player_group = app_commands.Group(
        name="player",
        description="Get player statistics and information"
    )
    
    register_weapon_subcommand(player_group, channel_check)
    register_performance_subcommand(player_group, channel_check)
    register_kills_subcommand(player_group, channel_check)
    register_deaths_subcommand(player_group, channel_check)
    register_contributions_subcommand(player_group, channel_check)
    register_maps_subcommand(player_group, channel_check)
    register_nemesis_subcommand(player_group, channel_check)
    register_victim_subcommand(player_group, channel_check)
    
    tree.add_command(player_group)
