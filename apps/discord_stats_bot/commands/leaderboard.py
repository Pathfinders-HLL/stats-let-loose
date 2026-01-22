"""
Leaderboard command group with subcommands for various statistics.
"""

import logging

from discord import app_commands

from apps.discord_stats_bot.subcommands.leaderboard import (
    register_kills_subcommand,
    register_deaths_subcommand,
    register_contributions_subcommand,
    register_performance_subcommand,
    register_weapon_subcommand,
    register_alltime_weapons_subcommand,
    register_100killgames_subcommand,
)

logger = logging.getLogger(__name__)


def setup_leaderboard_command(tree: app_commands.CommandTree, channel_check=None) -> None:
    """Register the /leaderboard command group with all subcommands."""
    leaderboard_group = app_commands.Group(
        name="leaderboard",
        description="View leaderboards for various statistics"
    )
    
    register_weapon_subcommand(leaderboard_group, channel_check)
    register_100killgames_subcommand(leaderboard_group, channel_check)
    register_alltime_weapons_subcommand(leaderboard_group, channel_check)
    register_performance_subcommand(leaderboard_group, channel_check)
    register_kills_subcommand(leaderboard_group, channel_check)
    register_deaths_subcommand(leaderboard_group, channel_check)
    register_contributions_subcommand(leaderboard_group, channel_check)
    
    tree.add_command(leaderboard_group)
