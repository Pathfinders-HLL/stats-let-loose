"""
Leaderboard command group with subcommands for various statistics.
"""

import logging

from discord import app_commands

from apps.discord_stats_bot.subcommands.leaderboard.killgames100 import register_100killgames_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.alltime_weapons import register_alltime_weapons_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.performance import register_performance_subcommand as register_leaderboard_performance_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.weapon import register_weapon_subcommand as register_leaderboard_weapon_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.kills import register_kills_subcommand as register_leaderboard_kills_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.deaths import register_deaths_subcommand as register_leaderboard_deaths_subcommand
from apps.discord_stats_bot.subcommands.leaderboard.contributions import register_contributions_subcommand as register_leaderboard_contributions_subcommand

logger = logging.getLogger(__name__)


def setup_leaderboard_command(tree: app_commands.CommandTree, channel_check=None) -> None:
    """Register the /leaderboard command group with all subcommands."""
    leaderboard_group = app_commands.Group(
        name="leaderboard",
        description="View leaderboards for various statistics"
    )
    
    # Register all subcommands
    register_leaderboard_weapon_subcommand(leaderboard_group, channel_check)
    register_100killgames_subcommand(leaderboard_group, channel_check)
    register_alltime_weapons_subcommand(leaderboard_group, channel_check)
    register_leaderboard_performance_subcommand(leaderboard_group, channel_check)
    register_leaderboard_kills_subcommand(leaderboard_group, channel_check)
    register_leaderboard_deaths_subcommand(leaderboard_group, channel_check)
    register_leaderboard_contributions_subcommand(leaderboard_group, channel_check)
    
    # Add the group to the command tree
    tree.add_command(leaderboard_group)

