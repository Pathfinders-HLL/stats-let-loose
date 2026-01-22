"""
Shared autocomplete functions for Discord bot commands.
"""

from typing import List

import discord
from discord import app_commands

from apps.discord_stats_bot.common.constants import (
    KILL_TYPE_CHOICES,
    DEATH_TYPE_CHOICES,
    SCORE_TYPE_CHOICES,
    STAT_TYPE_CHOICES,
    AGGREGATE_BY_CHOICES,
    ORDER_BY_CHOICES,
)


def _filter_choices(
    choices: List[app_commands.Choice[str]], 
    current: str
) -> List[app_commands.Choice[str]]:
    """Filter choices based on current input."""
    if not current:
        return choices[:25]
    
    current_lower = current.lower()
    matching = [
        choice for choice in choices
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]


async def kill_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for kill_type parameter."""
    return _filter_choices(KILL_TYPE_CHOICES, current)


async def death_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for death_type parameter."""
    return _filter_choices(DEATH_TYPE_CHOICES, current)


async def score_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for score_type parameter."""
    return _filter_choices(SCORE_TYPE_CHOICES, current)


async def stat_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for stat_type parameter."""
    return _filter_choices(STAT_TYPE_CHOICES, current)


async def aggregate_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for aggregate_by parameter."""
    return _filter_choices(AGGREGATE_BY_CHOICES, current)


async def order_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for order_by parameter."""
    return _filter_choices(ORDER_BY_CHOICES, current)
