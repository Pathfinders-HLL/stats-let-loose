"""
Monospace table builder for Discord embeds.

Provides utilities for formatting leaderboard statistics as compact
monospace tables suitable for Discord embed fields.
"""

import re

import discord

from datetime import datetime
from typing import List, Dict, Any

from apps.discord_stats_bot.common.constants import DEFAULT_COMPACT_VIEW_PLAYERS, PATHFINDER_COLOR


def format_compact_value(value: Any, value_format: str, width: int) -> str:
    """
    Format a value for compact display with dynamic width.
    
    Args:
        value: The value to format
        value_format: Format type ('int' or 'float')
        width: Available width for the value
    
    Returns:
        Formatted value string
    """
    if value_format == "int":
        v = int(value)
        if v >= 10000:
            # Use decimal 'k' suffix for values >= 10,000 (e.g., "10.2k", "1.5k")
            return f"{v / 1000:.1f}k".rjust(width)
        # Use all available width for values < 10,000
        return f"{v:>{width}}"
    elif value_format == "float":
        v = float(value)
        if v >= 100:
            # For 100+, show as integer
            return f"{int(v):>{width}}"
        elif v >= 10:
            # For 10-99.9, show one decimal
            return f"{v:>{width}.1f}"
        else:
            # For 0-9.99, show one decimal
            return f"{v:>{width}.1f}"
    return str(value)[:width].rjust(width)


def format_stat_monospace_table(
    results: List[Dict[str, Any]],
    value_abbrev: str,
    value_format: str = "int",
    max_rows: int = DEFAULT_COMPACT_VIEW_PLAYERS
) -> str:
    """
    Format a stat as a monospace table with dynamic column widths.
    
    Format per row (25 chars max):
    - Rank: 3 chars (right-aligned with dot)
    - Space: 1 char
    - Player: variable chars (left-aligned, no padding)
    - Space: 1 char
    - Value: remaining chars up to max (right-aligned)
    
    The player name and value columns share the remaining 21 characters dynamically.
    Shorter player names allow more space for values.
    
    Args:
        results: List of result dictionaries with 'player_name'/'player_id' and 'value' keys
        value_abbrev: 3-4 character abbreviation for the value column header
        value_format: Format type ('int' or 'float')
        max_rows: Maximum number of rows to display
    
    Returns:
        Formatted monospace table string wrapped in code blocks
    """
    # Total width for the table: 25 chars
    # Rank (3) + Space (1) + Player + Space (1) + Value = 25
    # So Player + Value = 20
    total_width = 25
    rank_width = 3
    spaces = 2  # Two spaces (after rank, after player)
    content_width = total_width - rank_width - spaces  # 20 chars for player + value
    
    # Minimum widths to ensure readability
    min_player_width = 8
    min_value_width = 4 if value_format == "float" else 3
    max_player_width = content_width - min_value_width
    
    # Header row with max player width and min value width
    header = f"{'#':>3} {'Player':<{max_player_width}} {value_abbrev:>{min_value_width}}"
    
    if not results:
        return f"```\n{header}\nNo data available\n```"
    
    lines = [header]
    
    for rank, row in enumerate(results[:max_rows], 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        # Trim "PF | " or "PFr | " from player name
        trimmed_player_name = re.sub(r'^(?:PF|PFr)\s*\|\s*', '', player_name)
        
        # Calculate dynamic widths based on actual player name length
        actual_player_len = min(len(trimmed_player_name), max_player_width)
        actual_player_len = max(actual_player_len, min_player_width)
        
        # Give remaining space to value column
        value_width = content_width - actual_player_len
        
        rank_str = f"{str(rank) + '.':>3}"
        player_str = trimmed_player_name[:actual_player_len].ljust(actual_player_len)
        value_str = format_compact_value(value, value_format, value_width)
        
        lines.append(f"{rank_str} {player_str} {value_str}")
    
    return "```\n" + "\n".join(lines) + "\n```"


def build_compact_leaderboard_embed(
    stats: Dict[str, List[Dict[str, Any]]],
    stat_configs: List[Dict[str, Any]],
    timeframe_label: str,
    updated_timestamp: datetime,
    compact_view_players: int = DEFAULT_COMPACT_VIEW_PLAYERS
) -> discord.Embed:
    """
    Build a single compact embed with stats displayed 2 per row.
    Uses monospace tables with bold titles.
    
    Args:
        stats: Dictionary mapping stat keys to result lists
        stat_configs: List of stat configuration dictionaries, each containing:
            - 'key': Stat key used in stats dict
            - 'compact_title': Title to display for the stat
            - 'value_abbrev': 3-character abbreviation for value column
            - 'value_format': Format type ('int' or 'float')
        timeframe_label: Label for the timeframe (e.g., "Last 7 Days")
        updated_timestamp: Timestamp when data was last updated
        compact_view_players: Number of players to show per stat table
    
    Returns:
        Discord embed with compact leaderboard display
    """
    embed = discord.Embed(
        title=f"🏅 Pathfinder Leaderboards ({timeframe_label})",
        color=PATHFINDER_COLOR
    )
    
    # Process stats in pairs for 2-column layout
    for i in range(0, len(stat_configs), 2):
        config1 = stat_configs[i]
        
        # First stat of the pair
        results1 = stats.get(config1["key"], [])
        field_name1 = config1["compact_title"]
        field_value1 = format_stat_monospace_table(
            results1,
            config1["value_abbrev"],
            config1["value_format"],
            max_rows=compact_view_players
        )
        embed.add_field(name=field_name1, value=field_value1, inline=True)
        
        # Second stat of the pair (if exists)
        if i + 1 < len(stat_configs):
            config2 = stat_configs[i + 1]
            results2 = stats.get(config2["key"], [])
            field_name2 = config2["compact_title"]
            field_value2 = format_stat_monospace_table(
                results2,
                config2["value_abbrev"],
                config2["value_format"],
                max_rows=compact_view_players
            )
            embed.add_field(name=field_name2, value=field_value2, inline=True)
        
        # Add invisible spacer field to force new row (except after last pair)
        if i + 2 < len(stat_configs):
            embed.add_field(name="\u200b", value="\u200b", inline=False)
    
    # Footer with timestamp
    unix_ts = int(updated_timestamp.timestamp())
    embed.set_footer(text=f"Use Stats Let Loose slash commands to view advanced leaderboards and personal stats.")
    
    return embed
