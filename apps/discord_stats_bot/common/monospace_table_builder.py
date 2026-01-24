"""
Monospace table builder for Discord embeds.

Provides utilities for formatting leaderboard statistics as compact
monospace tables suitable for Discord embed fields.
"""

from datetime import datetime
from typing import List, Dict, Any

import discord


# Default number of players to show in compact view
DEFAULT_COMPACT_VIEW_PLAYERS = 10


def format_compact_value(value: Any, value_format: str) -> str:
    """Format a value to fit in 3 characters for compact display."""
    if value_format == "int":
        v = int(value)
        if v >= 1000:
            # Use 'k' suffix for thousands (e.g., 1.2k, 15k)
            if v >= 10000:
                return f"{v // 1000:>2}k"
            return f"{v / 1000:.0f}k"
        return f"{v:>3}"
    elif value_format == "float":
        v = float(value)
        if v >= 100:
            return f"{int(v):>3}"
        elif v >= 10:
            return f"{v:.1f}"[:3].rjust(3)
        return f"{v:.1f}"[:3].rjust(3)
    return str(value)[:3].rjust(3)


def format_stat_monospace_table(
    results: List[Dict[str, Any]],
    value_abbrev: str,
    value_format: str = "int",
    max_rows: int = DEFAULT_COMPACT_VIEW_PLAYERS
) -> str:
    """
    Format a stat as a monospace table.
    
    Format per row (28 chars max):
    - Rank: 3 chars (left-aligned)
    - Space: 1 char
    - Player: 20 chars (left-aligned, padded)
    - Space: 1 char
    - Value: 3 chars (right-aligned)
    
    Args:
        results: List of result dictionaries with 'player_name'/'player_id' and 'value' keys
        value_abbrev: 3-character abbreviation for the value column header
        value_format: Format type ('int' or 'float')
        max_rows: Maximum number of rows to display
    
    Returns:
        Formatted monospace table string wrapped in code blocks
    """
    # Header row: #   Player               Val
    header = f"{'#':<3} {'Player':<20} {value_abbrev:>3}"
    
    if not results:
        return f"```\n{header}\nNo data available\n```"
    
    lines = [header]
    
    for rank, row in enumerate(results[:max_rows], 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        rank_str = f"{rank:<3}"
        player_str = player_name[:20].ljust(20)
        value_str = format_compact_value(value, value_format)
        
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
        color=discord.Color.from_rgb(0, 128, 128)  # Teal color
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
    embed.set_footer(text=f"Last Refreshed • <t:{unix_ts}:R>")
    
    return embed
