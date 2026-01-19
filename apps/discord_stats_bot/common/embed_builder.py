"""
Shared utilities for building Discord embeds for leaderboards.
"""

from typing import List, Dict, Any, Optional
import discord


def build_leaderboard_embed(
    title: str,
    results: List[Dict[str, Any]],
    value_key: str,
    value_label: str,
    color: discord.Color = discord.Color.from_rgb(16, 74, 0),
    filter_text: str = ""
) -> discord.Embed:
    """
    Build a Discord embed for leaderboard results with three columns.
    
    Args:
        title: Embed title (without filter_text)
        results: List of result dictionaries with 'player_name' or 'player_id' and value_key
        value_key: Key in result dict for the value column
        value_label: Label for the value column header
        color: Embed color (default: Pathfinder green)
        filter_text: Additional text to append to title (e.g., " (Pathfinders Only)")
        
    Returns:
        Formatted Discord embed with three inline fields: Rank, Player, Value
    """
    embed = discord.Embed(
        title=f"{title}{filter_text}",
        color=color
    )
    
    rank_values = []
    player_values = []
    value_values = []
    
    for rank, row in enumerate(results, 1):
        # Use player_name if available, otherwise use player_id
        display_name = row.get('player_name') or row.get('player_id', 'Unknown')
        value = row.get(value_key, 0)
        
        rank_values.append(f"#{rank}")
        player_values.append(display_name)
        
        # Format value based on type
        if isinstance(value, (int, float)):
            value_values.append(f"{value:,}")
        else:
            value_values.append(str(value))
    
    # Add the three columns as inline fields (side-by-side)
    embed.add_field(name="Rank", value="\n".join(rank_values), inline=True)
    embed.add_field(name="Player", value="\n".join(player_values), inline=True)
    embed.add_field(name=value_label, value="\n".join(value_values), inline=True)
    
    return embed
