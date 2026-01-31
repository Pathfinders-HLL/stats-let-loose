"""
Discord embed building functions for Pathfinder leaderboards.

Contains functions to build:
- Paginated stat embeds for detailed view
- Overview stat embeds for first page display
- Combined leaderboard embeds for multiple stats
"""

import discord

from datetime import datetime
from typing import List, Dict, Any

from apps.discord_stats_bot.common.constants import (
    PLAYERS_PER_PAGE,
    LEADERBOARD_STAT_CONFIGS,
)


def _get_total_pages(results: List[Dict[str, Any]]) -> int:
    """Calculate total pages for results."""
    if not results:
        return 1
    return max(1, (len(results) + PLAYERS_PER_PAGE - 1) // PLAYERS_PER_PAGE)


def _build_stat_embed_page(
    results: List[Dict[str, Any]],
    stat_config: Dict[str, Any],
    page: int,
    total_pages: int,
    timeframe_label: str,
    updated_timestamp: datetime
) -> discord.Embed:
    """Build a single page of a stat category embed with 3 columns."""
    title = f"{stat_config['title']} ({timeframe_label})"
    color = stat_config["color"]
    value_label = stat_config["value_label"]
    value_format = stat_config["value_format"]
    
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        # Still show footer with page info
        unix_ts = int(updated_timestamp.timestamp())
        embed.set_footer(text=f"Page {page}/{total_pages} • {stat_config['title'].split(' ', 1)[1]} • {timeframe_label} • Updated <t:{unix_ts}:R>")
        return embed
    
    # Calculate which results to show for this page
    start_idx = (page - 1) * PLAYERS_PER_PAGE
    end_idx = start_idx + PLAYERS_PER_PAGE
    page_results = results[start_idx:end_idx]
    
    ranks = []
    players = []
    values = []
    
    for rank, row in enumerate(page_results, start_idx + 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        
        if value_format == "int":
            values.append(f"{int(value):,}")
        elif value_format == "float":
            values.append(f"{float(value):.2f}")
        else:
            values.append(str(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    # Build footer: "Page 2/5 • Most Infantry Kills • Last 7 Days • Updated <timestamp>"
    stat_name = stat_config['title'].split(' ', 1)[1]  # Remove emoji
    unix_ts = int(updated_timestamp.timestamp())
    footer_text = f"Page {page}/{total_pages} • {stat_name} • {timeframe_label} • Updated <t:{unix_ts}:R>"
    embed.set_footer(text=footer_text)
    
    return embed


def _build_stat_embed(
    title: str,
    results: List[Dict[str, Any]],
    value_label: str,
    color: discord.Color,
    value_format: str = "int",
    footer_note: str = ""
) -> discord.Embed:
    """Build a single stat category embed with 3 columns (for first page overview)."""
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        return embed
    
    # Only show first page (25 players) for overview
    page_results = results[:PLAYERS_PER_PAGE]
    
    ranks = []
    players = []
    values = []
    
    for rank, row in enumerate(page_results, 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get("value", 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        
        if value_format == "int":
            values.append(f"{int(value):,}")
        elif value_format == "float":
            values.append(f"{float(value):.2f}")
        else:
            values.append(str(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    if footer_note:
        embed.set_footer(text=footer_note)
    
    return embed


def build_leaderboard_embeds(
    stats: Dict[str, List[Dict[str, Any]]],
    timeframe_label: str
) -> List[discord.Embed]:
    """Build all leaderboard embeds from stats data (first page overview for each stat)."""
    embeds = []
    
    for config in LEADERBOARD_STAT_CONFIGS:
        embed = _build_stat_embed(
            f"{config['title']} ({timeframe_label})",
            stats.get(config["key"], []),
            config["value_label"],
            config["color"],
            value_format=config["value_format"],
            footer_note=config["footer_note"]
        )
        embeds.append(embed)
    
    return embeds
