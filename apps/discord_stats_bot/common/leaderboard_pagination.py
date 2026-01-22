"""
Shared pagination views for leaderboard subcommands.

Provides reusable UI components for paginated leaderboard displays with
timeframe selection and navigation buttons.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import discord

logger = logging.getLogger(__name__)

# Pagination settings
TOP_PLAYERS_LIMIT = 150  # Top 150 players per leaderboard
PLAYERS_PER_PAGE = 25    # 25 players per page = 6 pages max

# Timeframe options for commands with over_last_days
TIMEFRAME_OPTIONS = {
    "1d": {"days": 1, "label": "Last 24 Hours"},
    "7d": {"days": 7, "label": "Last 7 Days"},
    "30d": {"days": 30, "label": "Last 30 Days"},
    "all": {"days": 0, "label": "All Time"},
}


def get_total_pages(results: List[Dict[str, Any]]) -> int:
    """Calculate total pages for results."""
    if not results:
        return 1
    return max(1, (len(results) + PLAYERS_PER_PAGE - 1) // PLAYERS_PER_PAGE)


def build_paginated_embed(
    title: str,
    results: List[Dict[str, Any]],
    page: int,
    total_pages: int,
    value_key: str,
    value_label: str,
    color: discord.Color,
    format_value: Callable[[Any], str] = lambda x: f"{x:,}",
    footer_extra: str = "",
    updated_timestamp: Optional[datetime] = None
) -> discord.Embed:
    """
    Build a single page of a leaderboard embed with 3 columns.
    
    Args:
        title: Embed title
        results: Full list of results (will be sliced for page)
        page: Current page number (1-indexed)
        total_pages: Total number of pages
        value_key: Key in result dict for the value column
        value_label: Label for the value column
        color: Embed color
        format_value: Function to format values
        footer_extra: Extra text for footer
        updated_timestamp: Optional timestamp for footer
    
    Returns:
        discord.Embed with the paginated data
    """
    embed = discord.Embed(title=title, color=color)
    
    if not results:
        embed.description = "No data available"
        embed.set_footer(text=f"Page {page}/{total_pages}")
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
        value = row.get(value_key, 0)
        
        ranks.append(f"#{rank}")
        players.append(player_name[:20])  # Truncate long names
        values.append(format_value(value))
    
    embed.add_field(name="Rank", value="\n".join(ranks), inline=True)
    embed.add_field(name="Player", value="\n".join(players), inline=True)
    embed.add_field(name=value_label, value="\n".join(values), inline=True)
    
    # Build footer
    footer_parts = [f"Page {page}/{total_pages}"]
    if footer_extra:
        footer_parts.append(footer_extra)
    if updated_timestamp:
        unix_ts = int(updated_timestamp.timestamp())
        footer_parts.append(f"Updated <t:{unix_ts}:R>")
    
    embed.set_footer(text=" • ".join(footer_parts))
    
    return embed


class LeaderboardTimeframeSelect(discord.ui.Select):
    """Dropdown select for choosing leaderboard timeframe."""
    
    def __init__(self, current_timeframe: str = "30d"):
        options = [
            discord.SelectOption(
                label="Last 24 Hours",
                value="1d",
                description="View stats from the past day",
                emoji="📅",
                default=(current_timeframe == "1d")
            ),
            discord.SelectOption(
                label="Last 7 Days",
                value="7d",
                description="View stats from the past week",
                emoji="📆",
                default=(current_timeframe == "7d")
            ),
            discord.SelectOption(
                label="Last 30 Days",
                value="30d",
                description="View stats from the past month",
                emoji="🗓️",
                default=(current_timeframe == "30d")
            ),
            discord.SelectOption(
                label="All Time",
                value="all",
                description="View all-time stats",
                emoji="♾️",
                default=(current_timeframe == "all")
            ),
        ]
        super().__init__(
            placeholder="Select a timeframe...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle timeframe selection - refresh data with new timeframe."""
        view: PaginatedLeaderboardView = self.view
        new_timeframe = self.values[0]
        
        if new_timeframe != view.current_timeframe:
            view.current_timeframe = new_timeframe
            view.current_page = 1  # Reset to first page when changing timeframe
            
            # Fetch new data
            await interaction.response.defer()
            try:
                days = TIMEFRAME_OPTIONS[new_timeframe]["days"]
                new_results = await view.fetch_data_func(days)
                view.results = new_results
                await view.update_message_after_fetch(interaction)
            except Exception as e:
                logger.error(f"Error fetching data for timeframe {new_timeframe}: {e}", exc_info=True)
                await interaction.followup.send(
                    "❌ An error occurred while fetching data.",
                    ephemeral=True
                )
        else:
            await interaction.response.defer()


class PaginatedLeaderboardView(discord.ui.View):
    """
    Paginated view for leaderboard commands with timeframe selection.
    
    This view displays leaderboard data with:
    - Timeframe dropdown to switch between time periods
    - Pagination buttons to navigate through results
    """
    
    def __init__(
        self,
        results: List[Dict[str, Any]],
        title_template: str,
        value_key: str,
        value_label: str,
        color: discord.Color,
        format_value: Callable[[Any], str] = lambda x: f"{x:,}",
        footer_extra: str = "",
        current_timeframe: str = "30d",
        fetch_data_func: Optional[Callable[[int], Coroutine[Any, Any, List[Dict[str, Any]]]]] = None,
        show_timeframe_in_title: bool = True
    ):
        """
        Initialize the paginated view.
        
        Args:
            results: Initial list of results
            title_template: Title template (timeframe will be appended if show_timeframe_in_title)
            value_key: Key in result dict for the value column
            value_label: Label for the value column
            color: Embed color
            format_value: Function to format values
            footer_extra: Extra text for footer
            current_timeframe: Initial timeframe key
            fetch_data_func: Async function to fetch data for a given number of days
            show_timeframe_in_title: Whether to append timeframe to title
        """
        super().__init__(timeout=300)  # 5 minute timeout for ephemeral messages
        
        self.results = results
        self.title_template = title_template
        self.value_key = value_key
        self.value_label = value_label
        self.color = color
        self.format_value = format_value
        self.footer_extra = footer_extra
        self.current_timeframe = current_timeframe
        self.current_page = 1
        self.fetch_data_func = fetch_data_func
        self.show_timeframe_in_title = show_timeframe_in_title
        self.updated_timestamp = datetime.now(timezone.utc)
        
        # Add timeframe selector if fetch function provided
        if fetch_data_func:
            self.add_item(LeaderboardTimeframeSelect(current_timeframe))
    
    def _get_total_pages(self) -> int:
        """Get total pages for current results."""
        return get_total_pages(self.results)
    
    def _get_title(self) -> str:
        """Get the full title with timeframe."""
        if self.show_timeframe_in_title:
            timeframe_label = TIMEFRAME_OPTIONS.get(self.current_timeframe, {}).get("label", "")
            if timeframe_label:
                return f"{self.title_template} ({timeframe_label})"
        return self.title_template
    
    def build_embed(self) -> discord.Embed:
        """Build the current page embed."""
        total_pages = self._get_total_pages()
        
        return build_paginated_embed(
            title=self._get_title(),
            results=self.results,
            page=self.current_page,
            total_pages=total_pages,
            value_key=self.value_key,
            value_label=self.value_label,
            color=self.color,
            format_value=self.format_value,
            footer_extra=self.footer_extra,
            updated_timestamp=self.updated_timestamp
        )
    
    async def update_message(self, interaction: discord.Interaction):
        """Update the message with current state."""
        embed = self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def update_message_after_fetch(self, interaction: discord.Interaction):
        """Update the message after fetching new data (already deferred)."""
        self.updated_timestamp = datetime.now(timezone.utc)
        embed = self.build_embed()
        
        # Rebuild the view to update dropdown state
        new_view = PaginatedLeaderboardView(
            results=self.results,
            title_template=self.title_template,
            value_key=self.value_key,
            value_label=self.value_label,
            color=self.color,
            format_value=self.format_value,
            footer_extra=self.footer_extra,
            current_timeframe=self.current_timeframe,
            fetch_data_func=self.fetch_data_func,
            show_timeframe_in_title=self.show_timeframe_in_title
        )
        new_view.current_page = self.current_page
        new_view.updated_timestamp = self.updated_timestamp
        
        await interaction.edit_original_response(embed=embed, view=new_view)
    
    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary, row=1)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to first page."""
        self.current_page = 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary, row=1)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        if self.current_page > 1:
            self.current_page -= 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary, row=1)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        total_pages = self._get_total_pages()
        if self.current_page < total_pages:
            self.current_page += 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, row=1)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to last page."""
        self.current_page = self._get_total_pages()
        await self.update_message(interaction)
