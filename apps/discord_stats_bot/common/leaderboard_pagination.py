"""
Shared pagination views for leaderboard subcommands.

Provides reusable UI components for paginated leaderboard displays with
timeframe selection, navigation buttons, and multiple display formats.

Supported formats:
- cards: Discord embeds (default)
- table: ASCII tables using tabulate
- list: Simple numbered list
"""

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import discord
from tabulate import tabulate

from apps.discord_stats_bot.common.cache import get_format_preference, DEFAULT_FORMAT

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


def build_paginated_table(
    title: str,
    results: List[Dict[str, Any]],
    page: int,
    total_pages: int,
    value_key: str,
    value_label: str,
    format_value: Callable[[Any], str] = lambda x: f"{x:,}",
    footer_extra: str = "",
    updated_timestamp: Optional[datetime] = None
) -> str:
    """
    Build a single page of a leaderboard as an ASCII table.
    
    Returns:
        Formatted message string with ASCII table
    """
    if not results:
        return f"**{title}**\n\nNo data available\n\n*Page {page}/{total_pages}*"
    
    # Calculate which results to show for this page
    start_idx = (page - 1) * PLAYERS_PER_PAGE
    end_idx = start_idx + PLAYERS_PER_PAGE
    page_results = results[start_idx:end_idx]
    
    # Build table data
    table_data = []
    for rank, row in enumerate(page_results, start_idx + 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get(value_key, 0)
        
        table_data.append([
            rank,
            player_name[:20],  # Truncate long names
            format_value(value)
        ])
    
    headers = ["#", "Player", value_label]
    
    # Build the table
    table_str = tabulate(table_data, headers=headers, tablefmt="github")
    
    # Build footer
    footer_parts = [f"Page {page}/{total_pages}"]
    if footer_extra:
        footer_parts.append(footer_extra)
    if updated_timestamp:
        unix_ts = int(updated_timestamp.timestamp())
        footer_parts.append(f"Updated <t:{unix_ts}:R>")
    
    footer = " • ".join(footer_parts)
    
    # Assemble message
    message = f"**{title}**\n```\n{table_str}\n```\n*{footer}*"
    
    return message


def build_paginated_list(
    title: str,
    results: List[Dict[str, Any]],
    page: int,
    total_pages: int,
    value_key: str,
    value_label: str,
    format_value: Callable[[Any], str] = lambda x: f"{x:,}",
    footer_extra: str = "",
    updated_timestamp: Optional[datetime] = None
) -> str:
    """
    Build a single page of a leaderboard as a numbered list.
    
    Returns:
        Formatted message string with numbered list
    """
    if not results:
        return f"**{title}**\n\nNo data available\n\n*Page {page}/{total_pages}*"
    
    # Calculate which results to show for this page
    start_idx = (page - 1) * PLAYERS_PER_PAGE
    end_idx = start_idx + PLAYERS_PER_PAGE
    page_results = results[start_idx:end_idx]
    
    # Build list
    lines = [f"**{title}**\n"]
    
    for rank, row in enumerate(page_results, start_idx + 1):
        player_name = row.get("player_name") or row.get("player_id", "Unknown")
        value = row.get(value_key, 0)
        formatted_value = format_value(value)
        
        lines.append(f"{rank}. **{player_name}** - {formatted_value} {value_label.lower()}")
    
    # Build footer
    footer_parts = [f"Page {page}/{total_pages}"]
    if footer_extra:
        footer_parts.append(footer_extra)
    if updated_timestamp:
        unix_ts = int(updated_timestamp.timestamp())
        footer_parts.append(f"Updated <t:{unix_ts}:R>")
    
    footer = " • ".join(footer_parts)
    
    lines.append(f"\n*{footer}*")
    
    return "\n".join(lines)


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
            await interaction.response.defer(ephemeral=True)
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
            await interaction.response.defer(ephemeral=True)


class PaginatedLeaderboardView(discord.ui.View):
    """
    Paginated view for leaderboard commands with timeframe selection.
    
    This view displays leaderboard data with:
    - Timeframe dropdown to switch between time periods
    - Pagination buttons to navigate through results
    - Support for multiple display formats (cards, table, list)
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
        show_timeframe_in_title: bool = True,
        display_format: str = "cards"
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
            display_format: Display format ("cards", "table", or "list")
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
        self.display_format = display_format
        
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
        """Build the current page embed (for cards format)."""
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
    
    def build_table(self) -> str:
        """Build the current page as ASCII table."""
        total_pages = self._get_total_pages()
        
        return build_paginated_table(
            title=self._get_title(),
            results=self.results,
            page=self.current_page,
            total_pages=total_pages,
            value_key=self.value_key,
            value_label=self.value_label,
            format_value=self.format_value,
            footer_extra=self.footer_extra,
            updated_timestamp=self.updated_timestamp
        )
    
    def build_list(self) -> str:
        """Build the current page as numbered list."""
        total_pages = self._get_total_pages()
        
        return build_paginated_list(
            title=self._get_title(),
            results=self.results,
            page=self.current_page,
            total_pages=total_pages,
            value_key=self.value_key,
            value_label=self.value_label,
            format_value=self.format_value,
            footer_extra=self.footer_extra,
            updated_timestamp=self.updated_timestamp
        )
    
    def build_content(self) -> Tuple[Optional[str], Optional[discord.Embed]]:
        """
        Build content based on display format.
        
        Returns:
            Tuple of (content, embed) - one will be None depending on format
        """
        if self.display_format == "cards":
            return None, self.build_embed()
        elif self.display_format == "table":
            return self.build_table(), None
        elif self.display_format == "list":
            return self.build_list(), None
        else:
            # Default to cards
            return None, self.build_embed()
    
    async def update_message(self, interaction: discord.Interaction):
        """Update the message with current state."""
        content, embed = self.build_content()
        
        if self.display_format == "cards":
            await interaction.response.edit_message(content=None, embed=embed, view=self)
        else:
            await interaction.response.edit_message(content=content, embed=None, view=self)
    
    async def update_message_after_fetch(self, interaction: discord.Interaction):
        """Update the message after fetching new data (already deferred)."""
        self.updated_timestamp = datetime.now(timezone.utc)
        
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
            show_timeframe_in_title=self.show_timeframe_in_title,
            display_format=self.display_format
        )
        new_view.current_page = self.current_page
        new_view.updated_timestamp = self.updated_timestamp
        
        content, embed = new_view.build_content()
        
        if self.display_format == "cards":
            await interaction.edit_original_response(content=None, embed=embed, view=new_view)
        else:
            await interaction.edit_original_response(content=content, embed=None, view=new_view)
    
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


async def send_paginated_leaderboard(
    interaction: discord.Interaction,
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
) -> None:
    """
    Send a paginated leaderboard response using the user's format preference.
    
    This is a convenience function that:
    1. Gets the user's format preference
    2. Creates the appropriate PaginatedLeaderboardView
    3. Sends the initial message
    
    Args:
        interaction: The Discord interaction (must already be deferred)
        results: List of result dictionaries
        title_template: Title template for the leaderboard
        value_key: Key in result dict for the value column
        value_label: Label for the value column
        color: Embed color (used for cards format)
        format_value: Function to format values
        footer_extra: Extra text for footer
        current_timeframe: Initial timeframe key
        fetch_data_func: Async function to fetch data for a given number of days
        show_timeframe_in_title: Whether to append timeframe to title
    """
    # Get user's format preference
    display_format = await get_format_preference(interaction.user.id)
    
    # Create the view
    view = PaginatedLeaderboardView(
        results=results,
        title_template=title_template,
        value_key=value_key,
        value_label=value_label,
        color=color,
        format_value=format_value,
        footer_extra=footer_extra,
        current_timeframe=current_timeframe,
        fetch_data_func=fetch_data_func,
        show_timeframe_in_title=show_timeframe_in_title,
        display_format=display_format
    )
    
    # Build and send content
    content, embed = view.build_content()
    
    if display_format == "cards":
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    else:
        await interaction.followup.send(content=content, view=view, ephemeral=True)
