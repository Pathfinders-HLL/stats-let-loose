"""
Discord UI components for Pathfinder leaderboards.

Contains:
- StatSelect: Dropdown for selecting which stat to view
- TimeframeSelect: Dropdown for selecting timeframe
- PaginatedLeaderboardView: Full interactive view with pagination
- LeaderboardView: Simple view with Advanced View button for main post
"""

import logging

import discord

from datetime import datetime, timezone
from typing import Tuple, Optional, List, Dict, Any

from apps.discord_stats_bot.common.constants import (
    TIMEFRAME_OPTIONS,
    LEADERBOARD_STAT_CONFIGS,
)
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_cache import get_cached_data
from apps.discord_stats_bot.jobs.pathfinder.pathfinder_embeds import (
    _build_stat_embed_page,
    _get_total_pages,
)

logger = logging.getLogger(__name__)


class StatSelect(discord.ui.Select):
    """Dropdown select for choosing which stat to view."""
    
    def __init__(self, current_stat_idx: int = 0):
        options = []
        for idx, config in enumerate(LEADERBOARD_STAT_CONFIGS):
            emoji = config["title"].split(" ")[0]
            stat_name = config["title"].split(" ", 1)[1]
            options.append(discord.SelectOption(
                label=stat_name,
                value=str(idx),
                emoji=emoji,
                default=(idx == current_stat_idx)
            ))
        
        super().__init__(
            custom_id="pathfinder_stat_select",
            placeholder="Select a stat...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle stat selection - update the view's stat index."""
        view: PaginatedLeaderboardView = self.view
        view.current_stat_idx = int(self.values[0])
        view.current_page = 1  # Reset to first page when changing stats
        await view.update_message(interaction)


class TimeframeSelect(discord.ui.Select):
    """Dropdown select for choosing leaderboard timeframe."""
    
    def __init__(self, current_timeframe: str = "7d"):
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
            custom_id="pathfinder_leaderboard_timeframe",
            placeholder="Select a timeframe...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle timeframe selection - update the view's timeframe."""
        view: PaginatedLeaderboardView = self.view
        view.current_timeframe = self.values[0]
        view.current_page = 1  # Reset to first page when changing timeframe
        await view.update_message(interaction)


class PaginatedLeaderboardView(discord.ui.View):
    """Persistent view with pagination, stat selection, and timeframe selector."""
    
    def __init__(
        self,
        current_stat_idx: int = 0,
        current_page: int = 1,
        current_timeframe: str = "7d"
    ):
        # Set timeout to None for persistent view
        super().__init__(timeout=None)
        
        self.current_stat_idx = current_stat_idx
        self.current_page = current_page
        self.current_timeframe = current_timeframe
        
        # Add stat selector (row 0)
        self.add_item(StatSelect(current_stat_idx))
        
        # Add timeframe selector (row 1)
        timeframe_select = TimeframeSelect(current_timeframe)
        timeframe_select.row = 1
        self.add_item(timeframe_select)
    
    def _get_cached_data(self) -> Tuple[Optional[Dict[str, Any]], datetime]:
        """Get cached data for current timeframe."""
        return get_cached_data(self.current_timeframe)
    
    def _get_current_results(self) -> Tuple[List[Dict[str, Any]], datetime]:
        """Get results for the current stat from cache."""
        stats, timestamp = self._get_cached_data()
        if stats is None:
            return [], timestamp
        
        stat_key = LEADERBOARD_STAT_CONFIGS[self.current_stat_idx]["key"]
        return stats.get(stat_key, []), timestamp
    
    def _get_total_pages(self) -> int:
        """Get total pages for current stat."""
        results, _ = self._get_current_results()
        return _get_total_pages(results)
    
    def build_embed(self) -> discord.Embed:
        """Build the current page embed."""
        results, timestamp = self._get_current_results()
        stat_config = LEADERBOARD_STAT_CONFIGS[self.current_stat_idx]
        timeframe_config = TIMEFRAME_OPTIONS.get(self.current_timeframe, TIMEFRAME_OPTIONS["7d"])
        total_pages = self._get_total_pages()
        
        return _build_stat_embed_page(
            results=results,
            stat_config=stat_config,
            page=self.current_page,
            total_pages=total_pages,
            timeframe_label=timeframe_config["label"],
            updated_timestamp=timestamp
        )
    
    async def update_message(self, interaction: discord.Interaction):
        """Update the message with current state."""
        embed = self.build_embed()
        
        # Rebuild the view to update button states
        new_view = PaginatedLeaderboardView(
            current_stat_idx=self.current_stat_idx,
            current_page=self.current_page,
            current_timeframe=self.current_timeframe
        )
        
        await interaction.response.edit_message(embed=embed, view=new_view)
    
    @discord.ui.button(emoji="⏮️", style=discord.ButtonStyle.secondary, custom_id="first_page", row=2)
    async def first_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to first page."""
        self.current_page = 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="◀️", style=discord.ButtonStyle.primary, custom_id="prev_page", row=2)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        if self.current_page > 1:
            self.current_page -= 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="▶️", style=discord.ButtonStyle.primary, custom_id="next_page", row=2)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        total_pages = self._get_total_pages()
        if self.current_page < total_pages:
            self.current_page += 1
        await self.update_message(interaction)
    
    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, custom_id="last_page", row=2)
    async def last_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to last page."""
        self.current_page = self._get_total_pages()
        await self.update_message(interaction)


class LeaderboardView(discord.ui.View):
    """Simple persistent view with a Browse Details button for the main post."""
    
    def __init__(self):
        # Set timeout to None for persistent view
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="Advanced View",
        emoji="🔍",
        style=discord.ButtonStyle.primary,
        custom_id="pathfinder_browse_details"
    )
    async def browse_details(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open the paginated leaderboard browser."""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Create paginated view starting at first stat, first page, 7-day timeframe
            view = PaginatedLeaderboardView(
                current_stat_idx=0,
                current_page=1,
                current_timeframe="7d"
            )
            
            embed = view.build_embed()
            
            await interaction.followup.send(
                embed=embed,
                view=view,
                ephemeral=True
            )
            
        except Exception as e:
            logger.error(f"Error opening browse details: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while fetching the leaderboards.",
                ephemeral=True
            )
