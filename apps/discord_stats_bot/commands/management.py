"""
Management commands for Discord bot.

This module contains profile management commands:
- /profile setid: Set your default player ID
- /profile clearid: Clear your stored player ID
- /profile format: Set your preferred leaderboard display format
"""

import logging
import time
from typing import List

import discord
from discord import app_commands

from apps.discord_stats_bot.common.player_id_cache import set_player_id, clear_player_id
from apps.discord_stats_bot.common.format_preference_cache import (
    get_format_preference,
    set_format_preference,
    VALID_FORMATS,
    FORMAT_DISPLAY_NAMES,
    DEFAULT_FORMAT,
)
from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    find_player_by_id_or_name,
    log_command_data,
    log_command_completion
)

logger = logging.getLogger(__name__)


def setup_profile_command(tree: app_commands.CommandTree, channel_check=None) -> None:
    """
    Register the /profile command group with setid and clearid subcommands.
    
    Args:
        tree: The bot's command tree to register the command with
        channel_check: Optional function to check if the channel is allowed
    """
    profile_group = app_commands.Group(
        name="profile",
        description="Manage your player profile settings"
    )
    
    @profile_group.command(name="setid", description="Set your default player ID so you don't have to enter it every time")
    @app_commands.describe(player="Your player ID or player name")
    async def profile_setid(interaction: discord.Interaction, player: str):
        """Set your default player ID so you don't have to enter it every time."""
        start_time = time.time()
        log_command_data(interaction, "profile setid", player=player)
        
        try:
            if channel_check and not channel_check(interaction):
                await interaction.response.send_message(
                    f"❌ This bot can only be used in the designated channel.",
                    ephemeral=True
                )
                log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
                return
            
            await interaction.response.defer(ephemeral=True)
            
            pool = await get_readonly_db_pool()
            async with pool.acquire() as conn:
                try:
                    # Verify player exists
                    player_id, found_player_name = await find_player_by_id_or_name(conn, player)
                    
                    if not player_id:
                        await interaction.followup.send(f"❌ Could not find player: `{player}` Try using a player ID or exact player name.", ephemeral=True)
                        log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
                        return
                    
                    # Store the mapping in cache
                    await set_player_id(interaction.user.id, player_id)
                    
                    display_name = found_player_name if found_player_name else player_id
                    await interaction.followup.send(
                        f"✅ Your player ID has been set to: `{display_name}` ({player_id})\n"
                        f"You can now use commands without specifying a player ID!",
                        ephemeral=True
                    )
                    log_command_completion("profile setid", start_time, success=True, interaction=interaction, kwargs={"player": player})
                    
                except ValueError as e:
                    logger.error(f"Configuration error: {e}", exc_info=True)
                    log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
                    await interaction.followup.send(
                        f"❌ Configuration error. Please check database connection settings.",
                        ephemeral=True
                    )
                except ConnectionError as e:
                    logger.error(f"Database connection error: {e}", exc_info=True)
                    log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
                    await interaction.followup.send(
                        f"❌ Failed to connect to database.",
                        ephemeral=True
                    )
                except Exception as e:
                    logger.error(f"Unexpected error in profile setid: {e}", exc_info=True)
                    log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
                    await interaction.followup.send(
                        f"❌ An unexpected error occurred: {str(e)}",
                        ephemeral=True
                    )
        except Exception as e:
            logger.error(f"Unexpected error in profile setid: {e}", exc_info=True)
            log_command_completion("profile setid", start_time, success=False, interaction=interaction, kwargs={"player": player})
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ An unexpected error occurred: {str(e)}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"❌ An unexpected error occurred: {str(e)}",
                    ephemeral=True
                )

    @profile_group.command(name="clearid", description="Clear your stored player ID")
    async def profile_clearid(interaction: discord.Interaction):
        """Clear your stored player ID so you can set a new one."""
        start_time = time.time()
        log_command_data(interaction, "profile clearid")
        
        try:
            if channel_check and not channel_check(interaction):
                await interaction.response.send_message(
                    f"❌ This bot can only be used in the designated channel.",
                    ephemeral=True
                )
                log_command_completion("profile clearid", start_time, success=False, interaction=interaction, kwargs={})
                return
            
            # Clear the stored player ID
            await clear_player_id(interaction.user.id)
            
            await interaction.response.send_message(f"✅ Your player ID has been cleared. Use `/profile setid` to set a new one.", ephemeral=True)
            log_command_completion("profile clearid", start_time, success=True, interaction=interaction, kwargs={})
            
        except Exception as e:
            logger.error(f"Unexpected error in profile clearid: {e}", exc_info=True)
            log_command_completion("profile clearid", start_time, success=False, interaction=interaction, kwargs={})
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ An unexpected error occurred: {str(e)}",
                    ephemeral=True
                )

    # Format choices for autocomplete
    FORMAT_CHOICES = [
        app_commands.Choice(name="Cards (Embeds)", value="cards"),
        app_commands.Choice(name="ASCII Table", value="table"),
        app_commands.Choice(name="Numbered List", value="list"),
    ]
    
    async def format_autocomplete(
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        """Autocomplete function for format parameter."""
        current_lower = current.lower()
        matching = [
            choice for choice in FORMAT_CHOICES
            if current_lower in choice.name.lower() or current_lower in choice.value.lower()
        ]
        return matching[:25]

    @profile_group.command(name="format", description="Set your preferred leaderboard display format")
    @app_commands.describe(
        format_type="Display format: Cards (embeds), ASCII Table, or Numbered List"
    )
    @app_commands.autocomplete(format_type=format_autocomplete)
    async def profile_format(interaction: discord.Interaction, format_type: str):
        """Set your preferred leaderboard display format."""
        start_time = time.time()
        log_command_data(interaction, "profile format", format_type=format_type)
        
        try:
            if channel_check and not channel_check(interaction):
                await interaction.response.send_message(
                    f"❌ This bot can only be used in the designated channel.",
                    ephemeral=True
                )
                log_command_completion("profile format", start_time, success=False, interaction=interaction, kwargs={"format_type": format_type})
                return
            
            # Normalize format type
            format_lower = format_type.lower().strip()
            
            # Handle display name to value mapping
            format_mapping = {
                "cards": "cards",
                "cards (embeds)": "cards",
                "embeds": "cards",
                "table": "table",
                "ascii table": "table",
                "ascii": "table",
                "list": "list",
                "numbered list": "list",
            }
            
            format_value = format_mapping.get(format_lower)
            
            if format_value not in VALID_FORMATS:
                await interaction.response.send_message(
                    f"❌ Invalid format: `{format_type}`. Valid formats: Cards (Embeds), ASCII Table, Numbered List",
                    ephemeral=True
                )
                log_command_completion("profile format", start_time, success=False, interaction=interaction, kwargs={"format_type": format_type})
                return
            
            # Store the preference
            await set_format_preference(interaction.user.id, format_value)
            
            display_name = FORMAT_DISPLAY_NAMES.get(format_value, format_value)
            await interaction.response.send_message(
                f"✅ Your leaderboard display format has been set to: **{display_name}**\n"
                f"All `/leaderboard` commands will now use this format.",
                ephemeral=True
            )
            log_command_completion("profile format", start_time, success=True, interaction=interaction, kwargs={"format_type": format_type})
            
        except Exception as e:
            logger.error(f"Unexpected error in profile format: {e}", exc_info=True)
            log_command_completion("profile format", start_time, success=False, interaction=interaction, kwargs={"format_type": format_type})
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ An unexpected error occurred: {str(e)}",
                    ephemeral=True
                )

    # Add the group to the command tree
    tree.add_command(profile_group)