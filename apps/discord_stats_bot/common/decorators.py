"""
Command decorators for Discord bot.
"""

import inspect
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional

import asyncpg
import discord

from apps.discord_stats_bot.common.logging import (
    log_command_data,
    log_command_completion,
)

logger = logging.getLogger(__name__)


async def handle_command_errors(
    interaction: discord.Interaction,
    command_name: str,
    start_time: float,
    error: Exception,
    use_ephemeral: bool = False,
    kwargs: Optional[dict] = None
) -> None:
    """Handle errors with appropriate logging and user-facing messages."""
    exc_info = (type(error), error, error.__traceback__)
    
    if isinstance(error, ValueError):
        logger.error(f"Configuration error in {command_name}: {error}", exc_info=exc_info)
        error_msg = "❌ Configuration error. Please check database connection settings. Go ask Gordon Bombay to fix this."
    elif isinstance(error, ConnectionError):
        logger.error(f"Database connection error in {command_name}: {error}", exc_info=exc_info)
        error_msg = "❌ Failed to connect to database. Go ask Gordon Bombay to fix this."
    elif isinstance(error, asyncpg.PostgresError):
        logger.error(f"Database query error in {command_name}: {error}", exc_info=exc_info)
        error_msg = "❌ Database error. Go ask Gordon Bombay to fix this."
    else:
        logger.error(f"Unexpected error in {command_name}: {error}", exc_info=exc_info)
        error_msg = "❌ An unexpected error occurred. Go ask Gordon Bombay to fix this."

    log_command_completion(command_name, start_time, success=False, interaction=interaction, kwargs=kwargs)

    if not interaction.response.is_done():
        await interaction.response.send_message(error_msg, ephemeral=use_ephemeral)
    else:
        await interaction.followup.send(error_msg, ephemeral=use_ephemeral)


def command_wrapper(
    command_name: str,
    channel_check: Optional[Callable[[discord.Interaction], bool]] = None,
    log_params: Optional[dict] = None
):
    """
    Decorator that handles channel checks, logging, error handling, and response deferral.
    
    Args:
        command_name: Name of the command for logging
        channel_check: Optional function to check if channel is allowed
        log_params: Optional additional params to log
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            command_start_time = time.time()

            log_kwargs = log_params or {}
            log_kwargs.update(kwargs)
            log_command_data(interaction, command_name, **log_kwargs)

            try:
                if channel_check and not channel_check(interaction):
                    await interaction.response.send_message(
                        "❌ This bot can only be used in the designated channel.",
                        ephemeral=True
                    )
                    log_command_completion(
                        command_name, command_start_time, 
                        success=False, interaction=interaction, kwargs=log_kwargs
                    )
                    return

                await interaction.response.defer()
                
                result = await func(interaction, *args, **kwargs)
                return result

            except Exception as e:
                await handle_command_errors(
                    interaction, command_name, command_start_time, e, kwargs=log_kwargs
                )
                return
        
        wrapper.__signature__ = inspect.signature(func)
        return wrapper
    return decorator
