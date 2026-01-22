"""
Command logging utilities for Discord bot.
"""

import logging
import time
from typing import Optional

import discord

logger = logging.getLogger(__name__)


def log_command_data(interaction: discord.Interaction, command_name: str, **kwargs) -> None:
    """Log command invocation with user, channel, and parameters."""
    user = interaction.user
    user_info = f"{user.name}#{user.discriminator} ({user.id})"
    channel_info = f"#{interaction.channel.name if interaction.channel else 'DM'} ({interaction.channel_id})"
    
    params = ", ".join([f"{k}={v}" for k, v in kwargs.items() if v is not None])
    params_str = f" | Params: {params}" if params else ""
    
    logger.info(
        f"Command: {command_name} | User: {user_info} | Channel: {channel_info}{params_str}"
    )


def get_command_latency_ms(start_time: float) -> float:
    """Calculate elapsed time in milliseconds since start_time."""
    return (time.time() - start_time) * 1000


def log_command_completion(
    command_name: str,
    start_time: float,
    success: bool = True,
    interaction: Optional[discord.Interaction] = None,
    kwargs: Optional[dict] = None
) -> None:
    """Log command completion status with latency and user info."""
    status = "SUCCESS" if success else "FAILED"
    latency_ms = get_command_latency_ms(start_time)
    
    user_info = ""
    if interaction and interaction.user:
        user = interaction.user
        user_info = f" | User: {user.name}#{user.discriminator} ({user.id})"
    
    params_str = ""
    if kwargs:
        filtered_kwargs = {k: v for k, v in kwargs.items() 
                          if v is not None and k not in ('command_start_time', 'interaction')}
        if filtered_kwargs:
            params = ", ".join([f"{k}={v}" for k, v in filtered_kwargs.items()])
            params_str = f" | Params: {params}"
    
    logger.info(
        f"Command: {command_name} | Status: {status} | Latency: {latency_ms:.2f}ms{user_info}{params_str}"
    )
