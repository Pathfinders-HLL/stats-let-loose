"""
Shared utility functions for common operations and reusable components.

Provides:
- Time and date formatting utilities
- Table message building with auto-truncation for Discord limits
"""

from datetime import datetime
from typing import List, Union

from tabulate import tabulate

from apps.discord_stats_bot.common.constants import DISCORD_MESSAGE_MAX_LENGTH


def format_time_seconds(seconds: Union[int, float]) -> str:
    """
    Format seconds as a human-readable time string.
    
    Examples:
        - 45 -> "45s"
        - 120 -> "2m"
        - 3660 -> "1h 1m"
        - 7200 -> "2h"
    
    Args:
        seconds: Time in seconds (can be int or float)
    
    Returns:
        Formatted time string (e.g., '1h 30m', '45m', '30s')
    """
    secs = int(round(seconds))
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        minutes = secs // 60
        return f"{minutes}m"
    else:
        hours = secs // 3600
        minutes = (secs % 3600) // 60
        if minutes > 0:
            return f"{hours}h {minutes}m"
        return f"{hours}h"


def format_date(date_value: Union[datetime, str]) -> str:
    """
    Format a date value as a consistent date string.
    
    Formats datetime objects as 'YYYY-MM-DD', and converts other types
    to string representation.
    
    Args:
        date_value: A datetime object or string representation
    
    Returns:
        Formatted date string in 'YYYY-MM-DD' format, or string representation
        if not a datetime object
    """
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    return str(date_value)


def build_table_message(
    table_data: List[List],
    headers: List[str],
    message_prefix_lines: List[str],
    item_name: str = "rows"
) -> str:
    """
    Build a Discord-compatible table message with auto-truncation.
    
    Creates a message containing a table formatted for Discord, automatically
    truncating rows if the message exceeds Discord's character limit.
    
    Args:
        table_data: List of rows, where each row is a list of values
        headers: Column headers for the table
        message_prefix_lines: Lines to display before the table (e.g., title)
        item_name: Name of items for truncation message (e.g., "matches", "players")
    
    Returns:
        Formatted message string that fits within Discord's message length limit
    """
    for num_rows in range(len(table_data), 0, -1):
        table_str = tabulate(
            table_data[:num_rows],
            headers=headers,
            tablefmt="github"
        )
        
        message_lines = message_prefix_lines.copy()
        message_lines.append("```")
        message_lines.append(table_str)
        message_lines.append("```")
        
        if num_rows < len(table_data):
            message_lines.append(
                f"\n*Showing {num_rows} of {len(table_data)} {item_name} (message length limit)*"
            )
        
        message = "\n".join(message_lines)
        
        if len(message) <= DISCORD_MESSAGE_MAX_LENGTH:
            return message
    
    # Fallback: return just the prefix with an error message
    return "\n".join(message_prefix_lines + ["```", "No data to display", "```"])
