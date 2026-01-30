"""
Shared utility functions for common operations and reusable components.
"""

from datetime import datetime
from typing import Union


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
