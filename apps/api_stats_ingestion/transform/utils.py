"""
Utility functions for API ingestion processes.

This module contains utility functions used for data transformation
and processing in the API ingestion pipeline.
"""

from __future__ import annotations

from datetime import datetime


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse ISO 8601 timestamp string to datetime object.
    
    Handles formats like:
    - "2025-12-20T03:27:06" (without microseconds)
    - "2025-12-20T03:27:06.123456" (with microseconds)
    
    Args:
        timestamp_str: ISO 8601 formatted timestamp string
    
    Returns:
        datetime object
    
    Raises:
        ValueError: If the timestamp string cannot be parsed
    """
    # Remove microseconds if present for consistent parsing
    if '.' in timestamp_str:
        timestamp_str = timestamp_str.split('.')[0]
    return datetime.fromisoformat(timestamp_str)


def calculate_winning_team(allies_score: int, axis_score: int) -> str:
    """
    Calculate winning team based on scores.
    
    Args:
        allies_score: Score for the Allies team
        axis_score: Score for the Axis team
    
    Returns:
        "Allies", "Axis", or "Tie"
    """
    if allies_score > axis_score:
        return "Allies"
    elif axis_score > allies_score:
        return "Axis"
    else:
        return "Tie"


def calculate_duration(start_time: datetime, end_time: datetime) -> int:
    """
    Calculate duration between two timestamps in seconds.
    
    Args:
        start_time: Start datetime
        end_time: End datetime
    
    Returns:
        Duration in seconds as an integer
    """
    delta = end_time - start_time
    return int(delta.total_seconds())

