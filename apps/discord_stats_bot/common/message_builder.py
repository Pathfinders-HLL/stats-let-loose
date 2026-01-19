"""
Shared utilities for building Discord messages with tables and embeds.
"""

from typing import List, Optional
from tabulate import tabulate

from apps.discord_stats_bot.common.constants import DISCORD_MESSAGE_MAX_LENGTH


def build_table_message(
    title: str,
    table_data: List[List],
    headers: List[str],
    prefix_lines: Optional[List[str]] = None,
    max_length: int = DISCORD_MESSAGE_MAX_LENGTH,
    truncation_message: Optional[str] = None
) -> str:
    """
    Build a Discord message with a table, automatically reducing rows if needed.
    
    Args:
        title: Title/header for the message
        table_data: List of rows (each row is a list of values)
        headers: Column headers
        prefix_lines: Additional lines to include before the table
        max_length: Maximum message length (default: 2000 for Discord)
        truncation_message: Custom message when rows are truncated.
                          Use {num_rows} and {total_rows} placeholders.
        
    Returns:
        Formatted message string
    """
    prefix_lines = prefix_lines or []
    truncation_message = truncation_message or f"\n*Showing {{num_rows}} of {{total_rows}} results (message length limit)*"
    
    message_prefix_lines = [title] + prefix_lines
    
    # Try with all rows first
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
            message_lines.append(truncation_message.format(
                num_rows=num_rows,
                total_rows=len(table_data)
            ))
        
        message = "\n".join(message_lines)
        
        if len(message) <= max_length:
            return message
    
    # Fallback: return empty message if even 1 row doesn't fit
    return title + "\n*Message too long to display*"
