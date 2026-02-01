"""
Cache management for Pathfinder leaderboard data.

Handles:
- In-memory cache storage for pre-computed leaderboard results
- Persistent state storage for message ID tracking
- SQL query logging for debugging
- Background refresh task for keeping cache fresh
"""

import asyncio
import json
import logging
import os

from datetime import datetime, timezone
from discord.ext import tasks
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from apps.discord_stats_bot.common import format_sql_query_with_params
from apps.discord_stats_bot.common.constants import TIMEFRAME_OPTIONS

logger = logging.getLogger(__name__)

# Persistent storage paths
CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))
LEADERBOARD_STATE_FILE = CACHE_DIR / "leaderboard_state.json"
SQL_LOG_FILE = CACHE_DIR / "sql_queries.log"

# State management locks
_leaderboard_state_lock = asyncio.Lock()
_stored_message_id: Optional[int] = None
_stored_channel_id: Optional[int] = None

# Global cache for pre-computed leaderboard results
# Structure: {timeframe_key: {"stats": {}, "embeds": [], "timestamp": datetime}}
_leaderboard_cache: Dict[str, Dict[str, Any]] = {}

# Track which SQL queries have been logged to avoid duplicate logging
_logged_queries: set = set()
# Accumulate SQL queries in memory to write all at once
_sql_query_logs: List[str] = []


# =============================================================================
# SQL Logging Functions
# =============================================================================

def _log_sql_query_once(query_name: str, query: str, query_params: List[Any]) -> None:
    """Log SQL query only once per query name to avoid duplicate logs on message edits."""
    if query_name not in _logged_queries:
        formatted_query = format_sql_query_with_params(query, query_params)
        log_entry = f"SQL Query [{query_name}]: {formatted_query}\n"
        _sql_query_logs.append(log_entry)
        _logged_queries.add(query_name)


def _write_sql_logs_to_file() -> None:
    """Write accumulated SQL queries to file, overwriting previous contents."""
    if not _sql_query_logs:
        return
    
    try:
        # Ensure cache directory exists
        SQL_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Overwrite file (mode 'w') to prevent file size growth
        with open(SQL_LOG_FILE, 'w', encoding='utf-8') as f:
            f.writelines(_sql_query_logs)
    except Exception as e:
        logger.warning(f"Failed to write SQL queries to file: {e}", exc_info=True)


def _clear_sql_logs() -> None:
    """Clear accumulated SQL query logs and reset tracking."""
    global _logged_queries, _sql_query_logs
    _logged_queries.clear()
    _sql_query_logs.clear()


# =============================================================================
# State Persistence Functions
# =============================================================================

async def _load_leaderboard_state() -> None:
    """Load persisted leaderboard message ID from disk."""
    global _stored_message_id, _stored_channel_id
    
    if not LEADERBOARD_STATE_FILE.exists():
        logger.info(f"Leaderboard state file not found at {LEADERBOARD_STATE_FILE}, starting fresh")
        return
    
    try:
        with open(LEADERBOARD_STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        _stored_message_id = data.get("message_id")
        _stored_channel_id = data.get("channel_id")
        
        if _stored_message_id and _stored_channel_id:
            logger.info(f"Loaded leaderboard state: message_id={_stored_message_id}, channel_id={_stored_channel_id}")
        else:
            logger.info("Leaderboard state file exists but contains no valid IDs")
            
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load leaderboard state from {LEADERBOARD_STATE_FILE}: {e}. Starting fresh.")


async def _save_leaderboard_state(message_id: int, channel_id: int) -> None:
    """Save leaderboard message ID to disk."""
    global _stored_message_id, _stored_channel_id
    
    try:
        LEADERBOARD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "message_id": message_id,
            "channel_id": channel_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        temp_file = LEADERBOARD_STATE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(LEADERBOARD_STATE_FILE)
        
        async with _leaderboard_state_lock:
            _stored_message_id = message_id
            _stored_channel_id = channel_id
        
        logger.info(f"Saved leaderboard state: message_id={message_id}, channel_id={channel_id}")
        
    except IOError as e:
        logger.error(f"Failed to save leaderboard state to {LEADERBOARD_STATE_FILE}: {e}", exc_info=True)


# =============================================================================
# Cache Access Functions
# =============================================================================

def get_leaderboard_cache() -> Dict[str, Dict[str, Any]]:
    """Get the global leaderboard cache."""
    return _leaderboard_cache


def get_cached_data(timeframe: str) -> Tuple[Optional[Dict[str, Any]], datetime]:
    """
    Get cached data for a specific timeframe.
    
    Args:
        timeframe: Timeframe key (e.g., "7d", "30d", "all")
        
    Returns:
        Tuple of (stats_dict, timestamp) or (None, current_time) if not cached
    """
    cached = _leaderboard_cache.get(timeframe)
    if cached and cached.get("stats"):
        return cached["stats"], cached["timestamp"]
    return None, datetime.now(timezone.utc)


async def get_stored_message_state() -> Tuple[Optional[int], Optional[int]]:
    """Get the stored message ID and channel ID."""
    async with _leaderboard_state_lock:
        return _stored_message_id, _stored_channel_id


# =============================================================================
# Cache Refresh Task
# =============================================================================

@tasks.loop(minutes=20)
async def refresh_leaderboard_cache():
    """
    Pre-compute and cache leaderboard data for all timeframes.
    Runs every 20 minutes to keep cache fresh.
    """
    global _leaderboard_cache
    
    # Import here to avoid circular imports
    from apps.discord_stats_bot.jobs.pathfinder.pathfinder_queries import fetch_all_leaderboard_stats
    from apps.discord_stats_bot.jobs.pathfinder.pathfinder_embeds import build_leaderboard_embeds
    
    try:
        logger.info("Starting leaderboard cache refresh...")
        
        # Clear SQL logs at the start of each refresh cycle
        _clear_sql_logs()
        
        now_utc = datetime.now(timezone.utc)
        
        # Pre-compute stats for all timeframes
        cached_timeframes = 0
        cached_info = []
        for timeframe_key, config in TIMEFRAME_OPTIONS.items():
            try:
                days = config["days"]
                label = config["label"]
                
                # Fetch stats and build embeds
                stats = await fetch_all_leaderboard_stats(days)
                embeds = build_leaderboard_embeds(stats, label)
                
                # Store in cache
                _leaderboard_cache[timeframe_key] = {
                    "stats": stats,
                    "embeds": embeds,
                    "timestamp": now_utc,
                    "label": label
                }
                
                cached_info.append(f"{timeframe_key} ({label})")
                cached_timeframes += 1
                
            except Exception as e:
                logger.error(f"Error caching leaderboard data for {timeframe_key}: {e}", exc_info=True)
        
        if cached_info:
            logger.info(f"Cached leaderboard data for {', '.join(cached_info)}")
        
        # Write accumulated SQL queries to file at the end of refresh cycle
        _write_sql_logs_to_file()
        
        not_cached = len(TIMEFRAME_OPTIONS) - cached_timeframes
        logger.info(
            f"Leaderboard cache refresh complete. Cached {cached_timeframes}/{len(TIMEFRAME_OPTIONS)} "
            f"timeframes ({not_cached} not cached)."
        )
        
    except Exception as e:
        logger.error(f"Error in refresh_leaderboard_cache task: {e}", exc_info=True)


# =============================================================================
# Module Initialization
# =============================================================================

def _init_sql_logger():
    """Initialize the SQL logger in the queries module."""
    from apps.discord_stats_bot.jobs.pathfinder.pathfinder_queries import set_sql_logger
    set_sql_logger(_log_sql_query_once)


# Initialize SQL logger when module is imported
_init_sql_logger()
