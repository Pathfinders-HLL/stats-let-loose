"""
In-memory cache with JSON persistence for Discord user display format preferences.

Supported formats:
- "cards": Discord embeds (default)
- "table": ASCII tables using tabulate
- "list": Simple numbered list
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional, Literal

from cachetools import LRUCache

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))
FORMAT_CACHE_FILE = CACHE_DIR / "format_preference_cache.json"

_cache_lock = asyncio.Lock()
_cache: LRUCache[int, str] = LRUCache(maxsize=1000000)
_cache_initialized = False

# Valid format options
VALID_FORMATS = {"cards", "table", "list"}
DEFAULT_FORMAT = "cards"

# Display names for formats
FORMAT_DISPLAY_NAMES = {
    "cards": "Cards (Embeds)",
    "table": "ASCII Table",
    "list": "Numbered List"
}


async def _load_cache_from_disk() -> None:
    """Load persisted cache from disk."""
    global _cache, _cache_initialized
    
    if not FORMAT_CACHE_FILE.exists():
        logger.info(f"Format preference cache file not found at {FORMAT_CACHE_FILE}, starting with empty cache")
        _cache_initialized = True
        return
    
    try:
        with open(FORMAT_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Rebuild cache from JSON data
        # JSON keys are strings, but Discord user IDs are integers
        async with _cache_lock:
            _cache.clear()
            for user_id_str, format_pref in data.items():
                user_id = int(user_id_str)
                if format_pref in VALID_FORMATS:
                    _cache[user_id] = format_pref
        
        logger.info(f"Loaded {len(_cache)} format preferences from cache file")
        _cache_initialized = True
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load format cache from {FORMAT_CACHE_FILE}: {e}. Starting with empty cache.")
        _cache_initialized = True


async def _save_cache_to_disk(data: dict = None) -> None:
    """Persist cache to disk atomically.
    
    Args:
        data: Optional pre-serialized cache data. If None, will read from cache.
    """
    try:
        FORMAT_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # If data not provided, read from cache (with lock)
        if data is None:
            async with _cache_lock:
                data = {str(user_id): format_pref for user_id, format_pref in _cache.items()}
        
        temp_file = FORMAT_CACHE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(FORMAT_CACHE_FILE)
        logger.info(f"Saved {len(data)} format preferences")
    except IOError as e:
        logger.error(f"Failed to save format cache to {FORMAT_CACHE_FILE}: {e}", exc_info=True)


async def get_format_preference(discord_user_id: int) -> str:
    """Get stored format preference for a Discord user, or default if not found."""
    async with _cache_lock:
        return _cache.get(discord_user_id, DEFAULT_FORMAT)


async def set_format_preference(discord_user_id: int, format_pref: str) -> None:
    """Store or update format preference for a Discord user."""
    if format_pref not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format_pref}. Valid formats: {', '.join(VALID_FORMATS)}")
    
    async with _cache_lock:
        _cache[discord_user_id] = format_pref
        # Copy data while holding lock, then release before saving
        data_to_save = {str(user_id): fmt for user_id, fmt in _cache.items()}
    await _save_cache_to_disk(data_to_save)


async def clear_format_preference(discord_user_id: int) -> None:
    """Remove stored format preference for a Discord user (resets to default)."""
    should_save = False
    async with _cache_lock:
        if discord_user_id in _cache:
            del _cache[discord_user_id]
            should_save = True
            # Copy data while holding lock, then release before saving
            data_to_save = {str(user_id): fmt for user_id, fmt in _cache.items()}
            logger.info(f"Cleared format preference for Discord user {discord_user_id}")
        else:
            logger.info(f"No format preference found for Discord user {discord_user_id}")
    
    if should_save:
        await _save_cache_to_disk(data_to_save)


async def initialize_format_cache() -> None:
    """Initialize the cache by loading from disk. Should be called during bot startup."""
    await _load_cache_from_disk()
