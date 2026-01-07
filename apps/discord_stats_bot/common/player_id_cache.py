"""
In-memory cache with JSON persistence for Discord user to player ID mappings.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from cachetools import LRUCache

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))
CACHE_FILE = CACHE_DIR / "player_id_cache.json"

_cache_lock = asyncio.Lock()
_cache: LRUCache[int, str] = LRUCache(maxsize=1000000)
_cache_initialized = False


async def _load_cache_from_disk() -> None:
    """Load persisted cache from disk."""
    global _cache, _cache_initialized
    
    if not CACHE_FILE.exists():
        logger.info(f"Cache file not found at {CACHE_FILE}, starting with empty cache")
        _cache_initialized = True
        return
    
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Rebuild cache from JSON data
        # JSON keys are strings, but Discord user IDs are integers
        async with _cache_lock:
            _cache.clear()
            for user_id_str, player_id in data.items():
                user_id = int(user_id_str)
                _cache[user_id] = player_id
        
        logger.info(f"Loaded {len(_cache)} player ID mappings from cache file")
        _cache_initialized = True
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load cache from {CACHE_FILE}: {e}. Starting with empty cache.")
        _cache_initialized = True


async def _save_cache_to_disk(data: dict = None) -> None:
    """Persist cache to disk atomically.
    
    Args:
        data: Optional pre-serialized cache data. If None, will read from cache.
    """
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # If data not provided, read from cache (with lock)
        if data is None:
            async with _cache_lock:
                data = {str(user_id): player_id for user_id, player_id in _cache.items()}
        
        temp_file = CACHE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(CACHE_FILE)
        logger.info(f"Saved {len(data)} player ID mappings")
    except IOError as e:
        logger.error(f"Failed to save cache to {CACHE_FILE}: {e}", exc_info=True)


async def get_player_id(discord_user_id: int) -> Optional[str]:
    """Get stored player ID for a Discord user, or None if not found."""
    async with _cache_lock:
        return _cache.get(discord_user_id)


async def set_player_id(discord_user_id: int, player_id: str) -> None:
    """Store or update player ID for a Discord user."""
    async with _cache_lock:
        _cache[discord_user_id] = player_id
        # Copy data while holding lock, then release before saving
        data_to_save = {str(user_id): pid for user_id, pid in _cache.items()}
    await _save_cache_to_disk(data_to_save)


async def clear_player_id(discord_user_id: int) -> None:
    """Remove stored player ID for a Discord user."""
    should_save = False
    async with _cache_lock:
        if discord_user_id in _cache:
            del _cache[discord_user_id]
            should_save = True
            # Copy data while holding lock, then release before saving
            data_to_save = {str(user_id): pid for user_id, pid in _cache.items()}
            logger.info(f"Cleared player ID for Discord user {discord_user_id}")
        else:
            logger.info(f"No player ID found for Discord user {discord_user_id}")
    
    if should_save:
        await _save_cache_to_disk(data_to_save)


async def initialize_cache() -> None:
    """Initialize the cache by loading from disk. Should be called during bot startup."""
    await _load_cache_from_disk()

