"""
In-memory caches with JSON persistence for Discord bot.

Provides caching for:
- Player ID mappings (Discord user ID -> game player ID)
- Format preferences (Discord user ID -> display format)
"""

import asyncio
import json
import logging
import os

from cachetools import LRUCache

from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))

# =============================================================================
# Player ID Cache
# =============================================================================

_PLAYER_ID_CACHE_FILE = CACHE_DIR / "player_id_cache.json"
_player_id_lock = asyncio.Lock()
_player_id_cache: LRUCache[int, str] = LRUCache(maxsize=1000000)
_player_id_cache_initialized = False


async def _load_player_id_cache() -> None:
    """Load persisted player ID cache from disk."""
    global _player_id_cache, _player_id_cache_initialized
    
    if not _PLAYER_ID_CACHE_FILE.exists():
        logger.info(f"Player ID cache file not found at {_PLAYER_ID_CACHE_FILE}, starting with empty cache")
        _player_id_cache_initialized = True
        return
    
    try:
        with open(_PLAYER_ID_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        async with _player_id_lock:
            _player_id_cache.clear()
            for user_id_str, player_id in data.items():
                user_id = int(user_id_str)
                _player_id_cache[user_id] = player_id
        
        logger.info(f"Loaded {len(_player_id_cache)} player ID mappings from cache file")
        _player_id_cache_initialized = True
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load player ID cache from {_PLAYER_ID_CACHE_FILE}: {e}. Starting with empty cache.")
        _player_id_cache_initialized = True


async def _save_player_id_cache(data: dict = None) -> None:
    """Persist player ID cache to disk atomically."""
    try:
        _PLAYER_ID_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        if data is None:
            async with _player_id_lock:
                data = {str(user_id): player_id for user_id, player_id in _player_id_cache.items()}
        
        temp_file = _PLAYER_ID_CACHE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(_PLAYER_ID_CACHE_FILE)
        logger.info(f"Saved {len(data)} player ID mappings")
    except IOError as e:
        logger.error(f"Failed to save player ID cache to {_PLAYER_ID_CACHE_FILE}: {e}", exc_info=True)


async def get_player_id(discord_user_id: int) -> Optional[str]:
    """Get stored player ID for a Discord user, or None if not found."""
    async with _player_id_lock:
        return _player_id_cache.get(discord_user_id)


async def set_player_id(discord_user_id: int, player_id: str) -> None:
    """Store or update player ID for a Discord user."""
    async with _player_id_lock:
        _player_id_cache[discord_user_id] = player_id
        data_to_save = {str(user_id): pid for user_id, pid in _player_id_cache.items()}
    await _save_player_id_cache(data_to_save)


async def clear_player_id(discord_user_id: int) -> None:
    """Remove stored player ID for a Discord user."""
    should_save = False
    async with _player_id_lock:
        if discord_user_id in _player_id_cache:
            del _player_id_cache[discord_user_id]
            should_save = True
            data_to_save = {str(user_id): pid for user_id, pid in _player_id_cache.items()}
            logger.info(f"Cleared player ID for Discord user {discord_user_id}")
        else:
            logger.info(f"No player ID found for Discord user {discord_user_id}")
    
    if should_save:
        await _save_player_id_cache(data_to_save)


async def initialize_cache() -> None:
    """Initialize the player ID cache by loading from disk. Should be called during bot startup."""
    await _load_player_id_cache()


# =============================================================================
# Format Preference Cache
# =============================================================================

_FORMAT_CACHE_FILE = CACHE_DIR / "format_preference_cache.json"
_format_lock = asyncio.Lock()
_format_cache: LRUCache[int, str] = LRUCache(maxsize=1000000)
_format_cache_initialized = False

# Valid format options
VALID_FORMATS = {"cards", "table", "list"}
DEFAULT_FORMAT = "cards"

# Display names for formats
FORMAT_DISPLAY_NAMES = {
    "cards": "Cards (Embeds)",
    "table": "ASCII Table",
    "list": "Numbered List"
}


async def _load_format_cache() -> None:
    """Load persisted format preference cache from disk."""
    global _format_cache, _format_cache_initialized
    
    if not _FORMAT_CACHE_FILE.exists():
        logger.info(f"Format preference cache file not found at {_FORMAT_CACHE_FILE}, starting with empty cache")
        _format_cache_initialized = True
        return
    
    try:
        with open(_FORMAT_CACHE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        async with _format_lock:
            _format_cache.clear()
            for user_id_str, format_pref in data.items():
                user_id = int(user_id_str)
                if format_pref in VALID_FORMATS:
                    _format_cache[user_id] = format_pref
        
        logger.info(f"Loaded {len(_format_cache)} format preferences from cache file")
        _format_cache_initialized = True
    except (json.JSONDecodeError, ValueError, IOError) as e:
        logger.warning(f"Failed to load format cache from {_FORMAT_CACHE_FILE}: {e}. Starting with empty cache.")
        _format_cache_initialized = True


async def _save_format_cache(data: dict = None) -> None:
    """Persist format preference cache to disk atomically."""
    try:
        _FORMAT_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        if data is None:
            async with _format_lock:
                data = {str(user_id): format_pref for user_id, format_pref in _format_cache.items()}
        
        temp_file = _FORMAT_CACHE_FILE.with_suffix('.json.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        temp_file.replace(_FORMAT_CACHE_FILE)
        logger.info(f"Saved {len(data)} format preferences")
    except IOError as e:
        logger.error(f"Failed to save format cache to {_FORMAT_CACHE_FILE}: {e}", exc_info=True)


async def get_format_preference(discord_user_id: int) -> str:
    """Get stored format preference for a Discord user, or default if not found."""
    async with _format_lock:
        return _format_cache.get(discord_user_id, DEFAULT_FORMAT)


async def set_format_preference(discord_user_id: int, format_pref: str) -> None:
    """Store or update format preference for a Discord user."""
    if format_pref not in VALID_FORMATS:
        raise ValueError(f"Invalid format: {format_pref}. Valid formats: {', '.join(VALID_FORMATS)}")
    
    async with _format_lock:
        _format_cache[discord_user_id] = format_pref
        data_to_save = {str(user_id): fmt for user_id, fmt in _format_cache.items()}
    await _save_format_cache(data_to_save)


async def clear_format_preference(discord_user_id: int) -> None:
    """Remove stored format preference for a Discord user (resets to default)."""
    should_save = False
    async with _format_lock:
        if discord_user_id in _format_cache:
            del _format_cache[discord_user_id]
            should_save = True
            data_to_save = {str(user_id): fmt for user_id, fmt in _format_cache.items()}
            logger.info(f"Cleared format preference for Discord user {discord_user_id}")
        else:
            logger.info(f"No format preference found for Discord user {discord_user_id}")
    
    if should_save:
        await _save_format_cache(data_to_save)


async def initialize_format_cache() -> None:
    """Initialize the format preference cache by loading from disk. Should be called during bot startup."""
    await _load_format_cache()
