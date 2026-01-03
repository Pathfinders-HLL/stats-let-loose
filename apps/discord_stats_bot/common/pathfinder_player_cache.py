"""
S3-backed cache for Pathfinder player IDs with 24-hour refresh.
Downloads pathfinder_player_ids.txt from S3 bucket 'stats-let-loose' and caches locally.
"""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Set, Optional

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path(os.getenv("DISCORD_BOT_CACHE_DIR", "/app/data/cache"))
CACHE_FILE = CACHE_DIR / "pathfinder_player_ids.txt"
CACHE_DURATION_SECONDS = 24 * 60 * 60  # 24 hours

# S3 configuration hardcoded cause I don't want to deal with setting up dynamic config mao
S3_BUCKET = "stats-let-loose"
S3_KEY = "pathfinder_player_ids.txt"

# Thread-safe TTL cache - single key stores the set of player IDs
_cache_lock = threading.Lock()
_cache: TTLCache[str, Set[str]] = TTLCache(maxsize=1, ttl=CACHE_DURATION_SECONDS)
CACHE_KEY = "pathfinder_ids"

def _download_from_s3() -> Optional[str]:
    """
    Download pathfinder_player_ids.txt from S3.
    Returns the file content as a string, or None on error.
    """
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Downloading {S3_KEY} from S3 bucket {S3_BUCKET}")
        
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        content = response['Body'].read().decode('utf-8')
        
        logger.info(f"Successfully downloaded {S3_KEY} from S3 ({len(content)} bytes)")
        return content
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            logger.error(f"S3 object {S3_KEY} not found in bucket {S3_BUCKET}")
        elif error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket {S3_BUCKET} not found")
        else:
            logger.error(f"S3 ClientError downloading {S3_KEY}: {e}", exc_info=True)
        return None
    except BotoCoreError as e:
        logger.error(f"S3 BotoCoreError downloading {S3_KEY}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading from S3: {e}", exc_info=True)
        return None


def _load_from_local_file() -> Optional[Set[str]]:
    """
    Load player IDs from local cache file if it exists.
    Returns a set of player IDs, or None if file doesn't exist or can't be read.
    """
    if not CACHE_FILE.exists():
        return None
    
    try:
        player_ids = set[str]()
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    player_ids.add(line)
        
        logger.info(f"Loaded {len(player_ids)} player IDs from local cache file")
        return player_ids
    except Exception as e:
        logger.warning(f"Error loading player IDs from local cache file: {e}", exc_info=True)
        return None


def _save_to_local_file(player_ids: Set[str]) -> None:
    """
    Save player IDs to local cache file atomically.
    """
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temporary file first, then rename atomically
        temp_file = CACHE_FILE.with_suffix('.txt.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            for player_id in sorted(player_ids):
                f.write(f"{player_id}\n")
        
        temp_file.replace(CACHE_FILE)
        logger.info(f"Saved {len(player_ids)} player IDs to local cache file")
    except Exception as e:
        logger.error(f"Error saving player IDs to local cache file: {e}", exc_info=True)


def _refresh_cache() -> Set[str]:
    """
    Refresh the cache by downloading from S3.
    Falls back to local file if S3 download fails.
    Returns the set of player IDs.
    """
    # Try to download from S3
    content = _download_from_s3()
    
    if content is not None:
        # Parse content from S3
        player_ids = set[str]()
        for line in content.splitlines():
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith('#'):
                player_ids.add(line)
        
        # Save to local cache for future fallback
        _save_to_local_file(player_ids)
        
        logger.info(f"Refreshed cache from S3: {len(player_ids)} player IDs")
        return player_ids
    
    # Fallback to local file if S3 download failed
    logger.warning("S3 download failed, attempting to use local cache file")
    local_ids = _load_from_local_file()
    if local_ids is not None:
        logger.info(f"Using local cache file: {len(local_ids)} player IDs")
        return local_ids
    
    # No cache available, return empty set
    logger.warning("No cache available (S3 download failed and no local file), returning empty set")
    return set[str]()


def _load_cache_from_disk() -> None:
    """Load persisted cache from disk on module initialization."""
    local_ids = _load_from_local_file()
    if local_ids is not None:
        with _cache_lock:
            _cache[CACHE_KEY] = local_ids
        logger.info(f"Loaded {len(local_ids)} player IDs from disk into cache")


def get_pathfinder_player_ids() -> Set[str]:
    """
    Get Pathfinder player IDs, loading from S3 if cache is stale (>24 hours).
    Uses thread-safe TTL cache with automatic expiration.
    
    Returns:
        Set of player ID strings
    """
    with _cache_lock:
        # Check if cache entry exists and is still valid (TTLCache handles expiration)
        if CACHE_KEY in _cache:
            cached_ids = _cache[CACHE_KEY]
            logger.debug(f"Returning cached player IDs: {len(cached_ids)} IDs")
            return cached_ids
        
        # Cache is expired or doesn't exist, refresh it
        logger.info("Cache expired or missing, refreshing from S3")
        start_time = time.time()
        player_ids = _refresh_cache()
        _cache[CACHE_KEY] = player_ids
        elapsed = time.time() - start_time
        logger.info(f"Refreshed cache: {len(player_ids)} player IDs in {elapsed:.2f}s")
        
        return player_ids


# Load cache from disk on module initialization
_load_cache_from_disk()
