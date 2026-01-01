"""
Map name autocomplete with caching for Discord bot commands.
"""

import csv
import logging
from typing import Dict, List, Set, Tuple

from discord import app_commands

from libs.hll_data import MAP_ID_NAME_MAPPINGS_PATH

logger = logging.getLogger(__name__)

# Cache for unique map names (friendly names)
_MAP_NAMES_CACHE: List[str] = []
_MAP_NAMES_LOWER_CACHE: List[Tuple[str, str]] = []

# Cache for map_id -> pretty_name mapping
_MAP_ID_TO_NAME_CACHE: Dict[str, str] = {}

# Cache for pretty_name -> set of map_ids mapping
_MAP_NAME_TO_IDS_CACHE: Dict[str, Set[str]] = {}


def _load_map_names() -> None:
    """Load map names from CSV and cache them."""
    global _MAP_NAMES_CACHE, _MAP_NAMES_LOWER_CACHE, _MAP_ID_TO_NAME_CACHE, _MAP_NAME_TO_IDS_CACHE
    
    if _MAP_NAMES_CACHE:
        # Already loaded
        return
    
    if not MAP_ID_NAME_MAPPINGS_PATH.exists():
        logger.warning(f"Map ID name mappings file not found: {MAP_ID_NAME_MAPPINGS_PATH}")
        return
    
    try:
        with open(MAP_ID_NAME_MAPPINGS_PATH, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
            reader = csv.DictReader(f)
            for row in reader:
                map_id = row.get('map_id', '').strip()
                map_pretty_name = row.get('map_pretty_name', '').strip()
                
                if not map_id or not map_pretty_name:
                    continue
                
                # Store map_id -> pretty_name
                _MAP_ID_TO_NAME_CACHE[map_id] = map_pretty_name
                
                # Store pretty_name -> set of map_ids
                if map_pretty_name not in _MAP_NAME_TO_IDS_CACHE:
                    _MAP_NAME_TO_IDS_CACHE[map_pretty_name] = set()
                _MAP_NAME_TO_IDS_CACHE[map_pretty_name].add(map_id)
        
        # Get unique map names sorted alphabetically
        _MAP_NAMES_CACHE = sorted(_MAP_NAME_TO_IDS_CACHE.keys())
        _MAP_NAMES_LOWER_CACHE = [(name.lower(), name) for name in _MAP_NAMES_CACHE]
        
        logger.info(f"Loaded {len(_MAP_NAMES_CACHE)} unique map names from {len(_MAP_ID_TO_NAME_CACHE)} map IDs")
    
    except Exception as e:
        logger.error(f"Failed to load map name mappings: {e}", exc_info=True)
        _MAP_NAMES_CACHE = []
        _MAP_NAMES_LOWER_CACHE = []
        _MAP_ID_TO_NAME_CACHE = {}
        _MAP_NAME_TO_IDS_CACHE = {}


# Load on module import
_load_map_names()


async def map_name_autocomplete(
    interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Return matching map names for autocomplete (up to 25)."""
    if not _MAP_NAMES_CACHE:
        _load_map_names()
    
    if not current:
        return [
            app_commands.Choice(name=name, value=name)
            for name in _MAP_NAMES_CACHE[:25]
        ]
    
    current_lower = current.lower()
    matching = [
        app_commands.Choice(name=original, value=original)
        for lower, original in _MAP_NAMES_LOWER_CACHE
        if current_lower in lower
    ]
    
    return matching[:25]


def get_map_names() -> List[str]:
    """Get the cached list of unique map names."""
    if not _MAP_NAMES_CACHE:
        _load_map_names()
    return _MAP_NAMES_CACHE.copy()


def get_map_ids_for_name(map_pretty_name: str) -> Set[str]:
    """Get all map IDs that correspond to a given pretty map name."""
    if not _MAP_NAME_TO_IDS_CACHE:
        _load_map_names()
    return _MAP_NAME_TO_IDS_CACHE.get(map_pretty_name, set()).copy()


def get_map_name_for_id(map_id: str) -> str:
    """Get the pretty map name for a given map ID."""
    if not _MAP_ID_TO_NAME_CACHE:
        _load_map_names()
    return _MAP_ID_TO_NAME_CACHE.get(map_id, "")


def find_map_name_case_insensitive(map_name: str) -> str:
    """Find the properly cased map name from a case-insensitive input.
    
    Returns the properly cased map name if found, otherwise returns the input as-is.
    """
    if not _MAP_NAMES_CACHE:
        _load_map_names()
    
    map_name_lower = map_name.lower().strip()
    for lower, original in _MAP_NAMES_LOWER_CACHE:
        if lower == map_name_lower:
            return original
    
    return map_name
