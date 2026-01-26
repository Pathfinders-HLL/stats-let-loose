"""
Autocomplete functions for Discord bot commands.

Provides autocomplete for:
- Kill types, death types, score types, stat types
- Aggregate and order by options
- Weapon categories (with caching from CSV)
- Map names (with caching from CSV)
"""

import csv
import logging

import discord

from typing import Dict, List, Set, Tuple
from discord import app_commands

from libs.hll_data import WEAPON_SCHEMAS_PATH, MAP_ID_NAME_MAPPINGS_PATH
from apps.discord_stats_bot.common.constants import (
    KILL_TYPE_CHOICES,
    DEATH_TYPE_CHOICES,
    SCORE_TYPE_CHOICES,
    STAT_TYPE_CHOICES,
    AGGREGATE_BY_CHOICES,
    ORDER_BY_CHOICES,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Generic choice filtering
# =============================================================================

def _filter_choices(
    choices: List[app_commands.Choice[str]], 
    current: str
) -> List[app_commands.Choice[str]]:
    """Filter choices based on current input."""
    if not current:
        return choices[:25]
    
    current_lower = current.lower()
    matching = [
        choice for choice in choices
        if current_lower in choice.name.lower() or current_lower in choice.value.lower()
    ]
    return matching[:25]


# =============================================================================
# Type autocomplete functions
# =============================================================================

async def kill_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for kill_type parameter."""
    return _filter_choices(KILL_TYPE_CHOICES, current)


async def death_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for death_type parameter."""
    return _filter_choices(DEATH_TYPE_CHOICES, current)


async def score_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for score_type parameter."""
    return _filter_choices(SCORE_TYPE_CHOICES, current)


async def stat_type_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for stat_type parameter."""
    return _filter_choices(STAT_TYPE_CHOICES, current)


async def aggregate_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for aggregate_by parameter."""
    return _filter_choices(AGGREGATE_BY_CHOICES, current)


async def order_by_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for order_by parameter."""
    return _filter_choices(ORDER_BY_CHOICES, current)


# =============================================================================
# Weapon autocomplete with caching
# =============================================================================

_WEAPON_NAMES_CACHE: List[str] = []
_WEAPON_NAMES_LOWER_CACHE: List[Tuple[str, str]] = []
_WEAPON_MAPPING_CACHE: Dict[str, str] = {}


def _load_weapon_names() -> None:
    """Load weapon names from CSV and cache them."""
    global _WEAPON_NAMES_CACHE, _WEAPON_NAMES_LOWER_CACHE
    
    if _WEAPON_NAMES_CACHE:
        return
    
    if not WEAPON_SCHEMAS_PATH.exists():
        logger.warning(f"Weapon schemas file not found: {WEAPON_SCHEMAS_PATH}")
        return
    
    friendly_names = []
    
    try:
        with open(WEAPON_SCHEMAS_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                friendly_names_str = row.get('FriendlyName', '').strip()
                if not friendly_names_str:
                    continue
                names = [name.strip() for name in friendly_names_str.split(';') if name.strip()]
                friendly_names.extend(names)
        
        seen = set()
        unique_names = []
        for name in friendly_names:
            if name.lower() not in seen:
                seen.add(name.lower())
                unique_names.append(name)
        
        _WEAPON_NAMES_CACHE = sorted(unique_names)
        _WEAPON_NAMES_LOWER_CACHE = [(name.lower(), name) for name in _WEAPON_NAMES_CACHE]
        
        logger.info(f"Loaded {len(_WEAPON_NAMES_CACHE)} weapon names")
    
    except Exception as e:
        logger.error(f"Failed to load weapon schemas: {e}", exc_info=True)
        _WEAPON_NAMES_CACHE = []
        _WEAPON_NAMES_LOWER_CACHE = []


def _load_weapon_mapping() -> None:
    """Load friendly name -> column name mapping from CSV."""
    global _WEAPON_MAPPING_CACHE
    
    if _WEAPON_MAPPING_CACHE:
        return
    
    if not WEAPON_SCHEMAS_PATH.exists():
        logger.warning(f"Weapon schemas file not found: {WEAPON_SCHEMAS_PATH}")
        return
    
    try:
        with open(WEAPON_SCHEMAS_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                weapon_type = row.get('WeaponType', '').strip() or row.get('\ufeffWeaponType', '').strip()
                friendly_names_str = row.get('FriendlyName', '').strip()
                
                if not weapon_type or not friendly_names_str:
                    continue
                
                column_name = weapon_type.lower()
                friendly_names = [name.strip() for name in friendly_names_str.split(';') if name.strip()]
                for name in friendly_names:
                    _WEAPON_MAPPING_CACHE[name.lower()] = column_name
        
        logger.info(f"Loaded {len(_WEAPON_MAPPING_CACHE)} weapon mappings")
    
    except Exception as e:
        logger.error(f"Failed to load weapon mapping: {e}", exc_info=True)
        _WEAPON_MAPPING_CACHE = {}


# Initialize weapon caches on module import
_load_weapon_names()
_load_weapon_mapping()


async def weapon_category_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Return matching weapon categories for autocomplete (up to 25)."""
    if not _WEAPON_NAMES_CACHE:
        _load_weapon_names()
    
    if not current:
        return [
            app_commands.Choice(name=name, value=name)
            for name in _WEAPON_NAMES_CACHE[:25]
        ]
    
    current_lower = current.lower()
    matching = [
        app_commands.Choice(name=original, value=original)
        for lower, original in _WEAPON_NAMES_LOWER_CACHE
        if current_lower in lower
    ]
    
    return matching[:25]


def get_weapon_names() -> List[str]:
    """Get the cached list of weapon names."""
    if not _WEAPON_NAMES_CACHE:
        _load_weapon_names()
    return _WEAPON_NAMES_CACHE.copy()


def get_weapon_mapping() -> Dict[str, str]:
    """Get the cached weapon mapping (friendly name -> column name)."""
    if not _WEAPON_MAPPING_CACHE:
        _load_weapon_mapping()
    return _WEAPON_MAPPING_CACHE.copy()


# =============================================================================
# Map autocomplete with caching
# =============================================================================

_MAP_NAMES_CACHE: List[str] = []
_MAP_NAMES_LOWER_CACHE: List[Tuple[str, str]] = []
_MAP_ID_TO_NAME_CACHE: Dict[str, str] = {}
_MAP_NAME_TO_IDS_CACHE: Dict[str, Set[str]] = {}


def _load_map_names() -> None:
    """Load map names from CSV and cache them."""
    global _MAP_NAMES_CACHE, _MAP_NAMES_LOWER_CACHE, _MAP_ID_TO_NAME_CACHE, _MAP_NAME_TO_IDS_CACHE
    
    if _MAP_NAMES_CACHE:
        return
    
    if not MAP_ID_NAME_MAPPINGS_PATH.exists():
        logger.warning(f"Map ID name mappings file not found: {MAP_ID_NAME_MAPPINGS_PATH}")
        return
    
    try:
        with open(MAP_ID_NAME_MAPPINGS_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                map_id = row.get('map_id', '').strip()
                map_pretty_name = row.get('map_pretty_name', '').strip()
                
                if not map_id or not map_pretty_name:
                    continue
                
                _MAP_ID_TO_NAME_CACHE[map_id] = map_pretty_name
                
                if map_pretty_name not in _MAP_NAME_TO_IDS_CACHE:
                    _MAP_NAME_TO_IDS_CACHE[map_pretty_name] = set()
                _MAP_NAME_TO_IDS_CACHE[map_pretty_name].add(map_id)
        
        _MAP_NAMES_CACHE = sorted(_MAP_NAME_TO_IDS_CACHE.keys())
        _MAP_NAMES_LOWER_CACHE = [(name.lower(), name) for name in _MAP_NAMES_CACHE]
        
        logger.info(f"Loaded {len(_MAP_NAMES_CACHE)} unique map names from {len(_MAP_ID_TO_NAME_CACHE)} map IDs")
    
    except Exception as e:
        logger.error(f"Failed to load map name mappings: {e}", exc_info=True)
        _MAP_NAMES_CACHE = []
        _MAP_NAMES_LOWER_CACHE = []
        _MAP_ID_TO_NAME_CACHE = {}
        _MAP_NAME_TO_IDS_CACHE = {}


# Initialize map cache on module import
_load_map_names()


async def map_name_autocomplete(
    interaction: discord.Interaction,
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
