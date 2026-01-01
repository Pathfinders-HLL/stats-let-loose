"""
Weapon category autocomplete with caching for Discord bot commands.
"""

import csv
import logging
from typing import Dict, List, Tuple

from discord import app_commands

from libs.hll_data import WEAPON_SCHEMAS_PATH

logger = logging.getLogger(__name__)

_WEAPON_NAMES_CACHE: List[str] = []
_WEAPON_NAMES_LOWER_CACHE: List[Tuple[str, str]] = []


def _load_weapon_names() -> None:
    """Load weapon names from CSV and cache them."""
    global _WEAPON_NAMES_CACHE, _WEAPON_NAMES_LOWER_CACHE
    
    if _WEAPON_NAMES_CACHE:
        # Already loaded
        return
    
    if not WEAPON_SCHEMAS_PATH.exists():
        logger.warning(f"Weapon schemas file not found: {WEAPON_SCHEMAS_PATH}")
        return
    
    friendly_names = []
    
    try:
        with open(WEAPON_SCHEMAS_PATH, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
            reader = csv.DictReader(f)
            for row in reader:
                friendly_names_str = row.get('FriendlyName', '').strip()
                
                if not friendly_names_str:
                    continue
                
                # Split FriendlyName by semicolon and add each name
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


_load_weapon_names()


async def weapon_category_autocomplete(
    interaction,
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


_WEAPON_MAPPING_CACHE: Dict[str, str] = {}


def _load_weapon_mapping() -> None:
    """Load friendly name -> column name mapping from CSV."""
    global _WEAPON_MAPPING_CACHE
    
    if _WEAPON_MAPPING_CACHE:
        # Already loaded
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


_load_weapon_mapping()


def get_weapon_mapping() -> Dict[str, str]:
    """Get the cached weapon mapping (friendly name -> column name)."""
    if not _WEAPON_MAPPING_CACHE:
        _load_weapon_mapping()
    return _WEAPON_MAPPING_CACHE.copy()

