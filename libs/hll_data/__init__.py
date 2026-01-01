"""
Shared HLL data files for the StatsFinder project.

This package contains shared data files like weapon schemas
that are used by multiple services.
"""

from pathlib import Path

# Path to the weapon schemas CSV file
WEAPON_SCHEMAS_PATH = Path(__file__).parent / "weapon_schemas.csv"

# Path to the map ID/name mappings CSV file
MAP_ID_NAME_MAPPINGS_PATH = Path(__file__).parent / "map_id_name_mappings.csv"

__all__ = ['WEAPON_SCHEMAS_PATH', 'MAP_ID_NAME_MAPPINGS_PATH']

