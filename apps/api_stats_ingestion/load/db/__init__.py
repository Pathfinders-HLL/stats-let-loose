"""
Database operations submodule for loading match data into PostgreSQL.

This submodule provides organized database operations split across multiple modules:
- checks: Functions to check existing records before insertion
- utils: Utility functions for weapon schemas and player counts
- insert_match: Match history insertion
- insert_player: Player statistics insertion
- insert_weapons: Weapon statistics insertion (kills/deaths)
- insert_opponents: Opponent statistics insertion (victims/nemesis)
"""

from apps.api_stats_ingestion.load.db.checks import (
    check_existing_match_ids,
    check_existing_player_match_ids,
    check_existing_player_kill_ids,
    check_existing_player_death_ids,
    check_existing_player_victim_ids,
    check_existing_player_nemesis_ids,
)
from apps.api_stats_ingestion.load.db.insert_match import insert_match_history
from apps.api_stats_ingestion.load.db.insert_opponents import (
    insert_player_nemesis_stats,
    insert_player_victim_stats,
)
from apps.api_stats_ingestion.load.db.insert_player import insert_player_stats
from apps.api_stats_ingestion.load.db.insert_weapons import (
    insert_player_death_stats,
    insert_player_kill_stats,
)
from apps.api_stats_ingestion.load.db.utils import (
    load_weapon_schemas,
    map_weapon_to_column,
    update_match_player_counts,
)

__all__ = [
    # Checks
    "check_existing_match_ids",
    "check_existing_player_match_ids",
    "check_existing_player_kill_ids",
    "check_existing_player_death_ids",
    "check_existing_player_victim_ids",
    "check_existing_player_nemesis_ids",
    # Match insertion
    "insert_match_history",
    # Player insertion
    "insert_player_stats",
    # Weapon insertion
    "insert_player_kill_stats",
    "insert_player_death_stats",
    # Opponent insertion
    "insert_player_victim_stats",
    "insert_player_nemesis_stats",
    # Utilities
    "load_weapon_schemas",
    "map_weapon_to_column",
    "update_match_player_counts",
]
