"""
UI module for Discord bot.

Contains autocomplete, embeds, and formatting helpers.
"""

from apps.discord_stats_bot.common.player_id_cache import (
    get_player_id,
    set_player_id,
    clear_player_id,
)
from apps.discord_stats_bot.common.shared import (
    get_readonly_db_pool,
    close_db_pool,
    escape_sql_identifier,
    find_player_by_id_or_name,
    log_command_data,
    log_command_completion,
)
from apps.discord_stats_bot.common.weapon_autocomplete import (
    weapon_category_autocomplete,
    get_weapon_names,
    get_weapon_mapping,
)

__all__ = [
    # Shared utilities
    'get_readonly_db_pool',
    'close_db_pool',
    'escape_sql_identifier',
    'find_player_by_id_or_name',
    'log_command_data',
    'log_command_completion',
    # Weapon autocomplete
    'weapon_category_autocomplete',
    'get_weapon_names',
    'get_weapon_mapping',
    # Player ID cache
    'get_player_id',
    'set_player_id',
    'clear_player_id',
]

