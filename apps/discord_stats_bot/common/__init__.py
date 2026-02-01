"""
Common utilities module for Discord bot.

This module provides centralized access to shared functionality:
- Database operations
- Player lookup and caching
- Command logging
- Input validation
- SQL query building
- Autocomplete helpers
- Caching utilities
"""

# Database operations
from apps.discord_stats_bot.common.database import (
    get_readonly_db_pool,
    close_db_pool,
)

# Player utilities
from apps.discord_stats_bot.common.player_lookup import (
    find_player_by_id_or_name,
    get_pathfinder_player_ids,
    resolve_player_input,
    lookup_player,
    PlayerLookupResult,
    PLAYER_NOT_SET_ERROR,
    player_not_found_error,
)

# Caching (player IDs and format preferences)
from apps.discord_stats_bot.common.user_cache import (
    get_player_id,
    set_player_id,
    clear_player_id,
    initialize_cache,
    get_format_preference,
    set_format_preference,
    clear_format_preference,
    initialize_format_cache,
    VALID_FORMATS,
    FORMAT_DISPLAY_NAMES,
    DEFAULT_FORMAT,
)

# Command logging
from apps.discord_stats_bot.common.logging import (
    log_command_data,
    log_command_completion,
    get_command_latency_ms,
)

# Input validation
from apps.discord_stats_bot.common.validation import (
    validate_over_last_days,
    validate_choice_parameter,
)

# SQL building utilities
from apps.discord_stats_bot.common.sql_builders import (
    escape_sql_identifier,
    create_time_filter_params,
    build_player_time_query_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    format_sql_query_with_params,
)

# Command decorators
from apps.discord_stats_bot.common.decorators import (
    command_wrapper,
    handle_command_errors,
)

# Autocomplete functions (types, weapons, maps)
from apps.discord_stats_bot.common.autocomplete import (
    kill_type_autocomplete,
    death_type_autocomplete,
    score_type_autocomplete,
    stat_type_autocomplete,
    aggregate_by_autocomplete,
    order_by_autocomplete,
    weapon_category_autocomplete,
    get_weapon_names,
    get_weapon_mapping,
    map_name_autocomplete,
    get_map_names,
    get_map_ids_for_name,
    get_map_name_for_id,
    find_map_name_case_insensitive,
)

# Constants
from apps.discord_stats_bot.common.constants import (
    DISCORD_MESSAGE_MAX_LENGTH,
    LEADERBOARD_RESULT_LIMIT,
    PLAYER_TOP_MATCHES_LIMIT,
    MIN_PLAY_TIME_SECONDS,
    MIN_PLAY_TIME_MINUTES,
    MIN_PLAYERS_PER_MATCH,
    KILL_TYPE_CONFIG,
    KILL_TYPE_CHOICES,
    KILL_TYPE_VALID_VALUES,
    KILL_TYPE_DISPLAY_LIST,
    DEATH_TYPE_CONFIG,
    DEATH_TYPE_CHOICES,
    DEATH_TYPE_VALID_VALUES,
    DEATH_TYPE_DISPLAY_LIST,
    SCORE_TYPE_CONFIG,
    SCORE_TYPE_CHOICES,
    SCORE_TYPE_VALID_VALUES,
    SCORE_TYPE_DISPLAY_LIST,
    STAT_TYPE_CONFIG,
    STAT_TYPE_CHOICES,
    STAT_TYPE_VALID_VALUES,
    STAT_TYPE_DISPLAY_LIST,
    AGGREGATE_BY_CHOICES,
    AGGREGATE_BY_VALID_VALUES,
    AGGREGATE_BY_DISPLAY_LIST,
    ORDER_BY_CONFIG,
    ORDER_BY_CHOICES,
    ORDER_BY_VALID_VALUES,
    ORDER_BY_DISPLAY_LIST,
    PATHFINDER_COLOR,
)

# Monospace table builder
from apps.discord_stats_bot.common.monospace_table_builder import (
    format_compact_value,
    format_stat_monospace_table,
    build_compact_leaderboard_embed,
    DEFAULT_COMPACT_VIEW_PLAYERS,
)

# Shared formatting utilities
from apps.discord_stats_bot.common.shared import (
    format_time_seconds,
    format_date,
    build_table_message,
)

__all__ = [
    # Database
    'get_readonly_db_pool',
    'close_db_pool',
    # Player
    'find_player_by_id_or_name',
    'get_pathfinder_player_ids',
    'resolve_player_input',
    'lookup_player',
    'PlayerLookupResult',
    'PLAYER_NOT_SET_ERROR',
    'player_not_found_error',
    # Cache
    'get_player_id',
    'set_player_id',
    'clear_player_id',
    'initialize_cache',
    'get_format_preference',
    'set_format_preference',
    'clear_format_preference',
    'initialize_format_cache',
    'VALID_FORMATS',
    'FORMAT_DISPLAY_NAMES',
    'DEFAULT_FORMAT',
    # Logging
    'log_command_data',
    'log_command_completion',
    'get_command_latency_ms',
    # Validation
    'validate_over_last_days',
    'validate_choice_parameter',
    # SQL builders
    'escape_sql_identifier',
    'create_time_filter_params',
    'build_player_time_query_params',
    'build_pathfinder_filter',
    'build_lateral_name_lookup',
    'build_from_clause_with_time_filter',
    'build_where_clause',
    'format_sql_query_with_params',
    # Decorators
    'command_wrapper',
    'handle_command_errors',
    # Autocomplete
    'kill_type_autocomplete',
    'death_type_autocomplete',
    'score_type_autocomplete',
    'stat_type_autocomplete',
    'aggregate_by_autocomplete',
    'order_by_autocomplete',
    'weapon_category_autocomplete',
    'get_weapon_names',
    'get_weapon_mapping',
    'map_name_autocomplete',
    'get_map_names',
    'get_map_ids_for_name',
    'get_map_name_for_id',
    'find_map_name_case_insensitive',
    # Constants
    'DISCORD_MESSAGE_MAX_LENGTH',
    'LEADERBOARD_RESULT_LIMIT',
    'PLAYER_TOP_MATCHES_LIMIT',
    'MIN_PLAY_TIME_SECONDS',
    'MIN_PLAY_TIME_MINUTES',
    'MIN_PLAYERS_PER_MATCH',
    'KILL_TYPE_CONFIG',
    'KILL_TYPE_CHOICES',
    'KILL_TYPE_VALID_VALUES',
    'KILL_TYPE_DISPLAY_LIST',
    'DEATH_TYPE_CONFIG',
    'DEATH_TYPE_CHOICES',
    'DEATH_TYPE_VALID_VALUES',
    'DEATH_TYPE_DISPLAY_LIST',
    'SCORE_TYPE_CONFIG',
    'SCORE_TYPE_CHOICES',
    'SCORE_TYPE_VALID_VALUES',
    'SCORE_TYPE_DISPLAY_LIST',
    'STAT_TYPE_CONFIG',
    'STAT_TYPE_CHOICES',
    'STAT_TYPE_VALID_VALUES',
    'STAT_TYPE_DISPLAY_LIST',
    'AGGREGATE_BY_CHOICES',
    'AGGREGATE_BY_VALID_VALUES',
    'AGGREGATE_BY_DISPLAY_LIST',
    'ORDER_BY_CONFIG',
    'ORDER_BY_CHOICES',
    'ORDER_BY_VALID_VALUES',
    'ORDER_BY_DISPLAY_LIST',
    'PATHFINDER_COLOR',
    # Monospace table builder
    'format_compact_value',
    'format_stat_monospace_table',
    'build_compact_leaderboard_embed',
    'DEFAULT_COMPACT_VIEW_PLAYERS',
    # Shared formatting utilities
    'format_time_seconds',
    'format_date',
    'build_table_message',
]
