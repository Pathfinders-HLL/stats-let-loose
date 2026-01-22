"""
Shared utilities for Discord bot commands.

This module re-exports from focused modules for backward compatibility.
New code should import directly from the specific modules:
- database: get_readonly_db_pool, close_db_pool
- player: find_player_by_id_or_name, get_pathfinder_player_ids
- logging: log_command_data, log_command_completion
- validation: validate_over_last_days, validate_choice_parameter
- sql_builders: escape_sql_identifier, create_time_filter_params, etc.
- decorators: command_wrapper, handle_command_errors
"""

# Re-export from database module
from apps.discord_stats_bot.common.database import (
    get_readonly_db_pool,
    close_db_pool,
)

# Re-export from player module
from apps.discord_stats_bot.common.player import (
    find_player_by_id_or_name,
    get_pathfinder_player_ids,
)

# Re-export from logging module
from apps.discord_stats_bot.common.logging import (
    log_command_data,
    get_command_latency_ms,
    log_command_completion,
)

# Re-export from validation module
from apps.discord_stats_bot.common.validation import (
    validate_over_last_days,
    validate_choice_parameter,
)

# Re-export from sql_builders module
from apps.discord_stats_bot.common.sql_builders import (
    escape_sql_identifier,
    create_time_filter_params,
    build_pathfinder_filter,
    build_lateral_name_lookup,
    build_from_clause_with_time_filter,
    build_where_clause,
    format_sql_query_with_params,
)

# Re-export from decorators module
from apps.discord_stats_bot.common.decorators import (
    handle_command_errors,
    command_wrapper,
)

__all__ = [
    # Database
    'get_readonly_db_pool',
    'close_db_pool',
    # Player
    'find_player_by_id_or_name',
    'get_pathfinder_player_ids',
    # Logging
    'log_command_data',
    'get_command_latency_ms',
    'log_command_completion',
    # Validation
    'validate_over_last_days',
    'validate_choice_parameter',
    # SQL builders
    'escape_sql_identifier',
    'create_time_filter_params',
    'build_pathfinder_filter',
    'build_lateral_name_lookup',
    'build_from_clause_with_time_filter',
    'build_where_clause',
    'format_sql_query_with_params',
    # Decorators
    'handle_command_errors',
    'command_wrapper',
]
