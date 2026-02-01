"""
Transform module for API ingestion.

Contains functions for transforming raw API data into structured formats.
"""

from apps.api_stats_ingestion.transform.match_transformer import (
    transform_match_history_data,
    transform_match_history_data_batched,
    transform_player_stats_data,
    transform_player_stats_data_batched,
)
from apps.api_stats_ingestion.transform.transform_utils import (
    parse_timestamp,
    calculate_winning_team,
    calculate_duration,
)

__all__ = [
    'transform_match_history_data',
    'transform_match_history_data_batched',
    'transform_player_stats_data',
    'transform_player_stats_data_batched',
    'parse_timestamp',
    'calculate_winning_team',
    'calculate_duration',
]

