"""
Data integrity validation submodule for the stats ingestion pipeline.

This submodule provides functions to detect and repair partial match insertions
that can occur when the pipeline is interrupted mid-run (e.g., during a redeploy).

Modules:
- integrity_checks: Functions to detect missing or incomplete data across tables
"""

from apps.api_stats_ingestion.validate.integrity_checks import (
    IntegrityIssue,
    IntegrityReport,
    check_all_integrity,
    check_all_player_nemesis_stats,
    check_all_player_victim_stats,
    check_matches_missing_player_stats,
    check_matches_with_zero_player_count,
    check_player_stats_missing_death_stats,
    check_player_stats_missing_kill_stats,
    check_player_stats_missing_nemesis_stats,
    check_player_stats_missing_victim_stats,
    delete_incomplete_match_data,
)

__all__ = [
    # Data classes
    "IntegrityIssue",
    "IntegrityReport",
    # Individual checks
    "check_matches_missing_player_stats",
    "check_matches_with_zero_player_count",
    "check_player_stats_missing_kill_stats",
    "check_player_stats_missing_death_stats",
    "check_player_stats_missing_victim_stats",
    "check_player_stats_missing_nemesis_stats",
    # Thorough checks
    "check_all_player_victim_stats",
    "check_all_player_nemesis_stats",
    # Combined check
    "check_all_integrity",
    # Repair functions
    "delete_incomplete_match_data",
]
