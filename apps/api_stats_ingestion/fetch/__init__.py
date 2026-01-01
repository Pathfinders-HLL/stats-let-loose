"""
Fetch module for API ingestion.

Contains functions for fetching data from external APIs.
"""

from apps.api_stats_ingestion.fetch.all_matches import main as fetch_all_matches_main
from apps.api_stats_ingestion.fetch.match_history import main as fetch_match_history_main

__all__ = ['fetch_all_matches_main', 'fetch_match_history_main']

