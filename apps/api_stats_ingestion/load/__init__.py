"""
Load module for API ingestion.

Contains functions for loading transformed data into the database.
"""

from apps.api_stats_ingestion.load.match_loader import main as update_db_main

__all__ = ['update_db_main']

