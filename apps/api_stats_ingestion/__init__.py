"""
HLL API Ingestion package for the StatsFinder project.

This package handles the ingestion pipeline:
1. Fetches all matches list from API
2. Fetches detailed match scoreboards from API
3. Transforms and inserts data into PostgreSQL
"""

from apps.api_stats_ingestion.ingestion_cli import main, run_pipeline

__all__ = ['main', 'run_pipeline']
