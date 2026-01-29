"""
HLL API Ingestion package for the StatsFinder project.

This package handles the ingestion pipeline:
1. Fetches all matches list from API
2. Fetches detailed match scoreboards from API
3. Transforms and inserts data into PostgreSQL

Run the CLI with: python -m apps.api_stats_ingestion.ingestion_cli
"""
