"""
Shared database configuration and utilities for the StatsFinder project.

This package provides reusable database connection functions that can be used
across different services (API ingestion, Discord bot, etc.).
"""

from libs.db.config import get_db_config, get_db_config_dict, DatabaseConfig
from libs.db.database import get_db_connection, connect_to_database

__all__ = [
    'get_db_config',
    'get_db_config_dict',
    'DatabaseConfig',
    'get_db_connection',
    'connect_to_database',
]

