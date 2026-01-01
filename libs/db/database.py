"""
PostgreSQL connection utilities shared across services.
"""

from __future__ import annotations

from typing import Optional

import psycopg2
from psycopg2.extensions import connection

from libs.db.config import get_db_config


def get_db_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    verbose: bool = True,
) -> connection:
    """Connect to PostgreSQL. Uses environment config for any missing parameters."""
    if host is None or port is None or database is None or user is None:
        db_config = get_db_config()
        host = host or db_config.host
        port = port or db_config.port
        database = database or db_config.database
        user = user or db_config.user
        password = password or db_config.password
    
    if not all([host, port, database, user]):
        raise ValueError("Missing required database connection parameters")
    
    if verbose:
        print(f"Connecting to PostgreSQL at {host}:{port}/{database}...")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        if verbose:
            print("Successfully connected to database")
        return conn
    except psycopg2.Error as e:
        raise ConnectionError(f"Failed to connect to database: {e}") from e


def connect_to_database(
    host: str,
    port: int,
    database: str,
    user: str,
    password: Optional[str],
) -> connection:
    """Legacy wrapper for get_db_connection()."""
    return get_db_connection(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

