"""
PostgreSQL connection utilities shared across services using asyncpg.
"""

from __future__ import annotations

from typing import Optional

import asyncpg

from libs.db.config import get_db_config


async def get_db_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    verbose: bool = True,
) -> asyncpg.Connection:
    """Connect to PostgreSQL asynchronously. Uses environment config for any missing parameters."""
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
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        if verbose:
            print("Successfully connected to database")
        return conn
    except asyncpg.PostgresError as e:
        raise ConnectionError(f"Failed to connect to database: {e}") from e


async def create_db_pool(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    min_size: int = 2,
    max_size: int = 10,
    verbose: bool = True,
) -> asyncpg.Pool:
    """Create an asyncpg connection pool. Uses environment config for any missing parameters."""
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
        print(f"Creating connection pool for PostgreSQL at {host}:{port}/{database}...")
    
    try:
        pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=min_size,
            max_size=max_size,
        )
        if verbose:
            print("Successfully created database connection pool")
        return pool
    except asyncpg.PostgresError as e:
        raise ConnectionError(f"Failed to create database connection pool: {e}") from e


async def connect_to_database(
    host: str,
    port: int,
    database: str,
    user: str,
    password: Optional[str],
) -> asyncpg.Connection:
    """Legacy wrapper for get_db_connection()."""
    return await get_db_connection(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
