"""
Database connection pool management for the Discord bot.
"""

import logging
import os

import asyncpg

from typing import Optional

from libs.db.config import get_db_config

logger = logging.getLogger(__name__)

# Connection pool for async database operations
_db_pool: Optional[asyncpg.Pool] = None


async def get_readonly_db_pool() -> asyncpg.Pool:
    """
    Get or create the async PostgreSQL connection pool (read-only user).
    This pool is created on first call and reused for subsequent calls.
    """
    global _db_pool
    
    if _db_pool is not None:
        return _db_pool
    
    db_config = get_db_config()
    ro_user = os.getenv("POSTGRES_RO_USER")
    ro_password = os.getenv("POSTGRES_RO_PASSWORD")
    
    if not all([db_config.host, db_config.port, db_config.database, ro_user]):
        missing = []
        if not db_config.host:
            missing.append("POSTGRES_HOST")
        if not db_config.port:
            missing.append("POSTGRES_PORT")
        if not db_config.database:
            missing.append("POSTGRES_DB")
        if not ro_user:
            missing.append("POSTGRES_RO_USER")
        raise ValueError(f"Missing database config: {', '.join(missing)}")
    
    try:
        async def setup_connection(conn):
            """Set up connection defaults."""
            await conn.execute("SET statement_timeout = '60s'")
        
        _db_pool = await asyncpg.create_pool(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=ro_user,
            password=ro_password,
            min_size=2,
            max_size=10,
            command_timeout=60,
            max_inactive_connection_lifetime=300,
            setup=setup_connection,
        )
        logger.info("Created async database connection pool")
        return _db_pool
    except Exception as e:
        raise ConnectionError(f"Failed to create database connection pool: {e}") from e


async def close_db_pool() -> None:
    """Close the database connection pool if open."""
    global _db_pool
    if _db_pool is not None:
        await _db_pool.close()
        _db_pool = None
        logger.info("Closed database connection pool")
