"""
Minimal health check for the API ingestion service.

Verifies:
1. Scheduler loop is running (readiness file exists)
2. Database connection is working (can execute a simple query)

The scheduler creates READINESS_FILE at startup and removes it on shutdown.
This script exits 0 if healthy, 1 otherwise.
Invoked by Docker healthcheck: python -m apps.api_stats_ingestion.health_check
"""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg

READINESS_FILE = "/tmp/api-ingestion-ready"


async def check_database() -> bool:
    """Check if database connection is working."""
    try:
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DB")
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        
        if not all([host, port, database, user, password]):
            return False
        
        # Quick connection test with a simple query
        conn = await asyncpg.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
            timeout=5,
        )
        
        # Simple query to verify database is responsive
        await conn.fetchval("SELECT 1")
        await conn.close()
        return True
    except Exception:
        return False


async def is_healthy() -> bool:
    """Return True if scheduler is running AND database is reachable."""
    # Check if scheduler is running
    if not os.path.isfile(READINESS_FILE):
        return False
    
    # Check if database is working
    return await check_database()


async def main_async() -> int:
    healthy = await is_healthy()
    return 0 if healthy else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
