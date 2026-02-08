"""
Minimal health check for the Discord bot.

Verifies:
1. Bot is connected to Discord (readiness file exists)
2. Database connection is working (can execute a simple query)

The bot creates READINESS_FILE in on_ready and removes it on disconnect.
This script exits 0 if healthy, 1 otherwise.
"""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg

READINESS_FILE = "/tmp/discord-bot-ready"


async def check_database() -> bool:
    """Check if database connection is working."""
    try:
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT")
        database = os.getenv("POSTGRES_DB")
        user = os.getenv("POSTGRES_RO_USER")
        password = os.getenv("POSTGRES_RO_PASSWORD")
        
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
    """Return True only when bot is connected AND database is reachable."""
    # Check if bot is connected to Discord
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
