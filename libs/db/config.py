"""
Database configuration from environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    # dotenv is optional - if not installed, we'll just use environment variables
    load_dotenv = None


LIBS_DIR = Path(__file__).parent.parent
ROOT_DIR = LIBS_DIR.parent
ENV_FILE = ROOT_DIR / ".env"

if load_dotenv and ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE, override=False)


class DatabaseConfig:
    """PostgreSQL connection settings from environment variables."""
    
    def __init__(self) -> None:
        self.host: str = os.getenv("POSTGRES_HOST")
        self.port: int = int(os.getenv("POSTGRES_PORT"))
        self.database: str = os.getenv("POSTGRES_DB")
        self.user: str = os.getenv("POSTGRES_USER")
        self.password: str = os.getenv("POSTGRES_PASSWORD")
    
    def to_dict(self) -> dict[str, str | int | None]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }
    
    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, user={self.user!r}, password=***)"
        )


_db_config: Optional[DatabaseConfig] = None


def get_db_config() -> DatabaseConfig:
    """Get or create the singleton config instance."""
    global _db_config
    if _db_config is None:
        _db_config = DatabaseConfig()
    return _db_config


def get_db_config_dict() -> dict[str, str | int | None]:
    """Get config as a dictionary for compatibility with older code."""
    return get_db_config().to_dict()

