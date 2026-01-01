"""
API ingestion configuration from environment variables.
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

APPS_DIR = Path(__file__).parent.parent.parent
ROOT_DIR = APPS_DIR.parent
ENV_FILE = ROOT_DIR / ".env"

if load_dotenv and ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE, override=False)


class IngestionConfig:
    """Batch size settings for database inserts."""
    
    def __init__(self) -> None:
        self.match_history_batch_size: int = int(os.getenv("MATCH_HISTORY_BATCH_SIZE", "50"))
        self.player_stats_batch_size: int = int(os.getenv("PLAYER_STATS_BATCH_SIZE", "50"))
    
    def __repr__(self) -> str:
        return (
            f"IngestionConfig("
            f"match_history_batch_size={self.match_history_batch_size}, "
            f"player_stats_batch_size={self.player_stats_batch_size})"
        )


class APIConfig:
    """CRCON API endpoint configuration."""
    
    def __init__(self) -> None:
        base_url = os.getenv("INGESTION_BASE_URL")
        if not base_url:
            raise ValueError("INGESTION_BASE_URL environment variable is required")
        self.base_url: str = base_url
        
        # API endpoints
        self.scoreboard_maps_endpoint: str = "/api/get_scoreboard_maps"
        self.map_scoreboard_endpoint: str = "/api/get_map_scoreboard"
    
    @property
    def scoreboard_maps_url(self) -> str:
        return f"{self.base_url}{self.scoreboard_maps_endpoint}"
    
    @property
    def map_scoreboard_url(self) -> str:
        return f"{self.base_url}{self.map_scoreboard_endpoint}"
    
    def __repr__(self) -> str:
        return (
            f"APIConfig(base_url={self.base_url!r}, "
            f"scoreboard_maps_endpoint={self.scoreboard_maps_endpoint!r}, "
            f"map_scoreboard_endpoint={self.map_scoreboard_endpoint!r})"
        )


_ingestion_config: Optional[IngestionConfig] = None
_api_config: Optional[APIConfig] = None


def get_ingestion_config() -> IngestionConfig:
    """Get or create the singleton config instance."""
    global _ingestion_config
    if _ingestion_config is None:
        _ingestion_config = IngestionConfig()
    return _ingestion_config


def get_api_config() -> APIConfig:
    """Get or create the singleton config instance."""
    global _api_config
    if _api_config is None:
        _api_config = APIConfig()
    return _api_config

