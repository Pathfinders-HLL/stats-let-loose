"""
Discord bot configuration from environment variables.
"""

from __future__ import annotations

import logging
import os

from pathlib import Path
from typing import Optional, Set

logger = logging.getLogger(__name__)
try:
    from dotenv import load_dotenv
except ImportError:
    # dotenv is optional - if not installed, we'll just use environment variables
    load_dotenv = None

APPS_DIR = Path(__file__).parent.parent.parent
ROOT_DIR = APPS_DIR.parent
ENV_FILE = ROOT_DIR / ".env"
DOCKER_ENV_FILE = ROOT_DIR / "infra" / "docker" / ".env"

if load_dotenv is None:
    logger.warning("dotenv not available, using system environment variables only")
else:
    env_loaded = False
    for env_path in [DOCKER_ENV_FILE, ENV_FILE]:
        if env_path.exists():
            try:
                load_dotenv(dotenv_path=env_path, override=False)
                logger.info(f"Loaded .env from {env_path}")
                env_loaded = True
                break
            except Exception as e:
                logger.error(f"Failed to load {env_path}: {e}", exc_info=True)
    if not env_loaded:
        logger.info("No .env file found")


class DiscordBotConfig:
    """Discord bot settings loaded from environment variables."""
    
    def __init__(self) -> None:
        self.token: Optional[str] = os.getenv("DISCORD_BOT_TOKEN")
        
        if not self.token:
            raise ValueError("DISCORD_BOT_TOKEN environment variable is required")
        
        allowed_channels_str = os.getenv("DISCORD_ALLOWED_CHANNEL_IDS", "")
        if allowed_channels_str:
            self.allowed_channel_ids: Set[int] = {
                int(channel_id.strip())
                for channel_id in allowed_channels_str.split(",")
                if channel_id.strip()
            }
        else:
            self.allowed_channel_ids: Set[int] = set()
        
        dev_guild_id_str = os.getenv("DISCORD_DEV_GUILD_ID")
        self.dev_guild_id: Optional[int] = int(dev_guild_id_str) if dev_guild_id_str else None
        
        stats_channel_id_str = os.getenv("DISCORD_STATS_CHANNEL_ID")
        self.stats_channel_id: Optional[int] = int(stats_channel_id_str) if stats_channel_id_str else None
        
        # Parse allowed role IDs for channel cleanup (comma-separated list)
        cleanup_allowed_roles_str = os.getenv("DISCORD_CLEANUP_ALLOWED_ROLE_IDS", "")
        if cleanup_allowed_roles_str:
            self.cleanup_allowed_role_ids: Set[int] = {
                int(role_id.strip())
                for role_id in cleanup_allowed_roles_str.split(",")
                if role_id.strip()
            }
        else:
            self.cleanup_allowed_role_ids: Set[int] = set()
    
    def __repr__(self) -> str:
        return (
            f"DiscordBotConfig("
            f"token=***, "
            f"allowed_channel_ids={self.allowed_channel_ids}, "
            f"dev_guild_id={self.dev_guild_id}, "
            f"stats_channel_id={self.stats_channel_id}, "
            f"cleanup_allowed_role_ids={self.cleanup_allowed_role_ids})"
        )


_bot_config: Optional[DiscordBotConfig] = None


def get_bot_config() -> DiscordBotConfig:
    """Get or create the singleton config instance."""
    global _bot_config
    if _bot_config is None:
        _bot_config = DiscordBotConfig()
    return _bot_config
