"""
Minimal health check for the Discord bot.

The bot touches READINESS_FILE in on_ready. This script exits 0 if the file
exists (bot ready), 1 otherwise. No loop — invoked by Docker healthcheck.
"""

from __future__ import annotations

import os
import sys

READINESS_FILE = "/tmp/discord-bot-ready"


def is_ready() -> bool:
    """Return True only when the bot has fired on_ready and is ready."""
    return os.path.isfile(READINESS_FILE)


def main() -> int:
    return 0 if is_ready() else 1


if __name__ == "__main__":
    sys.exit(main())
