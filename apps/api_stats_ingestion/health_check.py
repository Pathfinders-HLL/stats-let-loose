"""
Minimal health check for the API ingestion service.

The container entrypoint touches READINESS_FILE when the scheduler loop is running.
This script exits 0 if the file exists (service ready), 1 otherwise.
Invoked by Docker healthcheck: python -m apps.api_stats_ingestion.health_check
"""

from __future__ import annotations

import os
import sys

READINESS_FILE = "/tmp/api-ingestion-ready"


def is_ready() -> bool:
    """Return True if the ingestion scheduler has started and is considered ready."""
    return os.path.isfile(READINESS_FILE)


def main() -> int:
    return 0 if is_ready() else 1


if __name__ == "__main__":
    sys.exit(main())
