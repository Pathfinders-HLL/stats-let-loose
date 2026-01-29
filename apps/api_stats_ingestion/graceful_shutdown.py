"""
Graceful shutdown handling for the ingestion service.

This module provides signal handling to allow the ingestion process to complete
its current work before shutting down during redeployments.

When Docker sends SIGTERM (during `docker compose down` or redeployment),
this module sets a flag that the ingestion loop checks between batches,
allowing the current batch to complete before exiting gracefully.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from typing import Optional


class GracefulShutdown:
    """
    Manages graceful shutdown state for the ingestion service.
    
    Usage:
        shutdown_handler = GracefulShutdown()
        shutdown_handler.setup_signal_handlers()
        
        # In your processing loop:
        for batch in batches:
            if shutdown_handler.should_shutdown:
                print("Shutdown requested, completing current work...")
                break
            process_batch(batch)
    """
    
    def __init__(self) -> None:
        self._shutdown_requested = False
        self._shutdown_event: Optional[asyncio.Event] = None
    
    @property
    def should_shutdown(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_requested
    
    @property
    def shutdown_event(self) -> asyncio.Event:
        """Get or create the async shutdown event."""
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        return self._shutdown_event
    
    def request_shutdown(self) -> None:
        """Request a graceful shutdown."""
        if not self._shutdown_requested:
            self._shutdown_requested = True
            print("\n" + "=" * 60)
            print("SHUTDOWN REQUESTED - Completing current batch before exit...")
            print("=" * 60)
            if self._shutdown_event is not None:
                self._shutdown_event.set()
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals (SIGTERM, SIGINT)."""
        signal_name = signal.Signals(signum).name
        print(f"\nReceived {signal_name} signal")
        self.request_shutdown()
    
    def setup_signal_handlers(self) -> None:
        """
        Set up signal handlers for graceful shutdown.
        
        Handles:
        - SIGTERM: Sent by Docker during container stop
        - SIGINT: Sent when pressing Ctrl+C (for local development)
        """
        # SIGTERM is the standard signal sent by Docker
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # SIGINT for Ctrl+C during local development
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # On Windows, also try to handle SIGBREAK if available
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, self._signal_handler)
        
        print("Graceful shutdown handlers registered (SIGTERM, SIGINT)")


# Global singleton instance for easy access across modules
_shutdown_handler: Optional[GracefulShutdown] = None


def get_shutdown_handler() -> GracefulShutdown:
    """Get the global shutdown handler instance."""
    global _shutdown_handler
    if _shutdown_handler is None:
        _shutdown_handler = GracefulShutdown()
    return _shutdown_handler


def setup_graceful_shutdown() -> GracefulShutdown:
    """Set up graceful shutdown handling and return the handler."""
    handler = get_shutdown_handler()
    handler.setup_signal_handlers()
    return handler


def should_shutdown() -> bool:
    """Check if shutdown has been requested (convenience function)."""
    return get_shutdown_handler().should_shutdown
