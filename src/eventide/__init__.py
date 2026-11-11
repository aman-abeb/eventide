"""Eventide: an append-only JSON-lines event store."""

from eventide.store import (
    CorruptionError,
    Event,
    EventStore,
    EventStoreError,
    TruncatedTailError,
)

__all__ = [
    "CorruptionError",
    "Event",
    "EventStore",
    "EventStoreError",
    "TruncatedTailError",
]
__version__ = "0.1.0"
