"""Eventide: an append-only JSON-lines event store."""

from eventide.aggregate import (
    DEPOSIT,
    WITHDRAW,
    AggregateError,
    BankAccount,
    EventSpec,
    InsufficientFunds,
    InvalidAmount,
    UnknownEventError,
    execute,
    load_account,
)
from eventide.store import (
    CorruptionError,
    Event,
    EventStore,
    EventStoreError,
    TruncatedTailError,
)

__all__ = [
    "DEPOSIT",
    "WITHDRAW",
    "AggregateError",
    "BankAccount",
    "CorruptionError",
    "Event",
    "EventSpec",
    "EventStore",
    "EventStoreError",
    "InsufficientFunds",
    "InvalidAmount",
    "TruncatedTailError",
    "UnknownEventError",
    "execute",
    "load_account",
]
__version__ = "0.1.0"
