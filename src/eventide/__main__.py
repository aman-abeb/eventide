"""Runnable entrypoint: ``python -m eventide`` runs a short demo and exits."""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import count
from pathlib import Path

from eventide.store import Event, EventStore

_DEMO_START = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _demo_clock() -> Callable[[], datetime]:
    """Deterministic clock so the demo output is reproducible."""
    ticks = count()
    return lambda: _DEMO_START + timedelta(seconds=next(ticks))


def _apply(state: int, event: Event) -> int:
    sign = 1 if event.type == "deposit" else -1
    return state + sign * event.data["amount"]


def main() -> None:
    """Append two events to a temporary log, print the log, and fold a balance."""
    with tempfile.TemporaryDirectory() as tmp:
        store = EventStore(Path(tmp) / "log.jsonl", clock=_demo_clock())
        store.append("deposit", {"amount": 100})
        store.append("withdraw", {"amount": 30})
        print("eventide demo (temporary log):")
        for event in store.replay():
            print(event.to_json())
        print(f"balance: {store.fold(0, _apply)}")


if __name__ == "__main__":
    main()
