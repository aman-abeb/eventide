"""Append-only JSON-lines event store.

Events are persisted as one canonical JSON object per line. Appends are
atomic (a single ``O_APPEND`` write followed by ``fsync``), sequence numbers
are monotonic and gap-free, and replay detects a torn tail left behind by a
crash in the middle of an append.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

__all__ = [
    "CorruptionError",
    "Event",
    "EventStore",
    "EventStoreError",
    "TruncatedTailError",
]

S = TypeVar("S")


class EventStoreError(Exception):
    """Base class for all event store failures."""


class CorruptionError(EventStoreError):
    """A log line is not a well-formed event."""

    def __init__(self, path: Path, line_number: int, detail: str) -> None:
        self.path = path
        self.line_number = line_number
        self.detail = detail
        super().__init__(f"{path}: line {line_number}: {detail}")


class TruncatedTailError(CorruptionError):
    """The final log line is incomplete, e.g. a crash mid-append."""

    def __init__(self, path: Path, line_number: int, offset: int, detail: str) -> None:
        self.offset = offset
        super().__init__(path, line_number, detail)


def _format_timestamp(moment: datetime) -> str:
    """Render a datetime as an ISO-8601 UTC string with a ``Z`` suffix."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    stamp = moment.astimezone(UTC).isoformat(timespec="microseconds")
    return stamp.replace("+00:00", "Z")


@dataclass(frozen=True)
class Event:
    """One immutable event in the log."""

    seq: int
    timestamp: str
    type: str
    data: dict[str, Any]

    def to_json(self) -> str:
        """Serialize to one canonical JSON line (no trailing newline)."""
        return json.dumps(
            {"seq": self.seq, "timestamp": self.timestamp, "type": self.type, "data": self.data},
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )

    @classmethod
    def from_json(cls, line: str) -> Event:
        """Parse one log line, raising ``ValueError`` if it is not an event."""
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError("event line must be a JSON object")
        seq = payload.get("seq")
        timestamp = payload.get("timestamp")
        event_type = payload.get("type")
        data = payload.get("data")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
            raise ValueError("seq must be a positive integer")
        if not isinstance(timestamp, str) or not timestamp:
            raise ValueError("timestamp must be a non-empty string")
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("type must be a non-empty string")
        if not isinstance(data, dict):
            raise ValueError("data must be a JSON object")
        return cls(seq=seq, timestamp=timestamp, type=event_type, data=data)


class EventStore:
    """Append-only store for JSON-lines events backed by a single file."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Open the store at ``path`` (the file is created on first append).

        ``clock`` supplies event timestamps; it defaults to the system UTC
        clock and exists so tests can inject a deterministic one.
        """

        def system_clock() -> datetime:
            return datetime.now(UTC)

        self._path = Path(path)
        self._clock: Callable[[], datetime] = clock or system_clock
        self._last_seq = 0
        self._size = -1
        self._scanned = False

    @property
    def path(self) -> Path:
        """Location of the underlying JSON-lines file."""
        return self._path

    @property
    def last_seq(self) -> int:
        """Sequence number of the newest event, or 0 for an empty log."""
        self._ensure_scanned()
        return self._last_seq

    def append(self, event_type: str, data: dict[str, Any] | None = None) -> Event:
        """Append one event, assign the next sequence number, and return it.

        Raises ``TruncatedTailError`` if the log ends in a torn line; call
        ``recover()`` first.
        """
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValueError("event_type must be a non-empty string")
        payload = dict(data) if data is not None else {}
        try:
            json.dumps(payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"event data must be JSON-serializable: {exc}") from exc
        self._ensure_scanned()
        event = Event(
            seq=self._last_seq + 1,
            timestamp=_format_timestamp(self._clock()),
            type=event_type,
            data=payload,
        )
        self._write_line((event.to_json() + "\n").encode("utf-8"))
        self._last_seq = event.seq
        self._size = self._file_size()
        return event

    def replay(self) -> list[Event]:
        """Return every event in append order.

        Raises ``TruncatedTailError`` for a torn final line and
        ``CorruptionError`` for any other malformed line.
        """
        events, _ = self._parse()
        self._last_seq = events[-1].seq if events else 0
        self._size = self._file_size()
        self._scanned = True
        return events

    def fold(self, initial: S, reducer: Callable[[S, Event], S]) -> S:
        """Fold all events, in append order, into a single value."""
        state = initial
        for event in self.replay():
            state = reducer(state, event)
        return state

    def recover(self) -> list[Event]:
        """Drop a torn tail and return the intact prefix of the log.

        The file is truncated to the end of the last valid line so later
        appends continue with the correct sequence number. A clean log is
        left untouched.
        """
        events, valid_end = self._parse(strict=False)
        if valid_end < self._file_size():
            with self._path.open("r+b") as handle:
                handle.truncate(valid_end)
                handle.flush()
                os.fsync(handle.fileno())
        self._last_seq = events[-1].seq if events else 0
        self._size = self._file_size()
        self._scanned = True
        return events

    def __len__(self) -> int:
        return len(self.replay())

    def __iter__(self) -> Iterator[Event]:
        return iter(self.replay())

    def _ensure_scanned(self) -> None:
        if self._scanned and self._file_size() == self._size:
            return
        self.replay()

    def _file_size(self) -> int:
        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    def _write_line(self, payload: bytes) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fresh = not self._path.exists()
        fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            view = memoryview(payload)
            while view:
                view = view[os.write(fd, view) :]
            os.fsync(fd)
        finally:
            os.close(fd)
        if fresh and os.name == "posix":
            self._fsync_dir()

    def _fsync_dir(self) -> None:
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _iter_lines(self) -> Iterator[tuple[int, int, int, bytes, bool]]:
        """Yield ``(line_number, offset, next_pos, line, is_last)`` per line."""
        if not self._path.exists():
            return
        raw = self._path.read_bytes()
        pos = 0
        line_number = 0
        while pos < len(raw):
            line_number += 1
            newline = raw.find(b"\n", pos)
            if newline == -1:
                yield line_number, pos, len(raw), raw[pos:], True
                return
            yield line_number, pos, newline + 1, raw[pos:newline], newline + 1 >= len(raw)
            pos = newline + 1

    def _parse(self, *, strict: bool = True) -> tuple[list[Event], int]:
        """Parse the log into events plus the offset after the last valid line.

        With ``strict=True`` a torn final line raises ``TruncatedTailError``;
        with ``strict=False`` parsing stops at the torn tail instead.
        """
        events: list[Event] = []
        valid_end = 0
        for line_number, offset, next_pos, line, is_last in self._iter_lines():
            if not line.strip():
                valid_end = next_pos
                continue
            try:
                event = Event.from_json(line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                if is_last:
                    if strict:
                        raise TruncatedTailError(self._path, line_number, offset, str(exc)) from exc
                    return events, valid_end
                raise CorruptionError(self._path, line_number, str(exc)) from exc
            expected_seq = events[-1].seq + 1 if events else 1
            if event.seq != expected_seq:
                detail = f"expected seq {expected_seq}, found {event.seq}"
                raise CorruptionError(self._path, line_number, detail)
            events.append(event)
            valid_end = next_pos
        return events, valid_end
