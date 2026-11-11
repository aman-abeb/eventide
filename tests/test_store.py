"""Tests for the append-only JSON-lines event store."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from eventide.store import (
    CorruptionError,
    Event,
    EventStore,
    TruncatedTailError,
)

EPOCH = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Deterministic stand-in for the system clock."""

    def __init__(self, start: datetime = EPOCH) -> None:
        self.current = start

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: float = 1.0) -> None:
        self.current += timedelta(seconds=seconds)


def test_append_assigns_monotonic_sequences(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "log.jsonl", clock=FakeClock())
    events = [store.append("deposit", {"amount": amount}) for amount in (10, 20, 30)]
    assert [event.seq for event in events] == [1, 2, 3]
    assert [event.seq for event in store.replay()] == [1, 2, 3]
    assert [event.type for event in store] == ["deposit", "deposit", "deposit"]


def test_append_writes_canonical_json_lines(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    store.append("deposit", {"amount": 100})
    raw = path.read_bytes()
    assert raw.endswith(b"\n")
    (line,) = raw.decode("utf-8").splitlines()
    assert json.loads(line) == {
        "data": {"amount": 100},
        "seq": 1,
        "timestamp": "2024-01-01T12:00:00.000000Z",
        "type": "deposit",
    }


def test_append_creates_missing_parent_directories(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "nested" / "deep" / "log.jsonl", clock=FakeClock())
    store.append("deposit", {"amount": 1})
    assert len(store.replay()) == 1


def test_replay_and_fold_are_deterministic(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "log.jsonl", clock=FakeClock())
    for amount in (5, 7, 11):
        store.append("deposit", {"amount": amount})

    def add_amount(state: int, event: Event) -> int:
        return state + event.data["amount"]

    assert store.replay() == store.replay()
    assert store.fold(0, add_amount) == 23
    assert store.fold(0, add_amount) == 23
    assert store.fold([], lambda acc, event: acc + [event.seq]) == [1, 2, 3]


def test_sequence_continues_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    first = EventStore(path, clock=FakeClock())
    first.append("deposit", {"amount": 1})
    first.append("deposit", {"amount": 2})
    second = EventStore(path, clock=FakeClock())
    event = second.append("withdraw", {"amount": 1})
    assert event.seq == 3
    assert [e.seq for e in second.replay()] == [1, 2, 3]
    assert len(second) == 3
    assert second.last_seq == 3


def test_timestamps_are_utc_iso8601_and_increase(tmp_path: Path) -> None:
    clock = FakeClock()
    store = EventStore(tmp_path / "log.jsonl", clock=clock)
    first = store.append("deposit", {"amount": 1})
    clock.advance(seconds=2)
    second = store.append("deposit", {"amount": 2})
    assert first.timestamp == "2024-01-01T12:00:00.000000Z"
    assert second.timestamp == "2024-01-01T12:00:02.000000Z"
    assert first.timestamp < second.timestamp


def test_empty_store(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "log.jsonl", clock=FakeClock())
    assert store.replay() == []
    assert len(store) == 0
    assert store.last_seq == 0
    assert store.fold(0, lambda state, event: state + 1) == 0
    assert store.recover() == []


def test_append_rejects_invalid_input(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "log.jsonl", clock=FakeClock())
    with pytest.raises(ValueError, match="event_type"):
        store.append("", {})
    with pytest.raises(ValueError, match="JSON-serializable"):
        store.append("deposit", {"bad": object()})
    assert store.replay() == []


def test_truncated_tail_is_detected(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    store.append("deposit", {"amount": 100})
    store.append("withdraw", {"amount": 30})
    with path.open("ab") as handle:
        handle.write(b'{"seq":3,"timestamp":"2024-01-01T12:00:02.000000Z","ty')
    with pytest.raises(TruncatedTailError):
        store.replay()
    with pytest.raises(TruncatedTailError):
        store.append("deposit", {"amount": 1})


def test_recover_drops_torn_tail_and_appends_continue(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    store.append("deposit", {"amount": 100})
    store.append("withdraw", {"amount": 30})
    good_size = path.stat().st_size
    with path.open("ab") as handle:
        handle.write(b'{"seq":3,"type":"deposit","data":{"amo')
    recovered = store.recover()
    assert [event.seq for event in recovered] == [1, 2]
    assert path.stat().st_size == good_size
    assert [event.seq for event in store.replay()] == [1, 2]
    assert store.append("deposit", {"amount": 5}).seq == 3
    assert [event.seq for event in store.replay()] == [1, 2, 3]


def test_recover_on_clean_log_is_a_noop(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    store.append("deposit", {"amount": 1})
    size = path.stat().st_size
    assert store.recover() == store.replay()
    assert path.stat().st_size == size


def test_corrupt_middle_line_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    store.append("deposit", {"amount": 1})
    store.append("deposit", {"amount": 2})
    lines = path.read_bytes().splitlines(keepends=True)
    lines.insert(1, b"this is not json\n")
    path.write_bytes(b"".join(lines))
    with pytest.raises(CorruptionError) as excinfo:
        EventStore(path, clock=FakeClock()).replay()
    assert type(excinfo.value) is CorruptionError
    assert excinfo.value.line_number == 2


def test_non_monotonic_sequence_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    path.write_text(
        '{"data":{},"seq":1,"timestamp":"t","type":"a"}\n'
        '{"data":{},"seq":3,"timestamp":"t","type":"b"}\n',
        encoding="utf-8",
    )
    with pytest.raises(CorruptionError, match="expected seq 2"):
        EventStore(path).replay()


def test_event_json_roundtrip() -> None:
    event = Event(
        seq=7,
        timestamp="2024-01-01T00:00:00.000000Z",
        type="deposit",
        data={"amount": 42},
    )
    assert Event.from_json(event.to_json()) == event


@pytest.mark.parametrize(
    "line",
    [
        "not json",
        "[]",
        '{"data":{},"seq":0,"timestamp":"t","type":"a"}',
        '{"data":{},"seq":true,"timestamp":"t","type":"a"}',
        '{"data":{},"seq":1,"timestamp":"","type":"a"}',
        '{"data":{},"seq":1,"timestamp":"t","type":""}',
        '{"data":[],"seq":1,"timestamp":"t","type":"a"}',
    ],
)
def test_from_json_rejects_malformed_lines(line: str) -> None:
    with pytest.raises(ValueError):
        Event.from_json(line)
