"""Tests for the BankAccount aggregate: pure projection and command validation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from eventide.aggregate import (
    DEPOSIT,
    WITHDRAW,
    BankAccount,
    EventSpec,
    InsufficientFunds,
    InvalidAmount,
    UnknownEventError,
    execute,
    load_account,
)
from eventide.store import Event, EventStore

EPOCH = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Deterministic stand-in for the system clock."""

    def __init__(self, start: datetime = EPOCH) -> None:
        self.current = start

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: float = 1.0) -> None:
        self.current += timedelta(seconds=seconds)


def make_store(tmp_path: Path) -> EventStore:
    """Open a store on a fresh log file with a deterministic clock."""
    return EventStore(tmp_path / "log.jsonl", clock=FakeClock())


def test_deposit_and_withdraw_happy_path(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    account = BankAccount()
    account = execute(store, account, account.deposit(100))
    assert account.balance == 100
    account = execute(store, account, account.withdraw(30))
    assert account.balance == 70

    events = store.replay()
    assert [event.seq for event in events] == [1, 2]
    assert [event.type for event in events] == [DEPOSIT, WITHDRAW]
    assert [event.data for event in events] == [{"amount": 100}, {"amount": 30}]
    assert events[0].timestamp == "2024-01-01T12:00:00.000000Z"


def test_commands_emit_validated_event_specs() -> None:
    account = BankAccount(balance=50)
    assert account.deposit(25) == EventSpec(DEPOSIT, {"amount": 25})
    assert account.withdraw(20) == EventSpec(WITHDRAW, {"amount": 20})


def test_execute_returns_new_state_and_original_is_immutable(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    account = BankAccount()
    updated = execute(store, account, account.deposit(50))
    assert updated.balance == 50
    assert account.balance == 0
    assert updated is not account


def test_overdraft_is_rejected_and_appends_no_event(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    account = execute(store, BankAccount(), BankAccount().deposit(100))

    with pytest.raises(InsufficientFunds, match="cannot withdraw 999"):
        account.withdraw(999)
    with pytest.raises(InsufficientFunds, match="cannot withdraw 999"):
        execute(store, account, EventSpec(WITHDRAW, {"amount": 999}))

    assert [event.seq for event in store.replay()] == [1]
    assert load_account(store).balance == 100


def test_withdraw_exact_balance_is_allowed(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    account = execute(store, BankAccount(), BankAccount().deposit(100))
    account = execute(store, account, account.withdraw(100))
    assert account.balance == 0
    assert [event.type for event in store.replay()] == [DEPOSIT, WITHDRAW]


@pytest.mark.parametrize("bad", [0, -1, -100, 1.5, 2.0, "10", None, True])
def test_non_positive_or_non_integer_amounts_are_rejected(bad: object, tmp_path: Path) -> None:
    store = make_store(tmp_path)
    account = BankAccount(balance=10)
    with pytest.raises(InvalidAmount):
        account.deposit(bad)
    with pytest.raises(InvalidAmount):
        account.withdraw(bad)
    with pytest.raises(InvalidAmount):
        execute(store, account, EventSpec(DEPOSIT, {"amount": bad}))
    assert store.replay() == []


def test_replay_is_deterministic_across_reopened_store(tmp_path: Path) -> None:
    path = tmp_path / "log.jsonl"
    store = EventStore(path, clock=FakeClock())
    account = BankAccount()
    for amount in (100, 40, 5):
        account = execute(store, account, account.deposit(amount))
    account = execute(store, account, account.withdraw(25))
    assert account.balance == 120

    reopened = EventStore(path, clock=FakeClock())
    assert load_account(reopened) == account
    assert load_account(reopened) == load_account(reopened)
    assert BankAccount.project(reopened.replay()) == account
    assert BankAccount.project(store.replay()) == account
    assert load_account(EventStore(path)) == account


def test_unknown_event_type_is_rejected(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append("mystery", {"amount": 1})
    with pytest.raises(UnknownEventError, match="mystery"):
        load_account(store)
    with pytest.raises(UnknownEventError, match="mystery"):
        BankAccount().apply(Event(seq=1, timestamp="t", type="mystery", data={}))
    with pytest.raises(UnknownEventError, match="mystery"):
        execute(store, BankAccount(), EventSpec("mystery", {}))
    assert [event.type for event in store.replay()] == ["mystery"]


def test_malformed_event_payload_is_rejected_on_replay(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append(DEPOSIT, {"amount": -5})
    with pytest.raises(InvalidAmount):
        load_account(store)


def test_empty_log_projects_to_zero_balance(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    assert load_account(store) == BankAccount(balance=0)
    assert BankAccount.project([]) == BankAccount()
    event = Event(seq=1, timestamp="t", type=DEPOSIT, data={"amount": 7})
    assert BankAccount().apply(event) == BankAccount(balance=7)
