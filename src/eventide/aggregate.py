"""Bank-account aggregate: pure event projection plus validated commands.

The aggregate is deliberately store-agnostic. :class:`BankAccount` is a frozen
value object that projects domain events onto itself, while the
``deposit``/``withdraw`` commands enforce the invariants (positive amounts, no
overdraft) and return an :class:`EventSpec` describing the event to append.
Nothing is persisted in this module: :func:`execute` wires a validated spec to
an :class:`~eventide.store.EventStore` and returns the new state, and
:func:`load_account` rebuilds the current state by replaying the log.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from eventide.store import Event, EventStore

__all__ = [
    "DEPOSIT",
    "WITHDRAW",
    "AggregateError",
    "BankAccount",
    "EventSpec",
    "InsufficientFunds",
    "InvalidAmount",
    "UnknownEventError",
    "execute",
    "load_account",
]

DEPOSIT = "deposit"
WITHDRAW = "withdraw"

_KNOWN_TYPES = frozenset({DEPOSIT, WITHDRAW})


class AggregateError(Exception):
    """Base class for all aggregate (domain) failures."""


class InvalidAmount(AggregateError, ValueError):
    """An amount is not a positive integer."""


class InsufficientFunds(AggregateError):
    """A withdrawal would overdraw the account."""


class UnknownEventError(AggregateError):
    """The log contains an event type the aggregate cannot project."""


def _validate_amount(amount: Any) -> int:
    """Return ``amount`` unchanged if it is a positive integer.

    Raises ``InvalidAmount`` for anything else: booleans, floats, strings,
    zero, and negative numbers.
    """
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise InvalidAmount(f"amount must be a positive integer, got {amount!r}")
    if amount <= 0:
        raise InvalidAmount(f"amount must be positive, got {amount}")
    return amount


@dataclass(frozen=True)
class EventSpec:
    """A validated domain event that has not been appended to a store yet."""

    type: str
    data: dict[str, Any]


@dataclass(frozen=True)
class BankAccount:
    """A bank account whose state is rebuilt purely by folding domain events."""

    balance: int = 0

    def deposit(self, amount: int) -> EventSpec:
        """Validate a deposit and return the event spec to append."""
        return EventSpec(DEPOSIT, {"amount": _validate_amount(amount)})

    def withdraw(self, amount: int) -> EventSpec:
        """Validate a withdrawal and return the event spec to append.

        Raises ``InsufficientFunds`` when the amount exceeds the balance.
        """
        amount = _validate_amount(amount)
        if amount > self.balance:
            raise InsufficientFunds(
                f"cannot withdraw {amount} from an account holding {self.balance}"
            )
        return EventSpec(WITHDRAW, {"amount": amount})

    def apply(self, event: Event) -> BankAccount:
        """Project one event onto this account and return the next state.

        Projection is pure: it trusts the log, so the invariants are enforced
        at command time (``deposit``/``withdraw``), not here.
        """
        if event.type not in _KNOWN_TYPES:
            raise UnknownEventError(f"unknown event type: {event.type!r}")
        amount = _validate_amount(event.data.get("amount"))
        if event.type == DEPOSIT:
            return replace(self, balance=self.balance + amount)
        return replace(self, balance=self.balance - amount)

    @classmethod
    def project(cls, events: Iterable[Event]) -> BankAccount:
        """Fold an iterable of events into an account, starting from empty."""
        account = cls()
        for event in events:
            account = account.apply(event)
        return account


def load_account(store: EventStore) -> BankAccount:
    """Rebuild the account by replaying every event in ``store``."""
    return store.fold(BankAccount(), BankAccount.apply)


def execute(store: EventStore, account: BankAccount, command: EventSpec) -> BankAccount:
    """Append ``command`` to ``store`` and return the resulting account.

    The command is re-validated against ``account`` before anything is
    written, so a rejected command never appends an event.
    """
    if command.type not in _KNOWN_TYPES:
        raise UnknownEventError(f"unknown event type: {command.type!r}")
    amount = _validate_amount(command.data.get("amount"))
    if command.type == WITHDRAW and amount > account.balance:
        raise InsufficientFunds(
            f"cannot withdraw {amount} from an account holding {account.balance}"
        )
    event = store.append(command.type, command.data)
    return account.apply(event)
