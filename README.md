# Eventide

<!-- builder:badges:start -->
[![CI](https://github.com/aman-abeb/eventide/actions/workflows/ci.yml/badge.svg)](https://github.com/aman-abeb/eventide/actions/workflows/ci.yml) ![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white) ![License: MIT](https://img.shields.io/badge/license-MIT-green)
<!-- builder:badges:end -->


Eventide is an append-only JSON-lines event store: every event is written atomically to a local log with a monotonic sequence number, and any state is rebuilt by replaying the log.

## Features

- Append-only JSON-lines log with atomic writes (single `O_APPEND` write + `fsync`, parent directory fsynced on creation)
- Monotonic, gap-free sequence numbers that continue correctly when the store is reopened
- Deterministic `replay()` and `fold()` over the log
- Torn-tail detection: a crash mid-append raises `TruncatedTailError`, and `recover()` truncates the partial line so appends can continue
- Corruption detection for malformed lines and out-of-order sequence numbers

## Quickstart

```bash
pip install -e .        # install the package (src layout)
python -m eventide      # run a short append/replay demo
pytest                  # run the test suite (offline, no network or API keys)
```

If you prefer not to install, the test suite also runs straight from the repo root because `pyproject.toml` sets `pythonpath = ["src"]`.

## Example

`python -m eventide` appends two events to a temporary log and prints:

```text
eventide demo (temporary log):
{"data":{"amount":100},"seq":1,"timestamp":"2024-01-01T12:00:00.000000Z","type":"deposit"}
{"data":{"amount":30},"seq":2,"timestamp":"2024-01-01T12:00:01.000000Z","type":"withdraw"}
balance: 70
```

## How it works

- `src/eventide/store.py` holds the whole persistence layer. `EventStore.append()` serializes an `Event` to one canonical JSON line, writes it with a single `O_APPEND` write followed by `os.fsync`, and assigns `seq = last_seq + 1`.
- `EventStore.replay()` parses every line, validates the event shape, and enforces gap-free sequence numbers. A malformed final line is reported as `TruncatedTailError`; `recover()` truncates the file to the last valid line.
- `EventStore.fold(initial, reducer)` replays the log into a single value, which is how aggregates rebuild their state.
- Tests live in `tests/test_store.py` and cover append ordering, sequence continuity across store instances, replay determinism, torn-tail recovery, and corruption rejection.

## Running tests

```bash
pytest
```

The suite is fully offline: it uses `tmp_path` fixtures and an injected fake clock, and needs no network or API keys.

<!-- builder:roadmap:start -->
## Status

**Complete.** Eventide is a finished small-scale append-only JSON-lines event store featuring a BankAccount aggregate that enforces no-overdraft and positive-amount invariants, plus a Typer/Rich CLI (deposit/withdraw/balance/log/reset) backed by .eventide/log.jsonl. The implementation includes atomic fsync appends, monotonic sequence numbers, replay/fold with truncated-tail corruption detection, and a full offline pytest suite.

## Roadmap

- [x] **Core event store, aggregate, and CLI**: Ship a runnable package: an append-only JSON-lines EventStore (atomic append with fsync, monotonic sequence numbers, replay/fold, corruption detection on truncated last line), a BankAccount aggregate enforcing invariants (no overdraft, positive amounts) that emits domain events, and a Typer + Rich CLI with deposit/withdraw/balance/log/reset commands backed by a local .eventide/log.jsonl file. Include pytest coverage for append ordering, replay determinism, overdraft rejection, idempotent sequence assignment, and recovery from a truncated tail, plus a README with the exact showcase session.

## Ideas for future work

- Add CLI end-to-end integration tests invoking the installed binary
- Implement snapshot/compaction for large event logs
- Add event schema versioning for forward compatibility
<!-- builder:roadmap:end -->
