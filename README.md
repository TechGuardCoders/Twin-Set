# Twin Set

State replication with hot standby and failover for dict-like KV stores.

## What it does

`TwinSet` keeps a second copy of your state (the **twin**) in sync with the
**primary**. Every write to the primary is journaled; the twin replays the
journal. Lag is the number of journaled ops the twin has not yet applied.
When the primary dies (detected via an injectable health check), `promote()`
flips the twin to primary with no lost writes.

## Architecture

```
primary.apply(op) ──► Journal (append-only) ──► twin replays ──► lag = len(journal) - applied
       │                                                            ▲
       └── health_check() ── dead? ──► promote(): sync, swap, rebase journal
```

- **Journal** — append-only, thread-safe, sequence-numbered ops (`set`/`del`).
- **TwinSet** — wraps any dict-like primary and twin; `set`/`delete`/`apply`
  write to primary and journal; `sync()` replays pending entries onto the twin.
- **Failover** — `promote()` syncs pending ops first (no lost writes), swaps
  primary/twin, and rebases the journal from the new primary's state so a
  future twin replays from a consistent point.

## Why

In-memory state (caches, session stores, feature flags) is often single-copy.
A full database is overkill; a lost copy is not acceptable. Twin Set gives you
a hot standby with explicit, observable lag and a one-call promotion — pure
Python, no network, no dependencies.

## Usage

```python
from twin_set import TwinSet

primary, twin = {}, {}
ts = TwinSet(primary, twin, health_check=lambda: primary_alive())

ts.set("user:1", "alice")     # journaled + applied to primary
ts.lag()                      # 1 — twin hasn't replayed yet
ts.sync()                     # twin catches up; lag() == 0

if ts.detect_primary_death():
    new_primary = ts.promote()  # twin is now primary, journal rebased
```

## Results

- 18 tests, all passing (`uv run pytest`), no network access in CI.
- Failover preserves every committed write: `promote()` syncs before swapping.
- Lag is exact and monotonic between syncs; `sync()` is incremental and idempotent.

## Run tests

```bash
uv venv && uv pip install pytest && uv run pytest -q
```
