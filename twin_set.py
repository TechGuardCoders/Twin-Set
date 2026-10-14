"""Twin Set: state replication with hot standby and failover.

Primary holds the live dict-like KV store. Every write to the primary is
journaled; the twin replays the journal to stay in sync. Lag is the number
of journaled ops the twin has not yet applied. promote() flips the twin to
primary; detect_primary_death() uses an injectable health check.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Iterable, List, Optional


class Journal:
    """Append-only log of operations applied to the primary."""

    def __init__(self) -> None:
        self._entries: List[dict] = []
        self._lock = threading.Lock()

    def append(self, op: str, key: str, value: Any = None) -> int:
        with self._lock:
            self._entries.append({"seq": len(self._entries), "op": op, "key": key, "value": value})
            return len(self._entries) - 1

    def entries_from(self, seq: int) -> List[dict]:
        with self._lock:
            return list(self._entries[seq:])

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


class TwinSet:
    """Replicates a primary KV store to a hot-standby twin via a journal.

    The primary and twin are any dict-like objects (must support
    __getitem__, __setitem__, __delitem__, get, keys, clear, update).
    """

    def __init__(self, primary: Any, twin: Any, health_check: Optional[Callable[[], bool]] = None) -> None:
        self.primary = primary
        self.twin = twin
        self.journal = Journal()
        self._applied_seq = 0  # next journal seq the twin has NOT applied
        self._health_check = health_check or (lambda: True)
        self._lock = threading.RLock()

    # ---- writes (journaled, then applied to primary) ----

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self.journal.append("set", key, value)
            self.primary[key] = value

    def delete(self, key: str) -> None:
        with self._lock:
            self.journal.append("del", key)
            try:
                del self.primary[key]
            except KeyError:
                pass

    def apply(self, op: str, key: str, value: Any = None) -> None:
        """Generic entry point: apply(op, key, value) with op in {set, del}."""
        if op == "set":
            self.set(key, value)
        elif op == "del":
            self.delete(key)
        else:
            raise ValueError(f"unknown op: {op!r}")

    # ---- replication ----

    def lag(self) -> int:
        """Number of journaled ops the twin has not yet applied."""
        with self._lock:
            return len(self.journal) - self._applied_seq

    def sync(self) -> int:
        """Replay pending journal entries onto the twin. Returns ops applied."""
        applied = 0
        with self._lock:
            for entry in self.journal.entries_from(self._applied_seq):
                self._apply_to_twin(entry)
                self._applied_seq = entry["seq"] + 1
                applied += 1
        return applied

    def _apply_to_twin(self, entry: dict) -> None:
        if entry["op"] == "set":
            self.twin[entry["key"]] = entry["value"]
        elif entry["op"] == "del":
            try:
                del self.twin[entry["key"]]
            except KeyError:
                pass

    # ---- health / failover ----

    def is_primary_alive(self) -> bool:
        try:
            return bool(self._health_check())
        except Exception:
            return False

    def detect_primary_death(self) -> bool:
        """True if the health check reports the primary is dead."""
        return not self.is_primary_alive()

    def promote(self) -> Any:
        """Fail over: twin becomes the new primary.

        Syncs any pending journal first so no committed write is lost.
        Returns the new primary handle (the old twin).
        """
        with self._lock:
            self.sync()
            self.primary, self.twin = self.twin, self.primary
            # Rebase the journal onto the new primary's state.
            self._rebase_journal()
            return self.primary

    def _rebase_journal(self) -> None:
        """After promotion, rebuild the journal from the new primary's state
        so future twins replay from a consistent point."""
        with self._lock:
            self.journal = Journal()
            for key in list(self.primary.keys()):
                self.journal.append("set", key, self.primary[key])
            self._applied_seq = 0
            self.sync()

    # ---- convenience ----

    def get(self, key: str, default: Any = None) -> Any:
        return self.primary.get(key, default)

    def keys(self) -> Iterable:
        return self.primary.keys()

    def snapshot(self) -> Dict:
        return dict(self.primary)
