import pytest

from twin_set import Journal, TwinSet


def make_store():
    return {}


def test_basic_set_get():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    assert ts.get("a") == 1


def test_delete():
    ts = TwinSet({"a": 1}, make_store())
    ts.delete("a")
    assert ts.get("a") is None


def test_apply_generic():
    ts = TwinSet(make_store(), make_store())
    ts.apply("set", "k", "v")
    ts.apply("del", "k")
    assert ts.get("k") is None


def test_apply_unknown_op_raises():
    ts = TwinSet(make_store(), make_store())
    with pytest.raises(ValueError):
        ts.apply("frobnicate", "k")


def test_journal_records_ops():
    j = Journal()
    j.append("set", "a", 1)
    j.append("del", "a")
    assert len(j) == 2
    assert j.entries_from(0)[1]["op"] == "del"


def test_lag_zero_after_sync():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    ts.set("b", 2)
    assert ts.lag() == 2
    ts.sync()
    assert ts.lag() == 0


def test_sync_replicates_state():
    primary, twin = make_store(), make_store()
    ts = TwinSet(primary, twin)
    ts.set("x", 10)
    ts.set("y", 20)
    ts.sync()
    assert twin == {"x": 10, "y": 20}


def test_sync_applies_deletes_to_twin():
    twin = {"gone": 1}
    ts = TwinSet(make_store(), twin)
    ts.delete("gone")
    ts.sync()
    assert "gone" not in twin


def test_sync_returns_count_and_is_incremental():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    assert ts.sync() == 1
    ts.set("b", 2)
    assert ts.sync() == 1
    assert ts.sync() == 0


def test_promote_returns_new_primary():
    primary, twin = make_store(), make_store()
    ts = TwinSet(primary, twin)
    ts.set("k", "v")
    new_primary = ts.promote()
    assert new_primary is twin
    assert ts.primary is twin
    assert ts.get("k") == "v"


def test_promote_syncs_pending_writes_first():
    primary, twin = make_store(), make_store()
    ts = TwinSet(primary, twin)
    ts.set("a", 1)
    ts.set("b", 2)
    # no explicit sync before promote
    new_primary = ts.promote()
    assert new_primary == {"a": 1, "b": 2}


def test_writes_after_promote_go_to_new_primary():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    ts.promote()
    ts.set("b", 2)
    assert ts.get("b") == 2
    assert ts.lag() == 1
    ts.sync()
    assert ts.twin == {"a": 1, "b": 2}


def test_health_check_injectable():
    alive = {"ok": True}
    ts = TwinSet(make_store(), make_store(), health_check=lambda: alive["ok"])
    assert not ts.detect_primary_death()
    alive["ok"] = False
    assert ts.detect_primary_death()


def test_health_check_exception_counts_as_dead():
    def boom():
        raise RuntimeError("connection refused")

    ts = TwinSet(make_store(), make_store(), health_check=boom)
    assert ts.detect_primary_death()


def test_failover_workflow():
    primary, twin = make_store(), make_store()
    alive = {"ok": True}
    ts = TwinSet(primary, twin, health_check=lambda: alive["ok"])
    ts.set("user:1", "alice")
    ts.sync()

    alive["ok"] = False
    assert ts.detect_primary_death()
    new_primary = ts.promote()
    assert new_primary is twin
    assert new_primary == {"user:1": "alice"}

    # new primary keeps serving and replicating
    ts.set("user:2", "bob")
    ts.sync()
    assert ts.twin == {"user:1": "alice", "user:2": "bob"}


def test_double_promote_round_trip():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    ts.promote()
    ts.set("b", 2)
    ts.promote()
    assert ts.get("a") == 1 and ts.get("b") == 2


def test_snapshot():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    assert ts.snapshot() == {"a": 1}


def test_keys():
    ts = TwinSet(make_store(), make_store())
    ts.set("a", 1)
    ts.set("b", 2)
    assert set(ts.keys()) == {"a", "b"}
