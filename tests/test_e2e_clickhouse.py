"""End-to-end test against a real ClickHouse server.

SQL Server is replaced by an in-memory fake that implements Change Tracking
semantics (commit versions, net changes per key, retention). Everything else
-- supervisor, tasks, DDL, state store, EXCHANGE cut-over -- is the real code.

Run with:  CH_TEST_HOST=127.0.0.1 pytest tests/test_e2e_clickhouse.py
"""
from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(not os.environ.get("CH_TEST_HOST"), reason="CH_TEST_HOST not set")

import ch_sync.supervisor as sup_mod  # noqa: E402
import ch_sync.tasks as tasks_mod  # noqa: E402
from ch_sync.config import Config  # noqa: E402
from ch_sync.model import Column, TableSchema  # noqa: E402
from ch_sync.sink import ClickHouseSink  # noqa: E402
from ch_sync.source import ResyncRequired  # noqa: E402
from ch_sync.state import StateStore  # noqa: E402
from ch_sync.typemap import map_column  # noqa: E402

DB = "e2e_analytics"
META = "e2e_meta"


# --- fake SQL Server -----------------------------------------------------------

class FakeTable:
    def __init__(self, schema: str, name: str, cols: list[tuple]):
        self.schema, self.name = schema, name
        self.cols = cols  # (name, type, nullable, pk_ordinal, precision, scale)
        self.rows: dict[tuple, dict] = {}
        self.changes: dict[tuple, int] = {}  # pk -> last change version
        self.min_valid = 0

    def ts(self, exclude=()):
        return TableSchema(self.schema, self.name, [
            Column(n, t, nl, pk, map_column(t, p, s)) for n, t, nl, pk, p, s in self.cols if n not in exclude
        ])

    def pk_of(self, row):
        return tuple(row[c.name] for c in self.ts().pk)


def default_for(ch_type: str):
    if ch_type.startswith("DateTime64"):
        return datetime(1970, 1, 1)
    if ch_type == "String":
        return ""
    if ch_type == "UUID":
        return "00000000-0000-0000-0000-000000000000"
    return 0


class FakeSqlServer:
    def __init__(self):
        self.version = 100
        self.tables: dict[str, FakeTable] = {}
        self.on_first_snapshot_read = None

    def _commit(self, t: FakeTable, pk):
        self.version += 1
        t.changes[pk] = self.version

    def upsert(self, key, row):
        t = self.tables[key]
        pk = t.pk_of(row)
        t.rows[pk] = dict(row)
        self._commit(t, pk)

    def delete(self, key, pk):
        t = self.tables[key]
        del t.rows[pk]
        self._commit(t, pk)

    # --- SqlServerSource interface ---
    def close(self): pass
    def current_version(self): return self.version
    def snapshot_isolation_enabled(self): return True
    def discover_tables(self): return list(self.tables)
    def min_valid_version(self, ts): return self.tables[ts.key].min_valid
    def approx_rows(self, ts): return len(self.tables[ts.key].rows)

    def load_schema(self, schema, name, exclude=()):
        return self.tables[f"{schema}.{name}"].ts(exclude)

    def pk_bounds(self, ts):
        keys = [k[0] for k in self.tables[ts.key].rows]
        return (min(keys), max(keys)) if keys else (None, None)

    def iter_query(self, sql, params, fetch_size):
        assert sql == "FAKE_SNAPSHOT"
        ts, version, lo, hi, after = params
        if self.on_first_snapshot_read:
            hook, self.on_first_snapshot_read = self.on_first_snapshot_read, None
            hook()
        t = self.tables[ts.key]
        out = []
        for pk in sorted(t.rows):
            if lo is not None and pk[0] < lo:
                continue
            if hi is not None and pk[0] >= hi:
                continue
            if after is not None and pk <= tuple(after):
                continue
            row = t.rows[pk]
            out.append(tuple(row.get(c.name) for c in ts.columns) + (version, 0) + pk)
        for i in range(0, len(out), fetch_size):
            yield out[i:i + fetch_size]

    @contextmanager
    def change_session(self, ts, last_version):
        t = self.tables[ts.key]
        if last_version < t.min_valid:
            raise ResyncRequired(f"{ts.key}: checkpoint {last_version} < min valid {t.min_valid}")
        current = self.version
        rows = []
        for pk, ver in sorted(t.changes.items()):
            if last_version < ver <= current:
                live = t.rows.get(pk)
                vals = []
                for c in ts.columns:
                    if c.is_pk:
                        vals.append(pk[c.pk_ordinal - 1])
                    elif live is not None:
                        vals.append(live.get(c.name))
                    else:
                        vals.append(None if c.nullable else default_for(c.mapping.ch_type))
                rows.append(tuple(vals) + (ver, 0 if live is not None else 1))

        class S:
            current_version = current
            def batches(self_inner):
                for i in range(0, len(rows), 7):
                    yield rows[i:i + 7]
        yield S()


class InlineExecutor:
    def submit(self, fn, *args):
        f = Future()
        try:
            f.set_result(fn(*args))
        except Exception as exc:  # noqa: BLE001
            f.set_exception(exc)
        return f

    def shutdown(self, wait=True, cancel_futures=False):
        pass


# --- fixtures --------------------------------------------------------------------

def order_row(i, **over):
    row = {
        "OrderId": i, "CustomerId": i % 97,
        "Amount": None if i % 10 == 0 else Decimal(i) / 4,
        "CreatedAt": datetime(2024, 1, 1, 12, 0, 0, 123000),
        "Ref": str(uuid.UUID(int=i)) if i % 3 else None,
    }
    row.update(over)
    return row


@pytest.fixture
def env(monkeypatch):
    fake = FakeSqlServer()
    orders = FakeTable("dbo", "Orders", [
        ("OrderId", "bigint", False, 1, 19, 0), ("CustomerId", "int", False, None, 10, 0),
        ("Amount", "decimal", True, None, 18, 2), ("CreatedAt", "datetime2", False, None, 27, 3),
        ("Ref", "uniqueidentifier", True, None, 0, 0),
    ])
    lines = FakeTable("sales", "Lines", [
        ("OrderId", "int", False, 1, 10, 0), ("LineNo", "smallint", False, 2, 5, 0),
        ("Sku", "nvarchar", False, None, 0, 0),
    ])
    fake.tables = {"dbo.Orders": orders, "sales.Lines": lines}
    for i in range(1, 25_001):
        orders.rows[(i,)] = order_row(i)
    for o in range(1, 1001):
        for ln in range(1, 4):
            lines.rows[(o, ln)] = {"OrderId": o, "LineNo": ln, "Sku": f"SKU-{o}-{ln}"}
    fake.version = 5000

    cfg = Config.from_dict({
        "source": {"server": "fake", "database": "fake"},
        "target": {"host": os.environ["CH_TEST_HOST"], "database": DB, "meta_database": META},
        "sync": {"workers": 4, "poll_interval": 0, "batch_size": 1000, "snapshot_partitions": 4,
                 "min_rows_per_partition": 1000, "schema_refresh_seconds": 0, "state_flush_seconds": 0},
        "tables": [{"source": "dbo.Orders"}, {"source": "sales.Lines", "target": "lines"}],
    })
    cfg.source.fetch_size = 500

    monkeypatch.setattr(sup_mod, "SqlServerSource", lambda _cfg: fake)
    monkeypatch.setattr(tasks_mod, "SqlServerSource", lambda _cfg: fake)
    monkeypatch.setattr(tasks_mod, "snapshot_query",
                        lambda ts, v, lo, hi, after: ("FAKE_SNAPSHOT", [ts, v, lo, hi, after]))

    sink = ClickHouseSink(cfg.target)
    for db in (DB, META):
        sink.command(f"DROP DATABASE IF EXISTS {db} SYNC")
    tasks_mod.init_worker(cfg, threading.Event(), "INFO")

    def new_supervisor():
        s = sup_mod.Supervisor(cfg)
        s._new_pool = lambda: InlineExecutor()
        s.start()
        return s

    return fake, cfg, sink, new_supervisor


def run_until(sup, cond, max_steps=50):
    for _ in range(max_steps):
        sup.step(collect_timeout=0)
        if cond():
            sup._flush()
            return
    raise AssertionError("condition not reached; states: "
                         + str({k: (r.state.phase, r.state.last_error) for k, r in sup.tables.items()}))


def phases(sup):
    return {k: r.state.phase for k, r in sup.tables.items()}


def all_cdc(sup):
    return all(p == "cdc" for p in phases(sup).values())


def ch_orders(sink):
    rows = sink.rows(f"SELECT OrderId, CustomerId, Amount, CreatedAt, Ref FROM {DB}.Orders_current ORDER BY OrderId")
    return {r[0]: (r[1], r[2], r[3].replace(tzinfo=None), str(r[4]) if r[4] else None) for r in rows}


def src_orders(fake):
    return {
        pk[0]: (r["CustomerId"], r["Amount"], r["CreatedAt"], r["Ref"])
        for pk, r in fake.tables["dbo.Orders"].rows.items()
    }


# --- the test -------------------------------------------------------------------------

def test_full_lifecycle(env):
    fake, cfg, sink, new_supervisor = env

    # Writes that land while the initial snapshot is running (after V0 is taken).
    def concurrent_writes():
        fake.upsert("dbo.Orders", order_row(5, CustomerId=999_999))
        fake.delete("dbo.Orders", (6,))
        fake.upsert("dbo.Orders", order_row(30_000))

    fake.on_first_snapshot_read = concurrent_writes

    sup = new_supervisor()
    run_until(sup, lambda: all_cdc(sup))
    assert len(sup.tables["dbo.Orders"].state.partitions) == 0  # cleared after finalize
    log_rows = sink.rows(f"SELECT count() FROM {META}.sync_log WHERE task LIKE 'snapshot%' AND table_key = 'dbo.Orders'")
    assert log_rows[0][0] == 4, "expected 4 parallel snapshot partitions"

    # one CDC round picks up the concurrent writes
    run_until(sup, lambda: sup.tables["dbo.Orders"].state.last_version == fake.version)
    assert ch_orders(sink) == src_orders(fake)
    assert sink.rows(f"SELECT count() FROM {DB}.lines_current")[0][0] == 3000

    # --- steady-state CDC: updates, deletes, inserts, delete+reinsert ---
    for i in range(100, 200):
        fake.upsert("dbo.Orders", order_row(i, Amount=Decimal("1.25"), Ref=None))
    for i in range(200, 250):
        fake.delete("dbo.Orders", (i,))
    for i in range(40_000, 40_100):
        fake.upsert("dbo.Orders", order_row(i))
    fake.delete("dbo.Orders", (7,))
    fake.upsert("dbo.Orders", order_row(7, CustomerId=7777))
    fake.delete("sales.Lines", (1, 1))
    fake.upsert("sales.Lines", {"OrderId": 1, "LineNo": 9, "Sku": "NEW"})

    run_until(sup, lambda: all(r.state.last_version == fake.version for r in sup.tables.values()))
    assert ch_orders(sink) == src_orders(fake)
    lines = dict(((o, ln), s) for o, ln, s in sink.rows(f"SELECT OrderId, LineNo, Sku FROM {DB}.lines_current"))
    assert (1, 1) not in lines and lines[(1, 9)] == "NEW" and len(lines) == 3000

    # --- schema drift: new nullable column appears in the source ---
    fake.tables["dbo.Orders"].cols.append(("Note", "nvarchar", True, None, 0, 0))
    fake.upsert("dbo.Orders", order_row(11, Note="hello"))
    run_until(sup, lambda: sup.tables["dbo.Orders"].state.last_version == fake.version)
    assert sink.rows(f"SELECT Note FROM {DB}.Orders_current WHERE OrderId = 11")[0][0] == "hello"
    assert sink.rows(f"SELECT count() FROM {DB}.Orders_current WHERE Note IS NULL")[0][0] > 0

    # --- CT retention exceeded: automatic resync via staging + EXCHANGE ---
    # Delete a row whose change record is lost to retention cleanup.
    del fake.tables["dbo.Orders"].rows[(8,)]
    fake.tables["dbo.Orders"].changes.pop((8,), None)
    fake.tables["dbo.Orders"].min_valid = fake.version + 1
    fake.version += 1
    run_until(sup, lambda: sup.tables["dbo.Orders"].state.phase == "resync")
    fake.tables["dbo.Orders"].min_valid = fake.version
    run_until(sup, lambda: all_cdc(sup))
    got = ch_orders(sink)
    assert 8 not in got, "resync must drop rows deleted while CT history was lost"
    # (Note column is part of the source now)
    assert got == src_orders(fake)
    raw_total = sink.rows(f"SELECT count() FROM {DB}.Orders FINAL")[0][0]
    assert raw_total == len(fake.tables["dbo.Orders"].rows)

    # --- operator-requested resync, with a crash right after EXCHANGE ---
    store = StateStore(sink, META)
    store.request("sales.Lines")
    sup._next_requests = 0
    original_save = sup.store.save
    crashed = {"done": False}

    def crash_after_exchange(states):
        # Once "crashed", the old process is dead: it never writes state again.
        if crashed["done"] or any(s.table_key == "sales.Lines" and s.phase == "cdc" for s in states):
            crashed["done"] = True
            raise RuntimeError("simulated crash after EXCHANGE")
        return original_save(states)

    sup.store.save = crash_after_exchange
    run_until(sup, lambda: crashed["done"])
    assert sink.table_comment(DB, "lines").startswith("ch_sync:")  # swap happened

    # "restart": brand-new supervisor reading state from ClickHouse
    sup2 = new_supervisor()
    assert sup2.tables["sales.Lines"].state.phase == "snapshot"
    fake.upsert("sales.Lines", {"OrderId": 2, "LineNo": 9, "Sku": "AFTER-RESTART"})
    run_until(sup2, lambda: all_cdc(sup2) and sup2.tables["sales.Lines"].state.last_version == fake.version)
    lines = dict(((o, ln), s) for o, ln, s in sink.rows(f"SELECT OrderId, LineNo, Sku FROM {DB}.lines_current"))
    assert lines[(1, 9)] == "NEW" and lines[(2, 9)] == "AFTER-RESTART" and len(lines) == 3001
    assert not sink.exists(DB, "lines__staging")
    sup2._shutdown()
    sup.stopping = True


def test_naive_datetimes_are_utc(env, monkeypatch):
    """Guards against clickhouse-connect interpreting naive datetimes in local time."""
    _, cfg, sink, _ = env
    import time as _t
    monkeypatch.setenv("TZ", "Asia/Kolkata")
    _t.tzset()
    try:
        s = ClickHouseSink(cfg.target)
        s.command(f"CREATE DATABASE IF NOT EXISTS {DB}")
        s.command(f"CREATE TABLE {DB}.tz (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d")
        s.insert(DB, "tz", [[datetime(2024, 5, 1, 10, 30, 0)]], ["d"])
        assert s.rows(f"SELECT toString(d) FROM {DB}.tz")[0][0] == "2024-05-01 10:30:00.000"
    finally:
        monkeypatch.setenv("TZ", "UTC")
        _t.tzset()
