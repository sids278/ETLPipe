"""Supervisor: owns table lifecycle and schedules work onto a process pool.

Per table lifecycle:

    new/resync --plan--> snapshot --all partitions done--> finalize --> cdc
                             ^                                          |
                             +------ CT retention exceeded / resync <---+

Only the supervisor writes table state and performs DDL cut-overs, so there
are no races between workers. Workers only write their own partition
progress rows and data.
"""
from __future__ import annotations

import logging
import multiprocessing as mp
import signal
import time
import uuid
import zlib
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field

from .config import Config, TableConfig
from .ddl import create_table_sql, qch, view_sql
from .queries import plan_partitions
from .sink import ClickHouseSink
from .source import ChangeTrackingDisabled, SqlServerSource
from .state import (
    PHASE_CDC, PHASE_NEW, PHASE_RESYNC, PHASE_SNAPSHOT, PartitionProgress, StateStore, TableState,
)
from .tasks import TaskResult, init_worker, run_cdc, run_snapshot_partition

log = logging.getLogger("ch_sync.supervisor")

REQUEST_POLL_SECONDS = 10.0
HEARTBEAT_SECONDS = 60.0


def owns_table(key: str, shard_index: int, shard_count: int) -> bool:
    return zlib.crc32(key.lower().encode()) % shard_count == shard_index


@dataclass
class TableRuntime:
    tcfg: TableConfig
    state: TableState
    progress: dict[int, PartitionProgress] | None = None
    running: set = field(default_factory=set)
    next_run: float = 0.0
    blocked_until: float = 0.0
    failures: int = 0
    last_persist: float = 0.0

    @property
    def key(self) -> str:
        return self.tcfg.key


class Supervisor:
    def __init__(self, cfg: Config, log_level: str = "INFO"):
        self.cfg = cfg
        self.log_level = log_level
        self.stopping = False
        self.tables: dict[str, TableRuntime] = {}
        self.futures: dict[Future, tuple[str, tuple]] = {}
        self.dirty: dict[str, TableState] = {}
        self.log_buf: list[list] = []
        self.rows_since_heartbeat = 0

    # --- lifecycle -------------------------------------------------------------

    def _on_signal(self, signum, _frame) -> None:
        log.info("received signal %s, shutting down gracefully", signum)
        self.stopping = True

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        self.start()
        try:
            while not self.stopping:
                self.step()
        finally:
            self._shutdown()

    def start(self) -> None:
        cfg = self.cfg
        self.src = SqlServerSource(cfg.source)
        self.sink = ClickHouseSink(cfg.target)
        self.store = StateStore(self.sink, cfg.target.meta_database, cfg.target.replicated)
        self.store.bootstrap()
        self.sink.command(f"CREATE DATABASE IF NOT EXISTS {qch(cfg.target.database)}")
        if cfg.source.use_snapshot_isolation and not self.src.snapshot_isolation_enabled():
            raise SystemExit(
                "source.use_snapshot_isolation is true but ALLOW_SNAPSHOT_ISOLATION is OFF on the "
                "source database. Enable it (see sql/01_enable_change_tracking.sql) or set the option to false."
            )
        self._refresh_tables()
        self._mp_ctx = mp.get_context("spawn")
        self.stop_event = self._mp_ctx.Event()
        self.pool = self._new_pool()
        now = time.monotonic()
        self._next_discovery = now + cfg.sync.schema_refresh_seconds
        self._next_requests = 0.0
        self._next_flush = 0.0
        self._next_heartbeat = now + HEARTBEAT_SECONDS
        log.info(
            "started: %d tables (shard %d/%d), %d workers, %d max for snapshots",
            len(self.tables), cfg.sync.shard_index, cfg.sync.shard_count,
            cfg.sync.workers, cfg.sync.snapshot_worker_cap,
        )

    def step(self, collect_timeout: float = 0.5) -> None:
        """One scheduler iteration: refresh, schedule, collect results, persist."""
        cfg = self.cfg
        now = time.monotonic()
        if now >= self._next_discovery:
            self._refresh_tables()
            self._next_discovery = now + cfg.sync.schema_refresh_seconds
        if now >= self._next_requests:
            self._check_requests()
            self._next_requests = now + REQUEST_POLL_SECONDS
        self._schedule(now)
        self._collect(timeout=collect_timeout)
        if time.monotonic() >= self._next_flush:
            self._flush()
            self._next_flush = time.monotonic() + cfg.sync.state_flush_seconds
        if time.monotonic() >= self._next_heartbeat:
            self._heartbeat()
            self._next_heartbeat = time.monotonic() + HEARTBEAT_SECONDS

    def _new_pool(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self.cfg.sync.workers,
            mp_context=self._mp_ctx,
            initializer=init_worker,
            initargs=(self.cfg, self.stop_event, self.log_level),
        )

    def _shutdown(self) -> None:
        self.stop_event.set()
        deadline = time.monotonic() + self.cfg.sync.shutdown_timeout
        while self.futures and time.monotonic() < deadline:
            self._collect(timeout=1.0)
        try:
            self._flush()
        except Exception:  # noqa: BLE001
            log.exception("final state flush failed")
        procs = list(getattr(self.pool, "_processes", {}).values())
        self.pool.shutdown(wait=False, cancel_futures=True)
        for p in procs:
            if p.is_alive():
                p.terminate()
        self.src.close()
        self.sink.close()
        log.info("stopped")

    # --- table set ---------------------------------------------------------------

    def _refresh_tables(self) -> None:
        cfg = self.cfg
        desired: dict[str, TableConfig] = {}
        if cfg.sync.auto_discover:
            for key in self.src.discover_tables():
                desired[key.lower()] = TableConfig(source=key)
        for t in cfg.tables:
            desired[t.key.lower()] = t
        desired = {
            k: t for k, t in desired.items()
            if owns_table(t.key, cfg.sync.shard_index, cfg.sync.shard_count)
        }
        states = self.store.load_all()
        states_ci = {k.lower(): v for k, v in states.items()}
        for lk, tcfg in desired.items():
            if tcfg.key not in self.tables:
                st = states_ci.get(lk) or TableState(tcfg.key)
                st.table_key = tcfg.key
                self.tables[tcfg.key] = TableRuntime(tcfg, st)
                log.info("tracking %s (phase=%s)", tcfg.key, st.phase)
        for key in list(self.tables):
            if key.lower() not in desired and not self.tables[key].running:
                log.info("no longer tracking %s", key)
                del self.tables[key]

    def _check_requests(self) -> None:
        try:
            reqs = self.store.latest_requests("resync")
        except Exception:  # noqa: BLE001
            log.exception("could not read sync requests")
            return
        by_lower = {k.lower(): rt for k, rt in self.tables.items()}
        for key, ts in reqs.items():
            rt = by_lower.get(key.lower())
            if rt and ts > rt.state.resync_handled_at:
                log.warning("%s: resync requested", rt.key)
                rt.state.resync_handled_at = ts
                rt.state.phase = PHASE_RESYNC
                rt.blocked_until = 0.0
                rt.failures = 0
                self._mark(rt)

    # --- scheduling ----------------------------------------------------------------

    def _free_slots(self) -> int:
        return self.cfg.sync.workers - len(self.futures)

    def _snapshot_running(self) -> int:
        return sum(1 for _, tid in self.futures.values() if tid[0] == "snap")

    def _schedule(self, now: float) -> None:
        for rt in sorted(self.tables.values(), key=lambda r: r.next_run):
            if self._free_slots() <= 0:
                return
            if rt.blocked_until > now:
                continue
            try:
                self._schedule_table(rt, now)
            except Exception as exc:  # noqa: BLE001
                self._fail(rt, "plan", exc)

    def _schedule_table(self, rt: TableRuntime, now: float) -> None:
        st = rt.state
        if st.phase in (PHASE_NEW, PHASE_RESYNC):
            if not rt.running:  # let in-flight work drain before rebuilding
                self._plan_snapshot(rt)
            return

        if st.phase == PHASE_SNAPSHOT:
            if rt.progress is None and not self._resume_snapshot(rt):
                return
            assert rt.progress is not None
            for i, (lo, hi) in enumerate(st.partitions):
                if self._free_slots() <= 0 or self._snapshot_running() >= self.cfg.sync.snapshot_worker_cap:
                    break
                tid = ("snap", i)
                if rt.progress[i].done or tid in rt.running:
                    continue
                self._submit(rt, tid, run_snapshot_partition, rt.tcfg, st.snapshot_id, st.snapshot_version, i, lo, hi)
            if not rt.running and all(p.done for p in rt.progress.values()):
                self._finalize(rt)
            return

        if st.phase == PHASE_CDC and not rt.running and now >= rt.next_run:
            self._submit(rt, ("cdc",), run_cdc, rt.tcfg, st.last_version)

    def _submit(self, rt: TableRuntime, tid: tuple, fn, *args) -> None:
        try:
            fut = self.pool.submit(fn, *args)
        except BrokenProcessPool:
            self._rebuild_pool()
            fut = self.pool.submit(fn, *args)
        self.futures[fut] = (rt.key, tid)
        rt.running.add(tid)

    def _rebuild_pool(self) -> None:
        log.error("worker pool broke (worker crashed / OOM?); rebuilding")
        try:
            self.pool.shutdown(wait=False, cancel_futures=True)
        except Exception:  # noqa: BLE001
            pass
        self.pool = self._new_pool()

    # --- snapshot planning & cut-over -------------------------------------------------

    def _plan_snapshot(self, rt: TableRuntime) -> None:
        cfg, tcfg, st = self.cfg, rt.tcfg, rt.state
        db, repl = cfg.target.database, cfg.target.replicated
        ts = self.src.load_schema(tcfg.schema_name, tcfg.table_name, tcfg.exclude_columns)

        self.sink.command(create_table_sql(db, tcfg.target_table, ts, tcfg, repl))
        if cfg.target.create_views:
            self.sink.command(view_sql(db, tcfg.target_table + cfg.target.view_suffix, tcfg.target_table))

        min_valid = self.src.min_valid_version(ts)
        if min_valid is None:
            raise ChangeTrackingDisabled(f"Change Tracking is not enabled on {ts.key}")
        # Recorded BEFORE reading any rows: every change committed after this
        # point is replayed by CDC on top of the snapshot.
        version = max(self.src.current_version(), min_valid)

        snapshot_id = uuid.uuid4().hex
        self.sink.command(f"DROP TABLE IF EXISTS {qch(db)}.{qch(tcfg.staging_table)} SYNC")
        self.sink.command(
            create_table_sql(db, tcfg.staging_table, ts, tcfg, repl, comment=f"ch_sync:{snapshot_id}")
        )

        parts: list = [(None, None)]
        n = tcfg.snapshot_partitions or cfg.sync.snapshot_partitions
        if ts.single_int_pk() and n > 1:
            lo, hi = self.src.pk_bounds(ts)
            parts = plan_partitions(lo, hi, self.src.approx_rows(ts), n, cfg.sync.min_rows_per_partition)

        st.phase = PHASE_SNAPSHOT
        st.snapshot_id = snapshot_id
        st.snapshot_version = version
        st.partitions = [list(p) for p in parts]
        st.last_error = ""
        rt.progress = {i: PartitionProgress(i) for i in range(len(parts))}
        self.store.save([st])
        self.dirty.pop(rt.key, None)
        log.info("%s: snapshot %s planned at CT version %d with %d partition(s)",
                 rt.key, snapshot_id[:8], version, len(parts))

    def _resume_snapshot(self, rt: TableRuntime) -> bool:
        """After a restart: reload partition progress, or detect a half-finished cut-over."""
        db, tcfg, st = self.cfg.target.database, rt.tcfg, rt.state
        tag = f"ch_sync:{st.snapshot_id}"
        if self.sink.table_comment(db, tcfg.target_table) == tag:
            self._finalize(rt)  # exchange already happened; finish bookkeeping
            return False
        if self.sink.table_comment(db, tcfg.staging_table) != tag:
            log.warning("%s: staging table missing or stale, replanning snapshot", rt.key)
            st.phase = PHASE_NEW
            return False
        saved = self.store.load_progress(rt.key, st.snapshot_id)
        rt.progress = {i: saved.get(i) or PartitionProgress(i) for i in range(len(st.partitions))}
        done = sum(p.done for p in rt.progress.values())
        log.info("%s: resuming snapshot %s (%d/%d partitions done)",
                 rt.key, st.snapshot_id[:8], done, len(rt.progress))
        return True

    def _finalize(self, rt: TableRuntime) -> None:
        db, tcfg, st = self.cfg.target.database, rt.tcfg, rt.state
        tag = f"ch_sync:{st.snapshot_id}"
        # Idempotent: the comment tells us whether the swap already happened.
        if self.sink.table_comment(db, tcfg.target_table) != tag:
            self.sink.command(
                f"EXCHANGE TABLES {qch(db)}.{qch(tcfg.staging_table)} AND {qch(db)}.{qch(tcfg.target_table)}"
            )
        self.sink.command(f"DROP TABLE IF EXISTS {qch(db)}.{qch(tcfg.staging_table)} SYNC")
        if self.cfg.target.create_views:
            self.sink.command(view_sql(db, tcfg.target_table + self.cfg.target.view_suffix, tcfg.target_table))

        st.phase = PHASE_CDC
        st.last_version = st.snapshot_version
        st.partitions = []
        st.last_success = time.time()
        st.last_error = ""
        self.store.save([st])
        self.dirty.pop(rt.key, None)
        rt.progress = None
        rt.next_run = 0.0
        log.info("%s: snapshot complete, switched to CDC from version %d", rt.key, st.last_version)
        self.log_buf.append(StateStore.log_entry(rt.key, "finalize", "ok", 0, 0.0))

    # --- results -----------------------------------------------------------------

    def _collect(self, timeout: float) -> None:
        if not self.futures:
            time.sleep(timeout)
            return
        done, _ = wait(list(self.futures), timeout=timeout, return_when=FIRST_COMPLETED)
        broken = False
        for fut in done:
            key, tid = self.futures.pop(fut)
            rt = self.tables.get(key)
            if rt is None:
                continue
            rt.running.discard(tid)
            try:
                res: TaskResult = fut.result()
            except BrokenProcessPool as exc:
                broken = True
                self._fail(rt, tid[0], exc)
                continue
            except Exception as exc:  # noqa: BLE001
                self._fail(rt, tid[0], exc)
                continue
            self._on_result(rt, res)
        if broken:
            self._rebuild_pool()

    def _on_result(self, rt: TableRuntime, res: TaskResult) -> None:
        st = rt.state
        rt.failures = 0
        self.rows_since_heartbeat += res.rows
        if res.kind == "snapshot":
            if res.status == "done" and rt.progress is not None and res.part in rt.progress:
                rt.progress[res.part].done = True
            st.rows_synced += res.rows
            self._mark(rt)
            self.log_buf.append(StateStore.log_entry(rt.key, f"snapshot[{res.part}]", res.status, res.rows, res.duration))
            if res.status == "done":
                log.info("%s: partition %s loaded (%d rows in %.1fs)", rt.key, res.part, res.rows, res.duration)
            return

        if res.status == "resync":
            log.warning("%s: %s -> full resync", rt.key, res.message)
            st.phase = PHASE_RESYNC
            st.last_error = res.message
            self._mark(rt)
            self.log_buf.append(StateStore.log_entry(rt.key, "cdc", "resync", 0, res.duration, res.message))
            return

        changed = res.version != st.last_version or res.rows > 0 or st.last_error
        st.last_version = int(res.version or st.last_version)
        st.rows_synced += res.rows
        st.last_success = time.time()
        st.last_error = ""
        # Persist at least once a minute even when idle, so status shows freshness.
        if changed or time.time() - rt.last_persist > 60:
            self._mark(rt)
            rt.last_persist = time.time()
        if res.rows:
            self.log_buf.append(StateStore.log_entry(rt.key, "cdc", "ok", res.rows, res.duration))
            log.debug("%s: %d changes -> version %d (%.2fs)", rt.key, res.rows, st.last_version, res.duration)
        interval = rt.tcfg.poll_interval or self.cfg.sync.poll_interval
        rt.next_run = time.monotonic() + interval

    def _fail(self, rt: TableRuntime, task: str, exc: BaseException) -> None:
        rt.failures += 1
        delay = min(self.cfg.sync.max_backoff_seconds, 2 ** min(rt.failures, 16))
        rt.blocked_until = time.monotonic() + delay
        rt.state.last_error = f"{type(exc).__name__}: {exc}"
        self._mark(rt)
        self.log_buf.append(StateStore.log_entry(rt.key, task, "error", 0, 0.0, rt.state.last_error))
        log.error("%s: %s failed (%s); retry in %.0fs", rt.key, task, rt.state.last_error, delay)

    # --- persistence ---------------------------------------------------------------

    def _mark(self, rt: TableRuntime) -> None:
        self.dirty[rt.key] = rt.state

    def _flush(self) -> None:
        try:
            if self.dirty:
                self.store.save(list(self.dirty.values()))
                self.dirty.clear()
            if self.log_buf:
                self.store.write_log(self.log_buf)
                self.log_buf = []
        except Exception:  # noqa: BLE001
            log.exception("state flush failed; will retry")

    def _heartbeat(self) -> None:
        phases: dict[str, int] = {}
        errors = 0
        for rt in self.tables.values():
            phases[rt.state.phase] = phases.get(rt.state.phase, 0) + 1
            errors += bool(rt.state.last_error)
        log.info("heartbeat: tables=%s running=%d rows/min=%d erroring=%d",
                 phases, len(self.futures), self.rows_since_heartbeat, errors)
        self.rows_since_heartbeat = 0
