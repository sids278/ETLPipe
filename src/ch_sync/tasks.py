"""Work executed inside worker processes.

Each worker process keeps its own SQL Server + ClickHouse connections and a
TTL'd schema cache. Tasks are idempotent: any of them can crash at any point
and be re-run safely, because ClickHouse rows are deduplicated by
(primary key, _version) in ReplacingMergeTree.
"""
from __future__ import annotations

import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Any

from .config import Config, TableConfig
from .ddl import add_column_sql, view_sql
from .model import TableSchema
from .queries import decode_key, encode_key, snapshot_columns, snapshot_query
from .sink import ClickHouseSink
from .source import ResyncRequired, SqlServerSource
from .state import PartitionProgress, StateStore

log = logging.getLogger(__name__)

_ctx: dict[str, Any] = {}


# --- process setup -----------------------------------------------------------

def init_worker(cfg: Config, stop_event, log_level: str) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # supervisor owns shutdown
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    from .logutil import setup_logging

    setup_logging(log_level)
    _ctx.clear()
    _ctx.update(cfg=cfg, stop=stop_event, schemas={}, target_cols={})


def _source() -> SqlServerSource:
    if "source" not in _ctx:
        _ctx["source"] = SqlServerSource(_ctx["cfg"].source)
    return _ctx["source"]


def _sink() -> ClickHouseSink:
    if "sink" not in _ctx:
        _ctx["sink"] = ClickHouseSink(_ctx["cfg"].target)
    return _ctx["sink"]


def _reset_connections() -> None:
    for key in ("source", "sink"):
        conn = _ctx.pop(key, None)
        if conn is not None:
            conn.close()
    _ctx["schemas"] = {}
    _ctx["target_cols"] = {}


def _schema(tcfg: TableConfig) -> TableSchema:
    ttl = _ctx["cfg"].sync.schema_refresh_seconds
    cached = _ctx["schemas"].get(tcfg.key)
    if cached and time.monotonic() - cached[0] < ttl:
        return cached[1]
    ts = _source().load_schema(tcfg.schema_name, tcfg.table_name, tcfg.exclude_columns)
    _ctx["schemas"][tcfg.key] = (time.monotonic(), ts)
    return ts


# --- schema drift ------------------------------------------------------------

def ensure_columns(
    sink: ClickHouseSink, cfg: Config, ts: TableSchema, table: str, cache: dict | None = None,
    view_for: str | None = None,
) -> list[str]:
    """Add source columns missing in ClickHouse; recreate the view if needed."""
    db = cfg.target.database
    cache = cache if cache is not None else {}
    names = cache.get((db, table))
    if names is None or any(c.name not in names for c in ts.columns):
        names = set(sink.table_columns(db, table))
    added = []
    for col in ts.columns:
        if col.name not in names:
            sink.command(add_column_sql(db, table, col))
            names.add(col.name)
            added.append(col.name)
    cache[(db, table)] = names
    if added:
        log.info("%s: added columns %s to %s", ts.key, added, table)
        if view_for and cfg.target.create_views:
            sink.command(view_sql(db, view_for + cfg.target.view_suffix, view_for))
    return added


# --- results -----------------------------------------------------------------

@dataclass
class TaskResult:
    kind: str                 # "snapshot" | "cdc"
    table_key: str
    status: str               # "done" | "interrupted" | "ok" | "resync"
    rows: int = 0
    duration: float = 0.0
    part: int | None = None
    version: int | None = None
    message: str = ""


def _flush(sink, cfg: Config, table: str, buf: list, ncols: int) -> None:
    rows = buf if ncols is None else [r[:ncols] for r in buf]
    sink.insert(cfg.target.database, table, rows, _ctx["columns"])


# --- snapshot ----------------------------------------------------------------

def run_snapshot_partition(
    tcfg: TableConfig, snapshot_id: str, snapshot_version: int, part: int, lo: Any, hi: Any
) -> TaskResult:
    cfg: Config = _ctx["cfg"]
    stop = _ctx["stop"]
    started = time.monotonic()
    try:
        src, sink = _source(), _sink()
        ts = _schema(tcfg)
        ensure_columns(sink, cfg, ts, tcfg.staging_table, _ctx["target_cols"])
        store = StateStore(sink, cfg.target.meta_database, cfg.target.replicated)
        prog = store.load_progress(tcfg.key, snapshot_id).get(part) or PartitionProgress(part)
        if prog.done:
            return TaskResult("snapshot", tcfg.key, "done", 0, 0.0, part)

        cols = snapshot_columns(ts)
        ncols = len(cols)
        _ctx["columns"] = cols
        sql, params = snapshot_query(ts, snapshot_version, lo, hi, decode_key(prog.last_key))
        batch_size = cfg.sync.batch_size
        buf: list[tuple] = []
        rows_this_run = 0

        def checkpoint(done: bool) -> None:
            nonlocal buf, rows_this_run
            if buf:
                _flush(sink, cfg, tcfg.staging_table, buf, ncols)
                prog.last_key = encode_key(buf[-1][ncols:])
                prog.rows += len(buf)
                rows_this_run += len(buf)
                buf = []
            prog.done = done
            store.save_progress(tcfg.key, snapshot_id, prog)

        for batch in src.iter_query(sql, params, cfg.source.fetch_size):
            buf.extend(batch)
            if len(buf) >= batch_size:
                checkpoint(False)
                if stop.is_set():
                    return TaskResult("snapshot", tcfg.key, "interrupted", rows_this_run,
                                      time.monotonic() - started, part)
        checkpoint(True)
        return TaskResult("snapshot", tcfg.key, "done", rows_this_run, time.monotonic() - started, part)
    except Exception:
        _reset_connections()
        raise


# --- change tracking ---------------------------------------------------------

def run_cdc(tcfg: TableConfig, last_version: int) -> TaskResult:
    cfg: Config = _ctx["cfg"]
    started = time.monotonic()
    try:
        src, sink = _source(), _sink()
        ts = _schema(tcfg)
        ensure_columns(sink, cfg, ts, tcfg.target_table, _ctx["target_cols"], view_for=tcfg.target_table)
        _ctx["columns"] = snapshot_columns(ts)
        rows = 0
        try:
            with src.change_session(ts, last_version) as session:
                buf: list[tuple] = []
                for batch in session.batches():
                    buf.extend(batch)
                    if len(buf) >= cfg.sync.batch_size:
                        _flush(sink, cfg, tcfg.target_table, buf, None)
                        rows += len(buf)
                        buf = []
                if buf:
                    _flush(sink, cfg, tcfg.target_table, buf, None)
                    rows += len(buf)
                new_version = max(session.current_version, last_version)
        except ResyncRequired as exc:
            return TaskResult("cdc", tcfg.key, "resync", 0, time.monotonic() - started, message=str(exc))
        return TaskResult("cdc", tcfg.key, "ok", rows, time.monotonic() - started, version=new_version)
    except Exception:
        # Schema may have changed under us (dropped column etc.): reload next time.
        _reset_connections()
        raise
