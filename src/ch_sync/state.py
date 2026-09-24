"""Sync checkpoints, snapshot progress, control requests and run log.

All of it lives in ClickHouse (ReplacingMergeTree keyed by table), so the
pipeline needs no extra state database. Writes are batched by the supervisor
to keep part counts low even with hundreds of tables.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .ddl import meta_ddl, qch
from .sink import ClickHouseSink

PHASE_NEW = "new"
PHASE_SNAPSHOT = "snapshot"
PHASE_CDC = "cdc"
PHASE_RESYNC = "resync"

_STATE_COLS = [
    "table_key", "phase", "last_version", "snapshot_id", "snapshot_version", "partitions",
    "rows_synced", "last_success", "last_error", "resync_handled_at", "updated_at",
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class TableState:
    table_key: str
    phase: str = PHASE_NEW
    last_version: int = 0
    snapshot_id: str = ""
    snapshot_version: int = 0
    partitions: list = field(default_factory=list)  # [[lo, hi], ...]
    rows_synced: int = 0
    last_success: float = 0.0
    last_error: str = ""
    resync_handled_at: float = 0.0

    def row(self) -> list:
        return [
            self.table_key, self.phase, self.last_version, self.snapshot_id, self.snapshot_version,
            json.dumps(self.partitions), self.rows_synced, self.last_success, self.last_error[:4000],
            self.resync_handled_at, _now(),
        ]


@dataclass
class PartitionProgress:
    part: int
    last_key: str = ""
    rows: int = 0
    done: bool = False


class StateStore:
    def __init__(self, sink: ClickHouseSink, meta_db: str, replicated: bool = False):
        self.sink = sink
        self.db = meta_db
        self.replicated = replicated

    def bootstrap(self) -> None:
        for stmt in meta_ddl(self.db, self.replicated):
            self.sink.command(stmt)

    # --- table state ----------------------------------------------------------

    def load_all(self) -> dict[str, TableState]:
        rows = self.sink.rows(f"SELECT {', '.join(_STATE_COLS[:-1])} FROM {qch(self.db)}.sync_state FINAL")
        out = {}
        for r in rows:
            st = TableState(
                table_key=r[0], phase=r[1], last_version=int(r[2]), snapshot_id=r[3],
                snapshot_version=int(r[4]), partitions=json.loads(r[5] or "[]"), rows_synced=int(r[6]),
                last_success=float(r[7]), last_error=r[8], resync_handled_at=float(r[9]),
            )
            out[st.table_key] = st
        return out

    def save(self, states: list[TableState]) -> None:
        if states:
            self.sink.insert(self.db, "sync_state", [s.row() for s in states], _STATE_COLS)

    # --- snapshot progress ---------------------------------------------------

    def load_progress(self, table_key: str, snapshot_id: str) -> dict[int, PartitionProgress]:
        rows = self.sink.rows(
            f"SELECT part, last_key, rows, done FROM {qch(self.db)}.snapshot_progress FINAL "
            "WHERE table_key = {k:String} AND snapshot_id = {s:String}",
            {"k": table_key, "s": snapshot_id},
        )
        return {int(p): PartitionProgress(int(p), lk, int(n), bool(d)) for p, lk, n, d in rows}

    def save_progress(self, table_key: str, snapshot_id: str, p: PartitionProgress) -> None:
        self.sink.insert(
            self.db,
            "snapshot_progress",
            [[table_key, snapshot_id, p.part, p.last_key, p.rows, int(p.done), _now()]],
            ["table_key", "snapshot_id", "part", "last_key", "rows", "done", "updated_at"],
        )

    # --- control requests ------------------------------------------------------

    def request(self, table_key: str, action: str = "resync") -> None:
        self.sink.insert(
            self.db, "sync_requests", [[table_key, action, time.time()]], ["table_key", "action", "requested_at"]
        )

    def latest_requests(self, action: str = "resync") -> dict[str, float]:
        rows = self.sink.rows(
            f"SELECT table_key, max(requested_at) FROM {qch(self.db)}.sync_requests "
            "WHERE action = {a:String} GROUP BY table_key",
            {"a": action},
        )
        return {k: float(v) for k, v in rows}

    # --- run log ----------------------------------------------------------------

    def write_log(self, entries: list[list]) -> None:
        if entries:
            self.sink.insert(
                self.db, "sync_log", entries,
                ["ts", "table_key", "task", "status", "rows", "duration_ms", "message"],
            )

    @staticmethod
    def log_entry(table_key: str, task: str, status: str, rows: int, duration_s: float, message: str = "") -> list:
        return [_now(), table_key, task, status, int(rows), int(duration_s * 1000), message[:4000]]
