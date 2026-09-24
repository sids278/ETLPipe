from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StageTiming:
    stage: str
    seconds: float
    rows: int


@dataclass
class TableSyncStats:
    table: str
    source_rows: int
    target_rows: int
    snapshot_seconds: float
    rows_per_sec: float
    partitions: int


@dataclass
class RunMetrics:
    run_id: str
    started_at: str
    stages: list[StageTiming]
    tables: list[TableSyncStats]
    ch_sync_settings: dict

    def stage(self, name: str): ...

    def to_dict(self) -> dict: ...
