"""ClickHouse DDL builders (pure functions, no I/O)."""
from __future__ import annotations

from typing import Any

from .config import ConfigError, TableConfig
from .model import Column, TableSchema


def qch(name: str) -> str:
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def lit(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _setting(v: Any) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return lit(str(v))


def engine(kind: str, replicated: bool, args: str = "") -> str:
    return f"{'Replicated' if replicated else ''}{kind}({args})"


def order_by_columns(ts: TableSchema, tcfg: TableConfig) -> list[str]:
    pk = [c.name for c in ts.pk]
    order = tcfg.order_by or pk
    missing = [c for c in pk if c not in order]
    if missing:
        # ReplacingMergeTree deduplicates on the sorting key; without the full
        # PK, distinct source rows would collapse into one.
        raise ConfigError(f"{tcfg.key}: order_by must include all primary key columns, missing {missing}")
    unknown = [c for c in order if c not in ts.column_names]
    if unknown:
        raise ConfigError(f"{tcfg.key}: order_by references unknown columns {unknown}")
    return order


def create_table_sql(
    db: str, table: str, ts: TableSchema, tcfg: TableConfig, replicated: bool, comment: str = ""
) -> str:
    cols = [f"{qch(c.name)} {c.ch_type}" for c in ts.columns]
    cols += [
        "`_version` UInt64",
        "`_is_deleted` UInt8",
        "`_synced_at` DateTime64(3, 'UTC') DEFAULT now64(3)",
    ]
    settings = {"index_granularity": 8192, **tcfg.settings}
    parts = [
        f"CREATE TABLE IF NOT EXISTS {qch(db)}.{qch(table)}\n(\n    " + ",\n    ".join(cols) + "\n)",
        "ENGINE = " + engine("ReplacingMergeTree", replicated, "_version, _is_deleted"),
    ]
    if tcfg.partition_by:
        parts.append(f"PARTITION BY {tcfg.partition_by}")
    parts.append("ORDER BY (" + ", ".join(qch(c) for c in order_by_columns(ts, tcfg)) + ")")
    parts.append("SETTINGS " + ", ".join(f"{k} = {_setting(v)}" for k, v in settings.items()))
    if comment:
        parts.append(f"COMMENT {lit(comment)}")
    return "\n".join(parts)


def add_column_sql(db: str, table: str, col: Column) -> str:
    # Added columns are always Nullable: existing rows have no value for them.
    t = col.ch_type if col.ch_type.startswith("Nullable(") or col.is_pk else f"Nullable({col.ch_type})"
    return f"ALTER TABLE {qch(db)}.{qch(table)} ADD COLUMN IF NOT EXISTS {qch(col.name)} {t}"


def view_sql(db: str, view: str, table: str) -> str:
    return (
        f"CREATE OR REPLACE VIEW {qch(db)}.{qch(view)} AS "
        f"SELECT * EXCEPT (`_is_deleted`) FROM {qch(db)}.{qch(table)} FINAL WHERE `_is_deleted` = 0"
    )


def meta_ddl(meta_db: str, replicated: bool) -> list[str]:
    db = qch(meta_db)
    rmt = lambda ver: engine("ReplacingMergeTree", replicated, ver)  # noqa: E731
    mt = engine("MergeTree", replicated)
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",
        f"""CREATE TABLE IF NOT EXISTS {db}.sync_state
(
    table_key String,
    phase LowCardinality(String),
    last_version UInt64,
    snapshot_id String,
    snapshot_version UInt64,
    partitions String,
    rows_synced UInt64,
    last_success Float64,
    last_error String,
    resync_handled_at Float64,
    updated_at DateTime64(6, 'UTC')
)
ENGINE = {rmt('updated_at')}
ORDER BY table_key""",
        f"""CREATE TABLE IF NOT EXISTS {db}.snapshot_progress
(
    table_key String,
    snapshot_id String,
    part UInt32,
    last_key String,
    rows UInt64,
    done UInt8,
    updated_at DateTime64(6, 'UTC')
)
ENGINE = {rmt('updated_at')}
ORDER BY (table_key, snapshot_id, part)""",
        f"""CREATE TABLE IF NOT EXISTS {db}.sync_requests
(
    table_key String,
    action LowCardinality(String),
    requested_at Float64,
    created DateTime DEFAULT now()
)
ENGINE = {mt}
ORDER BY (table_key, requested_at)
TTL created + INTERVAL 30 DAY""",
        f"""CREATE TABLE IF NOT EXISTS {db}.sync_log
(
    ts DateTime64(3, 'UTC'),
    table_key String,
    task LowCardinality(String),
    status LowCardinality(String),
    rows UInt64,
    duration_ms UInt64,
    message String
)
ENGINE = {mt}
ORDER BY (table_key, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY""",
    ]
