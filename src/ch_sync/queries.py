"""SQL Server query builders and helpers (pure functions, no I/O)."""
from __future__ import annotations

import base64
import json
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Sequence

from .model import TableSchema, qi

META_COLUMNS = ["_version", "_is_deleted"]

CT_TABLES_SQL = """
SELECT s.name, t.name
FROM sys.change_tracking_tables AS ctt
JOIN sys.tables AS t ON t.object_id = ctt.object_id
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
ORDER BY s.name, t.name
"""

SCHEMA_SQL = """
SELECT c.name,
       CASE WHEN ty.is_user_defined = 1 AND ty.is_assembly_type = 0
            THEN TYPE_NAME(c.system_type_id) ELSE ty.name END AS type_name,
       c.precision, c.scale, c.is_nullable, pk.key_ordinal
FROM sys.columns AS c
JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
LEFT JOIN (
    SELECT ic.column_id, ic.key_ordinal
    FROM sys.indexes AS i
    JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 1
) AS pk ON pk.column_id = c.column_id
WHERE c.object_id = OBJECT_ID(?) AND c.is_column_set = 0
ORDER BY c.column_id
"""

APPROX_ROWS_SQL = """
SELECT COALESCE(SUM(p.rows), 0) FROM sys.partitions AS p
WHERE p.object_id = OBJECT_ID(?) AND p.index_id IN (0, 1)
"""


def pk_bounds_sql(ts: TableSchema) -> str:
    col = qi(ts.pk[0].name)
    return f"SELECT MIN({col}), MAX({col}) FROM {ts.sql_name}"


def keyset_predicate(cols: Sequence[str], values: Sequence[Any]) -> tuple[str, list[Any]]:
    """Row-value comparison (c1, c2, ...) > (v1, v2, ...) expanded for T-SQL."""
    if len(cols) != len(values):
        raise ValueError("keyset columns/values length mismatch")
    ors: list[str] = []
    params: list[Any] = []
    for i in range(len(cols)):
        terms = [f"{cols[j]} = ?" for j in range(i)] + [f"{cols[i]} > ?"]
        params.extend(values[: i + 1])
        ors.append("(" + " AND ".join(terms) + ")")
    return "(" + " OR ".join(ors) + ")", params


def snapshot_columns(ts: TableSchema) -> list[str]:
    """Columns inserted into ClickHouse by snapshot and change queries."""
    return ts.column_names + META_COLUMNS


def snapshot_query(
    ts: TableSchema,
    version: int,
    lo: int | None = None,
    hi: int | None = None,
    after: Sequence[Any] | None = None,
) -> tuple[str, list[Any]]:
    """Stream one PK range of the table in PK order.

    Output = mapped columns, _version, _is_deleted, then the raw PK values as
    __pk0.. (used only for resumable checkpoints, not inserted).
    """
    pk = ts.pk
    select = [c.select_expr("t") for c in ts.columns]
    select.append(f"CAST({int(version)} AS bigint) AS [_version]")
    select.append("CAST(0 AS tinyint) AS [_is_deleted]")
    select.extend(f"t.{qi(c.name)} AS [__pk{i}]" for i, c in enumerate(pk))

    where: list[str] = []
    params: list[Any] = []
    if lo is not None:
        where.append(f"t.{qi(pk[0].name)} >= ?")
        params.append(lo)
    if hi is not None:
        where.append(f"t.{qi(pk[0].name)} < ?")
        params.append(hi)
    if after:
        pred, p = keyset_predicate([f"t.{qi(c.name)}" for c in pk], after)
        where.append(pred)
        params.extend(p)

    sql = f"SELECT {', '.join(select)} FROM {ts.sql_name} AS t"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY " + ", ".join(f"t.{qi(c.name)}" for c in pk)
    return sql, params


def changes_query(ts: TableSchema, last_version: int, current_version: int) -> str:
    """Net changes since last_version, joined to the current row.

    A missing base row means the row is gone -> tombstone (_is_deleted = 1).
    This is also correct when CT reports U/I but the row was deleted later.
    """
    pk = ts.pk
    select = []
    for c in ts.columns:
        select.append(c.select_expr("ct") if c.is_pk else c.select_expr("t", tombstone_safe=True))
    select.append("CAST(ct.SYS_CHANGE_VERSION AS bigint) AS [_version]")
    select.append(
        f"CAST(CASE WHEN t.{qi(pk[0].name)} IS NULL THEN 1 ELSE 0 END AS tinyint) AS [_is_deleted]"
    )
    join = " AND ".join(f"t.{qi(c.name)} = ct.{qi(c.name)}" for c in pk)
    return (
        f"SELECT {', '.join(select)} "
        f"FROM CHANGETABLE(CHANGES {ts.sql_name}, {int(last_version)}) AS ct "
        f"LEFT JOIN {ts.sql_name} AS t ON {join} "
        f"WHERE ct.SYS_CHANGE_VERSION <= {int(current_version)}"
    )


def plan_partitions(
    lo: int | None, hi: int | None, approx_rows: int, max_parts: int, min_rows_per_part: int
) -> list[tuple[int | None, int | None]]:
    """Split an integer PK space into contiguous ranges.

    Outer bounds are open (None) so rows inserted beyond MIN/MAX during the
    load are still covered.
    """
    if lo is None or hi is None or max_parts <= 1:
        return [(None, None)]
    by_rows = approx_rows // max(1, min_rows_per_part)
    n = max(1, min(max_parts, by_rows, hi - lo + 1))
    if n == 1:
        return [(None, None)]
    span = hi - lo + 1
    bounds = [lo + (span * i) // n for i in range(1, n)]
    parts: list[tuple[int | None, int | None]] = []
    prev: int | None = None
    for b in bounds:
        parts.append((prev, b))
        prev = b
    parts.append((prev, None))
    return parts


# --- checkpoint codec for raw PK values -------------------------------------

def _enc(v: Any) -> list:
    if v is None:
        return ["null", None]
    if isinstance(v, bool):
        return ["b", v]
    if isinstance(v, int):
        return ["i", v]
    if isinstance(v, float):
        return ["f", v]
    if isinstance(v, Decimal):
        return ["n", str(v)]
    if isinstance(v, datetime):
        return ["dt", v.isoformat()]
    if isinstance(v, date):
        return ["d", v.isoformat()]
    if isinstance(v, time):
        return ["t", v.isoformat()]
    if isinstance(v, (bytes, bytearray, memoryview)):
        return ["x", base64.b64encode(bytes(v)).decode()]
    if isinstance(v, uuid.UUID):
        return ["s", str(v)]
    return ["s", str(v)]


def _dec(item: list) -> Any:
    tag, v = item
    return {
        "null": lambda x: None,
        "b": bool,
        "i": int,
        "f": float,
        "n": Decimal,
        "dt": datetime.fromisoformat,
        "d": date.fromisoformat,
        "t": time.fromisoformat,
        "x": base64.b64decode,
        "s": str,
    }[tag](v)


def encode_key(values: Sequence[Any]) -> str:
    return json.dumps([_enc(v) for v in values])


def decode_key(text: str) -> list[Any] | None:
    if not text:
        return None
    return [_dec(i) for i in json.loads(text)]
