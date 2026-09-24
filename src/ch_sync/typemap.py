"""SQL Server -> ClickHouse type mapping.

Every conversion is pushed into the SELECT expression so rows coming out of
pyodbc can be handed to ClickHouse untouched. At scale this matters: there is
no per-value Python conversion loop on the hot path.

Each mapping carries:
  ch_type     ClickHouse type (without Nullable)
  expr        SQL Server expression template; {col} is the qualified column
  default_sql literal used for NOT NULL columns in delete tombstones
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

INT_TYPES = {"tinyint", "smallint", "int", "bigint"}
STRING_TYPES = {"char", "varchar", "nchar", "nvarchar", "sysname"}

# ClickHouse DateTime64 / Date32 supported range.
_DT_LO = "1900-01-01T00:00:00"
_DT_HI = "2299-12-31T23:59:59"
_D_LO = "1900-01-01"
_D_HI = "2299-12-31"


@dataclass(frozen=True)
class ColumnMapping:
    ch_type: str
    expr: str = "{col}"
    default_sql: str = "0"


def _clamp_datetime(inner: str, precision: int) -> str:
    t = f"datetime2({precision})"
    lo, hi = f"CAST('{_DT_LO}' AS {t})", f"CAST('{_DT_HI}' AS {t})"
    return f"CASE WHEN {inner} < {lo} THEN {lo} WHEN {inner} > {hi} THEN {hi} ELSE {inner} END"


def _clamp_date(inner: str) -> str:
    lo, hi = f"CAST('{_D_LO}' AS date)", f"CAST('{_D_HI}' AS date)"
    return f"CASE WHEN {inner} < {lo} THEN {lo} WHEN {inner} > {hi} THEN {hi} ELSE {inner} END"


def map_column(sql_type: str, precision: int = 0, scale: int = 0) -> ColumnMapping:
    t = sql_type.lower()

    if t == "bit":
        return ColumnMapping("Bool", default_sql="CAST(0 AS bit)")
    if t == "tinyint":
        return ColumnMapping("UInt8")
    if t == "smallint":
        return ColumnMapping("Int16")
    if t == "int":
        return ColumnMapping("Int32")
    if t == "bigint":
        return ColumnMapping("Int64")
    if t in ("decimal", "numeric"):
        return ColumnMapping(f"Decimal({precision}, {scale})")
    if t == "money":
        return ColumnMapping("Decimal(19, 4)")
    if t == "smallmoney":
        return ColumnMapping("Decimal(10, 4)")
    if t == "float":
        return ColumnMapping("Float64" if precision > 24 else "Float32")
    if t == "real":
        return ColumnMapping("Float32")

    if t == "date":
        return ColumnMapping("Date32", _clamp_date("{col}"), "CAST('1970-01-01' AS date)")
    if t in ("datetime", "smalldatetime"):
        return ColumnMapping(
            "DateTime64(3, 'UTC')",
            _clamp_datetime("CAST({col} AS datetime2(3))", 3),
            "CAST('1970-01-01' AS datetime2(3))",
        )
    if t == "datetime2":
        p = min(max(scale, 0), 7)
        return ColumnMapping(
            f"DateTime64({p}, 'UTC')", _clamp_datetime("{col}", p), f"CAST('1970-01-01' AS datetime2({p}))"
        )
    if t == "datetimeoffset":
        p = min(max(scale, 0), 7)
        inner = f"CONVERT(datetime2({p}), SWITCHOFFSET({{col}}, '+00:00'))"
        return ColumnMapping(
            f"DateTime64({p}, 'UTC')", _clamp_datetime(inner, p), f"CAST('1970-01-01' AS datetime2({p}))"
        )
    if t == "time":
        return ColumnMapping("String", "CONVERT(varchar(16), {col})", "''")

    if t in STRING_TYPES:
        return ColumnMapping("String", default_sql="''")
    if t == "text":
        return ColumnMapping("String", "CAST({col} AS varchar(max))", "''")
    if t == "ntext":
        return ColumnMapping("String", "CAST({col} AS nvarchar(max))", "''")
    if t in ("binary", "varbinary"):
        return ColumnMapping("String", default_sql="0x")
    if t == "image":
        return ColumnMapping("String", "CAST({col} AS varbinary(max))", "0x")

    if t == "uniqueidentifier":
        # Text form avoids SQL Server's mixed-endian GUID byte layout.
        return ColumnMapping("UUID", "CONVERT(char(36), {col})", "'00000000-0000-0000-0000-000000000000'")
    if t in ("timestamp", "rowversion"):
        return ColumnMapping("Int64", "CAST({col} AS bigint)")
    if t == "xml":
        return ColumnMapping("String", "CAST({col} AS nvarchar(max))", "''")
    if t == "sql_variant":
        return ColumnMapping("String", "CAST({col} AS nvarchar(4000))", "''")
    if t == "hierarchyid":
        return ColumnMapping("String", "{col}.ToString()", "''")
    if t in ("geography", "geometry"):
        return ColumnMapping("String", "{col}.STAsText()", "''")

    log.warning("unmapped SQL Server type %r; syncing as String", sql_type)
    return ColumnMapping("String", "CAST({col} AS nvarchar(max))", "''")
