from datetime import date, datetime, time
from decimal import Decimal

import pytest

from ch_sync.config import Config, ConfigError, TableConfig, expand_env
from ch_sync.ddl import add_column_sql, create_table_sql, order_by_columns
from ch_sync.model import Column, TableSchema
from ch_sync.queries import (
    changes_query, decode_key, encode_key, keyset_predicate, plan_partitions, snapshot_query,
)
from ch_sync.supervisor import owns_table
from ch_sync.typemap import map_column


def col(name, t, nullable=False, pk=None, p=0, s=0):
    return Column(name, t, nullable, pk, map_column(t, p, s))


def orders():
    return TableSchema("dbo", "Orders", [
        col("OrderId", "bigint", pk=1),
        col("CustomerId", "int"),
        col("Amount", "decimal", True, p=18, s=2),
        col("CreatedAt", "datetime2", s=7),
        col("Ref", "uniqueidentifier", True),
    ])


def composite():
    return TableSchema("sales", "Lines", [
        col("OrderId", "int", pk=1),
        col("LineNo", "smallint", pk=2),
        col("Sku", "nvarchar"),
    ])


# --- typemap ---------------------------------------------------------------

@pytest.mark.parametrize("t,p,s,expected", [
    ("bit", 1, 0, "Bool"), ("tinyint", 3, 0, "UInt8"), ("bigint", 19, 0, "Int64"),
    ("decimal", 18, 2, "Decimal(18, 2)"), ("money", 19, 4, "Decimal(19, 4)"),
    ("float", 53, 0, "Float64"), ("float", 24, 0, "Float32"), ("date", 10, 0, "Date32"),
    ("datetime", 23, 3, "DateTime64(3, 'UTC')"), ("datetime2", 27, 7, "DateTime64(7, 'UTC')"),
    ("datetimeoffset", 34, 7, "DateTime64(7, 'UTC')"), ("uniqueidentifier", 0, 0, "UUID"),
    ("nvarchar", 0, 0, "String"), ("varbinary", 0, 0, "String"), ("xml", 0, 0, "String"),
    ("geography", 0, 0, "String"), ("timestamp", 0, 0, "Int64"),
])
def test_type_mapping(t, p, s, expected):
    assert map_column(t, p, s).ch_type == expected


def test_datetimeoffset_converted_to_utc_and_clamped():
    expr = map_column("datetimeoffset", 34, 3).expr.format(col="t.[x]")
    assert "SWITCHOFFSET(t.[x], '+00:00')" in expr
    assert "1900-01-01" in expr and "2299-12-31" in expr


def test_nullable_wrapping_and_pk_never_nullable():
    c = col("A", "int", nullable=True)
    assert c.ch_type == "Nullable(Int32)"
    assert col("B", "int", nullable=True, pk=1).ch_type == "Int32"


def test_tombstone_safe_only_for_not_null():
    assert "ISNULL" in col("A", "int").select_expr("t", tombstone_safe=True)
    assert "ISNULL" not in col("A", "int", nullable=True).select_expr("t", tombstone_safe=True)


# --- queries -----------------------------------------------------------------

def test_keyset_predicate():
    sql, params = keyset_predicate(["a", "b", "c"], [1, 2, 3])
    assert sql == "((a > ?) OR (a = ? AND b > ?) OR (a = ? AND b = ? AND c > ?))"
    assert params == [1, 1, 2, 1, 2, 3]


def test_snapshot_query_range_and_resume():
    sql, params = snapshot_query(orders(), 42, lo=100, hi=200, after=[150])
    assert "t.[OrderId] >= ?" in sql and "t.[OrderId] < ?" in sql
    assert "CAST(42 AS bigint) AS [_version]" in sql
    assert "[__pk0]" in sql and sql.endswith("ORDER BY t.[OrderId]")
    assert params == [100, 200, 150]


def test_snapshot_query_composite_no_range():
    sql, params = snapshot_query(composite(), 7)
    assert "WHERE" not in sql
    assert sql.endswith("ORDER BY t.[OrderId], t.[LineNo]")
    assert params == []


def test_changes_query():
    sql = changes_query(composite(), 10, 20)
    assert "CHANGETABLE(CHANGES [sales].[Lines], 10) AS ct" in sql
    assert "t.[OrderId] = ct.[OrderId] AND t.[LineNo] = ct.[LineNo]" in sql
    assert "ct.[OrderId] AS [OrderId]" in sql  # PK comes from CT (survives deletes)
    assert "ISNULL(t.[Sku], '') AS [Sku]" in sql
    assert "WHERE ct.SYS_CHANGE_VERSION <= 20" in sql


def test_plan_partitions_cover_space():
    parts = plan_partitions(1, 1_000_000, 50_000_000, 8, 2_000_000)
    assert len(parts) == 8
    assert parts[0][0] is None and parts[-1][1] is None
    for (a, b), (c, d) in zip(parts, parts[1:]):
        assert b == c


def test_plan_partitions_small_tables_single():
    assert plan_partitions(1, 1000, 1000, 8, 2_000_000) == [(None, None)]
    assert plan_partitions(None, None, 0, 8, 1) == [(None, None)]
    assert len(plan_partitions(1, 3, 10**9, 8, 1)) == 3  # never more parts than keys


def test_key_codec_roundtrip():
    vals = [1, "abc", Decimal("1.50"), datetime(2024, 1, 2, 3, 4, 5, 6), date(2024, 1, 1),
            time(1, 2, 3), b"\x00\xff", True, None, 1.5]
    assert decode_key(encode_key(vals)) == vals
    assert decode_key("") is None


# --- ddl ---------------------------------------------------------------------

def test_create_table_sql():
    ddl = create_table_sql("analytics", "Orders", orders(), TableConfig(source="dbo.Orders"), False, "ch_sync:x")
    assert "ENGINE = ReplacingMergeTree(_version, _is_deleted)" in ddl
    assert "ORDER BY (`OrderId`)" in ddl
    assert "`Amount` Nullable(Decimal(18, 2))" in ddl
    assert ddl.strip().endswith("COMMENT 'ch_sync:x'")


def test_replicated_engine():
    ddl = create_table_sql("a", "O", orders(), TableConfig(source="O"), True)
    assert "ReplicatedReplacingMergeTree(_version, _is_deleted)" in ddl


def test_order_by_must_contain_pk():
    with pytest.raises(ConfigError):
        order_by_columns(composite(), TableConfig(source="sales.Lines", order_by=["OrderId"]))
    ok = order_by_columns(composite(), TableConfig(source="sales.Lines", order_by=["Sku", "OrderId", "LineNo"]))
    assert ok == ["Sku", "OrderId", "LineNo"]


def test_added_columns_are_nullable():
    assert add_column_sql("a", "t", col("X", "int")).endswith("`X` Nullable(Int32)")


# --- config ------------------------------------------------------------------

def test_table_config_naming():
    assert TableConfig(source="Orders").key == "dbo.Orders"
    assert TableConfig(source="dbo.Orders").target_table == "Orders"
    assert TableConfig(source="[sales].[Lines]").target_table == "sales_Lines"
    assert TableConfig(source="sales.Lines", target="lines").staging_table == "lines__staging"


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("PW", "s3cret")
    assert expand_env("a: ${PW}\nb: ${NOPE:-x}") == "a: s3cret\nb: x"
    with pytest.raises(ConfigError):
        expand_env("${DEFINITELY_NOT_SET_VAR}")


def test_config_validation():
    base = {"source": {"server": "s", "database": "d"}, "target": {"host": "h"}}
    with pytest.raises(ConfigError):
        Config.from_dict(base)  # no tables
    with pytest.raises(ConfigError):
        Config.from_dict({**base, "tables": [{"source": "a"}], "sync": {"bogus": 1}})
    cfg = Config.from_dict({**base, "tables": [{"source": "a"}], "sync": {"workers": 8}})
    assert cfg.sync.snapshot_worker_cap == 4
    assert "PWD" not in cfg.source.connection_string()  # integrated auth


def test_sharding_is_stable_and_complete():
    keys = [f"dbo.T{i}" for i in range(200)]
    owners = [[owns_table(k, i, 3) for i in range(3)] for k in keys]
    assert all(sum(o) == 1 for o in owners)


def test_env_expansion_in_values_only(monkeypatch, tmp_path):
    monkeypatch.setenv("PW", "p@ss: #{x}")
    f = tmp_path / "c.yaml"
    f.write_text("# ${NOT_SET_IN_COMMENT}\nsource: {server: s, database: d, password: '${PW}'}\n"
                 "target: {host: h}\ntables: [{source: a}]\n")
    assert Config.load(str(f)).source.password == "p@ss: #{x}"


def test_dotenv_does_not_override(monkeypatch, tmp_path):
    from ch_sync.__main__ import load_dotenv
    f = tmp_path / ".env"
    f.write_text("# c\nA_TEST=1\nB_TEST='x y'\nC_TEST=\"q\"\n")
    monkeypatch.setenv("A_TEST", "keep")
    monkeypatch.delenv("B_TEST", raising=False)
    monkeypatch.delenv("C_TEST", raising=False)
    load_dotenv(str(f))
    import os
    assert (os.environ["A_TEST"], os.environ["B_TEST"], os.environ["C_TEST"]) == ("keep", "x y", "q")
