"""SQL Server access: schema introspection, snapshot streaming, change reading."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

from .config import SourceConfig
from .model import Column, TableSchema
from .queries import (
    APPROX_ROWS_SQL,
    CT_TABLES_SQL,
    SCHEMA_SQL,
    changes_query,
    pk_bounds_sql,
)
from .typemap import map_column

log = logging.getLogger(__name__)


class SourceError(RuntimeError):
    pass


class ResyncRequired(SourceError):
    """The CT checkpoint is older than the table's retention window."""


class ChangeTrackingDisabled(SourceError):
    pass


class SqlServerSource:
    def __init__(self, cfg: SourceConfig):
        import pyodbc  # imported lazily so pure modules/tests don't need the ODBC driver

        self.cfg = cfg
        self.conn = pyodbc.connect(cfg.connection_string(), autocommit=True, timeout=cfg.login_timeout)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass

    # --- small helpers ------------------------------------------------------

    def _scalar(self, sql: str, *params: Any) -> Any:
        cur = self.conn.cursor()
        try:
            row = cur.execute(sql, *params).fetchone()
            return row[0] if row else None
        finally:
            cur.close()

    def current_version(self) -> int:
        return int(self._scalar("SELECT CHANGE_TRACKING_CURRENT_VERSION()") or 0)

    def min_valid_version(self, ts: TableSchema) -> int | None:
        v = self._scalar("SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?))", ts.sql_name)
        return None if v is None else int(v)

    def snapshot_isolation_enabled(self) -> bool:
        return bool(self._scalar("SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()"))

    def discover_tables(self) -> list[str]:
        cur = self.conn.cursor()
        try:
            return [f"{s}.{t}" for s, t in cur.execute(CT_TABLES_SQL).fetchall()]
        finally:
            cur.close()

    # --- schema -------------------------------------------------------------

    def load_schema(self, schema: str, name: str, exclude: Sequence[str] = ()) -> TableSchema:
        full = f"[{schema.replace(']', ']]')}].[{name.replace(']', ']]')}]"
        cur = self.conn.cursor()
        try:
            rows = cur.execute(SCHEMA_SQL, full, full).fetchall()
        finally:
            cur.close()
        if not rows:
            raise SourceError(f"table {schema}.{name} not found")
        excluded = {e.lower() for e in exclude}
        cols = []
        for col_name, type_name, precision, scale, nullable, key_ordinal in rows:
            if col_name.lower() in excluded:
                if key_ordinal is not None:
                    raise SourceError(f"{schema}.{name}: cannot exclude primary key column {col_name}")
                continue
            cols.append(
                Column(
                    name=col_name,
                    sql_type=type_name,
                    nullable=bool(nullable),
                    pk_ordinal=key_ordinal,
                    mapping=map_column(type_name, int(precision or 0), int(scale or 0)),
                )
            )
        ts = TableSchema(schema, name, cols)
        if not ts.pk:
            raise SourceError(f"{ts.key} has no primary key; Change Tracking requires one")
        return ts

    def approx_rows(self, ts: TableSchema) -> int:
        return int(self._scalar(APPROX_ROWS_SQL, ts.sql_name) or 0)

    def pk_bounds(self, ts: TableSchema) -> tuple[Any, Any]:
        cur = self.conn.cursor()
        try:
            row = cur.execute(pk_bounds_sql(ts)).fetchone()
            return (row[0], row[1]) if row else (None, None)
        finally:
            cur.close()

    # --- streaming ----------------------------------------------------------

    def iter_query(self, sql: str, params: Sequence[Any], fetch_size: int) -> Iterator[list[tuple]]:
        """Yield row batches without materialising the whole result set."""
        cur = self.conn.cursor()
        cur.arraysize = fetch_size
        exhausted = False
        try:
            cur.execute(sql, *params)
            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    exhausted = True
                    return
                yield [tuple(r) for r in rows]
        finally:
            if not exhausted:
                try:
                    cur.cancel()
                except Exception:  # noqa: BLE001
                    pass
            cur.close()

    @contextmanager
    def change_session(self, ts: TableSchema, last_version: int) -> Iterator["ChangeSession"]:
        """Consistent read of changes since last_version (Microsoft's recommended pattern).

        Under SNAPSHOT isolation the retention check, current version and
        joined base rows all come from one point in time.
        """
        cur = self.conn.cursor()
        snapshot = self.cfg.use_snapshot_isolation
        try:
            if snapshot:
                cur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            cur.execute("BEGIN TRANSACTION")
            try:
                row = cur.execute(
                    "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(?))", ts.sql_name
                ).fetchone()
                min_valid = None if row is None or row[0] is None else int(row[0])
                if min_valid is None:
                    raise ChangeTrackingDisabled(f"Change Tracking is not enabled on {ts.key}")
                if last_version < min_valid:
                    raise ResyncRequired(
                        f"{ts.key}: checkpoint {last_version} < min valid version {min_valid}"
                    )
                current = int(cur.execute("SELECT CHANGE_TRACKING_CURRENT_VERSION()").fetchone()[0] or 0)
                yield ChangeSession(cur, ts, last_version, current, self.cfg.fetch_size)
                cur.execute("COMMIT")
            except BaseException:
                try:
                    cur.execute("IF @@TRANCOUNT > 0 ROLLBACK")
                except Exception:  # noqa: BLE001
                    pass
                raise
        finally:
            if snapshot:
                try:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                except Exception:  # noqa: BLE001
                    pass
            cur.close()


class ChangeSession:
    def __init__(self, cur, ts: TableSchema, last_version: int, current_version: int, fetch_size: int):
        self._cur = cur
        self.ts = ts
        self.last_version = last_version
        self.current_version = current_version
        self._fetch_size = fetch_size

    def batches(self) -> Iterator[list[tuple]]:
        if self.current_version <= self.last_version:
            return
        self._cur.arraysize = self._fetch_size
        self._cur.execute(changes_query(self.ts, self.last_version, self.current_version))
        while True:
            rows = self._cur.fetchmany(self._fetch_size)
            if not rows:
                return
            yield [tuple(r) for r in rows]
