"""ClickHouse access: DDL, batch inserts with retry, metadata lookups."""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Sequence

from .config import TargetConfig

log = logging.getLogger(__name__)


class ClickHouseSink:
    def __init__(self, cfg: TargetConfig):
        import clickhouse_connect
        from clickhouse_connect import common

        # Naive datetimes from SQL Server are UTC (datetimeoffset is converted
        # in SQL); interpret them with the column's 'UTC' zone rather than the
        # process-local zone.
        common.set_setting("naive_datetime_insert", "server")
        os.environ.setdefault("TZ", "UTC")

        self.cfg = cfg
        self.client = clickhouse_connect.get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            secure=cfg.secure,
            connect_timeout=cfg.connect_timeout,
            send_receive_timeout=cfg.send_receive_timeout,
            compress="lz4",
            settings=dict(cfg.settings),
        )

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:  # noqa: BLE001
            pass

    def command(self, sql: str) -> Any:
        return self.client.command(sql)

    def rows(self, sql: str, parameters: dict | None = None) -> list[tuple]:
        return list(self.client.query(sql, parameters=parameters).result_rows)

    def insert(
        self,
        database: str,
        table: str,
        rows: Sequence[Sequence[Any]],
        columns: Sequence[str],
        attempts: int = 5,
    ) -> None:
        """Insert one batch; retried because every write in this system is idempotent."""
        if not rows:
            return
        delay = 1.0
        for attempt in range(1, attempts + 1):
            try:
                self.client.insert(table=table, data=rows, column_names=list(columns), database=database)
                return
            except Exception as exc:  # noqa: BLE001
                if attempt == attempts:
                    raise
                log.warning("insert into %s.%s failed (attempt %d/%d): %s", database, table, attempt, attempts, exc)
                time.sleep(delay)
                delay = min(delay * 2, 30)

    def table_columns(self, database: str, table: str) -> dict[str, str]:
        rows = self.rows(
            "SELECT name, type FROM system.columns WHERE database = {db:String} AND table = {t:String} ORDER BY position",
            {"db": database, "t": table},
        )
        return {n: t for n, t in rows}

    def table_comment(self, database: str, table: str) -> str | None:
        rows = self.rows(
            "SELECT comment FROM system.tables WHERE database = {db:String} AND name = {t:String}",
            {"db": database, "t": table},
        )
        return rows[0][0] if rows else None

    def exists(self, database: str, table: str) -> bool:
        return self.table_comment(database, table) is not None
