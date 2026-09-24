from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from loadtest.generators.base import RowGenerator
from loadtest.schema import TableSpec


@dataclass
class LoadResult:
    table: str
    rows: int
    seconds: float

    @property
    def rows_per_sec(self) -> float: ...


def connect_mssql(connection_string: str): ...


def insert_batches(conn, table: TableSpec, batches: Iterable[list[tuple]]) -> LoadResult: ...


def seed_all(conn, generator: RowGenerator, row_counts: dict[str, int]) -> list[LoadResult]: ...
