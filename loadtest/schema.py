from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    nullable: bool
    generator_hint: str


@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    pk: str
    columns: list[ColumnSpec]
    depends_on: list[str]

    @property
    def key(self) -> str: ...


TABLES: list[TableSpec]


def load_order(tables: list[TableSpec]) -> list[TableSpec]: ...


def run_sql_file(conn, path: str) -> None: ...


def create_schema(conn, sql_dir: str, drop_existing: bool = False) -> None: ...


def truncate_all(conn) -> None: ...
