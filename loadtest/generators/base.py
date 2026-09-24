from __future__ import annotations

from typing import Iterator, Protocol

from loadtest.schema import TableSpec


class RowGenerator(Protocol):
    name: str

    def generate(self, table: TableSpec, count: int, start_id: int = 1) -> Iterator[list[tuple]]: ...

    def fk_range(self, parent: str, count: int) -> None: ...
