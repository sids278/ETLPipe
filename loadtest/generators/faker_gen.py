from __future__ import annotations

from typing import Iterator

from loadtest.schema import TableSpec


class FakerGenerator:
    name: str

    def __init__(self, seed: int, batch_size: int, locale: str): ...

    def generate(self, table: TableSpec, count: int, start_id: int = 1) -> Iterator[list[tuple]]: ...

    def fk_range(self, parent: str, count: int) -> None: ...

    def value_for(self, column_hint: str, sql_type: str): ...
