from __future__ import annotations

from typing import Iterator

from loadtest.schema import TableSpec


class MockarooGenerator:
    name: str

    def __init__(self, api_key: str, schema_ids: dict[str, str] | None = None, seed: int = 42): ...

    def generate(self, table: TableSpec, count: int, start_id: int = 1) -> Iterator[list[tuple]]: ...

    def fk_range(self, parent: str, count: int) -> None: ...

    def fetch_sample(self, table: TableSpec, rows: int) -> list[tuple]: ...
