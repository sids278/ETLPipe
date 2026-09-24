from __future__ import annotations

from loadtest.generators.base import RowGenerator


def get_generator(name: str, **kwargs) -> RowGenerator: ...
