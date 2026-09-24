from __future__ import annotations

from loadtest.metrics import RunMetrics
from loadtest.validate import TableValidation


def print_summary(metrics: RunMetrics, validations: list[TableValidation]) -> None: ...


def write_json(metrics: RunMetrics, validations: list[TableValidation], output_dir: str) -> str: ...


def write_markdown(metrics: RunMetrics, validations: list[TableValidation], output_dir: str) -> str: ...
