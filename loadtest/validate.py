from __future__ import annotations

from dataclasses import dataclass

from loadtest.schema import TableSpec


@dataclass
class CheckResult:
    check: str
    passed: bool
    source_value: object
    target_value: object
    detail: str


@dataclass
class TableValidation:
    table: str
    checks: list[CheckResult]
    seconds: float

    @property
    def passed(self) -> bool: ...


def check_row_count(src, dst, table: TableSpec) -> CheckResult: ...


def check_pk_bounds(src, dst, table: TableSpec) -> CheckResult: ...


def check_aggregates(src, dst, table: TableSpec) -> CheckResult: ...


def check_hash_buckets(src, dst, table: TableSpec, buckets: int) -> CheckResult: ...


def check_sample(src, dst, table: TableSpec, sample_size: int) -> CheckResult: ...


def normalize_value(value, sql_type: str): ...


def validate_table(src, dst, table: TableSpec, sample_size: int) -> TableValidation: ...


def validate_all(src, dst, tables: list[TableSpec], sample_size: int) -> list[TableValidation]: ...
