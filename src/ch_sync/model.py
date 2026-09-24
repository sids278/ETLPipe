"""Source table model shared by queries, DDL and tasks."""
from __future__ import annotations

from dataclasses import dataclass

from .typemap import INT_TYPES, ColumnMapping


def qi(name: str) -> str:
    """Quote a SQL Server identifier."""
    return "[" + name.replace("]", "]]") + "]"


@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool
    pk_ordinal: int | None
    mapping: ColumnMapping

    @property
    def is_pk(self) -> bool:
        return self.pk_ordinal is not None

    @property
    def ch_type(self) -> str:
        t = self.mapping.ch_type
        return f"Nullable({t})" if self.nullable and not self.is_pk else t

    def select_expr(self, alias: str, tombstone_safe: bool = False) -> str:
        expr = self.mapping.expr.format(col=f"{alias}.{qi(self.name)}")
        if tombstone_safe and not self.nullable:
            # Deleted rows come back with NULLs from the LEFT JOIN; NOT NULL
            # ClickHouse columns need a placeholder value.
            expr = f"ISNULL({expr}, {self.mapping.default_sql})"
        return f"{expr} AS {qi(self.name)}"


@dataclass
class TableSchema:
    schema: str
    name: str
    columns: list[Column]

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def sql_name(self) -> str:
        return f"{qi(self.schema)}.{qi(self.name)}"

    @property
    def pk(self) -> list[Column]:
        return sorted((c for c in self.columns if c.is_pk), key=lambda c: c.pk_ordinal or 0)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def single_int_pk(self) -> Column | None:
        pk = self.pk
        if len(pk) == 1 and pk[0].sql_type.lower() in INT_TYPES:
            return pk[0]
        return None
