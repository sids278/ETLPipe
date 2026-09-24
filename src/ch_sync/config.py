"""Configuration model and YAML loader.

Secrets are never stored in the file: use ${ENV_VAR} or ${ENV_VAR:-default}
placeholders in string values. They are expanded after parsing, so comments
are ignored and secrets containing YAML-special characters are safe.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, fields
from typing import Any

import yaml


class ConfigError(ValueError):
    pass


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def expand_env(text: str) -> str:
    def repl(m: re.Match) -> str:
        value = os.environ.get(m.group(1))
        if value is None:
            if m.group(2) is not None:
                return m.group(2)
            raise ConfigError(f"environment variable {m.group(1)} is not set")
        return value

    return _ENV_RE.sub(repl, text)


def expand_env_values(obj: Any) -> Any:
    if isinstance(obj, str):
        return expand_env(obj)
    if isinstance(obj, dict):
        return {k: expand_env_values(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_values(v) for v in obj]
    return obj


def _build(cls, data: dict | None, where: str):
    data = dict(data or {})
    known = {f.name for f in fields(cls)}
    unknown = set(data) - known
    if unknown:
        raise ConfigError(f"unknown keys in '{where}': {sorted(unknown)}")
    return cls(**data)


@dataclass
class SourceConfig:
    server: str
    database: str
    username: str | None = None
    password: str | None = None
    port: int = 1433
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: bool = True
    trust_server_certificate: bool = False
    # Read change batches inside a SNAPSHOT transaction (recommended; needs
    # ALLOW_SNAPSHOT_ISOLATION ON). Gives a consistent view of CT + base rows.
    use_snapshot_isolation: bool = True
    fetch_size: int = 10_000
    login_timeout: int = 30
    extra_params: dict[str, str] = field(default_factory=dict)

    def connection_string(self) -> str:
        parts: dict[str, str] = {
            "DRIVER": "{%s}" % self.driver,
            "SERVER": f"{self.server},{self.port}",
            "DATABASE": self.database,
            "Encrypt": "yes" if self.encrypt else "no",
            "TrustServerCertificate": "yes" if self.trust_server_certificate else "no",
            "APP": "ch-sync",
        }
        if self.username:
            parts["UID"] = self.username
            parts["PWD"] = "{" + (self.password or "").replace("}", "}}") + "}"
        else:
            parts["Trusted_Connection"] = "yes"
        parts.update({k: str(v) for k, v in self.extra_params.items()})
        return ";".join(f"{k}={v}" for k, v in parts.items())


@dataclass
class TargetConfig:
    host: str
    database: str = "analytics"
    meta_database: str = "ch_sync"
    port: int = 8123
    username: str = "default"
    password: str = ""
    secure: bool = False
    # Use ReplicatedReplacingMergeTree (for databases using the Replicated engine).
    replicated: bool = False
    # Create a `<table>_current` view (FINAL, deleted rows filtered out).
    create_views: bool = True
    view_suffix: str = "_current"
    connect_timeout: int = 10
    send_receive_timeout: int = 600
    settings: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableConfig:
    source: str                              # "schema.table" (schema defaults to dbo)
    target: str | None = None                # ClickHouse table name
    order_by: list[str] | None = None        # must contain every PK column
    partition_by: str | None = None          # raw ClickHouse expression, e.g. "toYYYYMM(created_at)"
    exclude_columns: list[str] = field(default_factory=list)
    snapshot_partitions: int | None = None   # override sync.snapshot_partitions
    poll_interval: float | None = None       # override sync.poll_interval
    settings: dict[str, Any] = field(default_factory=dict)  # extra MergeTree settings

    def __post_init__(self) -> None:
        schema, _, name = self.source.rpartition(".")
        self.schema_name = (schema or "dbo").strip("[]")
        self.table_name = name.strip("[]")
        if not self.table_name:
            raise ConfigError(f"invalid table source '{self.source}'")

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"

    @property
    def target_table(self) -> str:
        if self.target:
            return self.target
        if self.schema_name.lower() == "dbo":
            return self.table_name
        return f"{self.schema_name}_{self.table_name}"

    @property
    def staging_table(self) -> str:
        return f"{self.target_table}__staging"


@dataclass
class SyncConfig:
    workers: int = 4
    # Cap on workers used for initial loads so CDC always has capacity.
    max_snapshot_workers: int | None = None
    poll_interval: float = 15.0
    batch_size: int = 50_000
    snapshot_partitions: int = 8
    min_rows_per_partition: int = 2_000_000
    schema_refresh_seconds: float = 300.0
    # Sync every table that has Change Tracking enabled (tables: entries become overrides).
    auto_discover: bool = False
    shard_index: int = 0
    shard_count: int = 1
    max_backoff_seconds: float = 300.0
    state_flush_seconds: float = 2.0
    shutdown_timeout: float = 30.0

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ConfigError("sync.workers must be >= 1")
        if not 0 <= self.shard_index < self.shard_count:
            raise ConfigError("sync.shard_index must be in [0, shard_count)")
        if self.max_snapshot_workers is None:
            self.max_snapshot_workers = max(1, self.workers // 2)

    @property
    def snapshot_worker_cap(self) -> int:
        return max(1, min(self.workers, int(self.max_snapshot_workers or 1)))


@dataclass
class Config:
    source: SourceConfig
    target: TargetConfig
    sync: SyncConfig
    tables: list[TableConfig]

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        if not isinstance(data, dict):
            raise ConfigError("config root must be a mapping")
        unknown = set(data) - {"source", "target", "sync", "tables"}
        if unknown:
            raise ConfigError(f"unknown top-level keys: {sorted(unknown)}")
        tables = [_build(TableConfig, t, "tables[]") for t in (data.get("tables") or [])]
        keys = [t.key.lower() for t in tables]
        if len(keys) != len(set(keys)):
            raise ConfigError("duplicate table entries in 'tables'")
        cfg = cls(
            source=_build(SourceConfig, data.get("source"), "source"),
            target=_build(TargetConfig, data.get("target"), "target"),
            sync=_build(SyncConfig, data.get("sync"), "sync"),
            tables=tables,
        )
        if not cfg.tables and not cfg.sync.auto_discover:
            raise ConfigError("no tables configured and sync.auto_discover is false")
        return cfg

    @classmethod
    def load(cls, path: str) -> "Config":
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return cls.from_dict(expand_env_values(data))
