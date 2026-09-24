from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LoadTestSettings:
    ch_sync_config_path: str
    row_counts: dict[str, int]
    generator: str
    mockaroo_api_key: str | None
    insert_batch_size: int
    seed: int
    sync_timeout_s: float
    validation_sample_size: int
    output_dir: str

    @classmethod
    def load(cls, path: str | None = None) -> LoadTestSettings: ...

    def ch_sync_config(self): ...
