"""CLI: ch-sync {run,check,status,resync} --config config.yaml"""
from __future__ import annotations

import argparse
import os
import sys
import time

from .config import Config, ConfigError, TableConfig
from .logutil import setup_logging


def _load(args) -> Config:
    cfg = Config.load(args.config)
    if getattr(args, "shard_index", None) is not None:
        cfg.sync.shard_index = args.shard_index
    if getattr(args, "shard_count", None) is not None:
        cfg.sync.shard_count = args.shard_count
    cfg.sync.__post_init__()
    return cfg


def cmd_run(args) -> int:
    from .supervisor import Supervisor

    Supervisor(_load(args), args.log_level).run()
    return 0


def cmd_check(args) -> int:
    """Validate connectivity and Change Tracking, and print the DDL that would be created."""
    from .ddl import create_table_sql
    from .sink import ClickHouseSink
    from .source import SqlServerSource

    cfg = _load(args)
    src = SqlServerSource(cfg.source)
    print(f"SQL Server OK: database={cfg.source.database}, CT current version={src.current_version()}")
    snap = src.snapshot_isolation_enabled()
    print(f"ALLOW_SNAPSHOT_ISOLATION: {'ON' if snap else 'OFF'}"
          + ("" if snap or not cfg.source.use_snapshot_isolation else "  <-- required by config"))
    sink = ClickHouseSink(cfg.target)
    print(f"ClickHouse OK: version {sink.command('SELECT version()')}")

    tables = list(cfg.tables)
    if cfg.sync.auto_discover:
        known = {t.key.lower() for t in tables}
        tables += [TableConfig(source=k) for k in src.discover_tables() if k.lower() not in known]
    ok = True
    for t in tables:
        try:
            ts = src.load_schema(t.schema_name, t.table_name, t.exclude_columns)
            mv = src.min_valid_version(ts)
            ct = "enabled" if mv is not None else "NOT ENABLED"
            print(f"\n-- {t.key}: {len(ts.columns)} columns, pk={[c.name for c in ts.pk]}, "
                  f"~{src.approx_rows(ts):,} rows, change tracking {ct}")
            ok &= mv is not None
            if args.ddl:
                print(create_table_sql(cfg.target.database, t.target_table, ts, t, cfg.target.replicated) + ";")
        except Exception as exc:  # noqa: BLE001
            ok = False
            print(f"\n-- {t.key}: ERROR {exc}")
    return 0 if ok else 1


def cmd_status(args) -> int:
    from .sink import ClickHouseSink
    from .state import StateStore

    cfg = _load(args)
    store = StateStore(ClickHouseSink(cfg.target), cfg.target.meta_database)
    states = store.load_all()
    if not states:
        print("no state yet")
        return 0
    now = time.time()
    print(f"{'table':40} {'phase':9} {'version':>12} {'rows synced':>14} {'last ok':>10}  error")
    for key in sorted(states):
        st = states[key]
        age = f"{now - st.last_success:,.0f}s" if st.last_success else "-"
        phase = st.phase
        if st.phase == "snapshot":
            prog = store.load_progress(key, st.snapshot_id)
            done = sum(p.done for p in prog.values())
            phase = f"snap {done}/{len(st.partitions)}"
        print(f"{key:40} {phase:9} {st.last_version:>12} {st.rows_synced:>14,} {age:>10}  {st.last_error[:80]}")
    return 0


def cmd_resync(args) -> int:
    from .sink import ClickHouseSink
    from .state import StateStore

    cfg = _load(args)
    store = StateStore(ClickHouseSink(cfg.target), cfg.target.meta_database)
    for name in args.table:
        key = TableConfig(source=name).key
        store.request(key, "resync")
        print(f"resync requested for {key}; the running service picks it up within ~10s")
    return 0


def load_dotenv(path: str = ".env") -> None:
    """Load KEY=VALUE lines from .env without overriding real environment variables."""
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8-sig") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            os.environ.setdefault(key.strip(), value)


def main(argv: list[str] | None = None) -> int:
    load_dotenv(os.environ.get("CH_SYNC_ENV_FILE", ".env"))
    parser = argparse.ArgumentParser(prog="ch-sync", description="SQL Server -> ClickHouse sync")
    parser.add_argument("--config", "-c", default=os.environ.get("CH_SYNC_CONFIG", "config.yaml"))
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="run the sync service")
    p.add_argument("--shard-index", type=int, default=_env_int("SHARD_INDEX"))
    p.add_argument("--shard-count", type=int, default=_env_int("SHARD_COUNT"))
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("check", help="validate connections and Change Tracking")
    p.add_argument("--ddl", action="store_true", help="print ClickHouse DDL per table")
    p.set_defaults(fn=cmd_check)

    sub.add_parser("status", help="show per-table sync status").set_defaults(fn=cmd_status)

    p = sub.add_parser("resync", help="request a full reload of tables")
    p.add_argument("table", nargs="+", help="schema.table")
    p.set_defaults(fn=cmd_resync)

    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    try:
        return args.fn(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2


def _env_int(name: str) -> int | None:
    v = os.environ.get(name)
    return int(v) if v else None


if __name__ == "__main__":
    sys.exit(main())
