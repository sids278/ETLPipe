# ch-sync: SQL Server → ClickHouse without Kafka

A small, self-hosted service that keeps ClickHouse tables in sync with SQL Server
using SQL Server's built-in **Change Tracking**. There is no Kafka, no S3, no
orchestrator, and no extra state database: the service, SQL Server, and ClickHouse
are the whole system.

```
┌──────────────────┐   CHANGETABLE()    ┌─────────────────────────────┐   batched    ┌────────────────────────────┐
│ SQL Server       │ ─────────────────▶ │ ch-sync                     │ ───────────▶ │ ClickHouse                 │
│ Change Tracking  │   PK-range scans   │  supervisor (1 process)     │   inserts    │  analytics.<table>         │
│ + snapshot iso.  │                    │  └─ N worker processes      │              │  ReplacingMergeTree        │
└──────────────────┘                    │     • snapshot partitions   │              │  analytics.<table>_current │
                                        │     • CDC polls             │              │  ch_sync.* (checkpoints)   │
                                        └─────────────────────────────┘              └────────────────────────────┘
                                          scale out: N instances, each owns a shard of tables
```

## How it works

Each table moves through a simple lifecycle:

```
new ──plan──▶ snapshot ──all partitions loaded──▶ EXCHANGE TABLES ──▶ cdc
                  ▲                                                   │
                  └──── CT retention exceeded, or `ch-sync resync` ◀──┘
```

The pipeline runs in four stages.

1. **Plan.** The supervisor records `CHANGE_TRACKING_CURRENT_VERSION()` as **V0** *before*
   reading any data. It then creates a `<table>__staging` table and, for tables
   with a single integer primary key, splits the key space into ranges.
2. **Snapshot.** Worker processes stream each range in PK order straight into
   staging, in batches. After every batch, they checkpoint the last primary key into ClickHouse. A crash resumes mid-range.
3. **Cut-over.** Once every range is loaded, `EXCHANGE TABLES` atomically swaps
   staging and the live table. Readers never see a half-loaded table. The cut-over is
   idempotent: a table comment records which snapshot is live, so a crash mid-cut-over never swaps back.
4. **CDC.** Every `poll_interval` seconds, per table, the service reads
   `CHANGETABLE(CHANGES …, last_version)` joined to the base table inside one
   **SNAPSHOT-isolation** transaction. Present rows are upserts. Missing rows become
   tombstones (`_is_deleted = 1`). `_version` is the CT commit version.

### Why this is correct

All of these correctness guarantees come from the version scheme.

- **Idempotent writes:** tables use `ReplacingMergeTree(_version, _is_deleted)` ordered by
  the source primary key. Re-inserting the same batch after a retry or crash is harmless, which is why
  every write is **insert first, checkpoint after**, giving at-least-once delivery with exactly-once results.
- **No lost changes during the initial load:** snapshot rows carry version V0, and every change
  committed after V0 has a higher version and is replayed by CDC.
- **Deletes are captured,** including a row updated and then deleted between two polls (the
  join finds no row, so a tombstone is written).
- **Resyncs drop stale rows:** a resync builds a fresh table and swaps it in, so rows deleted while
  CT history was unavailable disappear too.

## Quick start (local demo)

```bash
docker compose up -d --build        # SQL Server + ClickHouse + ch-sync, seeds 200k orders
docker compose logs -f ch-sync      # watch snapshot → cdc

# make changes in SQL Server, watch them arrive within seconds
docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa \
  -P Str0ng_Passw0rd -i /dev/stdin < sql/03_demo_changes.sql

docker compose exec ch-sync ch-sync --config /app/config.yaml status
docker compose exec clickhouse clickhouse-client --password ch_pass \
  -q "SELECT Status, count(), sum(Amount) FROM analytics.Orders_current GROUP BY Status"
```

## Production setup

### 1. Prepare SQL Server (once)

Run `sql/01_enable_change_tracking.sql` in the source database. The script does three things:

- It enables `ALLOW_SNAPSHOT_ISOLATION`.
- It turns on Change Tracking with a retention window.
- It enables tracking on every table that has a primary key, and lists the tables that don't have one.

A least-privilege login is included in the script's comments. It needs `db_datareader`, `VIEW CHANGE TRACKING`, and `VIEW DEFINITION`.

Choose the **retention** so it exceeds your longest initial load plus the longest outage you want
to recover from without a full reload. Three to seven days is typical. If retention is exceeded, ch-sync
automatically falls back to a full resync of that table only.

### 2. Set up the folder

```bash
./scripts/setup.sh                                              # Linux / macOS
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1     # Windows
```

This creates `.venv`, installs dependencies, creates `config.yaml` and `.env` (secrets, loaded automatically), checks for the ODBC driver, and runs the unit tests. Re-running it is safe and never overwrites your files.

### 3. Configure

Copy `config.example.yaml` to `config.yaml`. Secrets use `${ENV_VAR}` placeholders. Then validate the setup:

```bash
pip install .                     # needs Microsoft ODBC Driver 18 on the host (see Dockerfile)
ch-sync -c config.yaml check --ddl
```

`check` verifies both connections, snapshot isolation, and CT per table, and prints the
ClickHouse DDL it will create.

### 4. Run

```bash
ch-sync -c config.yaml run                              # or: docker run … ch-sync run
```

Run it as a single long-lived process per shard: a systemd unit, a Docker container with
`restart: unless-stopped`, or a Kubernetes Deployment with `replicas: 1` per shard. It
handles SIGTERM gracefully, and snapshot workers stop at the next batch boundary.

## Scaling

| Lever | What it does | Guidance |
|---|---|---|
| `sync.workers` | Worker processes (true parallelism, no GIL contention) | ~1 CPU core each on the sync host |
| `sync.max_snapshot_workers` | Caps workers used by initial loads | Default is half; keeps CDC fresh while big loads run |
| `snapshot_partitions` | Parallel PK ranges per table (single integer PK) | 8–32 for 100M+ row tables; ranges assume evenly spread keys |
| `sync.batch_size` | Rows per ClickHouse insert | 50k–200k; larger batches mean fewer parts and less merge pressure |
| `source.fetch_size` | Rows per ODBC round trip | 5k–20k |
| Sharding | `SHARD_INDEX` / `SHARD_COUNT` env vars | Run N instances; each owns a stable, disjoint subset of tables |
| `poll_interval` | CDC frequency (global or per table) | Lower means fresher data but more small inserts; 5–60s is typical |

For **hundreds of tables**, use `auto_discover: true` plus sharding. State writes are batched into a
single insert per flush, so the metadata tables stay small.

On the **SQL Server side**, the initial load reads by clustered-index ranges, so tables with a
clustered primary key scan efficiently. Run first loads off-peak for very large tables. Change Tracking
itself is lightweight: it stores only the primary key and version per change.

On the **ClickHouse side**, use `partition_by` (e.g. `toYYYYMM(CreatedAt)`) for large tables, and pick
an `order_by` that matches your query filters, but it must include all primary key columns. For heavy
dashboards, build materialized views or aggregates on top instead of running `FINAL` on huge
tables at query time.

## Operations

```bash
ch-sync -c config.yaml status                  # phase, CT version, rows, freshness, last error
ch-sync -c config.yaml resync dbo.Orders       # full reload via staging + atomic swap
```

The table below covers common situations.

| Situation | What happens / what to do |
|---|---|
| New column in SQL Server | Added automatically as a Nullable column in ClickHouse; the view is recreated |
| Column dropped in SQL Server | Stops being written; the ClickHouse column stays (drop it manually if wanted) |
| Column type changed | Run `resync` for that table |
| Sync down longer than CT retention | Detected automatically; that table resyncs by itself |
| Add a table | Add it to `tables:` (or enable CT on it with `auto_discover`); it's picked up automatically |
| Worker crash / OOM | The pool is rebuilt; failed tasks retry with exponential backoff (max 5 min) |
| Service killed mid-snapshot | Resumes from the last checkpointed key per partition |

Monitoring data lives in ClickHouse. For example:

```sql
SELECT table_key, phase, now() - toDateTime(last_success) AS lag_s, last_error
FROM ch_sync.sync_state FINAL ORDER BY lag_s DESC;

SELECT table_key, sum(rows), countIf(status = 'error')
FROM ch_sync.sync_log WHERE ts > now() - INTERVAL 1 HOUR GROUP BY table_key;
```

Alert when `lag_s` exceeds a few times `poll_interval`, or when `last_error` is non-empty.

## Querying in ClickHouse

Use the view for correct current state:

```sql
SELECT * FROM analytics.Orders_current WHERE CustomerId = 42;
```

The view is `SELECT … FROM analytics.Orders FINAL WHERE _is_deleted = 0`. You can also query the
base table with `FINAL` yourself. Background merges eventually collapse old versions and tombstones.

## Type mapping

| SQL Server | ClickHouse |
|---|---|
| bit | Bool |
| tinyint / smallint / int / bigint | UInt8 / Int16 / Int32 / Int64 |
| decimal(p,s), numeric | Decimal(p,s) |
| money / smallmoney | Decimal(19,4) / Decimal(10,4) |
| float / real | Float64 / Float32 |
| date | Date32 |
| datetime, smalldatetime | DateTime64(3,'UTC') |
| datetime2(p) | DateTime64(p,'UTC') |
| datetimeoffset(p) | DateTime64(p,'UTC'), converted to UTC |
| time | String |
| char/varchar/nchar/nvarchar/text/ntext, xml | String |
| binary/varbinary/image | String (raw bytes) |
| uniqueidentifier | UUID (converted via text, so no GUID byte-order issues) |
| rowversion | Int64 |
| hierarchyid, geography/geometry, sql_variant | String (`ToString()` / WKT / text) |

All conversions happen inside the SQL Server `SELECT`, so rows flow through Python untouched.
Dates outside ClickHouse's supported range (1900–2299) are clamped to the range limits.

## Limitations

- **Primary key required.** Change Tracking requires a primary key on every synced table.
- **Current state only.** Change Tracking gives net current state, not every intermediate version. If you need full
  change history or an audit trail, that calls for SQL Server CDC instead.
- **Primary key changes.** Updating a primary key value appears as a delete of the old key plus an insert of the new one, which is correct.
- **Composite or non-integer primary keys** snapshot as one ordered stream per table (still resumable); parallelism
  comes from loading many tables at once.
- **One instance per shard index.** Two instances with the same shard index would duplicate work. Results stay
  correct because writes are idempotent, but it is wasteful.
- **Cluster requirements.** `EXCHANGE TABLES` needs the Atomic database engine (the ClickHouse default). For
  clusters, use a `Replicated` database with `target.replicated: true`.

## Project layout

```
src/ch_sync/
  config.py      YAML config + env expansion + validation
  typemap.py     SQL Server → ClickHouse types (conversions pushed into SQL)
  model.py       table/column model
  queries.py     snapshot / CHANGETABLE queries, partition planner, checkpoint codec
  ddl.py         ClickHouse DDL (tables, views, metadata)
  source.py      pyodbc access, snapshot-isolated change sessions
  sink.py        ClickHouse client with retrying batch inserts
  state.py       checkpoints / progress / requests / run log in ClickHouse
  tasks.py       worker-process tasks: snapshot partition, CDC tick, schema drift
  supervisor.py  lifecycle state machine, scheduling, atomic cut-over
  __main__.py    CLI: run / check / status / resync (+ .env loading)
sql/             SQL Server enablement + demo scripts
scripts/         one-command setup (setup.sh / setup.ps1)
tests/           unit tests + end-to-end test against real ClickHouse
```

## Tests

```bash
pip install -e ".[dev]"
pytest                                   # unit tests
CH_TEST_HOST=127.0.0.1 pytest            # + end-to-end against a local ClickHouse
```

The end-to-end test drives the real supervisor, tasks, DDL, and state store against ClickHouse,
with an in-memory SQL Server that emulates Change Tracking semantics. It covers:

- the parallel initial load, including writes that land during the load;
- updates, deletes, and delete-plus-reinsert;
- composite primary keys;
- schema drift;
- automatic resync after lost CT history;
- a crash right after the atomic swap, followed by a restart.
