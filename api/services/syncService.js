import { spawn } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { clickhouse } from '../db/clickhouse.js';
import { loadTestDatabase } from '../db/mssql.js';
import { httpError } from '../httpError.js';
import { TABLES, getTableStats } from './schemaService.js';

// Load-test targets are separate from the demo's `analytics` / `ch_sync`, so runs never touch the demo.
const TARGET_DB = process.env.LOADTEST_CH_DATABASE ?? 'loadtest';
const META_DB = process.env.LOADTEST_CH_META_DATABASE ?? 'loadtest_meta';
const VIEW_SUFFIX = '_current';

// docker: run ch-sync from the compose `ch-sync` image (no ODBC driver needed on this machine).
// local:  run the `ch-sync` binary directly (needs `pip install .` and ODBC Driver 18).
const MODE = process.env.CH_SYNC_MODE ?? 'docker';
const LOCAL_BIN = process.env.CH_SYNC_BIN ?? 'ch-sync';
const CONTAINER = 'loadtest-ch-sync';

const REPO_ROOT = fileURLToPath(new URL('../../', import.meta.url));
const RUNTIME_DIR = fileURLToPath(new URL('../.runtime/', import.meta.url));
const CONFIG_PATH = `${RUNTIME_DIR}ch-sync.loadtest.yaml`;

const POLL_MS = 2000;
const LOG_LINES = 200;

// A ch-sync left running by `keepRunning: true`, stopped before the next run starts.
let leftRunning = null;

function intParam(value, fallback, min, max, name) {
  const v = value ?? fallback;
  if (!Number.isInteger(v) || v < min || v > max) throw httpError(400, `${name} must be a whole number between ${min} and ${max}`);
  return v;
}

// Validates the request body into run settings. Unset values fall back to sensible load-test defaults.
export function parseSyncParams(body = {}) {
  return {
    workers: intParam(body.workers, 4, 1, 64, 'workers'),
    snapshotPartitions: intParam(body.snapshotPartitions, 4, 1, 256, 'snapshotPartitions'),
    minRowsPerPartition: intParam(body.minRowsPerPartition, 250_000, 1_000, 100_000_000, 'minRowsPerPartition'),
    batchSize: intParam(body.batchSize, 100_000, 1_000, 1_000_000, 'batchSize'),
    fetchSize: intParam(body.fetchSize, 10_000, 100, 100_000, 'fetchSize'),
    timeoutSeconds: intParam(body.timeoutSeconds, 3600, 10, 86_400, 'timeoutSeconds'),
    resetTarget: body.resetTarget ?? true,
    keepRunning: body.keepRunning ?? false,
    mode: MODE,
  };
}

// ch-sync config for this run. JSON is valid YAML; ${...} placeholders are expanded by ch-sync
// from its own environment, so no password is written to disk.
function buildConfig(params) {
  const inDocker = MODE === 'docker';
  const ch = new URL(process.env.CH_URL ?? 'http://localhost:8123');
  return {
    source: {
      server: inDocker ? 'mssql' : (process.env.MSSQL_HOST ?? 'localhost'),
      port: inDocker ? 1433 : Number(process.env.MSSQL_PORT ?? 1433),
      database: loadTestDatabase,
      username: process.env.MSSQL_USER ?? 'sa',
      password: '${MSSQL_PASSWORD}',
      trust_server_certificate: true,
      fetch_size: params.fetchSize,
    },
    target: {
      host: inDocker ? 'clickhouse' : ch.hostname,
      port: inDocker ? 8123 : Number(ch.port || 8123),
      username: '${CH_USER:-default}',
      password: '${CH_PASSWORD}',
      database: TARGET_DB,
      meta_database: META_DB,
    },
    sync: {
      workers: params.workers,
      max_snapshot_workers: params.workers,
      poll_interval: 5,
      batch_size: params.batchSize,
      snapshot_partitions: params.snapshotPartitions,
      min_rows_per_partition: params.minRowsPerPartition,
    },
    tables: TABLES.map((t) => ({ source: `dbo.${t.name}` })),
  };
}

// Spawns a process and keeps the last LOG_LINES lines of its output.
function startProcess(cmd, args, options = {}) {
  const proc = { lines: [], exit: null };
  const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...options });
  const collect = (chunk) => {
    for (const line of chunk.toString().split('\n')) {
      if (!line.trim()) continue;
      proc.lines.push(line);
      if (proc.lines.length > LOG_LINES) proc.lines.shift();
    }
  };
  child.stdout.on('data', collect);
  child.stderr.on('data', collect);
  proc.exited = new Promise((resolve) => {
    child.on('error', (err) => {
      proc.lines.push(`failed to start ${cmd}: ${err.message}`);
      proc.exit ??= { code: -1, signal: null };
      resolve(proc.exit);
    });
    child.on('close', (code, signal) => {
      proc.exit ??= { code, signal };
      resolve(proc.exit);
    });
  });
  proc.child = child;
  return proc;
}

async function runToEnd(cmd, args, options) {
  const proc = startProcess(cmd, args, options);
  const { code } = await proc.exited;
  if (code !== 0) throw new Error(`${cmd} ${args.join(' ')} failed (exit ${code}):\n${proc.lines.slice(-20).join('\n')}`);
  return proc;
}

function startChSync() {
  if (MODE === 'local') {
    return startProcess(LOCAL_BIN, ['--config', CONFIG_PATH, 'run'], { cwd: REPO_ROOT, env: process.env });
  }
  // The compose service already mounts /app/config.yaml, so mount ours next to it and point ch-sync at it.
  return startProcess('docker', [
    'compose', 'run', '--rm', '--no-deps', '-T', '--name', CONTAINER,
    '-v', `${CONFIG_PATH}:/app/loadtest.yaml:ro`,
    '--entrypoint', 'ch-sync', 'ch-sync', '--config', '/app/loadtest.yaml', 'run',
  ], { cwd: REPO_ROOT, env: process.env });
}

// Graceful stop: SIGTERM lets ch-sync finish its current batch. Force-kills after 60s.
async function stopChSync(proc) {
  if (!proc || proc.exit) return;
  if (MODE === 'docker') {
    await runToEnd('docker', ['stop', '-t', '30', CONTAINER], { cwd: REPO_ROOT }).catch(() => {});
  } else {
    proc.child.kill('SIGTERM');
  }
  const timedOut = await Promise.race([proc.exited.then(() => false), sleep(60_000).then(() => true)]);
  if (timedOut) proc.child.kill('SIGKILL');
}

async function chQuery(query) {
  const rs = await clickhouse.query({ query, format: 'JSONEachRow' });
  return rs.json();
}

// ch-sync's per-table state. Empty until ch-sync has created its meta tables.
async function readStates() {
  try {
    return await chQuery(`SELECT table_key, phase, rows_synced, last_error FROM ${META_DB}.sync_state FINAL`);
  } catch (err) {
    if (/UNKNOWN_(DATABASE|TABLE)/.test(err.type ?? '') || /does(n't| not) exist/i.test(err.message)) return [];
    throw err;
  }
}

// Per-table snapshot timing from ch-sync's run log: first partition start -> cut-over (finalize).
async function readSnapshotLog() {
  const rows = await chQuery(`
    SELECT table_key,
           uniqExactIf(task, startsWith(task, 'snapshot['))                               AS partitions,
           sumIf(rows, startsWith(task, 'snapshot['))                                     AS rowsLoaded,
           countIf(task = 'finalize')                                                     AS finalized,
           dateDiff('millisecond',
                    minIf(ts - toIntervalMillisecond(duration_ms), startsWith(task, 'snapshot[')),
                    maxIf(ts, task = 'finalize')) / 1000                                  AS snapshotSeconds
    FROM ${META_DB}.sync_log
    GROUP BY table_key`);
  return new Map(rows.map((r) => [r.table_key, r]));
}

async function readTargetStorage() {
  const rows = await chQuery(`
    SELECT table,
           sum(rows)                    AS rows,
           sum(bytes_on_disk)           AS diskBytes,
           sum(data_uncompressed_bytes) AS rawBytes,
           count()                      AS parts
    FROM system.parts
    WHERE database = '${TARGET_DB}' AND active
    GROUP BY table`);
  return new Map(rows.map((r) => [r.table, r]));
}

const mb = (bytes) => Math.round((Number(bytes) / 1024 / 1024) * 10) / 10;
const round1 = (value) => Math.round(value * 10) / 10;

// Runs ch-sync against LoadTest from a cold start and times it until every table reaches CDC.
export async function runSync(params, ctx) {
  const progress = ctx.progress;
  const setStep = (step) => { progress.step = step; };
  const tableKeys = TABLES.map((t) => `dbo.${t.name}`);

  setStep('checking source');
  const source = await getTableStats();
  const missing = source.filter((t) => !t.exists).map((t) => t.table);
  if (missing.length) throw new Error(`Tables missing in ${loadTestDatabase}: ${missing.join(', ')}. Run POST /schema first.`);
  const sourceRows = Object.fromEntries(source.map((t) => [t.table, t.rows]));
  progress.totalSourceRows = Object.values(sourceRows).reduce((a, b) => a + b, 0);

  if (leftRunning) {
    setStep('stopping previous ch-sync');
    await stopChSync(leftRunning);
    leftRunning = null;
  }
  if (MODE === 'docker') await runToEnd('docker', ['rm', '-f', CONTAINER], { cwd: REPO_ROOT }).catch(() => {});

  if (params.resetTarget) {
    setStep('resetting ClickHouse target');
    await clickhouse.command({ query: `DROP DATABASE IF EXISTS ${TARGET_DB} SYNC` });
    await clickhouse.command({ query: `DROP DATABASE IF EXISTS ${META_DB} SYNC` });
  }

  await mkdir(RUNTIME_DIR, { recursive: true });
  await writeFile(CONFIG_PATH, JSON.stringify(buildConfig(params), null, 2));

  let buildSeconds = 0;
  if (MODE === 'docker') {
    setStep('building ch-sync image');
    const buildStart = performance.now();
    await runToEnd('docker', ['compose', 'build', 'ch-sync'], { cwd: REPO_ROOT, env: process.env });
    buildSeconds = round1((performance.now() - buildStart) / 1000);
  }
  if (ctx.isCancelled()) return { cancelled: true, buildSeconds };

  setStep('syncing');
  const syncStart = performance.now();
  const deadline = Date.now() + params.timeoutSeconds * 1000;
  const proc = startChSync();
  let finished = false;

  try {
    while (!ctx.isCancelled()) {
      if (proc.exit) {
        throw new Error(`ch-sync exited (code ${proc.exit.code}) before all tables were synced:\n${proc.lines.slice(-20).join('\n')}`);
      }
      const states = new Map((await readStates()).map((s) => [s.table_key, s]));
      const elapsed = (performance.now() - syncStart) / 1000;

      let synced = 0;
      progress.tables = Object.fromEntries(TABLES.map(({ name }) => {
        const st = states.get(`dbo.${name}`);
        const rows = Number(st?.rows_synced ?? 0); // UInt64 arrives as a string
        synced += Math.min(rows, sourceRows[name]);
        return [name, {
          phase: st?.phase ?? 'waiting',
          rowsSynced: rows,
          sourceRows: sourceRows[name],
          percent: sourceRows[name] ? Math.min(100, round1((rows / sourceRows[name]) * 100)) : 100,
          lastError: st?.last_error || undefined,
        }];
      }));
      progress.elapsedSeconds = round1(elapsed);
      progress.rowsSynced = synced;
      progress.rowsPerSec = Math.round(synced / Math.max(elapsed, 0.001));
      progress.etaSeconds = progress.rowsPerSec ? Math.round((progress.totalSourceRows - synced) / progress.rowsPerSec) : null;
      progress.logTail = proc.lines.slice(-10);

      if (tableKeys.every((k) => states.get(k)?.phase === 'cdc')) {
        finished = true;
        break;
      }
      if (Date.now() > deadline) throw new Error(`Timed out after ${params.timeoutSeconds}s; tables not in cdc yet`);
      await sleep(POLL_MS);
    }
    const syncSeconds = round1((performance.now() - syncStart) / 1000);
    if (!finished) return { cancelled: true, buildSeconds, elapsedSeconds: syncSeconds };

    // ch-sync flushes its run log every ~2s; wait until every table's cut-over is logged.
    setStep('collecting results');
    let log = new Map();
    for (let i = 0; i < 10; i++) {
      log = await readSnapshotLog();
      if (tableKeys.every((k) => Number(log.get(k)?.finalized ?? 0) > 0)) break;
      await sleep(1000);
    }
    const storage = await readTargetStorage();

    const tables = {};
    for (const { name } of TABLES) {
      const [{ c }] = await chQuery(`SELECT count() AS c FROM ${TARGET_DB}.\`${name}${VIEW_SUFFIX}\``);
      const entry = log.get(`dbo.${name}`);
      const part = storage.get(name);
      const snapshotSeconds = entry ? Number(entry.snapshotSeconds) : null;
      tables[name] = {
        sourceRows: sourceRows[name],
        targetRows: Number(c),
        match: Number(c) === sourceRows[name],
        snapshotSeconds,
        rowsPerSec: snapshotSeconds ? Math.round(sourceRows[name] / snapshotSeconds) : null,
        partitions: entry ? Number(entry.partitions) : null,
        diskMb: part ? mb(part.diskBytes) : 0,
        uncompressedMb: part ? mb(part.rawBytes) : 0,
        compressionRatio: part && Number(part.diskBytes) ? round1(Number(part.rawBytes) / Number(part.diskBytes)) : null,
        activeParts: part ? Number(part.parts) : 0,
      };
    }

    return {
      syncSeconds,
      buildSeconds,
      totalRows: progress.totalSourceRows,
      rowsPerSec: Math.round(progress.totalSourceRows / Math.max(syncSeconds, 0.001)),
      allRowCountsMatch: Object.values(tables).every((t) => t.match),
      tables,
      settings: { ...params, targetDatabase: TARGET_DB, metaDatabase: META_DB },
      chSyncLogTail: proc.lines.slice(-20),
    };
  } finally {
    if (finished && params.keepRunning) {
      leftRunning = proc;
    } else {
      setStep('stopping ch-sync');
      await stopChSync(proc);
    }
  }
}
