import { getPool } from '../db/mssql.js';
import { clickhouse } from '../db/clickhouse.js';

// Runs one check and reports up/down plus how long it took; never throws.
async function timed(check) {
  const start = performance.now();
  try {
    const details = await check();
    return { status: 'up', latencyMs: Math.round(performance.now() - start), ...details };
  } catch (err) {
    return { status: 'down', latencyMs: Math.round(performance.now() - start), error: describeError(err) };
  }
}

// Connection failures often arrive as an AggregateError with an empty message; fall back to its code.
function describeError(err) {
  if (err?.message) return err.message;
  if (err?.code) return `${err.code}${err.errors ? ': ' + err.errors.map((e) => e.message).join('; ') : ''}`;
  return String(err);
}

async function checkMssql() {
  const pool = await getPool();
  const { recordset } = await pool.request().query(`
    SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS version,
           CAST(SERVERPROPERTY('Edition') AS nvarchar(128))        AS edition,
           DB_NAME()                                               AS databaseName,
           d.snapshot_isolation_state_desc                         AS snapshotIsolation,
           CASE WHEN ct.database_id IS NULL THEN 0 ELSE 1 END      AS changeTracking
    FROM sys.databases d
    LEFT JOIN sys.change_tracking_databases ct ON ct.database_id = d.database_id
    WHERE d.database_id = DB_ID()`);
  const row = recordset[0];
  return {
    version: row.version,
    edition: row.edition,
    database: row.databaseName,
    snapshotIsolation: row.snapshotIsolation,
    changeTracking: row.changeTracking === 1,
  };
}

async function checkClickhouse() {
  const ping = await clickhouse.ping();
  if (!ping.success) throw ping.error;
  const rs = await clickhouse.query({
    query: 'SELECT version() AS version, uptime() AS uptimeSeconds',
    format: 'JSONEachRow',
  });
  const [row] = await rs.json();
  return row;
}

export async function getHealth() {
  const [mssql, ch] = await Promise.all([timed(checkMssql), timed(checkClickhouse)]);
  return {
    status: mssql.status === 'up' && ch.status === 'up' ? 'ok' : 'degraded',
    checkedAt: new Date().toISOString(),
    mssql,
    clickhouse: ch,
  };
}
