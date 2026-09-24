import sql from 'mssql';

const baseConfig = {
  server: process.env.MSSQL_HOST ?? 'localhost',
  port: Number(process.env.MSSQL_PORT ?? 1433),
  user: process.env.MSSQL_USER ?? 'sa',
  password: process.env.MSSQL_PASSWORD,
  connectionTimeout: 5000,
  // Seed batches insert hundreds of thousands of rows each, so allow long-running statements.
  requestTimeout: Number(process.env.MSSQL_REQUEST_TIMEOUT_MS ?? 300000),
  options: {
    encrypt: true,
    trustServerCertificate: process.env.MSSQL_TRUST_CERT !== 'false',
  },
};

export const defaultDatabase = process.env.MSSQL_DATABASE ?? 'master';
export const loadTestDatabase = process.env.LOADTEST_DATABASE ?? 'LoadTest';

const pools = new Map();

// One shared pool per database, created on first use. A failed connect is forgotten so the next call retries.
export function getPool(database = defaultDatabase) {
  if (!pools.has(database)) {
    const pool = new sql.ConnectionPool({ ...baseConfig, database }).connect().catch((err) => {
      pools.delete(database);
      throw err;
    });
    pools.set(database, pool);
  }
  return pools.get(database);
}

export { sql };
