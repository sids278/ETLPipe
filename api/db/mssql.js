import sql from 'mssql';

const config = {
  server: process.env.MSSQL_HOST ?? 'localhost',
  port: Number(process.env.MSSQL_PORT ?? 1433),
  database: process.env.MSSQL_DATABASE ?? 'master',
  user: process.env.MSSQL_USER ?? 'sa',
  password: process.env.MSSQL_PASSWORD,
  connectionTimeout: 5000,
  requestTimeout: 30000,
  options: {
    encrypt: true,
    trustServerCertificate: process.env.MSSQL_TRUST_CERT !== 'false',
  },
};

let poolPromise = null;

// One shared pool, created on first use. A failed connect is forgotten so the next call retries.
export function getPool() {
  if (!poolPromise) {
    poolPromise = new sql.ConnectionPool(config).connect().catch((err) => {
      poolPromise = null;
      throw err;
    });
  }
  return poolPromise;
}

export { sql };
