import { getPool, loadTestDatabase } from '../db/mssql.js';
import { httpError } from '../httpError.js';

// The 5 load-test tables, parents first. There are no FOREIGN KEY constraints on purpose:
// they slow bulk inserts and the pipeline doesn't need them. The seeder still only writes
// foreign-key values that exist in the parent table.
export const TABLES = [
  {
    name: 'Customers',
    pk: 'CustomerId',
    ddl: `CREATE TABLE dbo.Customers (
      CustomerId bigint        NOT NULL PRIMARY KEY CLUSTERED,
      FirstName  nvarchar(50)  NOT NULL,
      LastName   nvarchar(50)  NOT NULL,
      Email      nvarchar(120) NOT NULL,
      Country    char(2)       NOT NULL,
      CreatedAt  datetime2(3)  NOT NULL,
      IsActive   bit           NOT NULL
    )`,
  },
  {
    name: 'Products',
    pk: 'ProductId',
    ddl: `CREATE TABLE dbo.Products (
      ProductId bigint         NOT NULL PRIMARY KEY CLUSTERED,
      Sku       varchar(20)    NOT NULL,
      Name      nvarchar(100)  NOT NULL,
      Category  nvarchar(30)   NOT NULL,
      Price     decimal(10,2)  NOT NULL,
      CreatedAt datetime2(3)   NOT NULL
    )`,
  },
  {
    name: 'Orders',
    pk: 'OrderId',
    ddl: `CREATE TABLE dbo.Orders (
      OrderId     bigint           NOT NULL PRIMARY KEY CLUSTERED,
      CustomerId  bigint           NOT NULL,
      Status      nvarchar(20)     NOT NULL,
      OrderDate   datetime2(3)     NOT NULL,
      TotalAmount decimal(18,2)    NOT NULL,
      Currency    char(3)          NOT NULL,
      ExternalRef uniqueidentifier NOT NULL
    )`,
  },
  {
    name: 'OrderItems',
    pk: 'OrderItemId',
    ddl: `CREATE TABLE dbo.OrderItems (
      OrderItemId bigint        NOT NULL PRIMARY KEY CLUSTERED,
      OrderId     bigint        NOT NULL,
      ProductId   bigint        NOT NULL,
      Quantity    int           NOT NULL,
      UnitPrice   decimal(10,2) NOT NULL,
      Discount    decimal(5,2)  NULL
    )`,
  },
  {
    name: 'Payments',
    pk: 'PaymentId',
    ddl: `CREATE TABLE dbo.Payments (
      PaymentId bigint            NOT NULL PRIMARY KEY CLUSTERED,
      OrderId   bigint            NOT NULL,
      Method    nvarchar(20)      NOT NULL,
      Amount    decimal(18,2)     NOT NULL,
      PaidAt    datetimeoffset(3) NULL,
      Notes     nvarchar(max)     NULL
    )`,
  },
];

// Database names can't be passed as parameters, so only allow plain identifiers before quoting.
function quoteName(name) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw httpError(400, `Invalid database name: ${name}`);
  return `[${name}]`;
}

// Creates the LoadTest database and the 5 tables if missing. Safe to run repeatedly.
export async function createSchema({ dropExisting = false, changeTracking = true } = {}) {
  const db = quoteName(loadTestDatabase);
  const master = await getPool('master');

  await master.request().input('name', loadTestDatabase).query(`
    IF DB_ID(@name) IS NULL CREATE DATABASE ${db};`);
  await master.request().query(`
    ALTER DATABASE ${db} SET ALLOW_SNAPSHOT_ISOLATION ON;
    ALTER DATABASE ${db} SET RECOVERY SIMPLE;`);
  if (changeTracking) {
    await master.request().input('name', loadTestDatabase).query(`
      IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID(@name))
        ALTER DATABASE ${db} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);`);
  }

  const pool = await getPool(loadTestDatabase);
  for (const table of TABLES) {
    const qualified = `dbo.${table.name}`;
    if (dropExisting) await pool.request().query(`DROP TABLE IF EXISTS ${qualified};`);
    await pool.request().query(`IF OBJECT_ID(N'${qualified}', N'U') IS NULL ${table.ddl};`);
    if (changeTracking) {
      await pool.request().query(`
        IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID(N'${qualified}'))
          ALTER TABLE ${qualified} ENABLE CHANGE_TRACKING;`);
    }
  }

  return { database: loadTestDatabase, tables: await getTableStats() };
}

// Row count, reserved size and Change Tracking status for each load-test table.
export async function getTableStats() {
  const pool = await getPool(loadTestDatabase);
  const { recordset } = await pool.request().query(`
    SELECT t.name                                                         AS tableName,
           SUM(CASE WHEN p.index_id IN (0, 1) THEN p.row_count ELSE 0 END) AS [rows],
           SUM(p.reserved_page_count) * 8 / 1024.0                         AS reservedMb,
           CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END                AS changeTracking
    FROM sys.tables t
    JOIN sys.dm_db_partition_stats p ON p.object_id = t.object_id
    LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
    WHERE t.schema_id = SCHEMA_ID('dbo')
    GROUP BY t.name, ct.object_id`);

  const byName = new Map(recordset.map((r) => [r.tableName, r]));
  return TABLES.map(({ name }) => {
    const row = byName.get(name);
    if (!row) return { table: name, exists: false };
    return {
      table: name,
      exists: true,
      rows: Number(row.rows), // bigint arrives as a string
      reservedMb: Math.round(Number(row.reservedMb) * 10) / 10,
      changeTracking: row.changeTracking === 1,
    };
  });
}
