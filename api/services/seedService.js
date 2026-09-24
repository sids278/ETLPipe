import { getPool, sql, loadTestDatabase } from '../db/mssql.js';
import { httpError } from '../httpError.js';
import { TABLES, getTableStats } from './schemaService.js';

// Share of totalRows each table gets (sums to 1): few parents, many children, like a real shop.
// 10M total -> Customers 1M, Products 100k, Orders 2.5M, OrderItems 4M, Payments 2.4M.
const SHARES = { Customers: 0.1, Products: 0.01, Orders: 0.25, OrderItems: 0.4, Payments: 0.24 };

// Parent tables each table's foreign keys point into. Their ids run 1..max, so a child picks 1 + (h % max).
const PARENTS = { Customers: [], Products: [], Orders: ['Customers'], OrderItems: ['Orders', 'Products'], Payments: ['Orders'] };

const PK = Object.fromEntries(TABLES.map((t) => [t.name, t.pk]));

// Row generators run entirely inside SQL Server, so no data crosses the network.
// `n.i` is the new primary key (@start + 1 .. @start + @n); `x.h` is a deterministic hash of it that
// varies the other columns. Same id -> same values on every run (except ExternalRef, which is NEWID()).
const NUMBERS = `
WITH n AS (
  SELECT TOP (@n) @start + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
  FROM sys.all_columns a CROSS JOIN sys.all_columns b
)`;
const HASH = `CROSS APPLY (SELECT CAST((n.i * CAST(2654435761 AS bigint)) % 2147483647 AS int) AS h) x`;
const BASE_DATE = `CAST('2026-01-01' AS datetime2(3))`;

const INSERTS = {
  Customers: `${NUMBERS}
INSERT INTO dbo.Customers WITH (TABLOCK) (CustomerId, FirstName, LastName, Email, Country, CreatedAt, IsActive)
SELECT n.i,
       CHOOSE(1 + x.h % 10, N'Aarav', N'Priya', N'Rahul', N'Ananya', N'Vikram', N'Sara', N'John', N'Maria', N'Wei', N'Fatima'),
       CHOOSE(1 + (x.h / 10) % 10, N'Sharma', N'Patel', N'Singh', N'Iyer', N'Khan', N'Smith', N'Garcia', N'Chen', N'Müller', N'Okafor'),
       CONCAT(N'user', n.i, N'@example.com'),
       CHOOSE(1 + (x.h / 100) % 6, 'IN', 'US', 'GB', 'DE', 'SG', 'AE'),
       DATEADD(MINUTE, -(x.h % 1051200), ${BASE_DATE}),
       CASE WHEN x.h % 10 = 0 THEN 0 ELSE 1 END
FROM n ${HASH};`,

  Products: `${NUMBERS}
INSERT INTO dbo.Products WITH (TABLOCK) (ProductId, Sku, Name, Category, Price, CreatedAt)
SELECT n.i,
       CONCAT('SKU-', RIGHT(CONCAT('0000000000', n.i), 10)),
       CONCAT(N'Product ', n.i),
       CHOOSE(1 + x.h % 6, N'Electronics', N'Books', N'Home', N'Toys', N'Grocery', N'Fashion'),
       CAST(1 + (x.h % 100000) / 100.0 AS decimal(10,2)),
       DATEADD(MINUTE, -(x.h % 1051200), ${BASE_DATE})
FROM n ${HASH};`,

  Orders: `${NUMBERS}
INSERT INTO dbo.Orders WITH (TABLOCK) (OrderId, CustomerId, Status, OrderDate, TotalAmount, Currency, ExternalRef)
SELECT n.i,
       1 + x.h % @customers,
       CHOOSE(1 + x.h % 5, N'new', N'paid', N'shipped', N'delivered', N'cancelled'),
       DATEADD(MINUTE, -(x.h % 525600), ${BASE_DATE}),
       CAST((x.h % 5000000) / 100.0 AS decimal(18,2)),
       CHOOSE(1 + (x.h / 7) % 3, 'INR', 'USD', 'EUR'),
       NEWID()
FROM n ${HASH};`,

  OrderItems: `${NUMBERS}
INSERT INTO dbo.OrderItems WITH (TABLOCK) (OrderItemId, OrderId, ProductId, Quantity, UnitPrice, Discount)
SELECT n.i,
       1 + x.h % @orders,
       1 + (x.h / 3) % @products,
       1 + x.h % 5,
       CAST(1 + (x.h % 50000) / 100.0 AS decimal(10,2)),
       CASE WHEN x.h % 4 = 0 THEN NULL ELSE CAST((x.h % 30) / 100.0 AS decimal(5,2)) END
FROM n ${HASH};`,

  Payments: `${NUMBERS}
INSERT INTO dbo.Payments WITH (TABLOCK) (PaymentId, OrderId, Method, Amount, PaidAt, Notes)
SELECT n.i,
       1 + (n.i - 1) % @orders,
       CHOOSE(1 + x.h % 5, N'card', N'upi', N'netbanking', N'wallet', N'cod'),
       CAST((x.h % 5000000) / 100.0 AS decimal(18,2)),
       CASE WHEN x.h % 7 = 0 THEN NULL
            ELSE TODATETIMEOFFSET(DATEADD(MINUTE, -(x.h % 525600), ${BASE_DATE}), '+05:30') END,
       CASE WHEN x.h % 50 = 0 THEN REPLICATE(N'note ', 20) ELSE NULL END
FROM n ${HASH};`,
};

const MAX_TOTAL_ROWS = 1_000_000_000;

// Turns the request body into rows per table: either `rows` per table, or `totalRows` split by SHARES.
export function planRows({ totalRows = 10_000_000, rows } = {}) {
  if (rows !== undefined) {
    if (typeof rows !== 'object' || rows === null) throw httpError(400, '`rows` must be an object like { "Orders": 1000 }');
    const plan = Object.fromEntries(Object.keys(SHARES).map((t) => [t, 0]));
    for (const [table, count] of Object.entries(rows)) {
      if (!(table in SHARES)) throw httpError(400, `Unknown table in rows: ${table}. Use ${Object.keys(SHARES).join(', ')}`);
      if (!Number.isInteger(count) || count < 0) throw httpError(400, `rows.${table} must be a whole number >= 0`);
      plan[table] = count;
    }
    return plan;
  }
  if (!Number.isInteger(totalRows) || totalRows < 1 || totalRows > MAX_TOTAL_ROWS) {
    throw httpError(400, `totalRows must be a whole number between 1 and ${MAX_TOTAL_ROWS}`);
  }
  return Object.fromEntries(Object.entries(SHARES).map(([t, share]) => [t, Math.round(totalRows * share)]));
}

export function parseBatchSize(batchSize = 200_000) {
  if (!Number.isInteger(batchSize) || batchSize < 1_000 || batchSize > 1_000_000) {
    throw httpError(400, 'batchSize must be a whole number between 1000 and 1000000');
  }
  return batchSize;
}

async function maxId(pool, table) {
  const { recordset } = await pool.request().query(`SELECT ISNULL(MAX(${PK[table]}), 0) AS maxId FROM dbo.${table};`);
  return Number(recordset[0].maxId); // bigint arrives as a string
}

// Appends plan[table] rows to each table, parents first, in batches. Reports progress into ctx.progress
// after every batch and stops between batches when the job is cancelled.
export async function runSeed({ plan, batchSize }, ctx) {
  const pool = await getPool(loadTestDatabase);
  const progress = ctx.progress;
  const jobStart = performance.now();

  progress.totalTarget = Object.values(plan).reduce((a, b) => a + b, 0);
  progress.totalInserted = 0;
  progress.tables = Object.fromEntries(
    Object.keys(SHARES).map((t) => [t, { target: plan[t], inserted: 0, status: plan[t] ? 'pending' : 'skipped' }]),
  );

  for (const table of Object.keys(SHARES)) {
    const p = progress.tables[table];
    if (!p.target || ctx.isCancelled()) continue;

    const parentMax = {};
    for (const parent of PARENTS[table]) {
      parentMax[parent] = await maxId(pool, parent);
      if (parentMax[parent] === 0) throw new Error(`${table} needs rows in ${parent} first (it is empty)`);
    }

    let lastId = await maxId(pool, table);
    p.status = 'running';
    p.firstId = lastId + 1;
    const tableStart = performance.now();

    while (p.inserted < p.target && !ctx.isCancelled()) {
      const n = Math.min(batchSize, p.target - p.inserted);
      const request = pool.request().input('start', sql.BigInt, lastId).input('n', sql.Int, n);
      for (const [parent, max] of Object.entries(parentMax)) request.input(parent.toLowerCase(), sql.BigInt, max);
      await request.query(INSERTS[table]);

      lastId += n;
      p.inserted += n;
      progress.totalInserted += n;
      p.seconds = round1((performance.now() - tableStart) / 1000);
      p.rowsPerSec = Math.round(p.inserted / Math.max(p.seconds, 0.001));

      const elapsed = (performance.now() - jobStart) / 1000;
      progress.rowsPerSec = Math.round(progress.totalInserted / elapsed);
      progress.etaSeconds = Math.round((progress.totalTarget - progress.totalInserted) / Math.max(progress.rowsPerSec, 1));
    }
    p.status = p.inserted >= p.target ? 'done' : 'cancelled';
  }

  const seconds = round1((performance.now() - jobStart) / 1000);
  return {
    seconds,
    rowsInserted: progress.totalInserted,
    rowsPerSec: Math.round(progress.totalInserted / Math.max(seconds, 0.001)),
    tables: progress.tables,
    finalCounts: await getTableStats(),
  };
}

function round1(value) {
  return Math.round(value * 10) / 10;
}
