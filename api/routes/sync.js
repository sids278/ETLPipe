import { Router } from 'express';
import { startJob } from '../jobs/jobManager.js';
import { parseSyncParams, runSync } from '../services/syncService.js';

const router = Router();

/**
 * @openapi
 * /sync:
 *   post:
 *     summary: Time a full SQL Server -> ClickHouse sync of the load-test tables (background job)
 *     description: >
 *       Drops the ClickHouse load-test databases (cold start), writes a ch-sync config for the 5 tables,
 *       builds the ch-sync image (timed separately), starts ch-sync, and times it until every table has
 *       finished its snapshot and switched to CDC. Then reports per-table snapshot time, rows/sec,
 *       partitions, row-count match and ClickHouse storage, and stops ch-sync.
 *       Poll GET /jobs/{id} for progress. Run POST /schema and POST /seed first.
 *     tags: [Sync]
 *     requestBody:
 *       required: false
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               workers:
 *                 type: integer
 *                 default: 4
 *                 description: ch-sync worker processes (all may be used for the snapshot)
 *               snapshotPartitions:
 *                 type: integer
 *                 default: 4
 *                 description: Max parallel primary-key ranges per table
 *               minRowsPerPartition:
 *                 type: integer
 *                 default: 250000
 *                 description: A table is only split when each range gets at least this many rows
 *               batchSize:
 *                 type: integer
 *                 default: 100000
 *                 description: Rows per ClickHouse insert
 *               fetchSize:
 *                 type: integer
 *                 default: 10000
 *                 description: Rows per ODBC round trip from SQL Server
 *               timeoutSeconds:
 *                 type: integer
 *                 default: 3600
 *               resetTarget:
 *                 type: boolean
 *                 default: true
 *                 description: Drop the ClickHouse load-test databases first so the run starts cold
 *               keepRunning:
 *                 type: boolean
 *                 default: false
 *                 description: Leave ch-sync running in CDC mode after the snapshot finishes
 *     responses:
 *       202:
 *         description: Job started; returns jobId and the settings used
 *       400:
 *         description: Invalid setting
 *       409:
 *         description: Another job is still running
 */
router.post('/', (req, res) => {
  const params = parseSyncParams(req.body ?? {});
  const job = startJob('sync', params, (ctx) => runSync(params, ctx));
  res.status(202).location(`/jobs/${job.id}`).json({ jobId: job.id, status: job.status, settings: params });
});

export default router;
