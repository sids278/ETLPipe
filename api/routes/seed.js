import { Router } from 'express';
import { startJob } from '../jobs/jobManager.js';
import { parseBatchSize, planRows, runSeed } from '../services/seedService.js';

const router = Router();

/**
 * @openapi
 * /seed:
 *   post:
 *     summary: Fill the load-test tables with generated rows (background job)
 *     description: >
 *       Appends rows to the 5 tables, parents first, with set-based INSERTs that run inside SQL Server.
 *       Returns a job id immediately; poll GET /jobs/{id} for progress. Run POST /schema first.
 *       Send either totalRows (split Customers 10%, Products 1%, Orders 25%, OrderItems 40%, Payments 24%)
 *       or rows with an exact count per table.
 *     tags: [Seed]
 *     requestBody:
 *       required: false
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               totalRows:
 *                 type: integer
 *                 default: 10000000
 *                 example: 10000000
 *               rows:
 *                 type: object
 *                 description: Exact rows per table; overrides totalRows
 *                 example: { "Customers": 1000, "Products": 100, "Orders": 5000, "OrderItems": 15000, "Payments": 5000 }
 *                 additionalProperties:
 *                   type: integer
 *               batchSize:
 *                 type: integer
 *                 default: 200000
 *                 description: Rows per INSERT statement (1000 to 1000000)
 *     responses:
 *       202:
 *         description: Job started; returns jobId and the row plan
 *       400:
 *         description: Invalid totalRows, rows or batchSize
 *       409:
 *         description: Another job is still running
 */
router.post('/', (req, res) => {
  const body = req.body ?? {};
  const plan = planRows(body);
  const batchSize = parseBatchSize(body.batchSize);
  const job = startJob('seed', { plan, batchSize }, (ctx) => runSeed({ plan, batchSize }, ctx));
  res.status(202).location(`/jobs/${job.id}`).json({ jobId: job.id, status: job.status, plan, batchSize });
});

export default router;
