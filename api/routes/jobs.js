import { Router } from 'express';
import { cancelJob, getJob, listJobs } from '../jobs/jobManager.js';

const router = Router();

/**
 * @openapi
 * /jobs:
 *   get:
 *     summary: List jobs, newest first
 *     tags: [Jobs]
 *     responses:
 *       200:
 *         description: All jobs since the API started
 */
router.get('/', (req, res) => {
  res.json(listJobs());
});

/**
 * @openapi
 * /jobs/{id}:
 *   get:
 *     summary: Status and progress of one job
 *     tags: [Jobs]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Status, per-table progress, rows/sec, ETA, and the result once finished
 *       404:
 *         description: Unknown job id
 */
router.get('/:id', (req, res) => {
  res.json(getJob(req.params.id));
});

/**
 * @openapi
 * /jobs/{id}/cancel:
 *   post:
 *     summary: Stop a running job after its current batch
 *     tags: [Jobs]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Cancellation requested (status becomes "cancelling", then "cancelled")
 *       404:
 *         description: Unknown job id
 *       409:
 *         description: Job has already finished
 */
router.post('/:id/cancel', (req, res) => {
  res.json(cancelJob(req.params.id));
});

export default router;
