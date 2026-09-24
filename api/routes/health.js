import { Router } from 'express';
import { getHealth } from '../services/healthService.js';

const router = Router();

/**
 * @openapi
 * /health:
 *   get:
 *     summary: Check SQL Server and ClickHouse
 *     description: Connects to both databases and reports status, latency and version. For SQL Server it also reports snapshot isolation and Change Tracking for the configured database.
 *     tags: [Health]
 *     responses:
 *       200:
 *         description: Both databases are up
 *       503:
 *         description: At least one database is down (see the error field)
 */
router.get('/', async (req, res) => {
  const health = await getHealth();
  res.status(health.status === 'ok' ? 200 : 503).json(health);
});

export default router;
