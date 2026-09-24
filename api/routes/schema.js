import { Router } from 'express';
import { createSchema, getTableStats } from '../services/schemaService.js';

const router = Router();

/**
 * @openapi
 * /schema:
 *   post:
 *     summary: Create the LoadTest database and its 5 tables
 *     description: >
 *       Creates the database if missing, turns on snapshot isolation, SIMPLE recovery and
 *       Change Tracking, then creates Customers, Products, Orders, OrderItems and Payments.
 *       Safe to run repeatedly.
 *     tags: [Schema]
 *     requestBody:
 *       required: false
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               dropExisting:
 *                 type: boolean
 *                 default: false
 *                 description: Drop and recreate the 5 tables (deletes all their rows)
 *               changeTracking:
 *                 type: boolean
 *                 default: true
 *                 description: Enable Change Tracking on the database and tables (ch-sync needs it)
 *     responses:
 *       200:
 *         description: Database name and the state of each table
 */
router.post('/', async (req, res) => {
  const { dropExisting, changeTracking } = req.body ?? {};
  res.json(await createSchema({ dropExisting, changeTracking }));
});

/**
 * @openapi
 * /schema:
 *   get:
 *     summary: Row counts and sizes of the load-test tables
 *     tags: [Schema]
 *     responses:
 *       200:
 *         description: One entry per table with rows, reservedMb and changeTracking
 */
router.get('/', async (req, res) => {
  res.json(await getTableStats());
});

export default router;
