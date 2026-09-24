import { Router } from 'express';
import healthRouter from './routes/health.js';
import jobsRouter from './routes/jobs.js';
import schemaRouter from './routes/schema.js';
import seedRouter from './routes/seed.js';
import syncRouter from './routes/sync.js';

const router = Router();

router.use('/health', healthRouter);
router.use('/schema', schemaRouter);
router.use('/seed', seedRouter);
router.use('/sync', syncRouter);
router.use('/jobs', jobsRouter);

export default router;
