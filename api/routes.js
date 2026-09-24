import { Router } from 'express';
import healthRouter from './routes/health.js';

const router = Router();

router.use('/health', healthRouter);

export default router;
