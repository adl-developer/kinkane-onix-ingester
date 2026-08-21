import { Router } from 'express';
import authRoutes from './auth.routes';
import healthRoutes from './health.routes';
import ingestionRoutes from './ingestion.routes';
import gardnersRoutes from './gardners.routes';

const router = Router();

router.use('/health', healthRoutes);
router.use('/auth', authRoutes);
router.use('/ingestion', ingestionRoutes);
router.use('/gardners', gardnersRoutes);

export default router;
