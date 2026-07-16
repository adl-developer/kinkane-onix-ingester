import { Router, Request, Response } from 'express';
import authRoutes from './auth.routes';
import ingestionRoutes from './ingestion.routes';
import gardnersRoutes from './gardners.routes';

const router = Router();

router.get('/health', (_req: Request, res: Response) => {
  res.status(200).json({ status: 'ok', service: 'onix-ingester' });
});

router.use('/auth', authRoutes);
router.use('/ingestion', ingestionRoutes);
router.use('/gardners', gardnersRoutes);

export default router;
