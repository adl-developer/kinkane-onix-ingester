import { Router } from 'express';
import { requireAdminToken } from '../middleware/auth.middleware';
import { gardnersController } from '../controllers/gardners.controller';

const router = Router();

// All Gardners admin routes require a valid admin JWT
router.use(requireAdminToken);

/**
 * POST /gardners/bootstrap
 * Body: { coverBatchSize?: number, coverConcurrency?: number }
 * One-shot fresh-database bootstrap of the entire Gardners catalogue —
 * ONIX bibliographic data, stock/pricing, promotions, firm-sale flags,
 * ISBN redirects, market restrictions, hourly availability, and cover
 * images. Responds 202 immediately; runs in the background.
 */
router.post('/bootstrap', gardnersController.bootstrap);

export default router;
