import { Router } from 'express';
import { requireAdminToken } from '../middleware/auth.middleware';
import { healthController } from '../controllers/health.controller';

const router = Router();

/**
 * GET /health
 * Liveness. Public and dependency-free — this is the one to point an uptime
 * monitor at.
 */
router.get('/', healthController.live);

/**
 * GET /health/ready
 * Readiness: pings Postgres and Redis. 200 when both answer, 503 otherwise.
 * Public but reports only up/down and latency, never connection details.
 */
router.get('/ready', healthController.ready);

/**
 * GET /health/stats
 * Query: ?windowHours=24&failureLimit=10
 * Run statistics across both pipelines: ONIX job counts and last completed
 * file, per-feed Gardners status, queue depths, and recent failures.
 * Admin JWT required — the payload includes filenames and error messages.
 */
router.get('/stats', requireAdminToken, healthController.stats);

/**
 * GET /health/counts
 * Catalogue size and enrichment coverage — books, covers, excerpts,
 * embeddings. The only view that shows whether covers and excerpts are
 * actually landing; neither writes fetch-log rows visible in /stats.
 */
router.get('/counts', requireAdminToken, healthController.counts);

export default router;
