import { Router } from 'express';
import { requireAdminToken } from '../middleware/auth.middleware';
import { ingestionController } from '../controllers/ingestion.controller';

const router = Router();

// All ingestion routes require a valid admin JWT
router.use(requireAdminToken);

/**
 * POST /ingestion/presign
 * Body: { filename: "feed.xml", key?: "onix/feed.xml", expiresIn?: 14400 }
 * Returns a presigned PUT URL valid for up to 4 hours.
 * Use this to upload large files directly to R2, then call /trigger.
 */
router.post('/presign', ingestionController.presignUpload);

/**
 * POST /ingestion/trigger
 * Body: { "fileKey": "onix/20260501.xml" }
 * Enqueues a file-level BullMQ job and returns immediately.
 */
router.post('/trigger', ingestionController.trigger);

/**
 * GET /ingestion/jobs
 * Query: ?limit=20&offset=0
 * Lists ingestion jobs, newest first.
 */
router.get('/jobs', ingestionController.listJobs);

/**
 * GET /ingestion/jobs/:id
 * Returns full job detail including per-chunk breakdown.
 */
router.get('/jobs/:id', ingestionController.getJob);

/**
 * GET /ingestion/unprocessed
 * Lists ONIX files in R2 that have not yet been ingested.
 */
router.get('/unprocessed', ingestionController.listUnprocessed);

/**
 * POST /ingestion/jobs/:id/resume
 * Re-enqueues every not-yet-completed chunk of a job onto this process's
 * queue, rebuilt from Postgres/R2 state. Use when moving a job between
 * environments with separate Redis instances — make sure whatever was
 * previously processing it has actually stopped first.
 */
router.post('/jobs/:id/resume', ingestionController.resumeJob);

/**
 * POST /ingestion/backfill-embeddings
 * Generates and writes embeddings for all books where embedding IS NULL.
 * Long-running — returns { processed, failed, total } when complete.
 */
router.post('/backfill-embeddings', ingestionController.backfillEmbeddings);

/**
 * POST /ingestion/backfill-excerpts
 * Forces a full Jellybooks catalogue resync. Responds 202 immediately; the
 * sync runs in the background and results are logged on completion.
 */
router.post('/backfill-excerpts', ingestionController.backfillExcerpts);

export default router;
