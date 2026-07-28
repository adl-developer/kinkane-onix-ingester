import { Request, Response } from 'express';
import { z } from 'zod';
import { config } from '../config';
import { gardnersBootstrapService } from '../services/gardners/bootstrap.service';
import { logger } from '../lib/logger';

const bootstrapSchema = z.object({
  coverBatchSize: z.coerce.number().int().min(1).max(10_000).optional(),
  // Live-verified up to 20 concurrent FTP connections against
  // covers.gardners.com (zero failures, ~12x throughput over one
  // connection) — capped at 50 as an untested-beyond-this safety ceiling,
  // not a confirmed-safe value. Gardners has never documented a
  // concurrency limit for this server.
  coverConcurrency: z.coerce.number().int().min(1).max(50).optional(),
});

export const gardnersController = {
  /**
   * POST /gardners/bootstrap
   * Body: none. (The old coverBatchSize/coverConcurrency fields are accepted
   * but ignored — see below.)
   *
   * Loads the full ~1.98M-record Gardners catalogue from GARDBIB plus every
   * CSV feed. Responds 202 immediately; runs in the background and is
   * expected to finish well inside an hour. Books that already have a row
   * are skipped, so re-running is cheap and safe.
   *
   * Refuses to run (403) unless GARDNERS_INGESTION_ENABLED=true — same
   * switch that gates all the Gardners crons, since this endpoint is the
   * single biggest risk to database size regardless of cron state.
   *
   * Two things this endpoint intentionally does not do, because either turns
   * a sub-hour run into a multi-day one:
   *   - embeddings (new books land with embedded_at NULL for the existing
   *     backfill to pick up)
   *   - cover images (daily cron, or POST /gardners/covers/backfill)
   */
  async bootstrap(req: Request, res: Response): Promise<void> {
    if (!config.gardners.ingestionEnabled) {
      res.status(403).json({
        error:
          'Gardners ingestion is disabled (GARDNERS_INGESTION_ENABLED is not "true") — refusing to run the full bootstrap. This is almost certainly running against a database not sized for the full catalogue.',
      });
      return;
    }

    const parsed = bootstrapSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    res.status(202).json({
      message:
        'Gardners full bootstrap started — loads the entire ~1.98M-record catalogue from GARDBIB, skipping books that already exist, plus every CSV feed. Follow progress in the logs and via GET /ingestion/jobs (file_key "gardners/GARDBIB.zip"). Embeddings and cover images are handled by their own backfills, not by this endpoint.',
    });

    gardnersBootstrapService.runFullBootstrap(parsed.data).catch((err: unknown) => {
      const e = err as Error;
      logger.error('Gardners bootstrap failed', { error: e.message });
    });
  },
};
