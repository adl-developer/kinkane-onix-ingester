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
   * Body (optional): { coverBatchSize?: number, coverConcurrency?: number }
   *
   * One-shot fresh-database bootstrap: pulls the full Gardners catalogue
   * (ONIX bibliographic data + images) and every other feed. Responds 202
   * immediately; progress is logged. Intended for an initial load, not
   * routine use — every feed already runs on its own cron for ongoing
   * updates.
   *
   * Refuses to run (403) unless GARDNERS_INGESTION_ENABLED=true — same
   * switch that gates all the Gardners crons, since this endpoint is the
   * single biggest risk to database size regardless of cron state.
   *
   * The cover backfill walks the entire catalogue and is the slow part —
   * it defaults to one FTP connection (~1.4s/book measured live, so
   * ~2-4 weeks for a multi-million-title catalogue). Pass coverConcurrency
   * to run several connections in parallel instead (20 is live-verified:
   * ~114ms/book effective, ~2-3 days for the same catalogue).
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
        'Gardners full bootstrap started — this pulls the entire catalogue and can take a long time (the cover backfill in particular may take days-to-weeks depending on coverConcurrency). Follow progress in the logs and via GET /ingestion/jobs for the biblio ingestion job.',
    });

    gardnersBootstrapService.runFullBootstrap(parsed.data).catch((err: unknown) => {
      const e = err as Error;
      logger.error('Gardners bootstrap failed', { error: e.message });
    });
  },
};
