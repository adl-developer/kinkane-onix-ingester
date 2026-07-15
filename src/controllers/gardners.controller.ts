import { Request, Response } from 'express';
import { z } from 'zod';
import { gardnersBootstrapService } from '../services/gardners/bootstrap.service';
import { logger } from '../lib/logger';

const bootstrapSchema = z.object({
  coverBatchSize: z.coerce.number().int().min(1).max(10_000).optional(),
  coverDelayMs: z.coerce.number().int().min(0).max(60_000).optional(),
});

export const gardnersController = {
  /**
   * POST /gardners/bootstrap
   * Body (optional): { coverBatchSize?: number, coverDelayMs?: number }
   *
   * One-shot fresh-database bootstrap: pulls the full Gardners catalogue
   * (ONIX bibliographic data + images) and every other feed. Responds 202
   * immediately; progress is logged. Intended for an initial load, not
   * routine use — every feed already runs on its own cron for ongoing
   * updates.
   *
   * The cover backfill walks the entire catalogue and is the slow part —
   * at the default batch size/delay (tuned for a once-a-day cron tick) it
   * can take days for a multi-million-title catalogue. Pass a larger
   * coverBatchSize and/or smaller coverDelayMs to run it faster for this
   * one-off call.
   */
  async bootstrap(req: Request, res: Response): Promise<void> {
    const parsed = bootstrapSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    res.status(202).json({
      message:
        'Gardners full bootstrap started — this pulls the entire catalogue and can take a long time (the cover backfill in particular may take days at default settings). Follow progress in the logs and via GET /ingestion/jobs for the biblio ingestion job.',
    });

    gardnersBootstrapService.runFullBootstrap(parsed.data).catch((err: unknown) => {
      const e = err as Error;
      logger.error('Gardners bootstrap failed', { error: e.message });
    });
  },
};
