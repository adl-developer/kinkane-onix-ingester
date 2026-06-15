import cron from 'node-cron';
import { config } from '../config';
import { ingestionService } from '../services/ingestion.service';
import { coverService } from '../services/cover.service';
import { runFailedChunkCleanup } from './chunk-cleanup.cron';
import { logger } from '../lib/logger';

export function startCron(): void {
  // ── R2 poll ───────────────────────────────────────────────────────────────
  const r2Schedule = config.cron.r2PollSchedule;
  if (!cron.validate(r2Schedule)) {
    throw new Error(`Invalid cron schedule: ${r2Schedule}`);
  }

  cron.schedule(r2Schedule, async () => {
    logger.info('Polling R2 for new ONIX files');

    try {
      const unprocessed = await ingestionService.listUnprocessedR2Files();

      if (unprocessed.length === 0) {
        logger.info('No new ONIX files found');
        return;
      }

      logger.info('New ONIX files found', { count: unprocessed.length });

      for (const fileKey of unprocessed) {
        const result = await ingestionService.triggerIngestion(fileKey);
        logger.info('Enqueued file for ingestion', { fileKey, jobId: result.jobId });
      }
    } catch (err) {
      logger.error('R2 poll failed', { error: err instanceof Error ? err.message : String(err) });
    }
  });

  logger.info('R2 poll scheduled', { schedule: r2Schedule });

  // ── Cover fetch ───────────────────────────────────────────────────────────
  const coverSchedule = config.cron.coverFetchSchedule;
  if (!cron.validate(coverSchedule)) {
    throw new Error(`Invalid cover fetch cron schedule: ${coverSchedule}`);
  }

  cron.schedule(coverSchedule, async () => {
    logger.info('Starting cover fetch cron');
    try {
      await coverService.fetchMissingCovers();
    } catch (err) {
      logger.error('Cover fetch cron failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Cover fetch scheduled', { schedule: coverSchedule });

  // ── Failed chunk R2 cleanup ───────────────────────────────────────────────
  // Runs daily at 04:00 — deletes R2 payload files for failed chunks older
  // than 30 days. Successful chunks are cleaned up immediately by the worker.
  cron.schedule('0 4 * * *', async () => {
    logger.info('Running failed chunk R2 cleanup');
    try {
      await runFailedChunkCleanup();
    } catch (err) {
      logger.error('Failed chunk R2 cleanup error', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Failed chunk R2 cleanup scheduled (daily at 04:00)');
}
