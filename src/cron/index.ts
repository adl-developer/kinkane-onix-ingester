import cron from 'node-cron';
import { config } from '../config';
import { ingestionService } from '../services/ingestion.service';
import { coverService } from '../services/cover.service';
import { excerptService } from '../services/excerpt.service';
import { gardnersInventoryService } from '../services/gardners/gardners-inventory.service';
import { gardnersBiblioService } from '../services/gardners/biblio.service';
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

  // ── Excerpt sync ─────────────────────────────────────────────────────────
  const excerptSchedule = config.cron.excerptSyncSchedule;
  if (!cron.validate(excerptSchedule)) {
    throw new Error(`Invalid excerpt sync cron schedule: ${excerptSchedule}`);
  }

  cron.schedule(excerptSchedule, async () => {
    logger.info('Starting excerpt sync cron');
    try {
      await excerptService.syncExcerpts();
    } catch (err) {
      logger.error('Excerpt sync cron failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Excerpt sync scheduled', { schedule: excerptSchedule });

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

  // ── Gardners Bespoke Inventory poll ──────────────────────────────────────
  // Highest-priority Gardners feed — daily price/stock snapshot from the
  // dedicated edi.gardners.com account. See project memory / plan for the
  // full Gardners feed rollout; this is the first of 8 feeds being wired up.
  const gardnersInventorySchedule = config.gardners.cron.inventorySchedule;
  if (!cron.validate(gardnersInventorySchedule)) {
    throw new Error(`Invalid Gardners Inventory cron schedule: ${gardnersInventorySchedule}`);
  }

  cron.schedule(gardnersInventorySchedule, async () => {
    logger.info('Polling Gardners Bespoke Inventory feed');
    try {
      await gardnersInventoryService.sync();
    } catch (err) {
      logger.error('Gardners Inventory poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners Inventory poll scheduled', { schedule: gardnersInventorySchedule });

  // ── Gardners Biblio ONIX delta poll ──────────────────────────────────────
  // The full catalogue reload (gardnersBiblioService.syncFull) is NOT wired
  // to a cron — it's a rare, expensive (~1.7GB) operation triggered
  // manually via the admin API when an initial/re-sync load is needed.
  const gardnersBiblioSchedule = config.gardners.cron.biblioDeltaSchedule;
  if (!cron.validate(gardnersBiblioSchedule)) {
    throw new Error(`Invalid Gardners Biblio delta cron schedule: ${gardnersBiblioSchedule}`);
  }

  cron.schedule(gardnersBiblioSchedule, async () => {
    logger.info('Polling Gardners Biblio ONIX delta feed');
    try {
      await gardnersBiblioService.syncDelta();
    } catch (err) {
      logger.error('Gardners Biblio delta poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners Biblio delta poll scheduled', { schedule: gardnersBiblioSchedule });
}
