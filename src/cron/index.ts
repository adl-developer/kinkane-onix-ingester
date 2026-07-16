import cron from 'node-cron';
import { config } from '../config';
import { ingestionService } from '../services/ingestion.service';
import { coverService } from '../services/cover.service';
import { excerptService } from '../services/excerpt.service';
import { gardnersInventoryService } from '../services/gardners/gardners-inventory.service';
import { gardnersBiblioService } from '../services/gardners/biblio.service';
import { gardnersPromotionsService } from '../services/gardners/gardners-promotions.service';
import { gardnersFirmSaleService } from '../services/gardners/gardners-firm-sale.service';
import { gardnersIsbnSlipsService } from '../services/gardners/gardners-isbn-slips.service';
import { gardnersMarketRestrictionsService } from '../services/gardners/gardners-market-restrictions.service';
import { gardnersAvail13Service } from '../services/gardners/gardners-avail13.service';
import { gardnersCoverService } from '../services/gardners/gardners-cover-sync.service';
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
    // Gardners runs first — higher authority, ~99% catalogue coverage.
    // Google Books' own candidate query (coverUrl IS NULL) naturally only
    // picks up whatever Gardners didn't find moments earlier in this same
    // tick, so the two need no other coordination.
    if (config.gardners.ingestionEnabled) {
      try {
        await gardnersCoverService.syncFullCatalogue();
      } catch (err) {
        logger.error('Gardners cover full-catalogue sync failed', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
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

  // ── Gardners feed crons ───────────────────────────────────────────────────
  // Gated behind GARDNERS_INGESTION_ENABLED — pulling the full catalogue
  // (~2M rows + cover images) will overflow a database not provisioned for
  // it. Leave this off until running against one that is.
  if (!config.gardners.ingestionEnabled) {
    logger.warn(
      'Gardners ingestion disabled (GARDNERS_INGESTION_ENABLED is not "true") — skipping all Gardners feed crons',
    );
    return;
  }

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

  // ── Gardners Promotions poll ─────────────────────────────────────────────
  const gardnersPromotionsSchedule = config.gardners.cron.promotionsSchedule;
  if (!cron.validate(gardnersPromotionsSchedule)) {
    throw new Error(`Invalid Gardners Promotions cron schedule: ${gardnersPromotionsSchedule}`);
  }

  cron.schedule(gardnersPromotionsSchedule, async () => {
    logger.info('Polling Gardners Promotions feed');
    try {
      await gardnersPromotionsService.sync();
    } catch (err) {
      logger.error('Gardners Promotions poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners Promotions poll scheduled', { schedule: gardnersPromotionsSchedule });

  // ── Gardners isbn-slips poll ─────────────────────────────────────────────
  const gardnersIsbnSlipsSchedule = config.gardners.cron.isbnSlipsSchedule;
  if (!cron.validate(gardnersIsbnSlipsSchedule)) {
    throw new Error(`Invalid Gardners isbn-slips cron schedule: ${gardnersIsbnSlipsSchedule}`);
  }

  cron.schedule(gardnersIsbnSlipsSchedule, async () => {
    logger.info('Polling Gardners isbn-slips feed');
    try {
      await gardnersIsbnSlipsService.sync();
    } catch (err) {
      logger.error('Gardners isbn-slips poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners isbn-slips poll scheduled', { schedule: gardnersIsbnSlipsSchedule });

  // ── Gardners Firm Sale poll ──────────────────────────────────────────────
  const gardnersFirmSaleSchedule = config.gardners.cron.firmSaleSchedule;
  if (!cron.validate(gardnersFirmSaleSchedule)) {
    throw new Error(`Invalid Gardners Firm Sale cron schedule: ${gardnersFirmSaleSchedule}`);
  }

  cron.schedule(gardnersFirmSaleSchedule, async () => {
    logger.info('Polling Gardners Firm Sale feed');
    try {
      await gardnersFirmSaleService.sync();
    } catch (err) {
      logger.error('Gardners Firm Sale poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners Firm Sale poll scheduled', { schedule: gardnersFirmSaleSchedule });

  // ── Gardners market restrictions + regions poll ──────────────────────────
  // Both run on the same schedule — REGIONS.CSV is tiny and cheap to check
  // every time regardless of how rarely it actually changes.
  const gardnersMarketRestrictionsSchedule = config.gardners.cron.marketRestrictionsSchedule;
  if (!cron.validate(gardnersMarketRestrictionsSchedule)) {
    throw new Error(
      `Invalid Gardners market restrictions cron schedule: ${gardnersMarketRestrictionsSchedule}`,
    );
  }

  cron.schedule(gardnersMarketRestrictionsSchedule, async () => {
    logger.info('Polling Gardners market restrictions + regions feeds');
    try {
      await gardnersMarketRestrictionsService.syncRegions();
      await gardnersMarketRestrictionsService.syncRestrictions();
    } catch (err) {
      logger.error('Gardners market restrictions poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners market restrictions poll scheduled', {
    schedule: gardnersMarketRestrictionsSchedule,
  });

  // ── Gardners Avail13 hourly stock poll ───────────────────────────────────
  // Thin addition on top of gardners_stock (already populated by Inventory)
  // — see upsertStockRows's doc comment for how the two feeds coexist.
  const gardnersAvail13Schedule = config.gardners.cron.avail13Schedule;
  if (!cron.validate(gardnersAvail13Schedule)) {
    throw new Error(`Invalid Gardners Avail13 cron schedule: ${gardnersAvail13Schedule}`);
  }

  cron.schedule(gardnersAvail13Schedule, async () => {
    logger.info('Polling Gardners Avail13 feed');
    try {
      await gardnersAvail13Service.sync();
    } catch (err) {
      logger.error('Gardners Avail13 poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners Avail13 poll scheduled', { schedule: gardnersAvail13Schedule });

  // ── Gardners cover image weekly update poll ──────────────────────────────
  // syncFullCatalogue() (backfill for existing books) runs as part of the
  // main cover-fetch cron above, ahead of the Google Books fallback. This
  // one handles new/changed covers from Gardners' own weekly zip bundles.
  const gardnersCoversUpdateSchedule = config.gardners.cron.coversUpdateSchedule;
  if (!cron.validate(gardnersCoversUpdateSchedule)) {
    throw new Error(`Invalid Gardners covers update cron schedule: ${gardnersCoversUpdateSchedule}`);
  }

  cron.schedule(gardnersCoversUpdateSchedule, async () => {
    logger.info('Polling Gardners cover update feed');
    try {
      await gardnersCoverService.syncWeeklyUpdates();
    } catch (err) {
      logger.error('Gardners cover update poll failed', {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  logger.info('Gardners covers update poll scheduled', { schedule: gardnersCoversUpdateSchedule });
}
