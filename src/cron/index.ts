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
import { runCronTick } from './run-tick';
import { logger } from '../lib/logger';

/**
 * Validates a schedule and registers the tick, which is always wrapped in
 * runCronTick so it logs a start line, a completion line with duration, and
 * can never escape as an unhandled rejection.
 */
function schedule(job: string, expression: string, fn: () => Promise<unknown>): void {
  if (!cron.validate(expression)) {
    throw new Error(`Invalid cron schedule for ${job}: ${expression}`);
  }

  cron.schedule(expression, () => runCronTick(job, fn));
  logger.info('Cron registered', { job, schedule: expression });
}

export function startCron(): void {
  // ── R2 poll ───────────────────────────────────────────────────────────────
  schedule('r2-poll', config.cron.r2PollSchedule, async () => {
    const unprocessed = await ingestionService.listUnprocessedR2Files();

    if (unprocessed.length === 0) {
      return { filesFound: 0 };
    }

    logger.info('New ONIX files found', { count: unprocessed.length });

    for (const fileKey of unprocessed) {
      const result = await ingestionService.triggerIngestion(fileKey);
      logger.info('Enqueued file for ingestion', { fileKey, jobId: result.jobId });
    }

    // Enqueued, not ingested — the file-level result lands later as
    // 'ONIX file ingestion complete' from the chunk worker.
    return { filesFound: unprocessed.length, filesEnqueued: unprocessed.length };
  });

  // ── Cover fetch ───────────────────────────────────────────────────────────
  // Gardners runs first — higher authority, ~99% catalogue coverage.
  // Google Books' own candidate query (coverUrl IS NULL) naturally only
  // picks up whatever Gardners didn't find moments earlier in this same
  // tick, so the two need no other coordination.
  schedule('cover-fetch', config.cron.coverFetchSchedule, async () => {
    let gardnersProcessed = 0;

    if (config.gardners.ingestionEnabled) {
      // Kept inside its own try/catch: a Gardners failure must not skip the
      // Google Books fallback, which is the whole point of running both.
      try {
        gardnersProcessed = await gardnersCoverService.syncFullCatalogue();
      } catch (err) {
        logger.error('Gardners cover full-catalogue sync failed', {
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    await coverService.fetchMissingCovers();
    return { gardnersProcessed };
  });

  // ── Excerpt sync ─────────────────────────────────────────────────────────
  schedule('excerpt-sync', config.cron.excerptSyncSchedule, () => excerptService.syncExcerpts());

  // ── Failed chunk R2 cleanup ───────────────────────────────────────────────
  // Daily at 04:00 — deletes R2 payload files for failed chunks older than
  // 30 days. Successful chunks are cleaned up immediately by the worker.
  schedule('chunk-cleanup', '0 4 * * *', () => runFailedChunkCleanup());

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

  // Highest-priority Gardners feed — daily price/stock snapshot from the
  // dedicated edi.gardners.com account.
  schedule('gardners-inventory', config.gardners.cron.inventorySchedule, () =>
    gardnersInventoryService.sync(),
  );

  // The full catalogue reload (gardnersBiblioService.syncFull) is NOT wired
  // to a cron — it's a rare, expensive (~1.7GB) operation triggered manually
  // via the admin API when an initial/re-sync load is needed.
  schedule('gardners-biblio-delta', config.gardners.cron.biblioDeltaSchedule, () =>
    gardnersBiblioService.syncDelta(),
  );

  schedule('gardners-promotions', config.gardners.cron.promotionsSchedule, () =>
    gardnersPromotionsService.sync(),
  );

  schedule('gardners-isbn-slips', config.gardners.cron.isbnSlipsSchedule, () =>
    gardnersIsbnSlipsService.sync(),
  );

  schedule('gardners-firm-sale', config.gardners.cron.firmSaleSchedule, () =>
    gardnersFirmSaleService.sync(),
  );

  // Regions and restrictions share a schedule — REGIONS.CSV is tiny and
  // cheap to check every time regardless of how rarely it actually changes.
  schedule(
    'gardners-market-restrictions',
    config.gardners.cron.marketRestrictionsSchedule,
    async () => {
      await gardnersMarketRestrictionsService.syncRegions();
      await gardnersMarketRestrictionsService.syncRestrictions();
    },
  );

  // Thin addition on top of gardners_stock (already populated by Inventory)
  // — see upsertStockRows's doc comment for how the two feeds coexist.
  schedule('gardners-avail13', config.gardners.cron.avail13Schedule, () =>
    gardnersAvail13Service.sync(),
  );

  // syncFullCatalogue() (backfill for existing books) runs as part of the
  // main cover-fetch cron above, ahead of the Google Books fallback. This
  // one handles new/changed covers from Gardners' own weekly zip bundles.
  schedule('gardners-covers-update', config.gardners.cron.coversUpdateSchedule, () =>
    gardnersCoverService.syncWeeklyUpdates(),
  );
}
