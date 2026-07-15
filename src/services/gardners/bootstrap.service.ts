import { ingestionService } from '../ingestion.service';
import { logger } from '../../lib/logger';
import { gardnersBiblioService } from './biblio.service';
import { gardnersInventoryService } from './gardners-inventory.service';
import { gardnersPromotionsService } from './gardners-promotions.service';
import { gardnersFirmSaleService } from './gardners-firm-sale.service';
import { gardnersIsbnSlipsService } from './gardners-isbn-slips.service';
import { gardnersMarketRestrictionsService } from './gardners-market-restrictions.service';
import { gardnersAvail13Service } from './gardners-avail13.service';
import { gardnersCoverService } from './gardners-cover-sync.service';

export interface GardnersBootstrapOptions {
  // Overrides for the cover backfill loop only — the day-to-day defaults
  // (config.gardners.coverSync) are sized for a once-a-day cron tick, not
  // for catching up ~2M titles in one run.
  coverBatchSize?: number;
  coverDelayMs?: number;
}

async function waitForIngestionJob(jobId: number, pollMs = 15_000): Promise<string> {
  for (;;) {
    const job = await ingestionService.getJob(jobId);
    if (!job) throw new Error(`Ingestion job ${jobId} disappeared`);
    if (job.status === 'completed' || job.status === 'failed') return job.status;
    await new Promise((res) => setTimeout(res, pollMs));
  }
}

/**
 * One-shot bootstrap for a fresh database: pulls the full Gardners catalogue
 * (ONIX bibliographic data, stock/pricing, promotions, firm-sale flags,
 * ISBN redirects, market restrictions, hourly availability, and cover
 * images) instead of waiting for each feed's normal cron cadence to slowly
 * catch up. Intended to be run once via POST /gardners/bootstrap, not on a
 * schedule — every feed it touches already has its own cron for ongoing
 * updates (see cron/index.ts).
 *
 * Ordering matters for one thing only: cover backfill needs books.isbn13
 * rows to exist, so it waits for the full ONIX ingestion to actually land
 * rows before starting. Every other feed is ISBN-keyed with a nullable
 * bookId FK that's backfilled after the fact, so they're fired off
 * immediately in parallel with no ordering dependency on ONIX or each
 * other.
 */
async function runFullBootstrap(options: GardnersBootstrapOptions = {}): Promise<void> {
  logger.info('Gardners bootstrap: starting');

  const r2Key = await gardnersBiblioService.syncFull();
  if (!r2Key) {
    logger.warn(
      'Gardners bootstrap: no full Biblio ONIX file found on the server — skipping book ingestion, other feeds will still run',
    );
  } else {
    const { jobId } = await ingestionService.triggerIngestion(r2Key);
    logger.info('Gardners bootstrap: biblio ingestion triggered, waiting for it to complete', {
      jobId,
      r2Key,
    });

    const status = await waitForIngestionJob(jobId);
    logger.info('Gardners bootstrap: biblio ingestion finished', { jobId, status });

    if (status === 'failed') {
      logger.error(
        'Gardners bootstrap: biblio ingestion failed — books.isbn13 rows may be incomplete, cover backfill will still run against whatever exists',
      );
    }
  }

  const feedResults = await Promise.allSettled([
    gardnersInventoryService.sync(),
    gardnersPromotionsService.sync(),
    gardnersFirmSaleService.sync(),
    gardnersIsbnSlipsService.sync(),
    gardnersMarketRestrictionsService.syncRegions(),
    gardnersMarketRestrictionsService.syncRestrictions(),
    gardnersAvail13Service.sync(),
  ]);
  const feedNames = [
    'inventory',
    'promotions',
    'firmSale',
    'isbnSlips',
    'marketRestrictions.regions',
    'marketRestrictions.restrictions',
    'avail13',
  ];
  feedResults.forEach((result, i) => {
    if (result.status === 'rejected') {
      logger.error('Gardners bootstrap: feed enqueue failed', {
        feed: feedNames[i],
        error: result.reason instanceof Error ? result.reason.message : String(result.reason),
      });
    }
  });
  logger.info('Gardners bootstrap: all CSV feeds enqueued (they run async via BullMQ)');

  logger.info(
    'Gardners bootstrap: starting cover backfill loop — this walks the entire catalogue and can take a long time at default batch sizes',
    { coverBatchSize: options.coverBatchSize, coverDelayMs: options.coverDelayMs },
  );

  let totalProcessed = 0;
  let batchCount = 0;
  for (;;) {
    const processed = await gardnersCoverService.syncFullCatalogue({
      batchSize: options.coverBatchSize,
      delayMs: options.coverDelayMs,
    });
    if (processed === 0) break;

    totalProcessed += processed;
    batchCount++;
    if (batchCount % 10 === 0) {
      logger.info('Gardners bootstrap: cover backfill progress', { totalProcessed, batchCount });
    }
  }

  logger.info('Gardners bootstrap: cover backfill complete', { totalProcessed, batchCount });

  // Also catch up any weekly cover-image update zips that have accumulated
  // — oversized ones (see MAX_UPDATE_ZIP_BYTES) are skipped automatically
  // and logged for manual handling.
  await gardnersCoverService.syncWeeklyUpdates();

  logger.info('Gardners bootstrap: complete');
}

export const gardnersBootstrapService = { runFullBootstrap };
