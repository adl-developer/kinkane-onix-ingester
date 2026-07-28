import { logger } from '../../lib/logger';
import { gardnersGardbibService } from './gardbib.service';
import { gardnersInventoryService } from './gardners-inventory.service';
import { gardnersPromotionsService } from './gardners-promotions.service';
import { gardnersFirmSaleService } from './gardners-firm-sale.service';
import { gardnersIsbnSlipsService } from './gardners-isbn-slips.service';
import { gardnersMarketRestrictionsService } from './gardners-market-restrictions.service';
import { gardnersAvail13Service } from './gardners-avail13.service';

// Retained so an existing caller passing the old cover-tuning fields still
// gets a 202 rather than a 400 — bootstrap no longer runs covers at all, so
// the values are ignored. Use POST /gardners/covers/backfill instead.
export interface GardnersBootstrapOptions {
  coverBatchSize?: number;
  coverConcurrency?: number;
}

/**
 * One-shot bootstrap for a fresh database: pulls the full Gardners catalogue
 * (bibliographic data, stock/pricing, promotions, firm-sale flags, ISBN
 * redirects, market restrictions, and hourly availability) instead of
 * waiting for each feed's normal cron cadence to slowly catch up. Intended
 * to be run via POST /gardners/bootstrap, not on a schedule — every feed it
 * touches already has its own cron for ongoing updates (see cron/index.ts).
 *
 * Books come from GARDBIB (~1.98M records), not the ONIX full file. The ONIX
 * 3.1 feed only carries ~1.0M products — verified live 2026-07-27 by
 * counting <Product> elements in the landed full file — and is missing ~980K
 * titles Gardners actually stocks. See gardbib.service.ts.
 *
 * Two things deliberately do NOT happen here, because either would put the
 * run into the tens-of-hours range:
 *   - Embeddings. New books land with embedded_at NULL for the existing
 *     embedding backfill to pick up; vector search and recommendations lag
 *     behind text search for those titles until it catches up.
 *   - Cover images. They keep running on their own daily cron, and
 *     POST /gardners/covers/backfill still forces a full run on demand.
 *
 * Every feed is ISBN-keyed with a nullable bookId FK backfilled after the
 * fact, so the CSV feeds are fired off in parallel with the book load rather
 * than waiting on it.
 */
async function runFullBootstrap(_options: GardnersBootstrapOptions = {}): Promise<void> {
  logger.info('Gardners bootstrap: starting');

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

  // The long pole — full catalogue book load. Runs while the CSV feeds above
  // are still working through their BullMQ chunks.
  const gardbib = await gardnersGardbibService.syncFullCatalogue();

  logger.info('Gardners bootstrap: complete', {
    booksInserted: gardbib.booksInserted,
    skippedExisting: gardbib.skippedExisting,
    minutes: (gardbib.durationMs / 60000).toFixed(1),
  });
}

export const gardnersBootstrapService = { runFullBootstrap };
