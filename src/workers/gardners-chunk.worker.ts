import { Worker, Job } from 'bullmq';
import { redis } from '../queue';
import { gardnersFetcher } from '../services/gardners/fetcher.service';
import { gardnersUpserts } from '../services/gardners/upserts.service';
import { storageService } from '../services/storage.service';
import { logger } from '../lib/logger';
import { GardnersChunkJobData, GardnersChunkJobResult } from '../types/queue';
import {
  NewGardnersStock,
  NewGardnersPromotion,
  NewGardnersFirmSale,
  NewGardnersIsbnSlip,
  NewGardnersMarketRestriction,
} from '../db/schema';

// JSON round-tripping through R2 turns Date fields into plain strings —
// every Gardners row carries at least one Date field (stockUpdatedAt or
// syncedAt) that needs reviving before Drizzle's timestamp column binding,
// which expects an actual Date instance (see upserts.service.ts's Step 2
// bug writeup for the "value.toISOString is not a function" failure mode).
function reviveDate<T extends Record<string, unknown>>(row: T, field: keyof T): T {
  return { ...row, [field]: new Date(row[field] as string) };
}

// One case per feed type, mirroring gardners-file.worker.ts's dispatch.
async function upsertChunk(
  feed: GardnersChunkJobData['feed'],
  chunkKey: string,
): Promise<{ processed: number; failed: number }> {
  switch (feed) {
    case 'inventory':
    case 'avail13': {
      // Both feeds write to gardners_stock via the same source-aware
      // upsert (see upsertStockRows's doc comment) — Avail13 rows carry
      // null price/discount/report fields, so those columns are gated on
      // source = 'inventory' inside the upsert itself, not here.
      const rows = await storageService.getJson<NewGardnersStock[]>(chunkKey);
      const revived = rows.map((r) => reviveDate(r, 'stockUpdatedAt'));
      const result = await gardnersUpserts.upsertStockRows(revived);
      // Scoped to this batch's ISBNs only — see backfillStockBookIds's doc
      // comment for why this isn't a full-table catch-up.
      await gardnersUpserts.backfillStockBookIds(revived.map((r) => r.isbn13));
      return result;
    }
    case 'promotions': {
      const rows = await storageService.getJson<NewGardnersPromotion[]>(chunkKey);
      const revived = rows.map((r) => reviveDate(r, 'syncedAt'));
      const result = await gardnersUpserts.upsertPromotionRows(revived);
      await gardnersUpserts.backfillPromotionsBookIds(revived.map((r) => r.isbn13));
      return result;
    }
    case 'firm_sale': {
      const rows = await storageService.getJson<NewGardnersFirmSale[]>(chunkKey);
      const revived = rows.map((r) => reviveDate(r, 'syncedAt'));
      const result = await gardnersUpserts.upsertFirmSaleRows(revived);
      await gardnersUpserts.backfillFirmSaleBookIds(revived.map((r) => r.isbn13));
      return result;
    }
    case 'isbn_slips': {
      // No book_id column on gardners_isbn_slips — nothing to backfill.
      const rows = await storageService.getJson<NewGardnersIsbnSlip[]>(chunkKey);
      return gardnersUpserts.upsertIsbnSlipRows(rows.map((r) => reviveDate(r, 'syncedAt')));
    }
    case 'market_restrictions': {
      const rows = await storageService.getJson<NewGardnersMarketRestriction[]>(chunkKey);
      const revived = rows.map((r) => reviveDate(r, 'syncedAt'));
      const result = await gardnersUpserts.upsertRestrictionRows(revived);
      await gardnersUpserts.backfillMarketRestrictionsBookIds(revived.map((r) => r.isbn13));
      return result;
    }
    default:
      throw new Error(`Unsupported Gardners feed for chunk worker: ${feed}`);
  }
}

// Full-replace feeds delete anything this run didn't touch, exactly once,
// right after the chunk that completes the file. 'inventory' isn't a
// full-replace feed (both Inventory and Avail13 write to gardners_stock),
// so it's deliberately absent here — nothing to sweep.
async function sweepIfFullReplaceFeed(
  feed: GardnersChunkJobData['feed'],
  cutoff: Date,
): Promise<void> {
  try {
    switch (feed) {
      case 'promotions':
        return await gardnersUpserts.sweepStalePromotions(cutoff);
      case 'firm_sale':
        return await gardnersUpserts.sweepStaleFirmSale(cutoff);
      case 'isbn_slips':
        return await gardnersUpserts.sweepStaleIsbnSlips(cutoff);
      case 'market_restrictions':
        return await gardnersUpserts.sweepStaleMarketRestrictions(cutoff);
      default:
        return;
    }
  } catch (err) {
    logger.error('Gardners mark-and-sweep failed', {
      feed,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

async function processGardnersChunkJob(
  job: Job<GardnersChunkJobData>,
): Promise<GardnersChunkJobResult> {
  const { feed, logId, chunkKey } = job.data;

  const { processed, failed } = await upsertChunk(feed, chunkKey);

  const { isComplete, syncedAt } = await gardnersFetcher.incrementProcessedChunks(logId);
  if (isComplete) {
    await sweepIfFullReplaceFeed(feed, syncedAt);
  }

  // Only delete the R2 payload on full success, and only after the chunk's
  // completion is durably recorded above. Deleting first meant a DB failure
  // afterward (e.g. a connection-pool timeout acquiring a connection, before
  // the query ever reached the database — safe to retry from scratch) left
  // an unretryable job: BullMQ would retry it, but the payload it needs to
  // re-upsert from was already gone, failing identically ("specified key
  // does not exist") on every subsequent attempt until retries ran out. On
  // failure the payload is preserved for debugging, matching
  // chunk.worker.ts's ONIX equivalent.
  if (failed === 0) {
    try {
      await storageService.deleteObject(chunkKey);
    } catch (err) {
      logger.warn('Failed to delete Gardners chunk R2 file after processing', {
        worker: 'gardners-chunk',
        chunkKey,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  return { processedRows: processed, failedRows: failed };
}

export function startGardnersChunkWorker(
  concurrency = 5,
): Worker<GardnersChunkJobData, GardnersChunkJobResult> {
  const worker = new Worker<GardnersChunkJobData, GardnersChunkJobResult>(
    'gardners-chunk',
    processGardnersChunkJob,
    { connection: redis, concurrency },
  );

  worker.on('completed', (job, result) => {
    logger.info('Gardners chunk job completed', {
      worker: 'gardners-chunk',
      bullJobId: job.id,
      feed: job.data.feed,
      processedRows: result.processedRows,
      failedRows: result.failedRows,
    });
  });

  worker.on('failed', (job, err) => {
    logger.error('Gardners chunk job failed', {
      worker: 'gardners-chunk',
      bullJobId: job?.id,
      feed: job?.data.feed,
      error: err.message,
    });
  });

  return worker;
}
