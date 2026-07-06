import { Worker, Job } from 'bullmq';
import { redis } from '../queue';
import { gardnersFetcher } from '../services/gardners/fetcher.service';
import { gardnersUpserts } from '../services/gardners/upserts.service';
import { storageService } from '../services/storage.service';
import { logger } from '../lib/logger';
import { GardnersChunkJobData, GardnersChunkJobResult } from '../types/queue';
import { NewGardnersStock } from '../db/schema';

// One case per feed type, mirroring gardners-file.worker.ts's dispatch.
async function upsertChunk(
  feed: GardnersChunkJobData['feed'],
  chunkKey: string,
): Promise<{ processed: number; failed: number }> {
  switch (feed) {
    case 'inventory': {
      const rows = await storageService.getJson<NewGardnersStock[]>(chunkKey);
      // JSON round-tripping through R2 turns Date fields into plain strings —
      // revive them before handing off to Drizzle, whose timestamp column
      // binding expects an actual Date instance.
      const revived = rows.map((r) => ({ ...r, stockUpdatedAt: new Date(r.stockUpdatedAt) }));
      const result = await gardnersUpserts.upsertStockRows(revived);
      // Scoped to this batch's ISBNs only — see backfillStockBookIds's doc
      // comment for why this isn't a full-table catch-up.
      await gardnersUpserts.backfillStockBookIds(revived.map((r) => r.isbn13));
      return result;
    }
    default:
      throw new Error(`Unsupported Gardners feed for chunk worker: ${feed}`);
  }
}

async function processGardnersChunkJob(
  job: Job<GardnersChunkJobData>,
): Promise<GardnersChunkJobResult> {
  const { feed, logId, chunkKey } = job.data;

  const { processed, failed } = await upsertChunk(feed, chunkKey);

  // Only delete the R2 payload on full success — on failure it's preserved
  // for debugging, matching chunk.worker.ts's ONIX equivalent.
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

  await gardnersFetcher.incrementProcessedChunks(logId);

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
