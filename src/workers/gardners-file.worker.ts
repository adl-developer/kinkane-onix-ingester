import { createReadStream } from 'fs';
import { unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { Worker, Job } from 'bullmq';
import { redis, gardnersChunkQueue } from '../queue';
import { gardnersFetcher } from '../services/gardners/fetcher.service';
import { parseGardnersCsv, ParseGardnersCsvOptions } from '../services/gardners/csv.service';
import { mapInventoryRow } from '../services/gardners/gardners-inventory.service';
import {
  mapPromotionRow,
  PROMOTIONS_COLUMNS,
} from '../services/gardners/gardners-promotions.service';
import {
  mapFirmSaleRow,
  FIRM_SALE_COLUMNS,
} from '../services/gardners/gardners-firm-sale.service';
import {
  mapIsbnSlipRow,
  ISBN_SLIPS_COLUMNS,
} from '../services/gardners/gardners-isbn-slips.service';
import {
  mapRestrictionRow,
  MARKET_RESTRICTIONS_COLUMNS,
} from '../services/gardners/gardners-market-restrictions.service';
import {
  mapAvail13Row,
  AVAIL13_COLUMNS,
  avail13SourceForFilename,
} from '../services/gardners/gardners-avail13.service';
import { storageService } from '../services/storage.service';
import { logger } from '../lib/logger';
import { GardnersFileJobData, GardnersFileJobResult } from '../types/queue';

// One case per feed type — matches the plan's "one worker, feed
// discriminator" design. The file worker itself doesn't care about the row
// shape (it just batches whatever mapRow returns into R2 JSON chunks), so
// this returns ParseGardnersCsvOptions<unknown> rather than committing to
// one feed's row type.
function buildCsvOptions(job: Job<GardnersFileJobData>): ParseGardnersCsvOptions<unknown> {
  const { feed, file } = job.data;
  const ctx = { sourceFileKey: file.path, syncedAt: file.modifiedAt ?? new Date() };

  switch (feed) {
    case 'inventory':
      return {
        framed: true,
        mapRow: (record) =>
          mapInventoryRow(record, { sourceFileKey: ctx.sourceFileKey, stockUpdatedAt: ctx.syncedAt }),
      };
    case 'promotions':
      return {
        framed: false,
        columns: PROMOTIONS_COLUMNS,
        mapRow: (record) => mapPromotionRow(record, ctx),
      };
    case 'firm_sale':
      return {
        framed: false,
        columns: FIRM_SALE_COLUMNS,
        mapRow: (record) => mapFirmSaleRow(record, ctx),
      };
    case 'isbn_slips':
      return {
        framed: false,
        columns: ISBN_SLIPS_COLUMNS,
        mapRow: (record) => mapIsbnSlipRow(record, ctx),
      };
    case 'market_restrictions':
      return {
        framed: false,
        columns: MARKET_RESTRICTIONS_COLUMNS,
        mapRow: (record) => mapRestrictionRow(record, ctx),
      };
    case 'avail13': {
      const source = avail13SourceForFilename(file.filename);
      return {
        framed: false,
        columns: AVAIL13_COLUMNS,
        mapRow: (record) =>
          mapAvail13Row(record, { sourceFileKey: ctx.sourceFileKey, stockUpdatedAt: ctx.syncedAt, source }),
      };
    }
    default:
      throw new Error(`Unsupported Gardners feed for file worker: ${feed}`);
  }
}

async function processGardnersFileJob(
  job: Job<GardnersFileJobData>,
): Promise<GardnersFileJobResult> {
  const { feed, connection, file, logId } = job.data;
  const localPath = join(tmpdir(), `gardners-${feed}-${logId}.txt`);

  let chunkIndex = 0;

  try {
    // fastGet (concurrent requests) rather than a plain stream — see
    // downloadToLocalFile's doc comment. A single-request-at-a-time stream
    // read of this file took ~19 minutes in testing; fastGet-based transfer
    // is bandwidth- rather than latency-bound.
    await gardnersFetcher.downloadToLocalFile(connection, file, localPath);
    const stream = createReadStream(localPath);
    const csvOptions = buildCsvOptions(job);
    const generator = parseGardnersCsv(stream, csvOptions, 1000);

    let next = await generator.next();
    while (!next.done) {
      const batch = next.value;
      const chunkKey = `chunks/gardners/${feed}/${logId}/${chunkIndex}.json`;
      await storageService.uploadJson(chunkKey, batch);

      await gardnersChunkQueue.add(`gardners-chunk-${feed}-${logId}-${chunkIndex}`, {
        feed,
        logId,
        chunkKey,
        chunkIndex,
      });

      chunkIndex++;
      await job.updateProgress(Math.round((chunkIndex / (chunkIndex + 1)) * 50));
      next = await generator.next();
    }

    const summary = next.value;
    const totalLinesSeen = summary.totalRows + summary.skippedRows + summary.parseErrorRows;
    if (summary.trailerCount !== null && summary.trailerCount !== totalLinesSeen) {
      logger.warn('Gardners feed row count does not match TRAILER count', {
        feed,
        file: file.filename,
        trailerCount: summary.trailerCount,
        totalRows: summary.totalRows,
        skippedRows: summary.skippedRows,
        parseErrorRows: summary.parseErrorRows,
      });
    }
    if (summary.parseErrorRows > 0) {
      logger.warn('Gardners feed had unparseable rows', {
        feed,
        file: file.filename,
        parseErrorRows: summary.parseErrorRows,
      });
    }

    if (chunkIndex === 0) {
      // No data rows at all — nothing for a chunk worker to complete, so
      // resolve the fetch log directly instead of waiting on chunks that
      // will never run.
      await gardnersFetcher.markFetchCompleted(logId, { rowCount: summary.totalRows });
    } else {
      await gardnersFetcher.setChunkingComplete(logId, {
        totalChunks: chunkIndex,
        rowCount: summary.totalRows,
      });
    }

    return { totalChunks: chunkIndex, totalRows: summary.totalRows };
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    await gardnersFetcher.markFetchFailed(logId, error);
    throw error;
  } finally {
    await unlink(localPath).catch(() => undefined);
  }
}

export function startGardnersFileWorker(): Worker<GardnersFileJobData, GardnersFileJobResult> {
  const worker = new Worker<GardnersFileJobData, GardnersFileJobResult>(
    'gardners-file',
    processGardnersFileJob,
    { connection: redis, concurrency: 1 },
  );

  worker.on('completed', (job, result) => {
    logger.info('Gardners file job completed', {
      worker: 'gardners-file',
      bullJobId: job.id,
      feed: job.data.feed,
      totalChunks: result.totalChunks,
      totalRows: result.totalRows,
    });
  });

  worker.on('failed', (job, err) => {
    logger.error('Gardners file job failed', {
      worker: 'gardners-file',
      bullJobId: job?.id,
      feed: job?.data.feed,
      error: err.message,
    });
  });

  return worker;
}
