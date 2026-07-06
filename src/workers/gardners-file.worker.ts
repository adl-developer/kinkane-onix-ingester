import { createReadStream } from 'fs';
import { unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { Worker, Job } from 'bullmq';
import { redis, gardnersChunkQueue } from '../queue';
import { gardnersFetcher } from '../services/gardners/fetcher.service';
import { parseGardnersCsv, ParseGardnersCsvOptions } from '../services/gardners/csv.service';
import { mapInventoryRow } from '../services/gardners/gardners-inventory.service';
import { storageService } from '../services/storage.service';
import { logger } from '../lib/logger';
import { GardnersFileJobData, GardnersFileJobResult } from '../types/queue';
import { NewGardnersStock } from '../db/schema';

// One case per feed type — matches the plan's "one worker, feed
// discriminator" design. Only 'inventory' exists so far; Promotions,
// Firmsale, isbn-slips, and mkres add more cases here later, each reusing
// the same stream -> parse -> R2-chunk -> enqueue flow.
function buildCsvOptions(
  job: Job<GardnersFileJobData>,
): ParseGardnersCsvOptions<NewGardnersStock> {
  const { feed, file } = job.data;

  switch (feed) {
    case 'inventory':
      return {
        framed: true,
        mapRow: (record) =>
          mapInventoryRow(record, {
            sourceFileKey: file.path,
            stockUpdatedAt: file.modifiedAt ?? new Date(),
          }),
      };
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
    if (summary.trailerCount !== null && summary.trailerCount !== summary.totalRows + summary.skippedRows) {
      logger.warn('Gardners feed row count does not match TRAILER count', {
        feed,
        file: file.filename,
        trailerCount: summary.trailerCount,
        totalRows: summary.totalRows,
        skippedRows: summary.skippedRows,
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
