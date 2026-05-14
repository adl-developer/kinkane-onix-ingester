import { Worker, Job } from 'bullmq';
import { eq } from 'drizzle-orm';
import { redis, chunkQueue } from '../queue';
import { db } from '../db';
import { ingestionJobs, ingestionChunks } from '../db/schema';
import { storageService } from '../services/storage.service';
import { parseOnixStream } from '../services/parser.service';
import { config } from '../config';
import { logger } from '../lib/logger';
import { FileJobData, FileJobResult } from '../types/queue';

async function processFileJob(job: Job<FileJobData>): Promise<FileJobResult> {
  const { ingestionJobId, fileKey } = job.data;

  await db
    .update(ingestionJobs)
    .set({ status: 'processing', startedAt: new Date(), updatedAt: new Date() })
    .where(eq(ingestionJobs.id, ingestionJobId));

  let chunkIndex = 0;
  let totalBooks = 0;

  try {
    const stream = await storageService.getFileStream(fileKey);

    for await (const batch of parseOnixStream(stream, config.ingestion.chunkSize)) {
      // Store book data in PostgreSQL — keeps Redis payload tiny (~100 bytes vs ~4 MB)
      const [chunk] = await db
        .insert(ingestionChunks)
        .values({
          jobId: ingestionJobId,
          chunkIndex,
          bookCount: batch.length,
          status: 'pending',
          data: batch,
        })
        .returning({ id: ingestionChunks.id });

      const bullJob = await chunkQueue.add(`chunk-${ingestionJobId}-${chunkIndex}`, {
        ingestionJobId,
        chunkId: chunk.id,
        chunkIndex,
      });

      await db
        .update(ingestionChunks)
        .set({ bullJobId: String(bullJob.id), updatedAt: new Date() })
        .where(eq(ingestionChunks.id, chunk.id));

      totalBooks += batch.length;
      chunkIndex++;

      await job.updateProgress(Math.round((chunkIndex / (chunkIndex + 1)) * 50));
    }

    await db
      .update(ingestionJobs)
      .set({
        status: 'enqueued',
        totalChunks: chunkIndex,
        totalBooks,
        updatedAt: new Date(),
      })
      .where(eq(ingestionJobs.id, ingestionJobId));

    return { totalChunks: chunkIndex, totalBooks };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await db
      .update(ingestionJobs)
      .set({ status: 'failed', errorMessage: message, updatedAt: new Date() })
      .where(eq(ingestionJobs.id, ingestionJobId));
    throw err;
  }
}

export function startFileWorker(): Worker<FileJobData, FileJobResult> {
  const worker = new Worker<FileJobData, FileJobResult>('onix-file', processFileJob, {
    connection: redis,
    concurrency: 1,
  });

  worker.on('completed', (job, result) => {
    logger.info('File job completed', {
      worker: 'file',
      bullJobId: job.id,
      totalChunks: result.totalChunks,
      totalBooks: result.totalBooks,
    });
  });

  worker.on('failed', (job, err) => {
    logger.error('File job failed', { worker: 'file', bullJobId: job?.id, error: err.message });
  });

  return worker;
}
