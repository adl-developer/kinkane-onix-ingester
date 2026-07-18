import { eq, desc, inArray, and, ne } from 'drizzle-orm';
import { db } from '../db';
import { ingestionJobs, ingestionChunks } from '../db/schema';
import { fileQueue, chunkQueue } from '../queue';
import { storageService } from './storage.service';

export const ingestionService = {
  async triggerIngestion(fileKey: string): Promise<{ jobId: number; bullJobId: string }> {
    const fileExists = await storageService.fileExists(fileKey);
    if (!fileExists) {
      throw Object.assign(new Error(`File not found in R2: ${fileKey}`), { statusCode: 404 });
    }

    // Prevent duplicate ingestion — reject if an active or completed job already exists
    const [existing] = await db
      .select({ id: ingestionJobs.id, status: ingestionJobs.status })
      .from(ingestionJobs)
      .where(and(eq(ingestionJobs.fileKey, fileKey), ne(ingestionJobs.status, 'failed')))
      .limit(1);

    if (existing) {
      throw Object.assign(
        new Error(
          `Job already exists for ${fileKey} (jobId=${existing.id}, status=${existing.status})`,
        ),
        { statusCode: 409, jobId: existing.id, status: existing.status },
      );
    }

    const [job] = await db
      .insert(ingestionJobs)
      .values({ fileKey, status: 'pending' })
      .returning({ id: ingestionJobs.id });

    const bullJob = await fileQueue.add(`ingest-${fileKey}`, {
      ingestionJobId: job.id,
      fileKey,
    });

    return { jobId: job.id, bullJobId: String(bullJob.id) };
  },

  async getJob(jobId: number) {
    const [job] = await db
      .select()
      .from(ingestionJobs)
      .where(eq(ingestionJobs.id, jobId));

    if (!job) return null;

    const chunks = await db
      .select()
      .from(ingestionChunks)
      .where(eq(ingestionChunks.jobId, jobId))
      .orderBy(ingestionChunks.chunkIndex);

    return { ...job, chunks };
  },

  async listJobs(limit = 20, offset = 0) {
    return db
      .select()
      .from(ingestionJobs)
      .orderBy(desc(ingestionJobs.createdAt))
      .limit(limit)
      .offset(offset);
  },

  /**
   * Re-enqueues every not-yet-completed chunk of a job onto THIS process's
   * queue. Exists for moving a job between environments with separate Redis
   * instances (e.g. a local run switching to a deployed one) — the BullMQ
   * queue itself doesn't transfer, but ingestion_chunks (Postgres) and each
   * chunk's R2 payload (dataKey, only cleared on success) do, since both
   * live in shared storage. Rebuilds the queue from that durable state
   * rather than assuming the old queue is reachable.
   *
   * Assumes whatever worker was previously processing this job has already
   * stopped — it does not attempt to detect or avoid a still-active worker
   * elsewhere, since there's no reliable way to check that from here.
   */
  async resumeIncompleteChunks(jobId: number): Promise<{ requeued: number; alreadyCompleted: number }> {
    const job = await this.getJob(jobId);
    if (!job) {
      throw Object.assign(new Error(`Ingestion job ${jobId} not found`), { statusCode: 404 });
    }

    const incomplete = job.chunks.filter((c) => c.status !== 'completed');
    const alreadyCompleted = job.chunks.length - incomplete.length;

    for (const chunk of incomplete) {
      if (!chunk.dataKey) {
        // Shouldn't happen (dataKey is only nulled on success), but skip
        // rather than enqueue a job with nothing to process.
        continue;
      }

      await db
        .update(ingestionChunks)
        .set({ status: 'pending', errorMessage: null, updatedAt: new Date() })
        .where(eq(ingestionChunks.id, chunk.id));

      const bullJob = await chunkQueue.add(`chunk-${jobId}-${chunk.chunkIndex}`, {
        ingestionJobId: jobId,
        chunkId: chunk.id,
        chunkIndex: chunk.chunkIndex,
      });

      await db
        .update(ingestionChunks)
        .set({ bullJobId: String(bullJob.id) })
        .where(eq(ingestionChunks.id, chunk.id));
    }

    // The job-level status may be stuck showing an unrelated earlier error
    // (e.g. from a since-fixed schema issue) even though chunks have been
    // completing fine since — 'processing' is accurate again now that work
    // has been re-enqueued.
    await db
      .update(ingestionJobs)
      .set({ status: 'processing', errorMessage: null, updatedAt: new Date() })
      .where(eq(ingestionJobs.id, jobId));

    return { requeued: incomplete.length, alreadyCompleted };
  },

  async listUnprocessedR2Files(): Promise<string[]> {
    const r2Files = await storageService.listOnixFiles();
    const fileKeys = r2Files.map((f) => f.key);

    if (fileKeys.length === 0) return [];

    // Only treat a file as "processed" if it has an active or completed job.
    // Files whose only jobs are failed can be re-triggered.
    const activeOrDone = await db
      .select({ fileKey: ingestionJobs.fileKey })
      .from(ingestionJobs)
      .where(
        and(
          inArray(ingestionJobs.fileKey, fileKeys),
          ne(ingestionJobs.status, 'failed'),
        ),
      );

    const blockList = new Set(activeOrDone.map((r) => r.fileKey));
    return fileKeys.filter((k) => !blockList.has(k));
  },
};
