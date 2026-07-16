import { eq, desc, inArray, and, ne } from 'drizzle-orm';
import { db } from '../db';
import { ingestionJobs, ingestionChunks } from '../db/schema';
import { fileQueue } from '../queue';
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
