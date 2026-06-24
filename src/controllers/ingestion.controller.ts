import { Request, Response } from 'express';
import { z } from 'zod';
import path from 'path';
import { isNull, eq } from 'drizzle-orm';
import { ingestionService } from '../services/ingestion.service';
import { storageService } from '../services/storage.service';
import { embeddingService } from '../services/embedding.service';
import { excerptService } from '../services/excerpt.service';
import { db } from '../db';
import { books, bookContributors } from '../db/schema';
import { config } from '../config';
import { logger } from '../lib/logger';

const triggerSchema = z.object({
  fileKey: z.string().min(1),
});

const presignSchema = z.object({
  filename: z.string().min(1).refine((f) => f.endsWith('.xml'), {
    message: 'Only .xml files are accepted',
  }),
  key: z.string().optional(), // custom R2 key; defaults to onixPrefix + sanitised filename
  expiresIn: z.coerce.number().int().min(60).max(86400).default(4 * 60 * 60),
});

const listQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(20),
  offset: z.coerce.number().int().min(0).default(0),
});

async function runEmbeddingBackfill(): Promise<void> {
  // Fetch all books without embeddings, with their primary authors
  const nullBooks = await db
    .select({
      id: books.id,
      title: books.title,
      subtitle: books.subtitle,
      shortDescription: books.shortDescription,
      longDescription: books.longDescription,
    })
    .from(books)
    .where(isNull(books.embedding));

  if (nullBooks.length === 0) {
    logger.info('Embedding backfill: no books need embeddings');
    return;
  }

  // Fetch primary authors (role A01) for all affected books
  const bookIds = nullBooks.map((b) => b.id);
  const contributors = await db
    .select({ bookId: bookContributors.bookId, personName: bookContributors.personName })
    .from(bookContributors)
    .where(eq(bookContributors.role, 'A01'));

  const authorMap = new Map<number, string[]>();
  for (const c of contributors) {
    if (!bookIds.includes(c.bookId)) continue;
    if (!authorMap.has(c.bookId)) authorMap.set(c.bookId, []);
    if (c.personName) authorMap.get(c.bookId)!.push(c.personName);
  }

  // Build embedding texts from DB columns (same format as buildBookText)
  const texts = nullBooks.map((b) => {
    const authors = (authorMap.get(b.id) ?? []).join(', ');
    const parts = [
      b.title,
      b.subtitle,
      authors ? `By ${authors}` : null,
      b.shortDescription ?? b.longDescription?.slice(0, 500),
    ].filter(Boolean);
    return parts.join('. ');
  });

  // generateBatch handles batching + rate limiting internally
  const vectors = await embeddingService.generateBatch(texts);

  // Write embeddings back, counting successes and failures
  let processed = 0;
  let failed = 0;
  const now = new Date();

  for (let i = 0; i < nullBooks.length; i++) {
    const vector = vectors[i];
    if (!vector || vector.length === 0) {
      failed++;
      continue;
    }
    try {
      await db
        .update(books)
        .set({ embedding: vector, embeddedAt: now, updatedAt: now })
        .where(eq(books.id, nullBooks[i].id));
      processed++;
    } catch (err: unknown) {
      const e = err as Error;
      logger.error('Embedding write failed', { bookId: nullBooks[i].id, error: e.message });
      failed++;
    }
  }

  logger.info('Embedding backfill complete', { processed, failed, total: nullBooks.length });
}

export const ingestionController = {
  async trigger(req: Request, res: Response): Promise<void> {
    const parsed = triggerSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    try {
      const result = await ingestionService.triggerIngestion(parsed.data.fileKey);
      res.status(202).json({
        message: 'Ingestion job enqueued',
        jobId: result.jobId,
        bullJobId: result.bullJobId,
      });
    } catch (err: unknown) {
      const e = err as Error & { statusCode?: number };
      res.status(e.statusCode ?? 500).json({ error: e.message });
    }
  },

  async listJobs(req: Request, res: Response): Promise<void> {
    const parsed = listQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    try {
      const jobs = await ingestionService.listJobs(parsed.data.limit, parsed.data.offset);
      res.status(200).json({ jobs });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  async getJob(req: Request, res: Response): Promise<void> {
    const jobId = parseInt(req.params.id, 10);
    if (isNaN(jobId)) {
      res.status(400).json({ error: 'Invalid job ID' });
      return;
    }

    try {
      const job = await ingestionService.getJob(jobId);
      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }
      res.status(200).json({ job });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  async listUnprocessed(_req: Request, res: Response): Promise<void> {
    try {
      const files = await ingestionService.listUnprocessedR2Files();
      res.status(200).json({ files });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  /**
   * POST /ingestion/backfill-embeddings
   * Fetches all books with embedding IS NULL, generates embeddings in batches,
   * and writes them back. Responds immediately with 202; the work runs in the
   * background and results are logged on completion.
   */
  async backfillEmbeddings(_req: Request, res: Response): Promise<void> {
    res.status(202).json({ message: 'Embedding backfill started' });

    runEmbeddingBackfill().catch((err: unknown) => {
      const e = err as Error;
      logger.error('Embedding backfill failed', { error: e.message });
    });
  },

  /**
   * POST /ingestion/backfill-excerpts
   * Forces a full Jellybooks catalogue resync, regardless of whether
   * book_excerpts already has data. Useful for manually re-running the
   * backfill after a partial failure — the scheduled cron only auto-runs
   * the full backfill once, when the table is empty.
   */
  async backfillExcerpts(_req: Request, res: Response): Promise<void> {
    res.status(202).json({ message: 'Excerpt backfill started' });

    excerptService.backfillExcerpts().catch((err: unknown) => {
      const e = err as Error;
      logger.error('Excerpt backfill failed', { error: e.message });
    });
  },

  /**
   * POST /ingestion/presign
   * Returns a presigned PUT URL so the caller can upload a large ONIX file
   * directly to R2 without routing the bytes through this server.
   *
   * Body: { filename: "feed.xml", key?: "onix/custom.xml", expiresIn?: 14400 }
   * Response: { uploadUrl, fileKey, expiresIn }
   *
   * After the upload completes, call POST /ingestion/trigger with { fileKey }.
   */
  async presignUpload(req: Request, res: Response): Promise<void> {
    const parsed = presignSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    const { filename, key, expiresIn } = parsed.data;

    const ext = path.extname(filename);
    const base = path
      .basename(filename, ext)
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_-]/g, '');
    const sanitisedFilename = `${base}_${Date.now()}${ext}`;
    const fileKey = key ?? `${config.r2.onixPrefix}${sanitisedFilename}`;

    try {
      const uploadUrl = await storageService.getPresignedUploadUrl(fileKey, expiresIn);
      res.status(200).json({ uploadUrl, fileKey, expiresIn });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },
};
