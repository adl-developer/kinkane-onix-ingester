import { Worker, Job } from 'bullmq';
import { eq, inArray, sql } from 'drizzle-orm';
import { redis } from '../queue';
import { db } from '../db';
import { logger } from '../lib/logger';
import {
  books,
  bookContributors,
  bookSubjects,
  bookPrices,
  genres,
  bookGenres,
  ingestionJobs,
  ingestionChunks,
} from '../db/schema';
import { embeddingService } from '../services/embedding.service';
import { storageService } from '../services/storage.service';
import { OnixProduct } from '../types/onix';
import { ChunkJobData, ChunkJobResult } from '../types/queue';

function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_-]/g, '')
    .slice(0, 200);
}

async function upsertBook(book: OnixProduct): Promise<number | null> {
  if (book.notificationType === '05') {
    // ONIX "delete" notification — the title has been withdrawn upstream.
    // Soft-delete rather than actually deleting the row: a real delete
    // cascades to a user's posts/reviews, reading-list entries, and
    // interaction history for that book (see server's books.ts schema for
    // the FKs), silently destroying their content. isRemoved lets those
    // survive; un-set automatically below if the title reappears later.
    const match = book.isbn13
      ? eq(books.isbn13, book.isbn13)
      : eq(books.recordReference, book.recordReference);
    await db.update(books).set({ isRemoved: true, removedAt: new Date() }).where(match);
    return null;
  }

  const bookData = {
    recordReference: book.recordReference,
    isbn13: book.isbn13,
    notificationType: book.notificationType,
    productForm: book.productForm,
    productComposition: book.productComposition,
    editionNumber: book.editionNumber,
    pageCount: book.pageCount,
    heightMm: book.heightMm?.toString() ?? null,
    widthMm: book.widthMm?.toString() ?? null,
    thicknessMm: book.thicknessMm?.toString() ?? null,
    weightGr: book.weightGr?.toString() ?? null,
    countryOfManufacture: book.countryOfManufacture,
    productClassificationCode: book.productClassificationCode,
    title: book.title,
    subtitle: book.subtitle,
    shortDescription: book.shortDescription,
    longDescription: book.longDescription,
    publisherName: book.publisherName,
    imprintName: book.imprintName,
    countryOfPublication: book.countryOfPublication,
    publishingStatus: book.publishingStatus,
    publicationDate: book.publicationDate,
    availabilityCode: book.availabilityCode,
    returnsCode: book.returnsCode,
    orderTime: book.orderTime,
    // A normal notification for this recordReference means it's active
    // again — clears any prior withdrawal (e.g. a reissued title).
    isRemoved: false,
    removedAt: null,
    updatedAt: new Date(),
  };

  const [row] = await db
    .insert(books)
    .values(bookData)
    .onConflictDoUpdate({
      target: books.recordReference,
      set: {
        ...bookData,
        embeddedAt: sql`CASE
          WHEN excluded.title != books.title
            OR excluded.long_description IS DISTINCT FROM books.long_description
          THEN NULL
          ELSE books.embedded_at
        END`,
      },
    })
    .returning({ id: books.id });

  return row.id;
}

async function upsertRelations(bookId: number, book: OnixProduct): Promise<void> {
  await db.delete(bookContributors).where(eq(bookContributors.bookId, bookId));
  if (book.contributors.length > 0) {
    await db.insert(bookContributors).values(
      book.contributors.map((c) => ({
        bookId,
        sequenceNumber: c.sequenceNumber,
        role: c.role,
        personName: c.personName,
        personNameInverted: c.personNameInverted,
      })),
    );
  }

  await db.delete(bookSubjects).where(eq(bookSubjects.bookId, bookId));
  if (book.subjects.length > 0) {
    await db.insert(bookSubjects).values(
      book.subjects.map((s) => ({
        bookId,
        schemeIdentifier: s.schemeIdentifier,
        schemeVersion: s.schemeVersion,
        subjectCode: s.subjectCode,
        subjectHeadingText: s.subjectHeadingText,
        isMainSubject: s.isMainSubject,
      })),
    );
  }

  await db.delete(bookPrices).where(eq(bookPrices.bookId, bookId));
  if (book.prices.length > 0) {
    await db.insert(bookPrices).values(
      book.prices.map((p) => ({
        bookId,
        priceType: p.priceType,
        priceAmount: p.priceAmount?.toString() ?? null,
        currencyCode: p.currencyCode,
        taxRateCode: p.taxRateCode,
        taxRatePercent: p.taxRatePercent?.toString() ?? null,
      })),
    );
  }

  const themaSubjects = book.subjects.filter(
    (s) => s.schemeIdentifier === '93' && (s.subjectHeadingText || s.subjectCode),
  );

  if (themaSubjects.length > 0) {
    const slugsToInsert = themaSubjects
      .map((s) => ({
        name: s.subjectHeadingText ?? s.subjectCode ?? '',
        slug: slugify(s.subjectHeadingText ?? s.subjectCode ?? ''),
        subjectCode: s.subjectCode,
        schemeIdentifier: s.schemeIdentifier,
      }))
      .filter((g) => g.slug);

    if (slugsToInsert.length > 0) {
      await db.insert(genres).values(slugsToInsert).onConflictDoNothing();

      const slugList = slugsToInsert.map((g) => g.slug);
      const genreRows = await db
        .select({ id: genres.id })
        .from(genres)
        .where(inArray(genres.slug, slugList));

      await db.delete(bookGenres).where(eq(bookGenres.bookId, bookId));
      if (genreRows.length > 0) {
        await db
          .insert(bookGenres)
          .values(genreRows.map((g) => ({ bookId, genreId: g.id })))
          .onConflictDoNothing();
      }
    }
  }
}

async function processChunkJob(job: Job<ChunkJobData>): Promise<ChunkJobResult> {
  const { ingestionJobId, chunkId } = job.data;
  let processedBooks = 0;
  let failedBooks = 0;

  // Fetch the R2 key for this chunk's payload
  const [chunkRow] = await db
    .select({ dataKey: ingestionChunks.dataKey, status: ingestionChunks.status, processedBooks: ingestionChunks.processedBooks })
    .from(ingestionChunks)
    .where(eq(ingestionChunks.id, chunkId));

  // A stale retry of a chunk that already fully completed on an earlier
  // attempt (e.g. BullMQ retrying after the job-level counter update timed
  // out, even though this chunk's own work already committed) — re-running
  // the upserts is harmless, but re-running processedChunks/processedBooks
  // + 1 below is not: it has no per-chunk guard, so it would double-count.
  // Bail out before doing (or counting) any work again.
  if (chunkRow?.status === 'completed') {
    return { processedBooks: chunkRow.processedBooks ?? 0, failedBooks: 0 };
  }

  if (!chunkRow?.dataKey) {
    throw new Error(`No R2 data key found for chunk ${chunkId} — it may have already been processed`);
  }

  // Download the parsed book payload from R2
  const onixBooks = await storageService.getJson<OnixProduct[]>(chunkRow.dataKey);

  await db
    .update(ingestionChunks)
    .set({ status: 'processing', updatedAt: new Date() })
    .where(eq(ingestionChunks.id, chunkId));

  // 1. Upsert all books and their relations
  const embeddingTargets: { id: number; book: OnixProduct }[] = [];

  for (const onixBook of onixBooks) {
    try {
      const bookId = await upsertBook(onixBook);
      if (bookId !== null) {
        await upsertRelations(bookId, onixBook);
        embeddingTargets.push({ id: bookId, book: onixBook });
      }
      processedBooks++;
    } catch (err) {
      failedBooks++;
      logger.error('Failed to upsert book', {
        worker: 'chunk',
        chunkId,
        recordReference: onixBook.recordReference,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  await job.updateProgress(60);

  // 2. Generate embeddings for books that need them
  if (embeddingTargets.length > 0) {
    const booksNeedingEmbedding = await db
      .select({ id: books.id })
      .from(books)
      .where(
        sql`id = ANY(ARRAY[${sql.join(
          embeddingTargets.map((t) => sql`${t.id}`),
          sql`, `,
        )}]::int[]) AND embedded_at IS NULL`,
      );

    const needingIds = new Set(booksNeedingEmbedding.map((b) => b.id));
    const toEmbed = embeddingTargets.filter((t) => needingIds.has(t.id));

    if (toEmbed.length > 0) {
      try {
        const texts = toEmbed.map((t) => embeddingService.buildBookText(t.book));
        const embeddings = await embeddingService.generateBatch(texts);

        for (let i = 0; i < toEmbed.length; i++) {
          await db
            .update(books)
            .set({ embedding: embeddings[i], embeddedAt: new Date() })
            .where(eq(books.id, toEmbed[i].id));
        }
      } catch (err) {
        logger.error('Embedding generation failed', {
          worker: 'chunk',
          chunkId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }
  }

  await job.updateProgress(100);

  const chunkStatus = failedBooks === onixBooks.length ? 'failed' : 'completed';

  // Durably record the chunk's outcome — including the processedChunks/
  // processedBooks counters, which have no per-chunk idempotency guard of
  // their own — before touching R2. dataKey is deliberately left as-is
  // here (still pointing at the real object) rather than nulled yet; see
  // below.
  await db
    .update(ingestionChunks)
    .set({
      status: chunkStatus,
      processedBooks,
      updatedAt: new Date(),
    })
    .where(eq(ingestionChunks.id, chunkId));

  await db
    .update(ingestionJobs)
    .set({
      processedChunks: sql`processed_chunks + 1`,
      processedBooks: sql`processed_books + ${processedBooks}`,
      failedChunks: failedBooks > 0 ? sql`failed_chunks + 1` : sql`failed_chunks`,
      updatedAt: new Date(),
    })
    .where(eq(ingestionJobs.id, ingestionJobId));

  // A job is 'completed' once every chunk has been processed, regardless of
  // whether some chunks had individual book-level failures within them —
  // failed_chunks/processed_books remain visible on the row for a human to
  // review, but don't disqualify the file as a whole. Marking the whole job
  // 'failed' over a handful of bad records used to make listUnprocessedR2Files
  // treat an otherwise-finished file as untouched, causing the daily R2 poll
  // cron to silently re-trigger and fully reprocess it from scratch.
  await db.execute(sql`
    UPDATE ingestion_jobs
    SET
      status = CASE
        WHEN processed_chunks = total_chunks THEN 'completed'
        ELSE status
      END,
      completed_at = CASE
        WHEN processed_chunks = total_chunks THEN NOW()
        ELSE completed_at
      END,
      updated_at = NOW()
    WHERE id = ${ingestionJobId}
  `);

  // Only delete the R2 payload once the chunk's completion is durably
  // recorded above, and only clear dataKey once the delete itself succeeds.
  // Deleting first (the previous order) meant a DB failure afterward — e.g.
  // a connection-pool timeout waiting for a free connection, before the
  // query ever reached the database — left an unretryable chunk: a retry
  // would still see the old (non-null) dataKey, try to re-fetch the
  // now-deleted R2 object, and fail identically ("specified key does not
  // exist") on every subsequent attempt. The early-return above for
  // already-'completed' chunks is what makes retrying safe now — a retry
  // that lands here again re-upserts (idempotent) but never re-counts.
  // On failure, the payload is preserved for debugging.
  if (chunkStatus === 'completed') {
    try {
      await storageService.deleteObject(chunkRow.dataKey);
      await db
        .update(ingestionChunks)
        .set({ dataKey: null })
        .where(eq(ingestionChunks.id, chunkId));
    } catch (err) {
      logger.warn('Failed to delete chunk R2 file after successful processing', {
        worker: 'chunk',
        chunkId,
        dataKey: chunkRow.dataKey,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  return { processedBooks, failedBooks };
}

export function startChunkWorker(concurrency = 5): Worker<ChunkJobData, ChunkJobResult> {
  const worker = new Worker<ChunkJobData, ChunkJobResult>('onix-chunk', processChunkJob, {
    connection: redis,
    concurrency,
  });

  worker.on('completed', (job, result) => {
    logger.info('Chunk job completed', {
      worker: 'chunk',
      bullJobId: job.id,
      processedBooks: result.processedBooks,
      failedBooks: result.failedBooks,
    });
  });

  worker.on('failed', async (job, err) => {
    logger.error('Chunk job failed', {
      worker: 'chunk',
      bullJobId: job?.id,
      attempt: job?.attemptsMade,
      error: err.message,
    });

    // Only act when all retries are exhausted — earlier failures will be retried by BullMQ
    if (!job || job.attemptsMade < (job.opts.attempts ?? 1)) return;

    const { chunkId, ingestionJobId } = job.data;

    try {
      // dataKey is intentionally left intact on failure so the R2 payload
      // is available for debugging. The 30-day cleanup cron will remove it.
      await db
        .update(ingestionChunks)
        .set({ status: 'failed', errorMessage: err.message, updatedAt: new Date() })
        .where(eq(ingestionChunks.id, chunkId));

      await db
        .update(ingestionJobs)
        .set({
          processedChunks: sql`processed_chunks + 1`,
          failedChunks: sql`failed_chunks + 1`,
          updatedAt: new Date(),
        })
        .where(eq(ingestionJobs.id, ingestionJobId));

      // Resolve job status if this was the last chunk
      await db.execute(sql`
        UPDATE ingestion_jobs
        SET
          status = CASE
            WHEN processed_chunks = total_chunks AND failed_chunks = 0 THEN 'completed'
            WHEN processed_chunks = total_chunks THEN 'failed'
            ELSE status
          END,
          completed_at = CASE
            WHEN processed_chunks = total_chunks THEN NOW()
            ELSE completed_at
          END,
          updated_at = NOW()
        WHERE id = ${ingestionJobId}
      `);
    } catch (dbErr) {
      logger.error('Failed to record chunk failure in DB', {
        worker: 'chunk',
        chunkId,
        ingestionJobId,
        error: dbErr instanceof Error ? dbErr.message : String(dbErr),
      });
    }
  });

  return worker;
}
