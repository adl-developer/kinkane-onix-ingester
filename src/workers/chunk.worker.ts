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
  NewBookContributor,
  NewBookSubject,
  NewBookPrice,
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

function buildBookData(book: OnixProduct) {
  return {
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

  const bookData = buildBookData(book);

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

// Same net effect as calling upsertBook + upsertRelations once per book, but
// collapses what was ~7-9 sequential round trips per book (thousands per
// chunk at CHUNK_SIZE=500) into roughly a dozen bulk statements for the whole
// chunk. This is what actually bounds per-chunk time — the book loop was the
// dominant cost, not the Gemini/R2 calls. Throws on any error rather than
// isolating a single bad row; the caller falls back to the slow per-book
// path (which does isolate) so a single malformed record can't take down
// the whole chunk, it just costs the speedup for that one chunk.
async function batchUpsertChunkBooks(
  onixBooks: OnixProduct[],
): Promise<{ embeddingTargets: { id: number; book: OnixProduct }[] }> {
  const deletionBooks = onixBooks.filter((b) => b.notificationType === '05');
  const upsertCandidates = onixBooks.filter((b) => b.notificationType !== '05');

  if (deletionBooks.length > 0) {
    const isbnKeys = deletionBooks.filter((b) => b.isbn13).map((b) => b.isbn13!);
    const refKeys = deletionBooks.filter((b) => !b.isbn13).map((b) => b.recordReference);
    if (isbnKeys.length > 0) {
      await db
        .update(books)
        .set({ isRemoved: true, removedAt: new Date() })
        .where(inArray(books.isbn13, isbnKeys));
    }
    if (refKeys.length > 0) {
      await db
        .update(books)
        .set({ isRemoved: true, removedAt: new Date() })
        .where(inArray(books.recordReference, refKeys));
    }
  }

  if (upsertCandidates.length === 0) {
    return { embeddingTargets: [] };
  }

  // A single multi-row INSERT can't target the same ON CONFLICT row twice —
  // last occurrence wins, matching what sequential processing would leave
  // as the final DB state if the same recordReference appeared twice.
  const dedupedByRef = new Map<string, OnixProduct>();
  for (const b of upsertCandidates) dedupedByRef.set(b.recordReference, b);
  const dedupedBooks = Array.from(dedupedByRef.values());

  const bookDataArray = dedupedBooks.map((book) => buildBookData(book));

  // onConflictDoUpdate's `set` must reference `excluded.<col>` (not the
  // literal bookData values) for a multi-row insert — otherwise every
  // conflicting row would be overwritten with one shared row's values
  // instead of its own.
  const insertedRows = await db
    .insert(books)
    .values(bookDataArray)
    .onConflictDoUpdate({
      target: books.recordReference,
      set: {
        recordReference: sql`excluded.record_reference`,
        isbn13: sql`excluded.isbn13`,
        notificationType: sql`excluded.notification_type`,
        productForm: sql`excluded.product_form`,
        productComposition: sql`excluded.product_composition`,
        editionNumber: sql`excluded.edition_number`,
        pageCount: sql`excluded.page_count`,
        heightMm: sql`excluded.height_mm`,
        widthMm: sql`excluded.width_mm`,
        thicknessMm: sql`excluded.thickness_mm`,
        weightGr: sql`excluded.weight_gr`,
        countryOfManufacture: sql`excluded.country_of_manufacture`,
        productClassificationCode: sql`excluded.product_classification_code`,
        title: sql`excluded.title`,
        subtitle: sql`excluded.subtitle`,
        shortDescription: sql`excluded.short_description`,
        longDescription: sql`excluded.long_description`,
        publisherName: sql`excluded.publisher_name`,
        imprintName: sql`excluded.imprint_name`,
        countryOfPublication: sql`excluded.country_of_publication`,
        publishingStatus: sql`excluded.publishing_status`,
        publicationDate: sql`excluded.publication_date`,
        availabilityCode: sql`excluded.availability_code`,
        returnsCode: sql`excluded.returns_code`,
        orderTime: sql`excluded.order_time`,
        isRemoved: sql`excluded.is_removed`,
        removedAt: sql`excluded.removed_at`,
        updatedAt: sql`excluded.updated_at`,
        embeddedAt: sql`CASE
          WHEN excluded.title != books.title
            OR excluded.long_description IS DISTINCT FROM books.long_description
          THEN NULL
          ELSE books.embedded_at
        END`,
      },
    })
    .returning({ id: books.id, recordReference: books.recordReference });

  const idByRef = new Map(insertedRows.map((r) => [r.recordReference, r.id]));
  const embeddingTargets = dedupedBooks
    .map((book) => ({ id: idByRef.get(book.recordReference), book }))
    .filter((t): t is { id: number; book: OnixProduct } => t.id !== undefined);

  if (embeddingTargets.length === 0) {
    return { embeddingTargets: [] };
  }

  const touchedBookIds = embeddingTargets.map((t) => t.id);

  const allContributors: NewBookContributor[] = [];
  const allSubjects: NewBookSubject[] = [];
  const allPrices: NewBookPrice[] = [];
  const themaEntries: {
    bookId: number;
    name: string;
    slug: string;
    subjectCode: string | null;
    schemeIdentifier: string | null;
  }[] = [];

  for (const { id: bookId, book } of embeddingTargets) {
    for (const c of book.contributors) {
      allContributors.push({
        bookId,
        sequenceNumber: c.sequenceNumber,
        role: c.role,
        personName: c.personName,
        personNameInverted: c.personNameInverted,
      });
    }
    for (const s of book.subjects) {
      allSubjects.push({
        bookId,
        schemeIdentifier: s.schemeIdentifier,
        schemeVersion: s.schemeVersion,
        subjectCode: s.subjectCode,
        subjectHeadingText: s.subjectHeadingText,
        isMainSubject: s.isMainSubject,
      });
      if (s.schemeIdentifier === '93' && (s.subjectHeadingText || s.subjectCode)) {
        const name = s.subjectHeadingText ?? s.subjectCode ?? '';
        const slug = slugify(name);
        if (slug) {
          themaEntries.push({ bookId, name, slug, subjectCode: s.subjectCode, schemeIdentifier: s.schemeIdentifier });
        }
      }
    }
    for (const p of book.prices) {
      allPrices.push({
        bookId,
        priceType: p.priceType,
        priceAmount: p.priceAmount?.toString() ?? null,
        currencyCode: p.currencyCode,
        taxRateCode: p.taxRateCode,
        taxRatePercent: p.taxRatePercent?.toString() ?? null,
      });
    }
  }

  // Every touched book's contributors/subjects/prices are fully replaced,
  // same as the per-book path — the delete always runs, the insert only
  // when there's something to insert.
  await db.delete(bookContributors).where(inArray(bookContributors.bookId, touchedBookIds));
  if (allContributors.length > 0) await db.insert(bookContributors).values(allContributors);

  await db.delete(bookSubjects).where(inArray(bookSubjects.bookId, touchedBookIds));
  if (allSubjects.length > 0) await db.insert(bookSubjects).values(allSubjects);

  await db.delete(bookPrices).where(inArray(bookPrices.bookId, touchedBookIds));
  if (allPrices.length > 0) await db.insert(bookPrices).values(allPrices);

  // Mirrors the per-book path: bookGenres is only touched for books that
  // have thema subjects in this run — a book with none keeps whatever
  // genres it had before (pre-existing behaviour, not changed here).
  if (themaEntries.length > 0) {
    const uniqueGenres = new Map<
      string,
      { name: string; slug: string; subjectCode: string | null; schemeIdentifier: string | null }
    >();
    for (const e of themaEntries) if (!uniqueGenres.has(e.slug)) uniqueGenres.set(e.slug, e);

    await db.insert(genres).values(Array.from(uniqueGenres.values())).onConflictDoNothing();

    const genreRows = await db
      .select({ id: genres.id, slug: genres.slug })
      .from(genres)
      .where(inArray(genres.slug, Array.from(uniqueGenres.keys())));
    const genreIdBySlug = new Map(genreRows.map((g) => [g.slug, g.id]));

    const bookIdsWithThema = Array.from(new Set(themaEntries.map((e) => e.bookId)));
    await db.delete(bookGenres).where(inArray(bookGenres.bookId, bookIdsWithThema));

    const uniquePairs = new Map<string, { bookId: number; genreId: number }>();
    for (const e of themaEntries) {
      const genreId = genreIdBySlug.get(e.slug);
      if (genreId === undefined) continue;
      uniquePairs.set(`${e.bookId}:${genreId}`, { bookId: e.bookId, genreId });
    }
    if (uniquePairs.size > 0) {
      await db.insert(bookGenres).values(Array.from(uniquePairs.values())).onConflictDoNothing();
    }
  }

  return { embeddingTargets };
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

  // 1. Upsert all books and their relations — batched across the whole
  // chunk in one shot; falls back to the slow, per-book-isolated path only
  // if the batch itself throws (e.g. a constraint violation from a
  // malformed row that a bulk statement can't isolate).
  let embeddingTargets: { id: number; book: OnixProduct }[] = [];

  try {
    const result = await batchUpsertChunkBooks(onixBooks);
    embeddingTargets = result.embeddingTargets;
    processedBooks = onixBooks.length;
  } catch (err) {
    logger.warn('Batched chunk upsert failed, falling back to per-book processing', {
      worker: 'chunk',
      chunkId,
      error: err instanceof Error ? err.message : String(err),
    });

    for (const onixBook of onixBooks) {
      try {
        const bookId = await upsertBook(onixBook);
        if (bookId !== null) {
          await upsertRelations(bookId, onixBook);
          embeddingTargets.push({ id: bookId, book: onixBook });
        }
        processedBooks++;
      } catch (bookErr) {
        failedBooks++;
        logger.error('Failed to upsert book', {
          worker: 'chunk',
          chunkId,
          recordReference: onixBook.recordReference,
          error: bookErr instanceof Error ? bookErr.message : String(bookErr),
        });
      }
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

      // Resolve job status if this was the last chunk — same rule as the
      // success path: 'completed' once every chunk has been accounted for,
      // regardless of how many failed. See that path's comment for why
      // (marking the whole job 'failed' here caused the exact same silent
      // full-file re-triggering bug this fix was for).
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
