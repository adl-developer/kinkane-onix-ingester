import { createReadStream, createWriteStream } from 'fs';
import { stat, unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { pipeline } from 'stream/promises';
import { once } from 'events';
import * as unzipper from 'unzipper';
import { eq, sql } from 'drizzle-orm';
import { db, pgClient } from '../../db';
import { ingestionJobs } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersConnections } from './connections.service';
import { parseGardbibStream, GardbibRecord } from './gardbib-parser';

/**
 * Full-catalogue book loader for POST /gardners/bootstrap.
 *
 * Deliberately bypasses the R2 + BullMQ chunk pipeline that ONIX files go
 * through. That path costs ~7 round trips and an R2 object per 500 books and
 * took 54 hours to load 1M books; this one streams straight from the SFTP
 * file into COPY and does the whole 2M-record catalogue set-based, in
 * minutes. The chunk pipeline is still the right tool for ONIX deltas
 * (small, frequent, full-field updates) — it's the wrong one for a one-shot
 * bulk load.
 *
 * INSERT-ONLY: a book whose ISBN already has a `books` row is skipped
 * entirely and never updated. That's both what the endpoint is asked to do
 * and what protects the richer ONIX-sourced rows from being flattened by
 * GARDBIB's thinner ones. It also makes the whole operation idempotent and
 * cheap to re-run — a second run inserts nothing and finishes in the time it
 * takes to download and stage the file.
 *
 * Embeddings are NOT generated here. New rows land with embedded_at NULL and
 * are picked up by the existing embedding backfill; generating ~900K
 * embeddings inline is a multi-hour Gemini-bound operation that would blow
 * the time budget on its own.
 */

const REMOTE_PATH = '/Biblio/GARDBIB.zip';
const DONE_SENTINEL = '/Biblio/GARDBIB.DONE';
const JOB_FILE_KEY = 'gardners/GARDBIB.zip';

// Tuned against the real file: large enough that per-statement overhead is
// irrelevant, small enough that each statement stays well inside work_mem
// and progress is visible in the log every few seconds.
const INSERT_BATCH_SIZE = 25_000;

// Two concurrent runs would share the staging tables below, and the second
// one's DROP/CREATE would pull them out from under the first mid-load. The
// endpoint is a manual admin trigger, so a double-click is the likely way in.
const ADVISORY_LOCK_KEY = 4718201;

const STAGING_BOOKS = 'gardbib_stg_books';
const STAGING_CONTRIBUTORS = 'gardbib_stg_contributors';
const STAGING_SUBJECTS = 'gardbib_stg_subjects';
const STAGING_NEW_BOOKS = 'gardbib_stg_new_books';

export interface GardbibSyncOptions {
  // Skip the SFTP download and use a zip already on disk. For re-running a
  // failed load without re-fetching 315MB, and for validating changes to
  // this service against a known file.
  localZipPath?: string;
  // Stop parsing after this many records. Partial loads are harmless — the
  // load is insert-only and resumable, so a later full run completes it.
  maxRecords?: number;
  // Do everything except write to `books` — parse, stage, and report exactly
  // how many rows each step would insert. Staging tables are still created
  // and dropped; nothing in `books` or its relations is touched.
  dryRun?: boolean;
}

export interface GardbibSyncResult {
  jobId: number;
  recordsParsed: number;
  recordsStaged: number;
  booksInserted: number;
  contributorsInserted: number;
  subjectsInserted: number;
  skippedExisting: number;
  durationMs: number;
}

/**
 * COPY ... FROM STDIN text format: tab-separated, `\N` for NULL, and the
 * five characters below escaped. Getting this wrong doesn't error — it
 * silently shifts every following column by one — so it's centralised here
 * rather than inlined per field.
 */
function copyEscape(value: string | number | null): string {
  if (value === null || value === undefined) return '\\N';
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t');
}

function isRealIsbn13(isbn: string | null): isbn is string {
  // The catalogue also carries non-book EANs (jigsaws, cards, calendars —
  // e.g. 5055923785560). Only 978/979 prefixes are actual ISBNs, matching
  // the filter the Inventory feed already applies.
  return !!isbn && /^97[89]\d{10}$/.test(isbn);
}

async function createStagingTables(): Promise<void> {
  // UNLOGGED: these are rebuilt from the source file on every run, so paying
  // for WAL on ~10M staging rows buys nothing. Dropped again in `finally`.
  await db.execute(sql.raw(`
    DROP TABLE IF EXISTS ${STAGING_BOOKS}, ${STAGING_CONTRIBUTORS}, ${STAGING_SUBJECTS}, ${STAGING_NEW_BOOKS};

    CREATE UNLOGGED TABLE ${STAGING_BOOKS} (
      isbn13 varchar(13),
      title varchar(2000),
      subtitle varchar(2000),
      long_description text,
      publisher_name varchar(500),
      publication_date date,
      page_count integer,
      height_mm numeric(7,2),
      width_mm numeric(7,2),
      thickness_mm numeric(7,2),
      weight_gr numeric(9,2),
      product_form varchar(10)
    );

    CREATE UNLOGGED TABLE ${STAGING_CONTRIBUTORS} (
      isbn13 varchar(13),
      sequence_number integer,
      role varchar(10),
      person_name varchar(500)
    );

    CREATE UNLOGGED TABLE ${STAGING_SUBJECTS} (
      isbn13 varchar(13),
      subject_code varchar(50)
    );

    CREATE UNLOGGED TABLE ${STAGING_NEW_BOOKS} (
      book_id integer,
      isbn13 varchar(13)
    );
  `));
}

async function dropStagingTables(): Promise<void> {
  await db
    .execute(
      sql.raw(
        `DROP TABLE IF EXISTS ${STAGING_BOOKS}, ${STAGING_CONTRIBUTORS}, ${STAGING_SUBJECTS}, ${STAGING_NEW_BOOKS};`,
      ),
    )
    .catch((err) => {
      logger.warn('GARDBIB: failed to drop staging tables', {
        error: err instanceof Error ? err.message : String(err),
      });
    });
}

/**
 * Parses the zip straight into three local TSV files — one per staging
 * table. Writing to disk first (rather than three concurrent COPY streams)
 * keeps the parse decoupled from network throughput: the parse runs at local
 * disk speed, then each COPY is a single sequential upload with nothing else
 * competing for the connection.
 */
async function parseToTsvFiles(
  zipPath: string,
  paths: { books: string; contributors: string; subjects: string },
  maxRecords?: number,
): Promise<{ recordsParsed: number; recordsStaged: number }> {
  const bookOut = createWriteStream(paths.books);
  const contributorOut = createWriteStream(paths.contributors);
  const subjectOut = createWriteStream(paths.subjects);

  // A false return from write() means the buffer is full; ignoring it on a
  // ~2M-record loop would grow an unbounded in-memory queue.
  const write = async (stream: NodeJS.WritableStream, line: string): Promise<void> => {
    if (!stream.write(line)) await once(stream, 'drain');
  };

  let recordsParsed = 0;
  let recordsStaged = 0;
  const seenIsbns = new Set<string>();

  const zipStream = createReadStream(zipPath).pipe(unzipper.ParseOne());

  for await (const record of parseGardbibStream(zipStream)) {
    recordsParsed++;

    if (!isRealIsbn13(record.isbn13) || !record.title) continue;
    // The unique index on books.isbn13 would reject an intra-run duplicate,
    // and ON CONFLICT can't resolve two conflicting rows inside one
    // statement — so collapse duplicates here, first occurrence wins.
    if (seenIsbns.has(record.isbn13)) continue;
    seenIsbns.add(record.isbn13);
    recordsStaged++;

    await write(bookOut, bookRow(record));
    for (const c of record.contributors) {
      await write(
        contributorOut,
        `${copyEscape(record.isbn13)}\t${copyEscape(c.sequenceNumber)}\t${copyEscape(c.role)}\t${copyEscape(c.personName)}\n`,
      );
    }
    for (const code of record.subjectCodes) {
      await write(subjectOut, `${copyEscape(record.isbn13)}\t${copyEscape(code)}\n`);
    }

    if (recordsParsed % 250_000 === 0) {
      logger.info('GARDBIB: parsing', { recordsParsed, recordsStaged });
    }
    if (maxRecords && recordsParsed >= maxRecords) break;
  }

  // The generator is abandoned early on a maxRecords break; destroying the
  // unzip stream stops it filling a buffer nobody is reading.
  zipStream.destroy();

  for (const stream of [bookOut, contributorOut, subjectOut]) {
    stream.end();
    await once(stream, 'finish');
  }

  return { recordsParsed, recordsStaged };
}

function bookRow(record: GardbibRecord): string {
  return (
    [
      record.isbn13,
      record.title,
      record.subtitle,
      record.description,
      record.publisherName,
      record.publicationDate,
      record.pageCount,
      record.heightMm,
      record.widthMm,
      record.thicknessMm,
      record.weightGr,
      record.productForm,
    ]
      .map(copyEscape)
      .join('\t') + '\n'
  );
}

async function copyFileIntoTable(table: string, columns: string, filePath: string): Promise<void> {
  const { size } = await stat(filePath);
  const started = Date.now();

  // reserve() takes the connection out of the pool for the duration — a COPY
  // stream owns its connection until it finishes, and handing it back mid-copy
  // would corrupt whatever query got it next.
  const reserved = await pgClient.reserve();
  try {
    const writable = await reserved`COPY ${reserved(table)} (${reserved.unsafe(columns)}) FROM STDIN`.writable();
    await pipeline(createReadStream(filePath), writable);
  } finally {
    reserved.release();
  }

  logger.info('GARDBIB: staged file', {
    table,
    mb: (size / 1e6).toFixed(1),
    seconds: ((Date.now() - started) / 1000).toFixed(1),
  });
}

async function stageTsvFiles(paths: {
  books: string;
  contributors: string;
  subjects: string;
}): Promise<void> {
  await copyFileIntoTable(
    STAGING_BOOKS,
    'isbn13, title, subtitle, long_description, publisher_name, publication_date, page_count, height_mm, width_mm, thickness_mm, weight_gr, product_form',
    paths.books,
  );
  await copyFileIntoTable(
    STAGING_CONTRIBUTORS,
    'isbn13, sequence_number, role, person_name',
    paths.contributors,
  );
  await copyFileIntoTable(STAGING_SUBJECTS, 'isbn13, subject_code', paths.subjects);

  // Indexed after loading, not before — building the index once over the
  // finished table is far cheaper than maintaining it across 10M inserts.
  logger.info('GARDBIB: indexing staging tables');
  await db.execute(
    sql.raw(`
      CREATE INDEX ON ${STAGING_BOOKS} (isbn13);
      CREATE INDEX ON ${STAGING_CONTRIBUTORS} (isbn13);
      CREATE INDEX ON ${STAGING_SUBJECTS} (isbn13);
      ANALYZE ${STAGING_BOOKS};
      ANALYZE ${STAGING_CONTRIBUTORS};
      ANALYZE ${STAGING_SUBJECTS};
    `),
  );
}

/**
 * Inserts every staged ISBN that `books` doesn't already have, in keyset
 * batches over isbn13. Each batch is its own transaction, so an interrupted
 * run leaves the books it already committed in place and a re-run simply
 * skips them.
 */
async function countInsertableBooks(): Promise<number> {
  const [row] = await db.execute<{ count: string }>(sql.raw(`
    SELECT count(*)::text AS count
    FROM ${STAGING_BOOKS} s
    LEFT JOIN books existing ON existing.isbn13 = s.isbn13
    WHERE existing.id IS NULL
  `));
  return Number(row?.count ?? 0);
}

async function insertNewBooks(jobId: number): Promise<{ inserted: number; batches: number }> {
  let lastIsbn = '';
  let inserted = 0;
  let batches = 0;

  for (;;) {
    const [bound] = await db.execute<{ max_isbn: string | null }>(sql.raw(`
      SELECT max(isbn13) AS max_isbn
      FROM (
        SELECT isbn13 FROM ${STAGING_BOOKS}
        WHERE isbn13 > '${lastIsbn}'
        ORDER BY isbn13
        LIMIT ${INSERT_BATCH_SIZE}
      ) t
    `));

    const upperBound = bound?.max_isbn;
    if (!upperBound) break;

    // The new rows' ids are captured into a staging table in the same
    // statement, so the relation inserts below can target exactly the books
    // this run created and never touch a pre-existing book's contributors
    // or subjects.
    const result = await db.execute<{ count: string }>(sql.raw(`
      WITH batch AS (
        SELECT * FROM ${STAGING_BOOKS}
        WHERE isbn13 > '${lastIsbn}' AND isbn13 <= '${upperBound}'
      ),
      ins AS (
        INSERT INTO books (
          record_reference, isbn13, title, subtitle, long_description,
          publisher_name, publication_date, page_count,
          height_mm, width_mm, thickness_mm, weight_gr, product_form,
          is_removed, created_at, updated_at
        )
        SELECT
          b.isbn13, b.isbn13, b.title, b.subtitle, b.long_description,
          b.publisher_name, b.publication_date, b.page_count,
          b.height_mm, b.width_mm, b.thickness_mm, b.weight_gr, b.product_form,
          false, NOW(), NOW()
        FROM batch b
        LEFT JOIN books existing ON existing.isbn13 = b.isbn13
        WHERE existing.id IS NULL
        ON CONFLICT DO NOTHING
        RETURNING id, isbn13
      ),
      tracked AS (
        INSERT INTO ${STAGING_NEW_BOOKS} (book_id, isbn13)
        SELECT id, isbn13 FROM ins
        RETURNING 1
      )
      SELECT count(*)::text AS count FROM tracked
    `));

    inserted += Number(result[0]?.count ?? 0);
    batches++;
    lastIsbn = upperBound;

    if (batches % 10 === 0) {
      logger.info('GARDBIB: inserting books', { batches, inserted, lastIsbn });
      await db
        .update(ingestionJobs)
        .set({ processedBooks: inserted, processedChunks: batches, updatedAt: new Date() })
        .where(eq(ingestionJobs.id, jobId));
    }
  }

  return { inserted, batches };
}

async function insertRelationsForNewBooks(): Promise<{
  contributors: number;
  subjects: number;
}> {
  await db.execute(
    sql.raw(`CREATE INDEX ON ${STAGING_NEW_BOOKS} (isbn13); ANALYZE ${STAGING_NEW_BOOKS};`),
  );

  let contributors = 0;
  let subjects = 0;

  for (const [label, target, columns, source, selectList] of [
    [
      'contributors',
      'book_contributors',
      'book_id, sequence_number, role, person_name',
      STAGING_CONTRIBUTORS,
      'n.book_id, s.sequence_number, s.role, s.person_name',
    ],
    [
      'subjects',
      'book_subjects',
      'book_id, scheme_identifier, subject_code, is_main_subject',
      STAGING_SUBJECTS,
      // GARDBIB's BC codes are Thema (scheme 93) but carry no heading text,
      // so they're recorded as subjects only. Genre rows are deliberately
      // NOT derived from them — the genres table keys on a slug built from
      // the heading text, and slugs made from bare codes ("yfb") would be
      // user-facing nonsense.
      "n.book_id, '93', s.subject_code, false",
    ],
  ] as const) {
    let lastIsbn = '';
    let total = 0;

    for (;;) {
      const [bound] = await db.execute<{ max_isbn: string | null }>(sql.raw(`
        SELECT max(isbn13) AS max_isbn
        FROM (
          SELECT isbn13 FROM ${STAGING_NEW_BOOKS}
          WHERE isbn13 > '${lastIsbn}'
          ORDER BY isbn13
          LIMIT ${INSERT_BATCH_SIZE}
        ) t
      `));

      const upperBound = bound?.max_isbn;
      if (!upperBound) break;

      const result = await db.execute<{ count: string }>(sql.raw(`
        WITH ins AS (
          INSERT INTO ${target} (${columns})
          SELECT ${selectList}
          FROM ${STAGING_NEW_BOOKS} n
          JOIN ${source} s ON s.isbn13 = n.isbn13
          WHERE n.isbn13 > '${lastIsbn}' AND n.isbn13 <= '${upperBound}'
          RETURNING 1
        )
        SELECT count(*)::text AS count FROM ins
      `));

      total += Number(result[0]?.count ?? 0);
      lastIsbn = upperBound;
    }

    logger.info('GARDBIB: inserted relations', { relation: label, rows: total });
    if (label === 'contributors') contributors = total;
    else subjects = total;
  }

  return { contributors, subjects };
}

// The ISBN-keyed feed tables carry a nullable book_id that's backfilled once
// the corresponding book exists. Every book this run creates is one those
// feeds may already have been waiting on — 202,539 in-stock ISBNs had no
// book row as of 2026-07-27 — so link them here rather than leaving it to a
// later pass. Set-based against the staging table of new books, so it costs
// one statement per feed table regardless of volume.
const FEED_TABLES_WITH_BOOK_ID = [
  'gardners_stock',
  'gardners_promotions',
  'gardners_firm_sale',
  'gardners_market_restrictions',
];

async function backfillFeedBookIds(): Promise<Record<string, number>> {
  const linked: Record<string, number> = {};

  for (const table of FEED_TABLES_WITH_BOOK_ID) {
    const [row] = await db.execute<{ count: string }>(sql.raw(`
      WITH updated AS (
        UPDATE ${table} t
        SET book_id = n.book_id
        FROM ${STAGING_NEW_BOOKS} n
        WHERE t.isbn13 = n.isbn13 AND t.book_id IS NULL
        RETURNING 1
      )
      SELECT count(*)::text AS count FROM updated
    `));
    linked[table] = Number(row?.count ?? 0);
  }

  logger.info('GARDBIB: linked feed rows to new books', linked);
  return linked;
}

/**
 * Downloads and loads the whole Gardners catalogue. Safe to re-run: already
 * present ISBNs are skipped, so a second run is a no-op beyond the download.
 */
async function syncFullCatalogue(options: GardbibSyncOptions = {}): Promise<GardbibSyncResult> {
  const started = Date.now();

  // Session-scoped rather than transaction-scoped: the load spans many
  // separate transactions, so the lock has to outlive each of them. It's
  // held on one reserved connection and released in `finally`.
  const lockConnection = await pgClient.reserve();
  const [lock] = await lockConnection`SELECT pg_try_advisory_lock(${ADVISORY_LOCK_KEY}) AS acquired`;
  if (!lock.acquired) {
    lockConnection.release();
    throw new Error('A GARDBIB catalogue load is already running — refusing to start a second one');
  }

  const [job] = await db
    .insert(ingestionJobs)
    .values({
      fileKey: options.dryRun ? `${JOB_FILE_KEY} (dry-run)` : JOB_FILE_KEY,
      status: 'processing',
      startedAt: new Date(),
    })
    .returning({ id: ingestionJobs.id });

  const suffix = `${job.id}-${Date.now()}`;
  const zipPath = options.localZipPath ?? join(tmpdir(), `gardbib-${suffix}.zip`);
  const tsvPaths = {
    books: join(tmpdir(), `gardbib-books-${suffix}.tsv`),
    contributors: join(tmpdir(), `gardbib-contributors-${suffix}.tsv`),
    subjects: join(tmpdir(), `gardbib-subjects-${suffix}.tsv`),
  };

  try {
    if (options.localZipPath) {
      logger.info('GARDBIB: using local zip, skipping download', { zipPath });
    } else {
      logger.info('GARDBIB: downloading', { remotePath: REMOTE_PATH, jobId: job.id });
      await gardnersConnections.withGenericSftp(async (client) => {
        // Gardners writes the .DONE sentinel only once the zip is fully
        // written; without this check a run started mid-publish would parse a
        // partial file and silently stage a truncated catalogue.
        if (!(await client.exists(DONE_SENTINEL))) {
          throw new Error(`${DONE_SENTINEL} missing — GARDBIB.zip may still be uploading`);
        }
        await client.downloadToFile(REMOTE_PATH, zipPath);
      });
    }

    const { size } = await stat(zipPath);
    logger.info('GARDBIB: file ready', {
      mb: (size / 1e6).toFixed(1),
      seconds: ((Date.now() - started) / 1000).toFixed(0),
    });

    await createStagingTables();
    const { recordsParsed, recordsStaged } = await parseToTsvFiles(
      zipPath,
      tsvPaths,
      options.maxRecords,
    );
    logger.info('GARDBIB: parsed', { recordsParsed, recordsStaged });

    await stageTsvFiles(tsvPaths);

    if (options.dryRun) {
      const insertable = await countInsertableBooks();
      const result: GardbibSyncResult = {
        jobId: job.id,
        recordsParsed,
        recordsStaged,
        booksInserted: 0,
        contributorsInserted: 0,
        subjectsInserted: 0,
        skippedExisting: recordsStaged - insertable,
        durationMs: Date.now() - started,
      };
      await db
        .update(ingestionJobs)
        .set({
          status: 'completed',
          totalBooks: recordsStaged,
          errorMessage: `dry run — would have inserted ${insertable} books`,
          completedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(ingestionJobs.id, job.id));
      logger.info('GARDBIB: dry run complete', { ...result, wouldInsert: insertable });
      return result;
    }

    const { inserted } = await insertNewBooks(job.id);
    const relations = await insertRelationsForNewBooks();
    await backfillFeedBookIds();

    const result: GardbibSyncResult = {
      jobId: job.id,
      recordsParsed,
      recordsStaged,
      booksInserted: inserted,
      contributorsInserted: relations.contributors,
      subjectsInserted: relations.subjects,
      skippedExisting: recordsStaged - inserted,
      durationMs: Date.now() - started,
    };

    await db
      .update(ingestionJobs)
      .set({
        status: 'completed',
        totalBooks: recordsStaged,
        processedBooks: inserted,
        completedAt: new Date(),
        updatedAt: new Date(),
      })
      .where(eq(ingestionJobs.id, job.id));

    logger.info('GARDBIB: complete', { ...result, minutes: (result.durationMs / 60000).toFixed(1) });
    return result;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await db
      .update(ingestionJobs)
      .set({ status: 'failed', errorMessage: message, updatedAt: new Date() })
      .where(eq(ingestionJobs.id, job.id));
    logger.error('GARDBIB: failed', { jobId: job.id, error: message });
    throw err;
  } finally {
    await lockConnection`SELECT pg_advisory_unlock(${ADVISORY_LOCK_KEY})`.catch(() => undefined);
    lockConnection.release();
    await dropStagingTables();
    // A caller-supplied zip belongs to the caller — only clean up what this
    // run downloaded itself.
    const ownedFiles = options.localZipPath
      ? Object.values(tsvPaths)
      : [zipPath, ...Object.values(tsvPaths)];
    for (const path of ownedFiles) {
      await unlink(path).catch(() => undefined);
    }
  }
}

export const gardnersGardbibService = { syncFullCatalogue };
