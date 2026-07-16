import { unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import * as unzipper from 'unzipper';
import { and, eq, isNull, lt, or, sql } from 'drizzle-orm';
import { db } from '../../db';
import { books } from '../../db/schema';
import { config } from '../../config';
import { logger } from '../../lib/logger';
import { storageService } from '../storage.service';
import { gardnersConnections, GardnersRemoteClient } from './connections.service';
import { FetchFeedConfig, gardnersFetcher, RemoteFileDescriptor } from './fetcher.service';
import { isValidIsbn13 } from './parsing-utils';

// covers.gardners.com — plain FTP, not SFTP. Two of the four documented
// directories (/Books/InStock, /Books/Other) were verified live to be
// completely empty (not just filtered — a raw listing shows zero entries),
// so nothing is built against them here; only /Books/Full and /Books/Update
// actually contain data.
const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;

// /Books/Update holds weekly ZIP bundles (not per-image files as originally
// assumed) — verified live: each zip contains a single top-level folder
// (BookImages_wk{NN}/) of flat `{isbn13}.jpg` files, no prefix nesting.
// Sizes vary wildly in practice: wk01 (the "since 01 Jan 2026" catch-up) is
// ~29GB, other weeks range ~500MB-7GB. Anything above this threshold is
// skipped rather than risking disk space on whatever's running this —
// logged clearly so a human can decide how to handle an outlier manually.
const UPDATE_ZIP_RE = /^BookImages_wk\d+\.zip$/i;
const MAX_UPDATE_ZIP_BYTES = 5 * 1024 * 1024 * 1024;

export const gardnersCoverUpdateFeedConfig: FetchFeedConfig = {
  feed: 'covers_update',
  connection: 'coversFtp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Books/Update');
    return files.filter((f) => UPDATE_ZIP_RE.test(f.filename));
  },
};

function r2KeyForCover(isbn13: string): string {
  return `covers/${isbn13}.jpg`;
}

function publicUrlForKey(key: string): string {
  return `${config.r2.publicUrl.replace(/\/$/, '')}/${key}`;
}

export async function processUpdateZip(file: RemoteFileDescriptor): Promise<void> {
  const logId = await gardnersFetcher.claimFile('covers_update', file);
  const localPath = join(tmpdir(), `gardners-covers-${logId}.zip`);

  try {
    await gardnersFetcher.downloadToLocalFile('coversFtp', file, localPath);
    const directory = await unzipper.Open.file(localPath);

    let processed = 0;
    let skipped = 0;

    for (const entry of directory.files) {
      if (entry.type !== 'File') continue;

      const basename = entry.path.split('/').pop() ?? '';
      const isbn13 = basename.replace(/\.jpe?g$/i, '');
      if (!isValidIsbn13(isbn13)) {
        skipped++;
        continue;
      }

      const key = r2KeyForCover(isbn13);
      await storageService.uploadStream(key, entry.stream(), 'image/jpeg', entry.uncompressedSize);

      await db
        .update(books)
        .set({ coverUrl: publicUrlForKey(key), gardnersCoverCheckedAt: new Date() })
        .where(eq(books.isbn13, isbn13));

      processed++;
    }

    await gardnersFetcher.markFetchCompleted(logId, { rowCount: processed });
    logger.info('Gardners cover update zip processed', {
      filename: file.filename,
      processed,
      skipped,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    await gardnersFetcher.markFetchFailed(logId, error);
    throw error;
  } finally {
    await unlink(localPath).catch(() => undefined);
  }
}

async function syncWeeklyUpdates(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersCoverUpdateFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners cover update zips found');
    return;
  }

  for (const file of files) {
    if (file.size > MAX_UPDATE_ZIP_BYTES) {
      logger.warn('Skipping oversized Gardners cover update zip — needs manual handling', {
        filename: file.filename,
        sizeBytes: file.size,
      });
      const logId = await gardnersFetcher.claimFile('covers_update', file);
      await gardnersFetcher.markFetchFailed(
        logId,
        new Error(`File too large (${file.size} bytes) for automatic processing`),
      );
      continue;
    }

    await processUpdateZip(file);
  }
}

interface CoverCandidate {
  id: number;
  isbn13: string | null;
}

type CandidateOutcome = 'fetched' | 'notFound' | 'error';

/**
 * Checks /Books/Full for one book's cover and uploads it to R2 if present,
 * marking gardnersCoverCheckedAt either way. Shared by both the sequential
 * (syncFullCatalogue) and concurrent (runConcurrentFullCatalogueSync) full-
 * catalogue backfills — this is the only part that actually talks to the
 * FTP server or the DB per book.
 */
async function processOneCandidate(
  client: GardnersRemoteClient,
  book: CoverCandidate,
): Promise<CandidateOutcome> {
  const isbn13 = book.isbn13!;
  const remotePath = `/Books/Full/${isbn13.slice(0, 8)}/${isbn13}.jpg`;

  try {
    const size = await client.size(remotePath).catch(() => null);
    if (size === null) {
      await db.update(books).set({ gardnersCoverCheckedAt: new Date() }).where(eq(books.id, book.id));
      return 'notFound';
    }

    const stream = await client.readStream(remotePath);
    const key = r2KeyForCover(isbn13);
    await storageService.uploadStream(key, stream, 'image/jpeg', size);
    await db
      .update(books)
      .set({ coverUrl: publicUrlForKey(key), gardnersCoverCheckedAt: new Date() })
      .where(eq(books.id, book.id));
    return 'fetched';
  } catch (err) {
    logger.error('Gardners full-catalogue cover fetch failed', {
      bookId: book.id,
      isbn13,
      error: err instanceof Error ? err.message : String(err),
    });
    await db.update(books).set({ gardnersCoverCheckedAt: new Date() }).where(eq(books.id, book.id));
    return 'error';
  }
}

async function fetchCandidates(batchSize: number): Promise<CoverCandidate[]> {
  const thirtyDaysAgo = new Date(Date.now() - THIRTY_DAYS_MS);

  return db
    .select({ id: books.id, isbn13: books.isbn13 })
    .from(books)
    .where(
      and(
        sql`${books.isbn13} IS NOT NULL`,
        or(
          isNull(books.gardnersCoverCheckedAt),
          and(isNull(books.coverUrl), lt(books.gardnersCoverCheckedAt, thirtyDaysAgo)),
        ),
      ),
    )
    .limit(batchSize);
}

/**
 * Backfills covers for existing books from /Books/Full, one small batch per
 * call, over a single FTP connection. Tracks its own attempt cadence via
 * gardnersCoverCheckedAt, separate from coverFetchedAt (which the Google
 * Books fallback owns) — this is what lets Google Books act as a true last
 * resort: its candidate query requires gardnersCoverCheckedAt to already be
 * set, so it never races this function for the same untouched books, only
 * picks up what this function couldn't find.
 *
 * This is what the daily cron calls — fine for a small day-to-day trickle
 * of new books, but a single connection processing one book at a time
 * (measured live: ~1.4s/book including the FTP round trip and image
 * download) would take ~3-4 weeks for a multi-million-title catalogue. For
 * that case use runConcurrentFullCatalogueSync instead.
 */
async function syncFullCatalogue(overrides?: {
  batchSize?: number;
  delayMs?: number;
}): Promise<number> {
  const batchSize = overrides?.batchSize ?? config.gardners.coverSync.batchSize;
  const delayMs = overrides?.delayMs ?? config.gardners.coverSync.delayMs;

  const candidates = await fetchCandidates(batchSize);
  if (candidates.length === 0) {
    logger.info('Gardners cover full-catalogue sync: no books need covers');
    return 0;
  }

  const outcomes: CandidateOutcome[] = [];

  await gardnersConnections.withCoversFtp(async (client) => {
    for (const book of candidates) {
      outcomes.push(await processOneCandidate(client, book));
      if (delayMs > 0) {
        await new Promise((res) => setTimeout(res, delayMs));
      }
    }
  });

  logger.info('Gardners cover full-catalogue sync: batch complete', {
    fetched: outcomes.filter((o) => o === 'fetched').length,
    notFound: outcomes.filter((o) => o === 'notFound').length,
    errors: outcomes.filter((o) => o === 'error').length,
  });
  return candidates.length;
}

/**
 * Same backfill as syncFullCatalogue, but spreads the work across several
 * FTP connections running in parallel instead of one sequential connection
 * — for an initial bulk catch-up across a multi-million-title catalogue,
 * not for routine cron use (syncFullCatalogue stays the cron's function;
 * this is what gardners-bootstrap.service.ts calls instead).
 *
 * Live-measured against covers.gardners.com: 20 concurrent connections
 * processed 186/186 real cover lookups with zero failures at ~8.7 files/sec
 * (~114ms/book effective, vs ~1.4s/book on one connection) — no server-side
 * connection limit was hit, but this hasn't been tried above 20, and
 * Gardners has never documented a concurrency limit for this server, so
 * treat higher values as unverified.
 *
 * Each round fetches concurrency*batchSize candidates, splits them
 * round-robin across `concurrency` workers, and each worker holds one FTP
 * connection open for the whole round (rather than reconnecting per book)
 * before the next round is fetched. Loops until a round comes back empty.
 * Returns the total number of candidates processed across all rounds.
 */
async function runConcurrentFullCatalogueSync(overrides?: {
  concurrency?: number;
  batchSize?: number;
}): Promise<number> {
  const concurrency = overrides?.concurrency ?? 1;
  const batchSize = overrides?.batchSize ?? config.gardners.coverSync.batchSize;
  const roundSize = batchSize * concurrency;

  let totalProcessed = 0;

  for (;;) {
    const candidates = await fetchCandidates(roundSize);
    if (candidates.length === 0) break;

    const workerCount = Math.min(concurrency, candidates.length);
    const queues: CoverCandidate[][] = Array.from({ length: workerCount }, () => []);
    candidates.forEach((c, i) => queues[i % workerCount].push(c));

    const roundOutcomes: CandidateOutcome[] = [];
    await Promise.all(
      queues.map((queue) =>
        gardnersConnections.withCoversFtp(async (client) => {
          for (const book of queue) {
            roundOutcomes.push(await processOneCandidate(client, book));
          }
        }),
      ),
    );

    totalProcessed += candidates.length;
    logger.info('Gardners concurrent cover full-catalogue sync: round complete', {
      roundSize: candidates.length,
      concurrency: workerCount,
      fetched: roundOutcomes.filter((o) => o === 'fetched').length,
      notFound: roundOutcomes.filter((o) => o === 'notFound').length,
      errors: roundOutcomes.filter((o) => o === 'error').length,
      totalProcessed,
    });
  }

  return totalProcessed;
}

export const gardnersCoverService = {
  syncWeeklyUpdates,
  syncFullCatalogue,
  runConcurrentFullCatalogueSync,
};
