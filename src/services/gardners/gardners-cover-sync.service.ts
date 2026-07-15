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
import { gardnersConnections } from './connections.service';
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

/**
 * Backfills covers for existing books from /Books/Full, one small batch per
 * call. Tracks its own attempt cadence via gardnersCoverCheckedAt, separate
 * from coverFetchedAt (which the Google Books fallback owns) — this is what
 * lets Google Books act as a true last resort: its candidate query requires
 * gardnersCoverCheckedAt to already be set, so it never races this function
 * for the same untouched books, only picks up what this function couldn't
 * find.
 *
 * Batch-sized deliberately, not a full-catalogue walk in one call — for an
 * initial bulk catch-up across ~2M titles, invoke this repeatedly (e.g. a
 * small loop in a one-off script) or temporarily raise
 * GARDNERS_COVER_SYNC_BATCH_SIZE, same as this codebase already does for
 * the embedding backfill.
 */
async function syncFullCatalogue(): Promise<void> {
  const { batchSize, delayMs } = config.gardners.coverSync;
  const thirtyDaysAgo = new Date(Date.now() - THIRTY_DAYS_MS);

  const candidates = await db
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

  if (candidates.length === 0) {
    logger.info('Gardners cover full-catalogue sync: no books need covers');
    return;
  }

  let fetched = 0;
  let notFound = 0;
  let errors = 0;

  await gardnersConnections.withCoversFtp(async (client) => {
    for (const book of candidates) {
      const isbn13 = book.isbn13!;
      const remotePath = `/Books/Full/${isbn13.slice(0, 8)}/${isbn13}.jpg`;

      try {
        const size = await client.size(remotePath).catch(() => null);
        if (size === null) {
          notFound++;
          await db
            .update(books)
            .set({ gardnersCoverCheckedAt: new Date() })
            .where(eq(books.id, book.id));
        } else {
          const stream = await client.readStream(remotePath);
          const key = r2KeyForCover(isbn13);
          await storageService.uploadStream(key, stream, 'image/jpeg', size);
          await db
            .update(books)
            .set({ coverUrl: publicUrlForKey(key), gardnersCoverCheckedAt: new Date() })
            .where(eq(books.id, book.id));
          fetched++;
        }
      } catch (err) {
        errors++;
        logger.error('Gardners full-catalogue cover fetch failed', {
          bookId: book.id,
          isbn13,
          error: err instanceof Error ? err.message : String(err),
        });
        await db
          .update(books)
          .set({ gardnersCoverCheckedAt: new Date() })
          .where(eq(books.id, book.id));
      }

      if (delayMs > 0) {
        await new Promise((res) => setTimeout(res, delayMs));
      }
    }
  });

  logger.info('Gardners cover full-catalogue sync: batch complete', { fetched, notFound, errors });
}

export const gardnersCoverService = { syncWeeklyUpdates, syncFullCatalogue };
