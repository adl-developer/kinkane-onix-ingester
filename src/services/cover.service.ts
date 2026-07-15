import { eq, sql, isNull, or, and, lt } from 'drizzle-orm';
import { db } from '../db';
import { books, bookContributors } from '../db/schema';
import { config } from '../config';
import { logger } from '../lib/logger';
import { storageService } from './storage.service';

interface GoogleBooksResponse {
  items?: Array<{
    volumeInfo?: {
      imageLinks?: {
        thumbnail?: string;
        smallThumbnail?: string;
      };
    };
  }>;
}

const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;

/** Returns the highest-resolution Google Books cover URL for a given thumbnail URL. */
function buildHighResUrl(thumbnail: string): string {
  const u = new URL(thumbnail);
  u.protocol = 'https:';
  u.searchParams.set('zoom', '5');
  u.searchParams.set('fife', 'w800');
  u.searchParams.delete('edge');
  return u.toString();
}

/**
 * Extracts the Google Books volume ID from an image URL.
 * Used as the R2 key suffix when no ISBN13 is available.
 */
function extractVolumeId(thumbnailUrl: string): string | null {
  const match = thumbnailUrl.match(/[?&]id=([^&]+)/);
  return match ? match[1] : null;
}

/**
 * Downloads the cover image from Google Books and uploads it to R2.
 * Returns the stable R2 public URL, or null if the download/upload fails.
 */
async function uploadCoverToR2(
  highResUrl: string,
  isbn13: string | null,
  publicBaseUrl: string,
): Promise<string | null> {
  const imageResponse = await fetch(highResUrl);
  if (!imageResponse.ok) {
    throw new Error(`Cover image download failed with ${imageResponse.status}`);
  }

  const buffer = Buffer.from(await imageResponse.arrayBuffer());
  const contentType = imageResponse.headers.get('content-type') ?? 'image/jpeg';
  const ext = contentType.includes('png') ? 'png' : 'jpg';

  // Prefer ISBN13 as the key; fall back to the Google Books volume ID
  const keyId = isbn13 ?? extractVolumeId(highResUrl);
  if (!keyId) {
    throw new Error('Cannot derive a stable R2 key: no ISBN13 and no volume ID in URL');
  }

  const key = `covers/${keyId}.${ext}`;
  await storageService.uploadBuffer(key, buffer, contentType);

  return `${publicBaseUrl.replace(/\/$/, '')}/${key}`;
}

async function fetchFromGoogleBooks(
  isbn13: string | null,
  title: string,
  authors: string[],
  apiKey: string,
): Promise<string | null> {
  let query: string;

  if (isbn13) {
    query = `isbn:${isbn13}`;
  } else {
    // Fall back to title + first author when no ISBN
    const parts = [`intitle:${title}`];
    if (authors.length > 0) parts.push(`inauthor:${authors[0]}`);
    query = parts.join('+');
  }

  const url =
    `https://www.googleapis.com/books/v1/volumes` +
    `?q=${encodeURIComponent(query)}` +
    `&key=${apiKey}` +
    `&fields=items/volumeInfo/imageLinks` +
    `&maxResults=1`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Google Books API responded with ${response.status}`);
  }

  const data = (await response.json()) as GoogleBooksResponse;
  const thumbnail = data.items?.[0]?.volumeInfo?.imageLinks?.thumbnail;

  if (!thumbnail) return null;

  const highResUrl = buildHighResUrl(thumbnail);

  // If R2 is configured, download the image and re-host it so we own the URL.
  // Otherwise fall back to the upgraded Google Books URL.
  const publicBaseUrl = config.r2.publicUrl;
  return uploadCoverToR2(highResUrl, isbn13, publicBaseUrl);
}

export const coverService = {
  async fetchMissingCovers(): Promise<void> {
    const apiKey = config.googleBooks.apiKey;

    if (!apiKey) {
      logger.warn('GOOGLE_BOOKS_API_KEY not set — skipping cover fetch');
      return;
    }

    const { coverFetchBatchSize, coverFetchDelayMs } = config.cron;
    const thirtyDaysAgo = new Date(Date.now() - THIRTY_DAYS_MS);

    // Last resort only: for books with an ISBN13, Gardners must have already
    // checked (gardnersCoverCheckedAt set) and found nothing, so this never
    // races gardnersCoverService.syncFullCatalogue for the same untouched
    // books. Books with no ISBN13 skip that gate entirely — Gardners has no
    // way to look those up at all, so Google Books is the only option.
    // Either way, only considers books Google Books itself hasn't tried yet
    // (or tried 30+ days ago and still has no cover).
    const candidates = await db
      .select({
        id: books.id,
        isbn13: books.isbn13,
        title: books.title,
      })
      .from(books)
      .where(
        and(
          isNull(books.coverUrl),
          or(isNull(books.isbn13), sql`${books.gardnersCoverCheckedAt} IS NOT NULL`),
          or(isNull(books.coverFetchedAt), lt(books.coverFetchedAt, thirtyDaysAgo)),
        ),
      )
      .limit(coverFetchBatchSize);

    if (candidates.length === 0) {
      logger.info('Cover fetch: no books need covers');
      return;
    }

    logger.info('Cover fetch: starting batch', { count: candidates.length });

    // Batch-load authors for all candidates in one query
    const ids = candidates.map((b) => b.id);
    const contributorRows = await db
      .select({
        bookId: bookContributors.bookId,
        personName: bookContributors.personName,
      })
      .from(bookContributors)
      .where(
        and(
          sql`${bookContributors.bookId} = ANY(ARRAY[${sql.join(ids.map((id) => sql`${id}`), sql`, `)}]::int[])`,
          eq(bookContributors.role, 'A01'),
        ),
      )
      .orderBy(bookContributors.sequenceNumber);

    const authorMap = new Map<number, string[]>();
    for (const row of contributorRows) {
      if (!authorMap.has(row.bookId)) authorMap.set(row.bookId, []);
      if (row.personName) authorMap.get(row.bookId)!.push(row.personName);
    }

    let fetched = 0;
    let notFound = 0;
    let errors = 0;

    for (const book of candidates) {
      const authors = authorMap.get(book.id) ?? [];

      try {
        const coverUrl = await fetchFromGoogleBooks(book.isbn13, book.title, authors, apiKey);

        await db
          .update(books)
          .set({ coverUrl, coverFetchedAt: new Date() })
          .where(eq(books.id, book.id));

        if (coverUrl) {
          fetched++;
        } else {
          notFound++;
        }
      } catch (err) {
        errors++;
        logger.error('Cover fetch failed for book', {
          bookId: book.id,
          isbn13: book.isbn13,
          error: err instanceof Error ? err.message : String(err),
        });

        // Still mark as attempted so we don't hammer a failing API
        await db
          .update(books)
          .set({ coverFetchedAt: new Date() })
          .where(eq(books.id, book.id));
      }

      // Respect Google Books rate limit — 200ms between requests = 5 req/s
      if (coverFetchDelayMs > 0) {
        await new Promise((res) => setTimeout(res, coverFetchDelayMs));
      }
    }

    logger.info('Cover fetch: batch complete', { fetched, notFound, errors });
  },
};
