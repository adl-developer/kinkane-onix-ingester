import { inArray, sql } from 'drizzle-orm';
import { db } from '../db';
import { books, bookExcerpts } from '../db/schema';
import { config } from '../config';
import { logger } from '../lib/logger';

interface JellybooksExcerpt {
  isbn13: string;
  title?: string;
  url?: string;
  available?: boolean;
  update_reason?: 'added' | 'withdrawn';
  updated_at?: string;
  created_at?: string;
}

interface JellybooksResponse {
  response_header: { status: string; [key: string]: unknown };
  excerpts: JellybooksExcerpt[];
}

const FETCH_TIMEOUT_MS = 20_000;
const MAX_FETCH_ATTEMPTS = 3;
const UPSERT_BATCH_SIZE = 500;

async function fetchWithTimeout(url: URL, headers: Record<string, string>): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, { headers, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function callJellybooks(path: string, params: Record<string, string>): Promise<JellybooksExcerpt[]> {
  const apiKey = config.jellybooks.apiKey;
  if (!apiKey) {
    throw new Error('JELLYBOOKS_API_KEY not set');
  }

  const url = new URL(path, config.jellybooks.baseUrl);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const headers = { Accept: 'application/json', 'JB-Discovery-Api-Key': apiKey };

  let lastError: unknown;
  for (let attempt = 1; attempt <= MAX_FETCH_ATTEMPTS; attempt++) {
    try {
      const response = await fetchWithTimeout(url, headers);

      if (!response.ok) {
        // 5xx is worth retrying (transient); 4xx (bad key, bad params) is not.
        if (response.status < 500) {
          throw new Error(`Jellybooks API responded with ${response.status}`);
        }
        throw Object.assign(new Error(`Jellybooks API responded with ${response.status}`), { retryable: true });
      }

      const data = (await response.json()) as JellybooksResponse;
      return data.excerpts ?? [];
    } catch (err) {
      lastError = err;
      const retryable = err instanceof Error && (err as Error & { retryable?: boolean }).retryable;
      const isAbort = err instanceof Error && err.name === 'AbortError';

      if (!retryable && !isAbort) throw err;

      if (attempt < MAX_FETCH_ATTEMPTS) {
        const delay = 1000 * 2 ** (attempt - 1);
        logger.warn('Jellybooks API call failed, retrying', {
          attempt,
          delayMs: delay,
          timedOut: isAbort,
          error: (err as Error).message,
        });
        await new Promise((res) => setTimeout(res, delay));
      }
    }
  }
  throw lastError;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function upsertExcerpts(excerpts: JellybooksExcerpt[]): Promise<{ upserted: number; withdrawn: number; skipped: number }> {
  if (excerpts.length === 0) {
    return { upserted: 0, withdrawn: 0, skipped: 0 };
  }

  // Only check ownership for the ISBNs in this batch — loading the entire
  // books table on every sync (including small incremental deltas) doesn't scale.
  const batchIsbns = [...new Set(excerpts.map((e) => e.isbn13))];
  const ownRows = await db
    .select({ isbn13: books.isbn13 })
    .from(books)
    .where(inArray(books.isbn13, batchIsbns));
  const ownIsbns = new Set(ownRows.map((r) => r.isbn13!));

  let upserted = 0;
  let withdrawn = 0;
  let skipped = 0;

  const owned = excerpts.filter((e) => {
    if (!ownIsbns.has(e.isbn13)) {
      skipped++;
      return false;
    }
    return true;
  });

  const now = new Date();
  const rows = owned.map((excerpt) => {
    const available = excerpt.update_reason === 'withdrawn' ? false : excerpt.available ?? true;
    if (available) upserted++;
    else withdrawn++;

    return {
      isbn13: excerpt.isbn13,
      title: excerpt.title,
      url: excerpt.url,
      available,
      jbUpdatedAt: excerpt.updated_at ? new Date(excerpt.updated_at) : null,
      fetchedAt: now,
    };
  });

  for (const batch of chunk(rows, UPSERT_BATCH_SIZE)) {
    await db
      .insert(bookExcerpts)
      .values(batch)
      .onConflictDoUpdate({
        target: bookExcerpts.isbn13,
        set: {
          title: sql`excluded.title`,
          url: sql`excluded.url`,
          available: sql`excluded.available`,
          jbUpdatedAt: sql`excluded.jb_updated_at`,
          fetchedAt: sql`excluded.fetched_at`,
          updatedAt: sql`now()`,
        },
      });
  }

  return { upserted, withdrawn, skipped };
}

export const excerptService = {
  /** Full catalogue snapshot — used for the initial backfill. */
  async backfillExcerpts(): Promise<void> {
    logger.info('Excerpt backfill: starting full feed fetch');

    const excerpts = await callJellybooks('/discovery/api/excerpts', {});

    if (excerpts.length === 0) {
      logger.info('Excerpt backfill: no excerpts returned');
      return;
    }

    const { upserted, withdrawn, skipped } = await upsertExcerpts(excerpts);
    logger.info('Excerpt backfill: complete', { upserted, withdrawn, skipped });
  },

  /** Incremental sync using the delta feed — run on a recurring schedule. */
  async syncExcerpts(): Promise<void> {
    if (!config.jellybooks.apiKey) {
      logger.warn('JELLYBOOKS_API_KEY not set — skipping excerpt sync');
      return;
    }

    const [{ maxFetchedAt }] = await db
      .select({ maxFetchedAt: sql<Date | null>`max(${bookExcerpts.fetchedAt})` })
      .from(bookExcerpts);

    if (!maxFetchedAt) {
      // No data yet — seed from the full feed instead of the delta feed.
      await this.backfillExcerpts();
      return;
    }

    // Delta feed has a 14-day retention window — clamp to that.
    const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
    const updatedSince = maxFetchedAt > fourteenDaysAgo ? maxFetchedAt : fourteenDaysAgo;

    logger.info('Excerpt sync: fetching delta feed', { updatedSince: updatedSince.toISOString() });

    const excerpts = await callJellybooks('/discovery/api/excerpt_updates', {
      updated_since: updatedSince.toISOString(),
    });

    if (excerpts.length === 0) {
      logger.info('Excerpt sync: no updates');
      return;
    }

    const { upserted, withdrawn, skipped } = await upsertExcerpts(excerpts);
    logger.info('Excerpt sync: complete', { upserted, withdrawn, skipped });
  },
};
