import { sql } from 'drizzle-orm';
import { db } from '../db';
import { redis, fileQueue, chunkQueue, gardnersFileQueue, gardnersChunkQueue } from '../queue';
import { gardnersCoverService } from './gardners/gardners-cover-sync.service';
import { config } from '../config';

const DEPENDENCY_TIMEOUT_MS = 3_000;

/**
 * Health checks must never be the thing that hangs. A wedged dependency
 * should surface as a fast "down", not as a request that sits open until the
 * monitor times out and reports something less specific.
 */
async function withTimeout<T>(label: string, p: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} check timed out after ${DEPENDENCY_TIMEOUT_MS}ms`)),
      DEPENDENCY_TIMEOUT_MS,
    );
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    clearTimeout(timer!);
  }
}

/**
 * Raw db.execute() rows skip Drizzle's column mapping, so timestamps arrive
 * as Postgres strings ("2026-05-25 10:59:50.513754+00"). Normalise to
 * ISO-8601 so consumers of this endpoint can parse dates without knowing
 * that detail.
 */
function toIso(value: string | null): string | null {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? value : d.toISOString();
}

export interface DependencyStatus {
  ok: boolean;
  latencyMs: number;
  error?: string;
}

/**
 * postgres.js connection failures often carry an empty `message` and put the
 * useful part in `code` (ECONNREFUSED, ENOTFOUND). Falling back through code
 * then constructor name keeps the check from reporting `error: ""`, which
 * tells an operator nothing.
 */
function describeError(err: unknown): string {
  if (err instanceof Error) {
    if (err.message) return err.message;
    const code = (err as NodeJS.ErrnoException).code;
    if (code) return code;
    return err.constructor?.name ?? 'Unknown error';
  }
  return String(err) || 'Unknown error';
}

async function checkDependency(label: string, probe: () => Promise<unknown>): Promise<DependencyStatus> {
  const startedAt = Date.now();
  try {
    await withTimeout(label, probe());
    return { ok: true, latencyMs: Date.now() - startedAt };
  } catch (err) {
    return {
      ok: false,
      latencyMs: Date.now() - startedAt,
      error: describeError(err),
    };
  }
}

/** Liveness + dependency reachability. Cheap enough to poll on a schedule. */
async function readiness(): Promise<{
  ok: boolean;
  dependencies: { database: DependencyStatus; redis: DependencyStatus };
}> {
  const [database, redisStatus] = await Promise.all([
    checkDependency('database', () => db.execute(sql`SELECT 1`)),
    checkDependency('redis', () => redis.ping()),
  ]);

  return {
    ok: database.ok && redisStatus.ok,
    dependencies: { database, redis: redisStatus },
  };
}

export interface StatusCounts {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
}

interface RecentFailure {
  source: string;
  reference: string;
  error: string | null;
  at: string | null;
}

/**
 * ONIX side: ingestion_jobs rolled up by status, plus the last file that
 * actually landed. 'enqueued' folds into pending — from an operator's point
 * of view both mean "accepted, not finished".
 */
async function onixStats(windowHours: number) {
  const [counts] = await db.execute<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
  }>(sql`
    SELECT
      COUNT(*) FILTER (WHERE status IN ('pending', 'enqueued'))::int AS pending,
      COUNT(*) FILTER (WHERE status = 'processing')::int AS processing,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
      COUNT(*) FILTER (WHERE status = 'failed')::int AS failed
    FROM ingestion_jobs
    WHERE created_at > NOW() - (${windowHours} * INTERVAL '1 hour')
  `);

  const [lastCompleted] = await db.execute<{
    file_key: string;
    completed_at: string;
    total_books: number | null;
    processed_books: number | null;
    failed_chunks: number | null;
  }>(sql`
    SELECT file_key, completed_at, total_books, processed_books, failed_chunks
    FROM ingestion_jobs
    WHERE status = 'completed'
    ORDER BY completed_at DESC NULLS LAST
    LIMIT 1
  `);

  return {
    window: `${windowHours}h`,
    counts: counts ?? { pending: 0, processing: 0, completed: 0, failed: 0 },
    lastCompleted: lastCompleted
      ? {
          fileKey: lastCompleted.file_key,
          completedAt: toIso(lastCompleted.completed_at),
          totalBooks: lastCompleted.total_books,
          processedBooks: lastCompleted.processed_books,
          failedChunks: lastCompleted.failed_chunks,
        }
      : null,
  };
}

/**
 * Gardners side: one row per feed showing its most recent run. This is the
 * "is anything silently stale?" view — a feed whose lastRunAt is days old on
 * a daily schedule is the signal, and it can't be seen from counts alone.
 */
async function gardnersStats(windowHours: number) {
  const perFeed = await db.execute<{
    feed: string;
    last_status: string;
    last_run_at: string | null;
    last_completed_at: string | null;
    last_row_count: number | null;
    runs_in_window: number;
    failures_in_window: number;
  }>(sql`
    WITH latest AS (
      SELECT DISTINCT ON (feed)
        feed, status, started_at, completed_at, row_count
      FROM gardners_fetch_log
      ORDER BY feed, started_at DESC
    ),
    windowed AS (
      SELECT
        feed,
        COUNT(*)::int AS runs,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failures
      FROM gardners_fetch_log
      WHERE started_at > NOW() - (${windowHours} * INTERVAL '1 hour')
      GROUP BY feed
    )
    SELECT
      latest.feed,
      latest.status AS last_status,
      latest.started_at AS last_run_at,
      latest.completed_at AS last_completed_at,
      latest.row_count AS last_row_count,
      COALESCE(windowed.runs, 0) AS runs_in_window,
      COALESCE(windowed.failures, 0) AS failures_in_window
    FROM latest
    LEFT JOIN windowed ON windowed.feed = latest.feed
    ORDER BY latest.feed
  `);

  return perFeed.map((r) => ({
    feed: r.feed,
    lastStatus: r.last_status,
    lastRunAt: toIso(r.last_run_at),
    lastCompletedAt: toIso(r.last_completed_at),
    lastRowCount: r.last_row_count,
    runsInWindow: r.runs_in_window,
    failuresInWindow: r.failures_in_window,
  }));
}

/** The most recent failures across both pipelines, newest first. */
async function recentFailures(limit: number): Promise<RecentFailure[]> {
  const rows = await db.execute<{
    source: string;
    reference: string;
    error: string | null;
    at: string | null;
  }>(sql`
    (
      SELECT 'onix' AS source, file_key AS reference, error_message AS error, updated_at AS at
      FROM ingestion_jobs
      WHERE status = 'failed'
      ORDER BY updated_at DESC
      LIMIT ${limit}
    )
    UNION ALL
    (
      SELECT 'gardners:' || feed AS source, remote_filename AS reference,
             error_message AS error, started_at AS at
      FROM gardners_fetch_log
      WHERE status = 'failed'
      ORDER BY started_at DESC
      LIMIT ${limit}
    )
    ORDER BY at DESC NULLS LAST
    LIMIT ${limit}
  `);

  return rows.map((r) => ({
    source: r.source,
    reference: r.reference,
    error: r.error,
    at: toIso(r.at),
  }));
}

/** Queue depths — the fastest way to spot a stalled or backed-up worker. */
async function queueStats() {
  const queues = [
    ['onix-file', fileQueue],
    ['onix-chunk', chunkQueue],
    ['gardners-file', gardnersFileQueue],
    ['gardners-chunk', gardnersChunkQueue],
  ] as const;

  const entries = await Promise.all(
    queues.map(async ([name, queue]) => {
      const counts = await queue.getJobCounts('waiting', 'active', 'delayed', 'failed');
      return [name, counts] as const;
    }),
  );

  return Object.fromEntries(entries);
}

/** Percentage helper — guards the empty-table case so we never divide by zero. */
function pct(part: number, whole: number): number {
  if (!whole) return 0;
  return Math.round((part / whole) * 10000) / 100;
}

/**
 * Catalogue size and enrichment coverage. This is the "what is actually in
 * the tables" view that job/feed stats deliberately can't answer: a feed can
 * be running perfectly and still be enriching nothing, and covers/excerpts
 * write no fetch-log rows at all, so this is the only place a silent failure
 * in either becomes visible.
 *
 * Cost note: these are unfiltered aggregates over the whole books table
 * (~1.1M rows) plus a semi-join against book_excerpts, so expect a few
 * hundred ms to low seconds. Deliberately kept out of readiness(), which is
 * polled on a schedule and must stay cheap.
 */
async function counts() {
  const [books] = await db.execute<{
    total: number;
    active: number;
    removed: number;
    with_cover: number;
    with_isbn: number;
    with_embedding: number;
    gardners_cover_checked: number;
    last_created_at: string | null;
    last_updated_at: string | null;
    last_cover_fetched_at: string | null;
  }>(sql`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE is_removed = FALSE)::int AS active,
      COUNT(*) FILTER (WHERE is_removed = TRUE)::int AS removed,
      COUNT(*) FILTER (WHERE is_removed = FALSE AND cover_url IS NOT NULL)::int AS with_cover,
      COUNT(*) FILTER (WHERE is_removed = FALSE AND isbn13 IS NOT NULL)::int AS with_isbn,
      COUNT(*) FILTER (WHERE is_removed = FALSE AND embedding IS NOT NULL)::int AS with_embedding,
      COUNT(*) FILTER (WHERE is_removed = FALSE AND gardners_cover_checked_at IS NOT NULL)::int
        AS gardners_cover_checked,
      MAX(created_at) AS last_created_at,
      MAX(updated_at) AS last_updated_at,
      MAX(cover_fetched_at) AS last_cover_fetched_at
    FROM books
  `);

  const [excerpts] = await db.execute<{
    total: number;
    available: number;
    last_fetched_at: string | null;
    last_jb_updated_at: string | null;
  }>(sql`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE available)::int AS available,
      MAX(fetched_at) AS last_fetched_at,
      MAX(jb_updated_at) AS last_jb_updated_at
    FROM book_excerpts
  `);

  // Books that actually resolve to an excerpt — the number that matters.
  // book_excerpts holds Jellybooks' whole catalogue, most of which is for
  // ISBNs we don't carry, so its row count on its own overstates coverage.
  const [matched] = await db.execute<{ books_with_excerpt: number }>(sql`
    SELECT COUNT(*)::int AS books_with_excerpt
    FROM books b
    WHERE b.is_removed = FALSE
      AND b.isbn13 IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM book_excerpts e
        WHERE e.isbn13 = b.isbn13 AND e.available
      )
  `);

  const activeBooks = books?.active ?? 0;
  const booksWithExcerpt = matched?.books_with_excerpt ?? 0;

  return {
    books: {
      total: books?.total ?? 0,
      active: activeBooks,
      removed: books?.removed ?? 0,
      withIsbn13: books?.with_isbn ?? 0,
      lastCreatedAt: toIso(books?.last_created_at ?? null),
      lastUpdatedAt: toIso(books?.last_updated_at ?? null),
    },
    covers: {
      withCover: books?.with_cover ?? 0,
      missingCover: activeBooks - (books?.with_cover ?? 0),
      coveragePct: pct(books?.with_cover ?? 0, activeBooks),
      gardnersChecked: books?.gardners_cover_checked ?? 0,
      lastCoverFetchedAt: toIso(books?.last_cover_fetched_at ?? null),
    },
    excerpts: {
      // Rows in book_excerpts — Jellybooks' catalogue, not our coverage.
      totalRows: excerpts?.total ?? 0,
      availableRows: excerpts?.available ?? 0,
      booksWithExcerpt,
      coveragePct: pct(booksWithExcerpt, activeBooks),
      lastFetchedAt: toIso(excerpts?.last_fetched_at ?? null),
      lastJellybooksUpdateAt: toIso(excerpts?.last_jb_updated_at ?? null),
    },
    embeddings: {
      withEmbedding: books?.with_embedding ?? 0,
      missingEmbedding: activeBooks - (books?.with_embedding ?? 0),
      coveragePct: pct(books?.with_embedding ?? 0, activeBooks),
    },
  };
}

async function stats(options: { windowHours: number; failureLimit: number }) {
  const { windowHours, failureLimit } = options;

  const [onix, gardners, failures, queues] = await Promise.all([
    onixStats(windowHours),
    gardnersStats(windowHours),
    recentFailures(failureLimit),
    queueStats(),
  ]);

  return {
    onix,
    gardners: {
      ingestionEnabled: config.gardners.ingestionEnabled,
      coverBackfillRunning: gardnersCoverService.isCoverBackfillRunning(),
      feeds: gardners,
    },
    queues,
    recentFailures: failures,
  };
}

export const healthService = { readiness, stats, counts };
