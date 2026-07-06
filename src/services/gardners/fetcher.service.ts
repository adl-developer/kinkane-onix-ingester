import { Readable } from 'stream';
import { and, eq, gt, inArray, ne, or, sql } from 'drizzle-orm';
import { db } from '../../db';
import { gardnersFetchLog, gardnersFeedEnum } from '../../db/schema';
import { storageService } from '../storage.service';
import {
  gardnersConnections,
  GardnersConnectionName,
  GardnersRemoteClient,
  RemoteFileDescriptor,
} from './connections.service';
import { logger } from '../../lib/logger';

export type GardnersFeed = (typeof gardnersFeedEnum.enumValues)[number];
export type { RemoteFileDescriptor, GardnersRemoteClient, GardnersConnectionName };

export interface FetchFeedConfig {
  feed: GardnersFeed;
  connection: GardnersConnectionName;
  // Feed-specific: knows its own path template/date logic, list a candidate
  // directory (or probe specific candidate paths) using the connected client.
  listRemoteFiles: (client: GardnersRemoteClient) => Promise<RemoteFileDescriptor[]>;
  // If true, a candidate file is only eligible once a sibling `${path}.DONE`
  // marker exists (Bespoke Inventory is the one feed that uses this).
  requiresDoneSentinel?: boolean;
}

function withConnection<T>(
  name: GardnersConnectionName,
  fn: (client: GardnersRemoteClient) => Promise<T>,
): Promise<T> {
  switch (name) {
    case 'bespokeSftp':
      return gardnersConnections.withBespokeSftp(fn);
    case 'genericSftp':
      return gardnersConnections.withGenericSftp(fn);
    case 'coversFtp':
      return gardnersConnections.withCoversFtp(fn);
  }
}

function openConnection(name: GardnersConnectionName) {
  switch (name) {
    case 'bespokeSftp':
      return gardnersConnections.openBespokeSftp();
    case 'genericSftp':
      return gardnersConnections.openGenericSftp();
    case 'coversFtp':
      return gardnersConnections.openCoversFtp();
  }
}

// A claim stuck in 'downloading'/'processing' for longer than this is
// treated as dead — a crashed process, a killed job, or a stream that
// silently hung without ever resolving to completed/failed (observed live:
// an SFTP connection drop mid-transfer that ssh2-sftp-client only logged by
// default, never rejecting the promise governing the claim) — and becomes
// eligible for retry. Comfortably longer than the largest file (~1.7GB
// Biblio Full) should ever take even on a slow connection.
const STALE_CLAIM_HOURS = 3;

/**
 * Lists candidate files for a feed and filters out ones already claimed.
 * `(feed, remotePath)` in gardners_fetch_log is the idempotency key —
 * Gardners filenames are date/week-stamped, so the remote path itself is a
 * stable "have I already claimed this exact file" identifier; no separate
 * cursor/watermark is needed. 'failed' rows are always retryable; a
 * 'downloading'/'processing' row blocks re-listing only while it's recent —
 * past STALE_CLAIM_HOURS it's assumed dead and becomes retryable too, or a
 * single crashed run would block that file forever. Results are sorted by
 * filename ascending, which matters for sequence-ordered feeds like Avail13.
 */
async function listUnprocessedFiles(cfg: FetchFeedConfig): Promise<RemoteFileDescriptor[]> {
  const candidates = await withConnection(cfg.connection, async (client) => {
    const files = await cfg.listRemoteFiles(client);
    if (!cfg.requiresDoneSentinel) return files;

    const gated: RemoteFileDescriptor[] = [];
    for (const file of files) {
      if (await client.exists(`${file.path}.DONE`)) {
        gated.push(file);
      }
    }
    return gated;
  });

  if (candidates.length === 0) return [];

  const paths = candidates.map((f) => f.path);
  const staleThreshold = new Date(Date.now() - STALE_CLAIM_HOURS * 60 * 60 * 1000);
  const claimed = await db
    .select({ remotePath: gardnersFetchLog.remotePath })
    .from(gardnersFetchLog)
    .where(
      and(
        eq(gardnersFetchLog.feed, cfg.feed),
        inArray(gardnersFetchLog.remotePath, paths),
        or(
          eq(gardnersFetchLog.status, 'completed'),
          and(ne(gardnersFetchLog.status, 'failed'), gt(gardnersFetchLog.startedAt, staleThreshold)),
        ),
      ),
    );

  const blocked = new Set(claimed.map((r) => r.remotePath));
  return candidates
    .filter((f) => !blocked.has(f.path))
    .sort((a, b) => a.filename.localeCompare(b.filename));
}

/**
 * Creates (or resets, on retry) the gardners_fetch_log row for a file before
 * any BullMQ job is enqueued — this is what makes the file "claimed" so
 * listUnprocessedFiles won't re-offer it on the next cron tick, even though
 * the actual download happens later inside a queued worker job (a live
 * stream can't be serialized into job data, so the claim and the stream
 * open are separate steps).
 *
 * A previously-failed file retains its (feed, remotePath) row rather than
 * getting a fresh one — the unique index on that pair means a second plain
 * INSERT would violate the constraint, so this upserts, resetting the
 * existing row back to a clean 'downloading' state for the retry.
 */
async function claimFile(feed: GardnersFeed, file: RemoteFileDescriptor): Promise<number> {
  const values = {
    feed,
    remotePath: file.path,
    remoteFilename: file.filename,
    remoteModifiedAt: file.modifiedAt ?? undefined,
    remoteSize: file.size,
    status: 'downloading' as const,
  };

  const [row] = await db
    .insert(gardnersFetchLog)
    .values(values)
    .onConflictDoUpdate({
      target: [gardnersFetchLog.feed, gardnersFetchLog.remotePath],
      set: {
        ...values,
        r2Key: null,
        totalChunks: null,
        processedChunks: 0,
        rowCount: null,
        errorMessage: null,
        startedAt: new Date(),
        completedAt: null,
      },
    })
    .returning({ id: gardnersFetchLog.id });
  return row.id;
}

/**
 * Opens a connection and returns a live stream for the caller to consume.
 * The connection is intentionally NOT closed when this function returns —
 * it closes automatically once the stream ends or errors. Does not touch
 * gardners_fetch_log; call claimFile first to get a logId.
 */
async function openStreamForFile(
  connection: GardnersConnectionName,
  file: RemoteFileDescriptor,
): Promise<Readable> {
  const conn = await openConnection(connection);
  const stream = await conn.client.readStream(file.path);
  const close = () => conn.close().catch(() => undefined);
  stream.once('end', close);
  stream.once('close', close);
  stream.once('error', close);
  return stream;
}

/**
 * Downloads a remote file to a local path using the protocol's
 * concurrent/optimized transfer method (SFTP's fastGet, 64 requests in
 * flight) rather than a plain single-request-at-a-time stream. Observed
 * live: plain streaming reads from Gardners' UK-hosted SFTP servers run at
 * ~160KB/s (latency-bound — each read waits for the previous one's
 * response), while fastGet and the `sftp` CLI both achieve ~9.7MB/s
 * (bandwidth-bound, many requests in flight at once). For anything beyond a
 * few MB this difference is the gap between minutes and hours. Callers
 * should read the resulting local file (e.g. via fs.createReadStream) for
 * any further streaming (unzip, CSV parsing, upload) rather than reverting
 * to openStreamForFile/readStream for large feeds.
 */
async function downloadToLocalFile(
  connection: GardnersConnectionName,
  file: RemoteFileDescriptor,
  localPath: string,
): Promise<void> {
  await withConnection(connection, (client) => client.downloadToFile(file.path, localPath));
}

/**
 * Convenience combining claimFile + openStreamForFile for feeds that fetch
 * and process a stream in one place without going through the chunk queue.
 */
async function downloadToStream(
  cfg: Pick<FetchFeedConfig, 'feed' | 'connection'>,
  file: RemoteFileDescriptor,
): Promise<{ logId: number; stream: Readable }> {
  const logId = await claimFile(cfg.feed, file);
  try {
    const stream = await openStreamForFile(cfg.connection, file);
    return { logId, stream };
  } catch (err) {
    await markFetchFailed(logId, err instanceof Error ? err : new Error(String(err)));
    throw err;
  }
}

/**
 * Streams a remote file straight into R2 under `r2Key`. Used only for feeds
 * that plug into a downstream R2-based pipeline (the ONIX Biblio zip, and
 * cover images) — everything else streams straight into CSV parsing without
 * an R2 hop (see downloadToStream).
 */
async function downloadToR2(
  cfg: Pick<FetchFeedConfig, 'feed' | 'connection'>,
  file: RemoteFileDescriptor,
  r2Key: string,
  contentType = 'application/octet-stream',
): Promise<{ logId: number; r2Key: string }> {
  const logId = await claimFile(cfg.feed, file);
  try {
    await withConnection(cfg.connection, async (client) => {
      const stream = await client.readStream(file.path);
      await storageService.uploadStream(r2Key, stream, contentType, file.size);
    });
    return { logId, r2Key };
  } catch (err) {
    await markFetchFailed(logId, err instanceof Error ? err : new Error(String(err)));
    throw err;
  }
}

/**
 * Marks the file-worker phase done: the file has been fully streamed and
 * split into `totalChunks` chunk jobs. Status moves to 'processing' — the
 * chunk workers drive it to 'completed' via incrementProcessedChunks once
 * every chunk finishes. If a file produces zero chunks (e.g. an empty
 * file), call markFetchCompleted directly instead — there's nothing for a
 * chunk worker to complete.
 */
async function setChunkingComplete(
  logId: number,
  info: { totalChunks: number; rowCount: number },
): Promise<void> {
  await db
    .update(gardnersFetchLog)
    .set({ status: 'processing', totalChunks: info.totalChunks, rowCount: info.rowCount })
    .where(eq(gardnersFetchLog.id, logId));
}

/**
 * Called by the chunk worker after each chunk finishes. Resolves the fetch
 * log to 'completed' once every chunk has been accounted for — mirrors the
 * ingestion_jobs/ingestion_chunks SQL CASE pattern in chunk.worker.ts.
 */
async function incrementProcessedChunks(logId: number): Promise<void> {
  await db.execute(sql`
    UPDATE gardners_fetch_log
    SET
      processed_chunks = processed_chunks + 1,
      status = CASE
        WHEN processed_chunks + 1 = total_chunks THEN 'completed'
        ELSE status
      END,
      completed_at = CASE
        WHEN processed_chunks + 1 = total_chunks THEN NOW()
        ELSE completed_at
      END
    WHERE id = ${logId}
  `);
}

async function markFetchCompleted(
  logId: number,
  info: { rowCount?: number; r2Key?: string } = {},
): Promise<void> {
  await db
    .update(gardnersFetchLog)
    .set({
      status: 'completed',
      rowCount: info.rowCount,
      r2Key: info.r2Key,
      completedAt: new Date(),
    })
    .where(eq(gardnersFetchLog.id, logId));
}

async function markFetchFailed(logId: number, err: Error): Promise<void> {
  logger.error('Gardners fetch failed', { logId, error: err.message });
  await db
    .update(gardnersFetchLog)
    .set({ status: 'failed', errorMessage: err.message })
    .where(eq(gardnersFetchLog.id, logId));
}

export const gardnersFetcher = {
  listUnprocessedFiles,
  claimFile,
  openStreamForFile,
  downloadToStream,
  downloadToR2,
  downloadToLocalFile,
  setChunkingComplete,
  incrementProcessedChunks,
  markFetchCompleted,
  markFetchFailed,
};
