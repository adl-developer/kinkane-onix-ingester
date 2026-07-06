import { Readable } from 'stream';
import { and, eq, inArray } from 'drizzle-orm';
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

/**
 * Lists candidate files for a feed and filters out ones already fully
 * processed. `(feed, remotePath)` in gardners_fetch_log is the idempotency
 * key — Gardners filenames are date/week-stamped, so the remote path itself
 * is a stable "have I processed this exact file" identifier; no separate
 * cursor/watermark is needed. Results are sorted by filename ascending,
 * which matters for sequence-ordered feeds like Avail13.
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
  const completed = await db
    .select({ remotePath: gardnersFetchLog.remotePath })
    .from(gardnersFetchLog)
    .where(
      and(
        eq(gardnersFetchLog.feed, cfg.feed),
        inArray(gardnersFetchLog.remotePath, paths),
        eq(gardnersFetchLog.status, 'completed'),
      ),
    );

  const done = new Set(completed.map((r) => r.remotePath));
  return candidates
    .filter((f) => !done.has(f.path))
    .sort((a, b) => a.filename.localeCompare(b.filename));
}

async function startFetchLog(feed: GardnersFeed, file: RemoteFileDescriptor): Promise<number> {
  const [row] = await db
    .insert(gardnersFetchLog)
    .values({
      feed,
      remotePath: file.path,
      remoteFilename: file.filename,
      remoteModifiedAt: file.modifiedAt ?? undefined,
      remoteSize: file.size,
      status: 'downloading',
    })
    .returning({ id: gardnersFetchLog.id });
  return row.id;
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

/**
 * Streams a remote file straight into R2 under `r2Key`. Used only for feeds
 * that plug into a downstream R2-based pipeline (the ONIX Biblio zip, and
 * cover images) — everything else streams straight into CSV parsing without
 * an R2 hop (see downloadToStream).
 */
async function downloadToR2(
  cfg: FetchFeedConfig,
  file: RemoteFileDescriptor,
  r2Key: string,
  contentType = 'application/octet-stream',
): Promise<{ logId: number; r2Key: string }> {
  const logId = await startFetchLog(cfg.feed, file);
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
 * Opens a connection and returns a live stream for the caller to consume —
 * the connection is intentionally NOT closed when this function returns
 * (unlike downloadToR2/listUnprocessedFiles, which use the auto-closing
 * withConnection wrapper), since the stream must stay readable afterwards.
 * The connection closes automatically once the stream ends or errors.
 * Caller must call markFetchCompleted/markFetchFailed on the returned logId
 * once done processing.
 */
async function downloadToStream(
  cfg: FetchFeedConfig,
  file: RemoteFileDescriptor,
): Promise<{ logId: number; stream: Readable }> {
  const logId = await startFetchLog(cfg.feed, file);
  try {
    const conn = await openConnection(cfg.connection);
    const stream = await conn.client.readStream(file.path);
    const close = () => conn.close().catch(() => undefined);
    stream.once('end', close);
    stream.once('close', close);
    stream.once('error', close);
    return { logId, stream };
  } catch (err) {
    await markFetchFailed(logId, err instanceof Error ? err : new Error(String(err)));
    throw err;
  }
}

export const gardnersFetcher = {
  listUnprocessedFiles,
  downloadToR2,
  downloadToStream,
  markFetchCompleted,
  markFetchFailed,
};
