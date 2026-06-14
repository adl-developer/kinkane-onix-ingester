import { lt, eq, and, isNotNull } from 'drizzle-orm';
import { db } from '../db';
import { ingestionChunks } from '../db/schema';
import { storageService } from '../services/storage.service';
import { logger } from '../lib/logger';

const RETENTION_DAYS = 30;

/**
 * Deletes R2 payload files for failed chunks older than RETENTION_DAYS.
 * Successful chunks have their R2 files removed immediately by the chunk worker,
 * so this cron only ever touches failed chunks that were kept for debugging.
 *
 * Runs once daily — schedule via the caller in cron/index.ts.
 */
export async function runFailedChunkCleanup(): Promise<void> {
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000);

  const staleChunks = await db
    .select({ id: ingestionChunks.id, dataKey: ingestionChunks.dataKey })
    .from(ingestionChunks)
    .where(
      and(
        eq(ingestionChunks.status, 'failed'),
        isNotNull(ingestionChunks.dataKey),
        lt(ingestionChunks.updatedAt, cutoff),
      ),
    );

  if (staleChunks.length === 0) return;

  logger.info('Failed chunk R2 cleanup started', { count: staleChunks.length });

  let deleted = 0;
  let errors = 0;

  for (const chunk of staleChunks) {
    try {
      if (chunk.dataKey) {
        await storageService.deleteObject(chunk.dataKey);
      }
      await db
        .update(ingestionChunks)
        .set({ dataKey: null })
        .where(eq(ingestionChunks.id, chunk.id));
      deleted++;
    } catch (err) {
      errors++;
      logger.warn('Failed to clean up chunk R2 file', {
        chunkId: chunk.id,
        dataKey: chunk.dataKey,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  logger.info('Failed chunk R2 cleanup complete', { deleted, errors });
}
