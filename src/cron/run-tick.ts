import { logger } from '../lib/logger';

/**
 * A tick may return a plain object of extra fields to surface on its
 * completion line. Anything else (a bare count, undefined) is ignored — the
 * services predate this wrapper and return whatever suits them.
 */
function extraFields(result: unknown): Record<string, unknown> {
  if (typeof result === 'object' && result !== null && !Array.isArray(result)) {
    return result as Record<string, unknown>;
  }
  return {};
}

/**
 * Wraps a cron callback so every tick emits a matched start/end pair.
 *
 * Previously each callback logged only on entry and on error, so a
 * successful run was indistinguishable from one that hung or from a process
 * that died mid-tick — "no error in the logs" was the only success signal.
 * Centralising the try/catch here also guarantees a throwing tick can never
 * take the process down via an unhandled rejection.
 *
 * Whatever the callback returns is spread onto the completion line, which is
 * how a tick reports what it actually did (files enqueued, rows swept).
 */
export async function runCronTick(job: string, fn: () => Promise<unknown>): Promise<void> {
  const startedAt = Date.now();
  logger.info('Cron tick started', { job });

  try {
    const result = await fn();
    logger.info('Cron tick complete', {
      job,
      durationMs: Date.now() - startedAt,
      ...extraFields(result),
    });
  } catch (err) {
    logger.error('Cron tick failed', {
      job,
      durationMs: Date.now() - startedAt,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}
