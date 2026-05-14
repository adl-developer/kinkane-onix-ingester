import app from './app';
import { config } from './config';
import { startFileWorker } from './workers/file.worker';
import { startChunkWorker } from './workers/chunk.worker';
import { startCron } from './cron';
import { logger } from './lib/logger';

async function main(): Promise<void> {
  // Start BullMQ workers
  const fileWorker = startFileWorker();
  const chunkWorker = startChunkWorker(5);

  // Start cron poller
  startCron();

  // Start HTTP server
  const server = app.listen(config.port, () => {
    logger.info('onix-ingester started', {
      port: config.port,
      env: config.nodeEnv,
      bullBoard: `http://localhost:${config.port}/bull-board`,
    });
  });

  // Disable socket timeout — large ONIX uploads can take many minutes
  server.setTimeout(0);

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    logger.info('Shutting down gracefully', { signal });
    server.close(() => logger.info('HTTP server closed'));
    await Promise.all([fileWorker.close(), chunkWorker.close()]);
    logger.info('Workers closed');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  logger.error('Fatal startup error', { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
