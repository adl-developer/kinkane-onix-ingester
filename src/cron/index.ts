import cron from 'node-cron';
import { config } from '../config';
import { ingestionService } from '../services/ingestion.service';
import { logger } from '../lib/logger';

export function startCron(): void {
  const schedule = config.cron.r2PollSchedule;

  if (!cron.validate(schedule)) {
    throw new Error(`Invalid cron schedule: ${schedule}`);
  }

  cron.schedule(schedule, async () => {
    logger.info('Polling R2 for new ONIX files');

    try {
      const unprocessed = await ingestionService.listUnprocessedR2Files();

      if (unprocessed.length === 0) {
        logger.info('No new ONIX files found');
        return;
      }

      logger.info('New ONIX files found', { count: unprocessed.length });

      for (const fileKey of unprocessed) {
        const result = await ingestionService.triggerIngestion(fileKey);
        logger.info('Enqueued file for ingestion', { fileKey, jobId: result.jobId });
      }
    } catch (err) {
      logger.error('R2 poll failed', { error: err instanceof Error ? err.message : String(err) });
    }
  });

  logger.info('R2 poll scheduled', { schedule });
}
