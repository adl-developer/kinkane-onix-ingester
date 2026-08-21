import { Request, Response } from 'express';
import { z } from 'zod';
import { healthService } from '../services/health.service';
import { config } from '../config';
import { logger } from '../lib/logger';

const statsQuerySchema = z.object({
  windowHours: z.coerce.number().int().min(1).max(720).default(24),
  failureLimit: z.coerce.number().int().min(1).max(100).default(10),
});

export const healthController = {
  /**
   * Liveness — deliberately touches nothing external so it stays fast and
   * always answers. "Can the process serve a request?" and nothing more.
   */
  live(_req: Request, res: Response): void {
    res.status(200).json({
      status: 'ok',
      service: 'onix-ingester',
      env: config.nodeEnv,
      uptimeSec: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    });
  },

  /**
   * Readiness — pings Postgres and Redis. 503 when either is unreachable so
   * an uptime monitor treats it as down without having to parse the body.
   */
  async ready(_req: Request, res: Response): Promise<void> {
    const result = await healthService.readiness();
    res.status(result.ok ? 200 : 503).json({
      status: result.ok ? 'ok' : 'degraded',
      service: 'onix-ingester',
      uptimeSec: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
      ...result,
    });
  },

  /** Run statistics. Admin-gated — exposes filenames and error messages. */
  async stats(req: Request, res: Response): Promise<void> {
    const parsed = statsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: 'Invalid query', details: parsed.error.flatten() });
      return;
    }

    try {
      const result = await healthService.stats(parsed.data);
      res.status(200).json({
        service: 'onix-ingester',
        uptimeSec: Math.floor(process.uptime()),
        timestamp: new Date().toISOString(),
        ...result,
      });
    } catch (err) {
      logger.error('Health stats query failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      res.status(500).json({ error: 'Failed to gather stats' });
    }
  },
};
