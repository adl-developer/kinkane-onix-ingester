import { Request, Response } from 'express';
import { z } from 'zod';
import path from 'path';
import { ingestionService } from '../services/ingestion.service';
import { storageService } from '../services/storage.service';
import { config } from '../config';

const triggerSchema = z.object({
  fileKey: z.string().min(1),
});

const presignSchema = z.object({
  filename: z.string().min(1).refine((f) => f.endsWith('.xml'), {
    message: 'Only .xml files are accepted',
  }),
  key: z.string().optional(), // custom R2 key; defaults to onixPrefix + sanitised filename
  expiresIn: z.coerce.number().int().min(60).max(86400).default(4 * 60 * 60),
});

const listQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(20),
  offset: z.coerce.number().int().min(0).default(0),
});

export const ingestionController = {
  async trigger(req: Request, res: Response): Promise<void> {
    const parsed = triggerSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    try {
      const result = await ingestionService.triggerIngestion(parsed.data.fileKey);
      res.status(202).json({
        message: 'Ingestion job enqueued',
        jobId: result.jobId,
        bullJobId: result.bullJobId,
      });
    } catch (err: unknown) {
      const e = err as Error & { statusCode?: number };
      res.status(e.statusCode ?? 500).json({ error: e.message });
    }
  },

  async listJobs(req: Request, res: Response): Promise<void> {
    const parsed = listQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    try {
      const jobs = await ingestionService.listJobs(parsed.data.limit, parsed.data.offset);
      res.status(200).json({ jobs });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  async getJob(req: Request, res: Response): Promise<void> {
    const jobId = parseInt(req.params.id, 10);
    if (isNaN(jobId)) {
      res.status(400).json({ error: 'Invalid job ID' });
      return;
    }

    try {
      const job = await ingestionService.getJob(jobId);
      if (!job) {
        res.status(404).json({ error: 'Job not found' });
        return;
      }
      res.status(200).json({ job });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  async listUnprocessed(_req: Request, res: Response): Promise<void> {
    try {
      const files = await ingestionService.listUnprocessedR2Files();
      res.status(200).json({ files });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },

  /**
   * POST /ingestion/presign
   * Returns a presigned PUT URL so the caller can upload a large ONIX file
   * directly to R2 without routing the bytes through this server.
   *
   * Body: { filename: "feed.xml", key?: "onix/custom.xml", expiresIn?: 14400 }
   * Response: { uploadUrl, fileKey, expiresIn }
   *
   * After the upload completes, call POST /ingestion/trigger with { fileKey }.
   */
  async presignUpload(req: Request, res: Response): Promise<void> {
    const parsed = presignSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.flatten().fieldErrors });
      return;
    }

    const { filename, key, expiresIn } = parsed.data;

    const ext = path.extname(filename);
    const base = path
      .basename(filename, ext)
      .toLowerCase()
      .replace(/\s+/g, '_')
      .replace(/[^a-z0-9_-]/g, '');
    const sanitisedFilename = `${base}_${Date.now()}${ext}`;
    const fileKey = key ?? `${config.r2.onixPrefix}${sanitisedFilename}`;

    try {
      const uploadUrl = await storageService.getPresignedUploadUrl(fileKey, expiresIn);
      res.status(200).json({ uploadUrl, fileKey, expiresIn });
    } catch (err: unknown) {
      const e = err as Error;
      res.status(500).json({ error: e.message });
    }
  },
};
