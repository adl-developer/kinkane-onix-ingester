import { Request, Response } from 'express';
import { z } from 'zod';
import busboy from 'busboy';
import path from 'path';
import { Readable } from 'stream';
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

  /**
   * POST /ingestion/upload
   * Multipart form-data fields:
   *   file     — the ONIX XML file (required)
   *   key      — custom R2 object key (optional; defaults to onixPrefix + original filename)
   *   trigger  — "true" to automatically start ingestion after upload (optional)
   */
  async uploadFile(req: Request, res: Response): Promise<void> {
    const bb = busboy({
      headers: req.headers,
      limits: { files: 1 }, // one file per request
    });

    let fileKey: string | null = null;
    let customKey: string | null = null;
    let autoTrigger = false;
    let uploadError: Error | null = null;
    let uploadDone = false;

    // Collect non-file fields first
    bb.on('field', (name, value) => {
      if (name === 'key') customKey = value.trim();
      if (name === 'trigger') autoTrigger = value === 'true';
    });

    bb.on('file', (_fieldname, fileStream, info) => {
      const { filename, mimeType } = info;

      if (!filename.endsWith('.xml')) {
        fileStream.resume(); // drain and discard
        uploadError = new Error('Only .xml files are accepted');
        return;
      }

      const ext = path.extname(filename);
      const base = path.basename(filename, ext)
        .toLowerCase()
        .replace(/\s+/g, '_')          // spaces → underscores
        .replace(/[^a-z0-9_-]/g, ''); // strip any remaining special chars
      const timestamp = Date.now();
      const sanitisedFilename = `${base}_${timestamp}${ext}`;

      const resolvedKey =
        customKey ?? `${config.r2.onixPrefix}${sanitisedFilename}`;

      fileKey = resolvedKey;

      storageService
        .uploadStream(fileStream as unknown as Readable, resolvedKey, mimeType)
        .then(() => {
          uploadDone = true;
        })
        .catch((err: Error) => {
          uploadError = err;
        });
    });

    bb.on('finish', async () => {
      // Wait for the in-flight upload promise to settle
      while (!uploadDone && !uploadError) {
        await new Promise((r) => setTimeout(r, 50));
      }

      if (uploadError) {
        res.status(500).json({ error: uploadError.message });
        return;
      }

      if (!fileKey) {
        res.status(400).json({ error: 'No file field found in the request' });
        return;
      }

      if (autoTrigger) {
        try {
          const job = await ingestionService.triggerIngestion(fileKey);
          res.status(202).json({
            message: 'File uploaded and ingestion enqueued',
            fileKey,
            jobId: job.jobId,
            bullJobId: job.bullJobId,
          });
        } catch (err: unknown) {
          const e = err as Error;
          res.status(500).json({ error: `Upload succeeded but trigger failed: ${e.message}` });
        }
      } else {
        res.status(200).json({ message: 'File uploaded successfully', fileKey });
      }
    });

    bb.on('error', (err: Error) => {
      res.status(400).json({ error: err.message });
    });

    req.pipe(bb);
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
