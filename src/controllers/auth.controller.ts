import { Request, Response } from 'express';
import { z } from 'zod';
import { authService } from '../services/auth.service';
import { config } from '../config';

const tokenRequestSchema = z.object({
  secret: z.string().min(1),
});

export const authController = {
  generateToken(req: Request, res: Response): void {
    const parsed = tokenRequestSchema.safeParse(req.body);

    if (!parsed.success) {
      res.status(400).json({ error: 'Body must include a "secret" field' });
      return;
    }

    if (parsed.data.secret !== config.auth.adminSecret) {
      res.status(401).json({ error: 'Invalid secret' });
      return;
    }

    const token = authService.generateAdminToken();
    res.status(200).json({ token, expiresIn: '72h' });
  },
};
