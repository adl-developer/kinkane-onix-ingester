import { Router } from 'express';
import rateLimit from 'express-rate-limit';
import { authController } from '../controllers/auth.controller';

const router = Router();

// 10 attempts per 15 minutes per IP — brute-force protection for the admin secret
const tokenLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  handler: (_req, res) => res.status(429).json({ error: 'Too many requests — please try again later' }),
});

/**
 * POST /auth/token
 * Body: { "secret": "<ADMIN_SECRET>" }
 * Returns a 30-minute JWT for use on protected routes.
 */
router.post('/token', tokenLimiter, authController.generateToken);

export default router;
