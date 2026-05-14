import { Router } from 'express';
import { authController } from '../controllers/auth.controller';

const router = Router();

/**
 * POST /auth/token
 * Body: { "secret": "<ADMIN_SECRET>" }
 * Returns a 30-minute JWT for use on protected routes.
 */
router.post('/token', authController.generateToken);

export default router;
