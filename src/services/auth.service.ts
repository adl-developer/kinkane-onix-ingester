import jwt from 'jsonwebtoken';
import { config } from '../config';

const TOKEN_TTL_SECONDS = 30 * 60; // 30 minutes

export interface AdminTokenPayload {
  role: 'admin';
  iat: number;
  exp: number;
}

export const authService = {
  generateAdminToken(): string {
    return jwt.sign({ role: 'admin' }, config.auth.jwtSecret, {
      expiresIn: TOKEN_TTL_SECONDS,
    });
  },

  verifyAdminToken(token: string): AdminTokenPayload {
    return jwt.verify(token, config.auth.jwtSecret) as AdminTokenPayload;
  },
};
