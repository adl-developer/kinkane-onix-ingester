import jwt from 'jsonwebtoken';
import { config } from '../config';

const TOKEN_TTL_SECONDS = 8 * 60 * 60; // 8 hours — long enough to monitor large ONIX ingestion jobs

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
