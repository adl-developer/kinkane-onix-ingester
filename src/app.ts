import express, { Request, Response, NextFunction } from 'express';
import { requireAdminToken } from './middleware/auth.middleware';
import { bullBoardRouter } from './queue/board';
import apiRoutes from './routes';
import { logger } from './lib/logger';

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Bull Board UI — admin JWT required
app.use('/bull-board', requireAdminToken, bullBoardRouter);

// REST API
app.use('/api', apiRoutes);

// 404 handler
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not found' });
});

// Global error handler
// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error('Unhandled express error', { error: err.message, stack: err.stack });
  res.status(500).json({ error: 'Internal server error' });
});

export default app;
