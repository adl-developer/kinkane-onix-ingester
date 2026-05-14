import * as dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  PORT: z.coerce.number().default(3001),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),

  DATABASE_URL: z.string().url(),

  REDIS_URL: z.string().url(),

  R2_ACCOUNT_ID: z.string().min(1),
  R2_ACCESS_KEY_ID: z.string().min(1),
  R2_SECRET_ACCESS_KEY: z.string().min(1),
  R2_BUCKET_NAME: z.string().min(1),
  R2_ENDPOINT: z.string().url(),
  R2_ONIX_PREFIX: z.string().default('onix/'),

  GEMINI_API_KEY: z.string().min(1),
  GEMINI_EMBEDDING_MODEL: z.string().default('text-embedding-004'),
  GEMINI_FLASH_MODEL: z.string().default('gemini-2.5-flash-lite'),

  ADMIN_SECRET: z.string().min(16),
  JWT_SECRET: z.string().min(16),

  CHUNK_SIZE: z.coerce.number().default(500),
  EMBEDDING_BATCH_SIZE: z.coerce.number().default(100),
  EMBEDDING_BATCH_DELAY_MS: z.coerce.number().default(200),

  R2_POLL_CRON: z.string().default('0 2 * * *'),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('Invalid environment variables:', parsed.error.flatten().fieldErrors);
  process.exit(1);
}

const env = parsed.data;

export const config = {
  port: env.PORT,
  nodeEnv: env.NODE_ENV,
  database: {
    url: env.DATABASE_URL,
  },
  redis: {
    url: env.REDIS_URL,
  },
  r2: {
    accountId: env.R2_ACCOUNT_ID,
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
    bucketName: env.R2_BUCKET_NAME,
    endpoint: env.R2_ENDPOINT,
    onixPrefix: env.R2_ONIX_PREFIX,
  },
  gemini: {
    apiKey: env.GEMINI_API_KEY,
    embeddingModel: env.GEMINI_EMBEDDING_MODEL,
    flashModel: env.GEMINI_FLASH_MODEL,
  },
  auth: {
    adminSecret: env.ADMIN_SECRET,
    jwtSecret: env.JWT_SECRET,
  },
  ingestion: {
    chunkSize: env.CHUNK_SIZE,
    embeddingBatchSize: env.EMBEDDING_BATCH_SIZE,
    embeddingBatchDelayMs: env.EMBEDDING_BATCH_DELAY_MS,
  },
  cron: {
    r2PollSchedule: env.R2_POLL_CRON,
  },
} as const;

export type Config = typeof config;
