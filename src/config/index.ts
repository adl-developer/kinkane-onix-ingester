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
  R2_PUBLIC_URL: z.string().url(),

  GEMINI_API_KEY: z.string().min(1),
  GEMINI_EMBEDDING_MODEL: z.string().default('text-embedding-004'),
  GEMINI_FLASH_MODEL: z.string().default('gemini-2.5-flash-lite'),

  ADMIN_SECRET: z.string().min(16),
  JWT_SECRET: z.string().min(16),

  CHUNK_SIZE: z.coerce.number().default(150),
  EMBEDDING_BATCH_SIZE: z.coerce.number().default(50),
  EMBEDDING_BATCH_DELAY_MS: z.coerce.number().default(35000),

  R2_POLL_CRON: z.string().default('0 2 * * *'),
  COVER_FETCH_CRON: z.string().default('0 3 * * *'),
  EXCERPT_SYNC_CRON: z.string().default('0 5 * * *'),

  // Optional — cover fetching is skipped if not set
  GOOGLE_BOOKS_API_KEY: z.string().optional(),
  COVER_FETCH_BATCH_SIZE: z.coerce.number().default(50),
  COVER_FETCH_DELAY_MS: z.coerce.number().default(200),

  // Optional — excerpt sync is skipped if not set
  JELLYBOOKS_API_KEY: z.string().optional(),
  JELLYBOOKS_BASE_URL: z.string().url().default('https://www.jellybooks.com'),

  // Master switch for all Gardners cron jobs and the POST /gardners/bootstrap
  // endpoint. Defaults to disabled so deploying this doesn't start pulling
  // ~2M rows + cover images into a database that isn't sized for it — only
  // the literal string 'true' enables it, anything else (unset, 'false',
  // '1', etc.) leaves it off.
  GARDNERS_INGESTION_ENABLED: z
    .string()
    .optional()
    .default('false')
    .transform((v) => v === 'true'),

  // Gardners Books — Bespoke Inventory SFTP (edi.gardners.com)
  GARDNERS_BESPOKE_SFTP_HOST: z.string().min(1),
  GARDNERS_BESPOKE_SFTP_PORT: z.coerce.number().default(22),
  GARDNERS_BESPOKE_SFTP_USERNAME: z.string().min(1),
  GARDNERS_BESPOKE_SFTP_PASSWORD: z.string().min(1),

  // Gardners Books — Generic Data SFTP (data.gardners.com)
  GARDNERS_GENERIC_SFTP_HOST: z.string().min(1),
  GARDNERS_GENERIC_SFTP_PORT: z.coerce.number().default(22),
  GARDNERS_GENERIC_SFTP_USERNAME: z.string().min(1),
  GARDNERS_GENERIC_SFTP_PASSWORD: z.string().min(1),

  // Gardners Books — Cover images FTP (covers.gardners.com, plain FTP)
  GARDNERS_COVERS_FTP_HOST: z.string().min(1),
  GARDNERS_COVERS_FTP_PORT: z.coerce.number().default(21),
  GARDNERS_COVERS_FTP_USERNAME: z.string().min(1),
  GARDNERS_COVERS_FTP_PASSWORD: z.string().min(1),

  // Gardners feed cron schedules (UTC). Gardners' own drop times are quoted
  // in GMT — schedules below build in a buffer to absorb BST offset and
  // publish-time jitter. Firm sale / isbn slips / market restrictions times
  // are best-guess placeholders pending a week or two of observed file mtimes.
  GARDNERS_INVENTORY_CRON: z.string().default('0 13 * * *'),
  GARDNERS_BIBLIO_DELTA_CRON: z.string().default('0 6 * * 1'),
  GARDNERS_AVAIL13_CRON: z.string().default('15 * * * *'),
  GARDNERS_PROMOTIONS_CRON: z.string().default('0 14 * * *'),
  GARDNERS_FIRM_SALE_CRON: z.string().default('0 7 * * 2'),
  GARDNERS_ISBN_SLIPS_CRON: z.string().default('30 7 * * 2'),
  GARDNERS_MARKET_RESTRICTIONS_CRON: z.string().default('0 8 * * *'),
  GARDNERS_COVERS_UPDATE_CRON: z.string().default('0 9 * * 1'),

  GARDNERS_COVER_SYNC_BATCH_SIZE: z.coerce.number().default(200),
  GARDNERS_COVER_SYNC_DELAY_MS: z.coerce.number().default(100),
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
    publicUrl: env.R2_PUBLIC_URL,
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
  googleBooks: {
    apiKey: env.GOOGLE_BOOKS_API_KEY,
  },
  cron: {
    r2PollSchedule: env.R2_POLL_CRON,
    coverFetchSchedule: env.COVER_FETCH_CRON,
    coverFetchBatchSize: env.COVER_FETCH_BATCH_SIZE,
    coverFetchDelayMs: env.COVER_FETCH_DELAY_MS,
    excerptSyncSchedule: env.EXCERPT_SYNC_CRON,
  },
  jellybooks: {
    apiKey: env.JELLYBOOKS_API_KEY,
    baseUrl: env.JELLYBOOKS_BASE_URL,
  },
  gardners: {
    ingestionEnabled: env.GARDNERS_INGESTION_ENABLED,
    bespokeSftp: {
      host: env.GARDNERS_BESPOKE_SFTP_HOST,
      port: env.GARDNERS_BESPOKE_SFTP_PORT,
      username: env.GARDNERS_BESPOKE_SFTP_USERNAME,
      password: env.GARDNERS_BESPOKE_SFTP_PASSWORD,
    },
    genericSftp: {
      host: env.GARDNERS_GENERIC_SFTP_HOST,
      port: env.GARDNERS_GENERIC_SFTP_PORT,
      username: env.GARDNERS_GENERIC_SFTP_USERNAME,
      password: env.GARDNERS_GENERIC_SFTP_PASSWORD,
    },
    coversFtp: {
      host: env.GARDNERS_COVERS_FTP_HOST,
      port: env.GARDNERS_COVERS_FTP_PORT,
      username: env.GARDNERS_COVERS_FTP_USERNAME,
      password: env.GARDNERS_COVERS_FTP_PASSWORD,
    },
    cron: {
      inventorySchedule: env.GARDNERS_INVENTORY_CRON,
      biblioDeltaSchedule: env.GARDNERS_BIBLIO_DELTA_CRON,
      avail13Schedule: env.GARDNERS_AVAIL13_CRON,
      promotionsSchedule: env.GARDNERS_PROMOTIONS_CRON,
      firmSaleSchedule: env.GARDNERS_FIRM_SALE_CRON,
      isbnSlipsSchedule: env.GARDNERS_ISBN_SLIPS_CRON,
      marketRestrictionsSchedule: env.GARDNERS_MARKET_RESTRICTIONS_CRON,
      coversUpdateSchedule: env.GARDNERS_COVERS_UPDATE_CRON,
    },
    coverSync: {
      batchSize: env.GARDNERS_COVER_SYNC_BATCH_SIZE,
      delayMs: env.GARDNERS_COVER_SYNC_DELAY_MS,
    },
  },
} as const;

export type Config = typeof config;
