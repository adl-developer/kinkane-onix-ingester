import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { config } from '../config';
import { FileJobData, ChunkJobData } from '../types/queue';

export const redis = new IORedis(config.redis.url, {
  maxRetriesPerRequest: null, // required by BullMQ
});

export const fileQueue = new Queue<FileJobData>('onix-file', {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 5000 },
    removeOnComplete: { count: 100 },
    removeOnFail: { count: 200 },
  },
});

export const chunkQueue = new Queue<ChunkJobData>('onix-chunk', {
  connection: redis,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: { count: 500 },
    removeOnFail: { count: 1000 },
  },
});
