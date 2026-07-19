import postgres from 'postgres';
import { drizzle } from 'drizzle-orm/postgres-js';
import { config } from '../config';
import * as schema from './schema';

const client = postgres(config.database.url, {
  // Worker concurrency alone can reach 12 at once (file:1 + chunk:5 for
  // both the ONIX and Gardners pipelines), each holding a connection for
  // part of its run — on top of HTTP requests and cron jobs sharing the
  // same pool. 10 was undersized: under real concurrent load, requests
  // were queuing for a free connection long enough to hit connect_timeout,
  // which could strand a job mid-processing after an irreversible step
  // (e.g. an R2 payload already deleted) had already happened.
  max: 20,
  idle_timeout: 30,
  connect_timeout: 10,
  ssl: config.database.url.includes('sslmode=require') ? 'require' : false,
});

export const db = drizzle(client, { schema });

export type DB = typeof db;
