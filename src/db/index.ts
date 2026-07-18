import postgres from 'postgres';
import { drizzle } from 'drizzle-orm/postgres-js';
import { config } from '../config';
import * as schema from './schema';

const client = postgres(config.database.url, {
  max: 10,
  idle_timeout: 30,
  connect_timeout: 10,
  ssl: config.database.url.includes('sslmode=require') ? 'require' : false,
});

export const db = drizzle(client, { schema });

export type DB = typeof db;
