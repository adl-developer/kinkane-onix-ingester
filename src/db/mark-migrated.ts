/**
 * Run this once when the migration SQL was applied manually/partially but
 * drizzle never recorded it in __drizzle_migrations.
 * It computes the same hash drizzle-kit would use and inserts the record.
 */
import * as dotenv from 'dotenv';
dotenv.config();

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL!, {
  ssl: process.env.NODE_ENV === 'production' ? 'require' : false,
});

async function main() {
  const migrationsDir = path.resolve(__dirname, '../../drizzle');
  const files = fs.readdirSync(migrationsDir).filter((f) => f.endsWith('.sql')).sort();

  const columns = await sql`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'drizzle' AND table_name = '__drizzle_migrations'
  `;
  console.log('Migration table columns:', columns.map((c) => c.column_name));

  const existing = await sql`SELECT hash FROM drizzle.__drizzle_migrations`;
  const appliedHashes = new Set(existing.map((r) => r.hash));

  for (const file of files) {
    const content = fs.readFileSync(path.join(migrationsDir, file), 'utf8');
    const hash = crypto.createHash('sha256').update(content).digest('hex');

    if (appliedHashes.has(hash)) {
      console.log(`Already recorded: ${file}`);
      continue;
    }

    await sql`
      INSERT INTO drizzle.__drizzle_migrations (hash, created_at)
      VALUES (${hash}, ${Date.now()})
    `;
    console.log(`Marked as applied: ${file} (hash: ${hash.slice(0, 12)}...)`);
  }

  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
