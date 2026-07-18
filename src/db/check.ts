import * as dotenv from 'dotenv';
dotenv.config();

import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL!, {
  ssl: process.env.DATABASE_URL!.includes('sslmode=require') ? 'require' : false,
});

async function main() {
  const tables = await sql`
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename
  `;
  console.log('Tables in public schema:', tables.map((r) => r.tablename));

  const migrations = await sql`
    SELECT name, created_at FROM drizzle.__drizzle_migrations ORDER BY created_at
  `.catch(() => []);
  console.log('Drizzle migrations applied:', migrations);

  await sql.end();
}

main().catch(console.error);
