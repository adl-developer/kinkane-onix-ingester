import * as dotenv from 'dotenv';
dotenv.config();

import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL!, {
  ssl: process.env.NODE_ENV === 'production' ? 'require' : false,
});

async function main() {
  console.log('Adding jsonb data column to ingestion_chunks...');
  await sql`ALTER TABLE ingestion_chunks ADD COLUMN IF NOT EXISTS data jsonb`;

  console.log('Creating search vector trigger...');
  await sql`ALTER TABLE books ADD COLUMN IF NOT EXISTS search_vector tsvector`;

  await sql`
    CREATE OR REPLACE FUNCTION update_book_search_vector()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.subtitle, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.isbn13, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.long_description, '')), 'C');
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
  `;

  await sql`DROP TRIGGER IF EXISTS trg_book_search_vector ON books`;
  await sql`
    CREATE TRIGGER trg_book_search_vector
    BEFORE INSERT OR UPDATE ON books
    FOR EACH ROW EXECUTE FUNCTION update_book_search_vector()
  `;

  console.log('Creating indexes...');
  await sql`CREATE INDEX IF NOT EXISTS idx_books_search_vector ON books USING GIN (search_vector)`;
  await sql`CREATE INDEX IF NOT EXISTS idx_books_title_trgm ON books USING GIN (title gin_trgm_ops)`;

  console.log('Done.');
  await sql.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
