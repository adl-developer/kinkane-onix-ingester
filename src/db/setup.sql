-- Run AFTER db:migrate to create triggers and special indexes.
-- Extensions must already be installed (run extensions.sql first).
-- Usage: psql $DATABASE_URL -f src/db/setup.sql

-- Full-text search: auto-maintained tsvector column
ALTER TABLE books ADD COLUMN IF NOT EXISTS search_vector tsvector;

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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_book_search_vector ON books;
CREATE TRIGGER trg_book_search_vector
BEFORE INSERT OR UPDATE ON books
FOR EACH ROW EXECUTE FUNCTION update_book_search_vector();

-- Backfill existing rows (run after initial load)
-- UPDATE books SET title = title;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_books_search_vector
  ON books USING GIN (search_vector);

-- pgvector ANN index — build after bulk load for speed
-- Requires at least one row before creation; run after first ingestion completes.
-- CREATE INDEX IF NOT EXISTS idx_books_embedding
--   ON books USING ivfflat (embedding vector_cosine_ops)
--   WITH (lists = 100);

-- Trigram indexes for fuzzy title/author search
CREATE INDEX IF NOT EXISTS idx_books_title_trgm
  ON books USING GIN (title gin_trgm_ops);
