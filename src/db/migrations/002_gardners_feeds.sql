-- Migration: add tables for Gardners Books SFTP/FTP feed ingestion
-- Purely additive — no existing tables are altered, so this is safe to run
-- independently of a deploy and has no in-flight-job compatibility concerns
-- (unlike 001_chunk_data_to_r2.sql, which touched an existing column).
-- Run this before deploying the Gardners fetcher/worker code.

-- 1. Enums
CREATE TYPE gardners_feed AS ENUM (
  'inventory',
  'biblio_delta',
  'biblio_full',
  'avail13',
  'promotions',
  'firm_sale',
  'isbn_slips',
  'market_restrictions',
  'regions',
  'covers_full',
  'covers_update',
  'covers_instock'
);

CREATE TYPE gardners_fetch_status AS ENUM (
  'downloading',
  'processing',
  'completed',
  'failed'
);

-- 2. Fetch log — idempotency + chunked-worker progress tracking for all feeds
CREATE TABLE gardners_fetch_log (
  id SERIAL PRIMARY KEY,
  feed gardners_feed NOT NULL,
  remote_path VARCHAR(1000) NOT NULL,
  remote_filename VARCHAR(500) NOT NULL,
  remote_modified_at TIMESTAMPTZ,
  remote_size INTEGER,
  r2_key VARCHAR(500),
  status gardners_fetch_status NOT NULL DEFAULT 'downloading',
  total_chunks INTEGER,
  processed_chunks INTEGER DEFAULT 0,
  row_count INTEGER,
  error_message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX uq_gardners_fetch_log_feed_remote_path ON gardners_fetch_log (feed, remote_path);
CREATE INDEX idx_gardners_fetch_log_feed_status ON gardners_fetch_log (feed, status);

-- 3. Stock — current price + stock level per ISBN13 (Bespoke Inventory + Avail13)
CREATE TABLE gardners_stock (
  id SERIAL PRIMARY KEY,
  isbn13 VARCHAR(13) NOT NULL,
  book_id INTEGER REFERENCES books (id) ON DELETE SET NULL,
  rrp_gbp NUMERIC(10, 2),
  discount_percent NUMERIC(5, 2),
  stock_qty INTEGER,
  report_code VARCHAR(10),
  report_date DATE,
  source VARCHAR(20) NOT NULL,
  source_file_key VARCHAR(500),
  stock_updated_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_gardners_stock_isbn13 ON gardners_stock (isbn13);
CREATE INDEX idx_gardners_stock_book_id ON gardners_stock (book_id);
CREATE INDEX idx_gardners_stock_updated_at ON gardners_stock (stock_updated_at);

-- 4. Promotions — full daily replace (GARDPROM13.CSV)
CREATE TABLE gardners_promotions (
  id SERIAL PRIMARY KEY,
  isbn13 VARCHAR(13) NOT NULL,
  book_id INTEGER REFERENCES books (id) ON DELETE SET NULL,
  title VARCHAR(2000),
  author VARCHAR(500),
  price NUMERIC(10, 2),
  discount_percent NUMERIC(5, 2),
  returns_flag VARCHAR(1),
  finish_date DATE,
  source_file_key VARCHAR(500),
  synced_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_gardners_promotions_isbn13 ON gardners_promotions (isbn13);
CREATE INDEX idx_gardners_promotions_book_id ON gardners_promotions (book_id);
CREATE INDEX idx_gardners_promotions_finish_date ON gardners_promotions (finish_date);

-- 5. Firm sale — pure ISBN membership, full weekly replace (FIRMSALE13.CSV)
CREATE TABLE gardners_firm_sale (
  isbn13 VARCHAR(13) PRIMARY KEY,
  book_id INTEGER REFERENCES books (id) ON DELETE SET NULL,
  report_code VARCHAR(10),
  source_file_key VARCHAR(500),
  synced_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_gardners_firm_sale_book_id ON gardners_firm_sale (book_id);

-- 6. ISBN slips — replaced-edition redirects, full weekly replace (ISBNSL13.CSV)
CREATE TABLE gardners_isbn_slips (
  old_isbn13 VARCHAR(13) PRIMARY KEY,
  new_isbn13 VARCHAR(13) NOT NULL,
  source_file_key VARCHAR(500),
  synced_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_gardners_isbn_slips_new_isbn ON gardners_isbn_slips (new_isbn13);

-- 7. Market restrictions — region lookup + raw per-(isbn13, region) rows
CREATE TABLE gardners_regions (
  code VARCHAR(10) PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  synced_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE gardners_market_restrictions (
  id SERIAL PRIMARY KEY,
  isbn13 VARCHAR(13) NOT NULL,
  book_id INTEGER REFERENCES books (id) ON DELETE SET NULL,
  flag VARCHAR(1) NOT NULL,
  region_code VARCHAR(10) NOT NULL,
  source_file_key VARCHAR(500),
  synced_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_gardners_market_restrictions_isbn ON gardners_market_restrictions (isbn13);
CREATE UNIQUE INDEX uq_gardners_market_restrictions_isbn_region
  ON gardners_market_restrictions (isbn13, region_code);
CREATE INDEX idx_gardners_market_restrictions_book_id ON gardners_market_restrictions (book_id);
