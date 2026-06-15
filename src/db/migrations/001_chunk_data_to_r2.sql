-- Migration: move chunk payload storage from PostgreSQL JSONB to R2
-- Run this migration before deploying the updated ingester code.
-- In-flight jobs at migration time: any chunk rows with data != NULL and
-- data_key IS NULL are from the old schema — they will fail gracefully (the
-- worker will log "No R2 data key found" and BullMQ will retry). Re-trigger
-- the parent ingestion job after migration to reprocess any affected files.

-- 1. Add the new data_key column
ALTER TABLE ingestion_chunks
  ADD COLUMN IF NOT EXISTS data_key VARCHAR(500);

-- 2. Drop the old data column (irreversible — back up first if needed)
ALTER TABLE ingestion_chunks
  DROP COLUMN IF EXISTS data;
