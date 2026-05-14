# ONIX Ingester

A production-ready Node.js service that ingests ONIX 3.1 XML feeds from Cloudflare R2 into PostgreSQL. Built to handle files of any size (tested against 27 GB feeds) without buffering data in memory or Redis.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Architecture Overview](#architecture-overview)
- [Why These Decisions Were Made](#why-these-decisions-were-made)
- [End-to-End Flow](#end-to-end-flow)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Database Setup](#database-setup)
- [Running Locally](#running-locally)
- [Deploying to Render](#deploying-to-render)
- [Uploading Large Files (27 GB+)](#uploading-large-files-27-gb)

---

## Prerequisites

Before you begin, make sure you have the following installed and available:

| Requirement | Minimum version | Notes |
|-------------|----------------|-------|
| Node.js | 20+ | ES2022 target; Node 22 recommended |
| npm | 9+ | Bundled with Node |
| PostgreSQL | 14+ | 16+ recommended; must support pgvector |
| Redis | 6+ | Must run with `maxmemory-policy noeviction` |
| pgvector | 0.7.0+ | See [Database Setup](#database-setup) for install steps |

You also need accounts / credentials for:

- **Cloudflare R2** — bucket + API token with read/write access
- **Google Gemini API** — key with access to `text-embedding-004`

---

## Project Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd onix_ingester
```

### 2. Install dependencies

```bash
npm install
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in every value. See [Environment Variables](#environment-variables) for the full list. The server will refuse to start if any required variable is missing.

> **Important:** `R2_ACCESS_KEY_ID` must be exactly 32 characters. Cloudflare sometimes displays tokens with extra whitespace — copy carefully.

### 4. Set up the database

```bash
npm run db:init
```

This runs three steps in sequence:

1. Installs the `vector` and `pg_trgm` PostgreSQL extensions
2. Applies all Drizzle migrations (creates all tables and enums)
3. Adds the `data` jsonb column, the full-text search trigger, and GIN indexes

All steps are idempotent — safe to re-run.

> If you get `type "vector" does not exist`, pgvector is not installed for your PostgreSQL version. See [Installing pgvector](#installing-pgvector).

### 5. Start the development server

```bash
npm run dev
```

The server starts on `http://localhost:3001` with hot reload via `tsx watch`.

### 6. Get an admin JWT

```bash
curl -X POST http://localhost:3001/auth/token \
  -H "Content-Type: application/json" \
  -d '{ "secret": "your_admin_secret" }'
```

Use the returned token as `Authorization: Bearer <token>` on all other requests. Tokens expire after 30 minutes.

### Available scripts

| Script | Description |
|--------|-------------|
| `npm run dev` | Start with hot reload (development) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run compiled output (production) |
| `npm run db:init` | Install extensions, run migrations, set up FTS |
| `npm run db:generate` | Generate a new migration from schema changes |
| `npm run db:migrate` | Apply pending migrations |
| `npm run db:reset` | Drop all tables and migration records (dev only) |

---

## Architecture Overview

```
Publisher / Cron
      │
      │  PUT (presigned URL) or direct R2 upload
      ▼
Cloudflare R2  ──────────────────────────────────────┐
      │                                               │
      │  POST /ingestion/trigger (file key)           │
      ▼                                               │
  onix-file queue (BullMQ)                           │
      │                                               │
      │  file.worker streams file from R2             │
      │  SAX-parses in 500-book chunks ──────────────┘
      │  stores each chunk as jsonb in PostgreSQL
      │  enqueues chunk job (carries only IDs)
      ▼
  onix-chunk queue (BullMQ, concurrency=5)
      │
      │  chunk.worker reads jsonb from PostgreSQL
      │  upserts books / contributors / subjects /
      │  genres / prices
      │  generates Gemini embeddings for new/changed books
      │  nulls chunk data to free DB space
      ▼
  PostgreSQL (books, search vectors, embeddings)
```

Two queues. One file produces many chunks. Each chunk is processed independently and in parallel.

---

## Why These Decisions Were Made

### SAX streaming + backpressure instead of DOM parsing

A 27 GB XML file cannot fit in memory. The service uses a SAX event-driven parser (`sax` library) wrapped in an async generator. The generator exposes backpressure: when 5 parsed batches are queued and not yet written to the database, the R2 source stream is paused — the TCP window fills, R2 stops sending bytes. Memory is bounded to roughly `5 × 500 books × 5 KB ≈ 12 MB` regardless of file size.

### Book data stored in PostgreSQL jsonb, not Redis

Each 500-book chunk is around 1.5–3 MB of JSON. A 27 GB feed can produce thousands of chunks. Storing that in Redis would exhaust memory immediately. Instead the parsed data is written to `ingestion_chunks.data` (a `jsonb` column) and cleared to `NULL` once the chunk worker finishes. Redis only ever sees a ~100-byte job payload carrying two integer IDs.

### Two-level BullMQ queue (file → chunks)

Separating file parsing from book upserts means:

- File parsing and chunk processing happen concurrently: chunk workers start before the file worker finishes.
- A transient failure in one chunk does not affect others.
- Concurrency is controllable at each level independently (file worker: 1, chunk worker: 5 by default).
- The Bull Board UI gives per-chunk visibility.

### Presigned URLs for large uploads

Render's reverse proxy enforces a 75-second request timeout that cannot be overridden from application code. Uploading a 27 GB file through the HTTP server would be killed mid-stream. The presigned URL approach routes the file bytes directly from the publisher to Cloudflare R2, bypassing the server entirely. The server only generates a signed URL and later processes the already-stored file.

### Gemini `text-embedding-004` for semantic search

768-dimension embeddings stored as pgvector `vector(768)`. Embeddings are generated only for books that are new or have had their title or description changed (tracked via `embedded_at`). A configurable inter-batch delay (`EMBEDDING_BATCH_DELAY_MS`, default 200 ms) prevents Gemini rate-limit errors under sustained load.

### PostgreSQL full-text search via tsvector trigger

A `BEFORE INSERT OR UPDATE` trigger on the `books` table automatically maintains a `tsvector` column combining title (weight A), subtitle (weight B), ISBN (weight A), and long description (weight C). This means full-text search requires no application-side work and stays in sync automatically.

---

## End-to-End Flow

### Path 1: Large file upload (recommended for files > 200 MB)

```
1. POST /ingestion/presign  { filename: "feed.xml" }
   ← { uploadUrl, fileKey, expiresIn }

2. PUT <uploadUrl>  (publisher uploads directly to R2, server not involved)

3. POST /ingestion/trigger  { fileKey }
   ← { jobId, bullJobId }

4. GET /ingestion/jobs/:jobId  (poll for progress)
```

### Path 2: Small file upload via server

```
1. POST /ingestion/upload  (multipart/form-data, file=<onix.xml>, trigger=true)
   ← { fileKey, jobId, bullJobId }
   (busboy streams directly to R2 via multipart upload — no disk buffering)

2. GET /ingestion/jobs/:jobId  (poll for progress)
```

### Path 3: Cron-triggered (publisher drops files in R2 directly)

```
1. Publisher uploads to R2 under the onix/ prefix by any means (rclone, AWS CLI, etc.)

2. Cron job runs on R2_POLL_CRON schedule (default: 2 AM daily)
   Calls listUnprocessedR2Files() → triggers ingestion for each new file

3. GET /ingestion/jobs  (view all jobs)
```

### What happens during ingestion

```
file.worker
  1. Marks ingestion_job as "processing"
  2. Streams file from R2 → SAX parser
  3. For every 500 books:
       a. Inserts chunk row into ingestion_chunks (status=pending, data=jsonb)
       b. Enqueues { ingestionJobId, chunkId, chunkIndex } into onix-chunk queue
  4. Updates job totals (total_chunks, total_books), sets status="enqueued"

chunk.worker (concurrency=5, runs in parallel with file.worker)
  1. Reads chunk.data from PostgreSQL
  2. Marks chunk as "processing"
  3. For each book:
       a. notificationType=05 → delete record, skip
       b. Otherwise → upsert into books (conflict on record_reference)
          If title or long_description changed → reset embedded_at to NULL
       c. Upsert contributors, subjects, prices (delete+insert)
       d. Upsert genres from Thema (scheme 93) subjects
  4. Queries books with embedded_at IS NULL from this chunk
  5. Generates embeddings in batches via Gemini batchEmbedContents
  6. Writes embeddings back to books.embedding + sets embedded_at
  7. Sets chunk.data = NULL (frees space)
  8. Updates job counters; if all chunks done, sets job status
```

---

## API Reference

All routes require `Authorization: Bearer <jwt>`. Get a token from `POST /auth/token`.

### Auth

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/auth/token` | `{ secret }` | Exchange `ADMIN_SECRET` for a 30-minute JWT |

### Ingestion

| Method | Path | Body / Query | Description |
|--------|------|------|-------------|
| POST | `/ingestion/presign` | `{ filename, key?, expiresIn? }` | Get a presigned R2 PUT URL (valid up to 4 h) |
| POST | `/ingestion/trigger` | `{ fileKey }` | Trigger ingestion for an R2 key |
| POST | `/ingestion/upload` | multipart: `file`, `key?`, `trigger?` | Upload file to R2 and optionally trigger |
| GET | `/ingestion/jobs` | `?limit&offset` | List ingestion jobs newest-first |
| GET | `/ingestion/jobs/:id` | — | Job detail including all chunks |
| GET | `/ingestion/unprocessed` | — | List R2 files not yet ingested |

### Bull Board

| Path | Description |
|------|-------------|
| `/bull-board` | Queue dashboard (requires admin JWT cookie or header) |

---

## Database Schema

### books

The central table. One row per ONIX `RecordReference`.

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| record_reference | varchar(100) UNIQUE | ONIX identity key, used as upsert target |
| isbn13 | varchar(13) UNIQUE | |
| notification_type | varchar(2) | 03=notification, 05=delete |
| product_form | varchar(10) | ONIX product form code |
| product_composition | varchar(2) | |
| edition_number | integer | |
| page_count | integer | |
| height_mm / width_mm / thickness_mm | numeric(7,2) | Physical dimensions |
| weight_gr | numeric(9,2) | |
| country_of_manufacture | varchar(2) | ISO country code |
| title | varchar(2000) NOT NULL | |
| subtitle | varchar(2000) | |
| short_description | text | TextContent type 02 |
| long_description | text | TextContent type 03 |
| publisher_name | varchar(500) | |
| imprint_name | varchar(500) | |
| country_of_publication | varchar(2) | |
| publishing_status | varchar(2) | |
| publication_date | date | |
| availability_code | varchar(2) | |
| returns_code | varchar(10) | |
| order_time | integer | Days |
| search_vector | tsvector | Maintained by DB trigger — never written by app |
| embedding | vector(768) | Gemini text-embedding-004 |
| embedded_at | timestamptz | NULL = needs (re)embedding |
| created_at / updated_at | timestamptz | |

**Indexes:** isbn13, title, publisher_name, availability_code, GIN on search_vector, GIN trigram on title.

**Trigger:** `trg_book_search_vector` — fires BEFORE INSERT OR UPDATE, combines title+subtitle+isbn13+long_description into a weighted tsvector.

### book_contributors

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| book_id | integer FK → books.id CASCADE DELETE | |
| sequence_number | integer | Display order |
| role | varchar(10) | A01=Author, B01=Editor, etc. |
| person_name | varchar(500) | Display form |
| person_name_inverted | varchar(500) | "Last, First" form |

Completely replaced on each upsert (delete + insert).

### book_subjects

All raw ONIX subject entries (BIC, Thema, BISAC, free text, etc.).

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| book_id | integer FK → books.id CASCADE DELETE | |
| scheme_identifier | varchar(10) | 93=Thema, 10=BISAC, 12=BIC |
| scheme_version | varchar(10) | |
| subject_code | varchar(50) | |
| subject_heading_text | varchar(500) | |
| is_main_subject | boolean | |

### genres

Normalised genre table derived from Thema subjects (scheme 93). Shared across books.

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| name | varchar(300) | Human-readable label |
| slug | varchar(300) UNIQUE | URL-safe, e.g. `fiction_crime_mystery` |
| subject_code | varchar(50) | Thema code |
| scheme_identifier | varchar(10) | Always 93 |

### book_genres

Many-to-many join table.

| Column | Type |
|--------|------|
| book_id | FK → books.id CASCADE DELETE |
| genre_id | FK → genres.id CASCADE DELETE |

Primary key is `(book_id, genre_id)`.

### book_prices

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| book_id | integer FK → books.id CASCADE DELETE | |
| price_type | varchar(2) | 01=RRP excl tax, 02=RRP incl tax |
| price_amount | numeric(12,2) | |
| currency_code | varchar(3) | ISO 4217 |
| tax_rate_code | varchar(2) | |
| tax_rate_percent | numeric(6,2) | |

### ingestion_jobs

One row per ONIX file triggered for ingestion.

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| file_key | varchar(1000) | R2 object key |
| status | enum | pending → processing → enqueued → completed / failed |
| total_chunks / processed_chunks / failed_chunks | integer | |
| total_books / processed_books | integer | |
| error_message | text | Set on failure |
| started_at / completed_at / created_at / updated_at | timestamptz | |

### ingestion_chunks

One row per 500-book chunk within a job.

| Column | Type | Notes |
|--------|------|-------|
| id | serial PK | |
| job_id | integer FK → ingestion_jobs.id CASCADE DELETE | |
| chunk_index | integer | 0-based position in file |
| status | enum | pending → processing → completed / failed |
| book_count | integer | Books in this chunk |
| processed_books | integer | |
| bull_job_id | varchar(200) | BullMQ job ID for cross-referencing |
| data | jsonb | Parsed OnixProduct[]. Set to NULL after processing. |
| error_message | text | |
| created_at / updated_at | timestamptz | |

---

## Project Structure

```
onix_ingester/
├── src/
│   ├── server.ts              # Entry point — starts workers, cron, HTTP server
│   ├── app.ts                 # Express app, middleware, routes
│   ├── config/index.ts        # Env validation (zod), typed config object
│   │
│   ├── routes/
│   │   ├── auth.routes.ts
│   │   └── ingestion.routes.ts
│   │
│   ├── controllers/
│   │   ├── auth.controller.ts
│   │   └── ingestion.controller.ts
│   │
│   ├── services/
│   │   ├── auth.service.ts        # JWT sign/verify
│   │   ├── ingestion.service.ts   # triggerIngestion, listJobs, listUnprocessed
│   │   ├── storage.service.ts     # R2: stream, upload, presign, list, exists
│   │   ├── parser.service.ts      # SAX async generator with backpressure
│   │   └── embedding.service.ts   # Gemini batchEmbedContents, retry, rate-limit delay
│   │
│   ├── workers/
│   │   ├── file.worker.ts         # Parses file, writes chunks to DB, enqueues chunk jobs
│   │   └── chunk.worker.ts        # Upserts books, generates embeddings, clears chunk data
│   │
│   ├── queue/
│   │   ├── index.ts               # fileQueue + chunkQueue (BullMQ)
│   │   └── board.ts               # Bull Board Express adapter
│   │
│   ├── middleware/
│   │   └── auth.middleware.ts     # requireAdminToken — validates JWT on every admin route
│   │
│   ├── db/
│   │   ├── index.ts               # Drizzle + postgres client
│   │   ├── schema/                # One file per table group
│   │   │   ├── books.ts
│   │   │   ├── contributors.ts
│   │   │   ├── genres.ts
│   │   │   ├── prices.ts
│   │   │   └── ingestion.ts
│   │   ├── install-extensions.ts  # CREATE EXTENSION vector + pg_trgm
│   │   ├── init.ts                # Add jsonb column, create FTS trigger + indexes
│   │   ├── mark-migrated.ts       # One-time: marks existing tables in drizzle migrations table
│   │   └── reset.ts               # Dev only: drops all tables and migration records
│   │
│   ├── cron/index.ts              # node-cron R2 poll on R2_POLL_CRON schedule
│   │
│   └── types/
│       ├── onix.ts                # OnixProduct, OnixContributor, OnixSubject, OnixPrice
│       └── queue.ts               # FileJobData, ChunkJobData, result types
│
├── drizzle/                       # Auto-generated migration SQL files
├── drizzle.config.ts
├── render.yaml                    # Render deploy config
├── package.json
└── tsconfig.json
```

---

## Environment Variables

Create a `.env` file in the project root. All variables are validated at startup via zod — the server will refuse to start if any required variable is missing or malformed.

```env
# Server
PORT=3001
NODE_ENV=development

# PostgreSQL (must have pgvector and pg_trgm extensions available)
DATABASE_URL=postgresql://user:password@host:5432/dbname

# Redis (BullMQ requires noeviction policy)
REDIS_URL=redis://localhost:6379

# Cloudflare R2
R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key        # Must be exactly 32 characters
R2_SECRET_ACCESS_KEY=your_secret_key
R2_BUCKET_NAME=your_bucket_name
R2_ENDPOINT=https://<account_id>.r2.cloudflarestorage.com
R2_ONIX_PREFIX=onix/                    # Prefix for ONIX files in R2

# Gemini
GEMINI_API_KEY=your_gemini_key
GEMINI_EMBEDDING_MODEL=text-embedding-004
GEMINI_FLASH_MODEL=gemini-2.5-flash-lite

# Auth (both must be at least 16 characters)
ADMIN_SECRET=your_admin_secret          # Exchanged for a JWT via POST /auth/token
JWT_SECRET=your_jwt_signing_secret

# Ingestion tuning
CHUNK_SIZE=500                          # Books per chunk job
EMBEDDING_BATCH_SIZE=100                # Books per Gemini API call
EMBEDDING_BATCH_DELAY_MS=200            # ms to sleep between Gemini calls (rate limit guard)

# Cron (standard cron syntax)
R2_POLL_CRON=0 2 * * *                  # Default: 2 AM daily
```

---

## Database Setup

### Requirements

| Dependency | Minimum version | Notes |
|-----------|----------------|-------|
| PostgreSQL | 14+ (16+ recommended) | |
| pgvector | 0.7.0+ | Must be compiled for your PG version |
| pg_trgm | Bundled with PostgreSQL | Enable via extension |
| Redis | 6+ | Must use `maxmemory-policy noeviction` |

### Installing pgvector

pgvector is not bundled with PostgreSQL. Install it from source:

```bash
git clone https://github.com/pgvector/pgvector.git
cd pgvector
make
sudo make install
```

If you have multiple PostgreSQL versions, specify the correct one:

```bash
sudo make install PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config
```

On managed databases (Render, Supabase, Neon), pgvector is pre-installed. On Render PostgreSQL 14+ plans, run `CREATE EXTENSION vector` and it works.

### Redis configuration

BullMQ requires Redis to never evict keys. Set in `redis.conf`:

```
maxmemory-policy noeviction
```

On Render Redis, this is configured in `render.yaml` automatically.

### Running the full setup

```bash
# 1. Install dependencies
npm install

# 2. Run the one-command setup (installs extensions, runs migrations, adds FTS trigger + indexes)
npm run db:init
```

`db:init` runs three scripts in sequence:

1. **`install-extensions.ts`** — `CREATE EXTENSION IF NOT EXISTS vector` and `pg_trgm`
2. **`drizzle-kit migrate`** — applies all pending SQL migrations from the `drizzle/` folder
3. **`init.ts`** — adds the `data jsonb` column to `ingestion_chunks`, creates the `tsvector` column and trigger on `books`, and creates the GIN indexes

All three steps are idempotent — safe to run again without side effects.

### Resetting the database (development only)

```bash
npm run db:reset   # Drops all tables, enums, and drizzle migration records
npm run db:init    # Recreates everything from scratch
```

### Migration workflow

Drizzle manages schema migrations. To generate a new migration after editing a schema file:

```bash
npm run db:generate   # Creates a new SQL file in drizzle/
npm run db:migrate    # Applies it
```

---

## Running Locally

```bash
# Install
npm install

# Set up environment
cp .env.example .env
# (edit .env with your values)

# Set up database
npm run db:init

# Start in development (tsx watch — hot reload)
npm run dev
```

The server starts on `PORT` (default 3001). Bull Board is at `http://localhost:3001/bull-board`.

To get an admin JWT:

```bash
curl -X POST http://localhost:3001/auth/token \
  -H "Content-Type: application/json" \
  -d '{ "secret": "your_admin_secret" }'
```

---

## Deploying to Render

`render.yaml` at the repo root defines three services:

| Service | Type | Notes |
|---------|------|-------|
| `onix-ingester` | Web service | Node.js, builds with `npm run build`, starts with `node dist/server.js` |
| `onix-postgres` | PostgreSQL | pgvector available on Render PostgreSQL 14+ |
| `onix-redis` | Redis | `maxmemoryPolicy: noeviction` set automatically |

### Pre-deploy command

The web service runs this before every deploy:

```
node dist/db/install-extensions.js && npm run db:migrate && node dist/db/init.js
```

This ensures extensions, migrations, and the FTS trigger are applied on every release — the server will never start against a stale schema.

### Build and start commands

```
Build:  npm run build
Start:  node dist/server.js
```

### Environment variables on Render

Set all variables from the [Environment Variables](#environment-variables) section in the Render dashboard under your web service's **Environment** tab. `DATABASE_URL` and `REDIS_URL` are injected automatically from the linked PostgreSQL and Redis services.

---

## Uploading Large Files (27 GB+)

Do not upload large files through the HTTP server. Use the presigned URL flow:

### Step 1 — get a signed upload URL

```bash
curl -X POST https://your-service.onrender.com/ingestion/presign \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{ "filename": "bigfeed_2026.xml", "expiresIn": 14400 }'
```

Response:
```json
{
  "uploadUrl": "https://your-account.r2.cloudflarestorage.com/...",
  "fileKey": "onix/bigfeed_2026_1747123456789.xml",
  "expiresIn": 14400
}
```

The URL is valid for the specified number of seconds (max 86400 = 24 h).

### Step 2 — upload directly to R2

```bash
curl -X PUT "<uploadUrl>" \
  -H "Content-Type: application/xml" \
  --data-binary @bigfeed_2026.xml
```

Or with any S3-compatible tool:

```bash
aws s3 cp bigfeed_2026.xml s3://your-bucket/onix/bigfeed_2026.xml \
  --endpoint-url https://your-account.r2.cloudflarestorage.com
```

### Step 3 — trigger ingestion

```bash
curl -X POST https://your-service.onrender.com/ingestion/trigger \
  -H "Authorization: Bearer <jwt>" \
  -H "Content-Type: application/json" \
  -d '{ "fileKey": "onix/bigfeed_2026_1747123456789.xml" }'
```

Response:
```json
{ "message": "Ingestion job enqueued", "jobId": 42, "bullJobId": "1001" }
```

### Step 4 — monitor progress

```bash
curl https://your-service.onrender.com/ingestion/jobs/42 \
  -H "Authorization: Bearer <jwt>"
```

The response includes `status`, `totalChunks`, `processedChunks`, `failedChunks`, `totalBooks`, `processedBooks`, and a full array of chunk records so you can see exactly which parts are still in flight.

---

## Memory and Performance Characteristics

| Resource | Peak usage | Why |
|----------|-----------|-----|
| Node.js heap (parse) | ~12 MB | 5 pending batches × 500 books × 5 KB |
| Redis | ~100 bytes/job | Only IDs in BullMQ payload |
| PostgreSQL `ingestion_chunks.data` | Transient | Cleared after each chunk is processed |
| Gemini API calls | 1 call per 100 books | Batched; 200 ms delay between calls |
| Wall-clock time (27 GB, 500k books) | ~2–4 hours | Dominated by Gemini embedding latency |

Embedding generation is the long pole. If speed matters more than cost, increase `EMBEDDING_BATCH_SIZE` and decrease `EMBEDDING_BATCH_DELAY_MS`. If you hit Gemini quota errors, do the opposite.
