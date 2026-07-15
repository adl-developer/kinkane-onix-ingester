import {
  pgTable,
  serial,
  varchar,
  text,
  integer,
  numeric,
  timestamp,
  date,
  customType,
  index,
} from 'drizzle-orm/pg-core';

// pgvector vector type
export const vector = customType<{
  data: number[];
  driverData: string;
  config: { dimensions: number };
}>({
  dataType(config) {
    return config ? `vector(${config.dimensions})` : 'vector';
  },
  toDriver(value: number[]): string {
    return `[${value.join(',')}]`;
  },
  fromDriver(value: string): number[] {
    return value.replace(/^\[|\]$/g, '').split(',').map(Number);
  },
});

// PostgreSQL tsvector type (maintained by DB trigger — never written from app)
export const tsvector = customType<{ data: string }>({
  dataType() {
    return 'tsvector';
  },
});

export const books = pgTable(
  'books',
  {
    id: serial('id').primaryKey(),

    // ONIX identity
    recordReference: varchar('record_reference', { length: 100 }).notNull().unique(),
    isbn13: varchar('isbn13', { length: 13 }).unique(),
    notificationType: varchar('notification_type', { length: 2 }),

    // Format / physical
    productForm: varchar('product_form', { length: 10 }),
    productComposition: varchar('product_composition', { length: 2 }),
    editionNumber: integer('edition_number'),
    pageCount: integer('page_count'),
    heightMm: numeric('height_mm', { precision: 7, scale: 2 }),
    widthMm: numeric('width_mm', { precision: 7, scale: 2 }),
    thicknessMm: numeric('thickness_mm', { precision: 7, scale: 2 }),
    weightGr: numeric('weight_gr', { precision: 9, scale: 2 }),
    countryOfManufacture: varchar('country_of_manufacture', { length: 2 }),
    productClassificationCode: varchar('product_classification_code', { length: 30 }),

    // Title
    title: varchar('title', { length: 2000 }).notNull(),
    subtitle: varchar('subtitle', { length: 2000 }),

    // Descriptions
    shortDescription: text('short_description'),
    longDescription: text('long_description'),

    // Publishing
    publisherName: varchar('publisher_name', { length: 500 }),
    imprintName: varchar('imprint_name', { length: 500 }),
    countryOfPublication: varchar('country_of_publication', { length: 2 }),
    publishingStatus: varchar('publishing_status', { length: 2 }),
    publicationDate: date('publication_date'),

    // Supply
    availabilityCode: varchar('availability_code', { length: 2 }),
    returnsCode: varchar('returns_code', { length: 10 }),
    orderTime: integer('order_time'),

    // Search & AI (managed outside of standard ORM inserts)
    searchVector: tsvector('search_vector'),
    embedding: vector('embedding', { dimensions: 768 }),
    embeddedAt: timestamp('embedded_at', { withTimezone: true }),

    // Cover image, sourced from Gardners first and Google Books as a fallback
    coverUrl: varchar('cover_url', { length: 500 }),
    // Set only by the Google Books fallback whenever IT attempts a fetch (even
    // if no cover was found), so it doesn't keep retrying. NULL = Google Books
    // hasn't tried yet. Retried after 30 days if still no cover. Google Books
    // only considers a book once gardnersCoverCheckedAt is set — see below.
    coverFetchedAt: timestamp('cover_fetched_at', { withTimezone: true }),
    // Set only by Gardners' cover full-catalogue probe (gardners-cover-sync
    // .service.ts), independent of coverFetchedAt above. Google Books' fallback
    // query requires this to be set (and coverUrl still null) before it will
    // ever try a book — this is what makes Google Books a true last resort.
    gardnersCoverCheckedAt: timestamp('gardners_cover_checked_at', { withTimezone: true }),

    createdAt: timestamp('created_at', { withTimezone: true }).defaultNow().notNull(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow().notNull(),
  },
  (t) => ({
    isbnIdx: index('idx_books_isbn13').on(t.isbn13),
    titleIdx: index('idx_books_title').on(t.title),
    publisherIdx: index('idx_books_publisher').on(t.publisherName),
    availabilityIdx: index('idx_books_availability').on(t.availabilityCode),
  }),
);

export type Book = typeof books.$inferSelect;
export type NewBook = typeof books.$inferInsert;
