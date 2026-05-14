import {
  pgTable,
  serial,
  integer,
  varchar,
  boolean,
  primaryKey,
  index,
} from 'drizzle-orm/pg-core';
import { books } from './books';

// Normalised genre list derived from Thema subjects
export const genres = pgTable('genres', {
  id: serial('id').primaryKey(),
  name: varchar('name', { length: 300 }).notNull(),
  slug: varchar('slug', { length: 300 }).notNull().unique(),
  subjectCode: varchar('subject_code', { length: 50 }),
  schemeIdentifier: varchar('scheme_identifier', { length: 10 }),
});

// Many-to-many: book ↔ genre
export const bookGenres = pgTable(
  'book_genres',
  {
    bookId: integer('book_id')
      .notNull()
      .references(() => books.id, { onDelete: 'cascade' }),
    genreId: integer('genre_id')
      .notNull()
      .references(() => genres.id, { onDelete: 'cascade' }),
  },
  (t) => ({
    pk: primaryKey({ columns: [t.bookId, t.genreId] }),
    genreIdIdx: index('idx_book_genres_genre_id').on(t.genreId),
  }),
);

// All raw ONIX subjects (BIC, Thema, BISAC, etc.)
export const bookSubjects = pgTable(
  'book_subjects',
  {
    id: serial('id').primaryKey(),
    bookId: integer('book_id')
      .notNull()
      .references(() => books.id, { onDelete: 'cascade' }),
    schemeIdentifier: varchar('scheme_identifier', { length: 10 }),
    schemeVersion: varchar('scheme_version', { length: 10 }),
    subjectCode: varchar('subject_code', { length: 50 }),
    subjectHeadingText: varchar('subject_heading_text', { length: 500 }),
    isMainSubject: boolean('is_main_subject').default(false),
  },
  (t) => ({
    bookIdIdx: index('idx_book_subjects_book_id').on(t.bookId),
  }),
);

export type Genre = typeof genres.$inferSelect;
export type NewGenre = typeof genres.$inferInsert;
export type BookSubject = typeof bookSubjects.$inferSelect;
export type NewBookSubject = typeof bookSubjects.$inferInsert;
