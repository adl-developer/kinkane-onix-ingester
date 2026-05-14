import { pgTable, serial, integer, varchar, index } from 'drizzle-orm/pg-core';
import { books } from './books';

export const bookContributors = pgTable(
  'book_contributors',
  {
    id: serial('id').primaryKey(),
    bookId: integer('book_id')
      .notNull()
      .references(() => books.id, { onDelete: 'cascade' }),
    sequenceNumber: integer('sequence_number'),
    role: varchar('role', { length: 10 }),       // A01 = author, B01 = editor, etc.
    personName: varchar('person_name', { length: 500 }),
    personNameInverted: varchar('person_name_inverted', { length: 500 }),
  },
  (t) => ({
    bookIdIdx: index('idx_book_contributors_book_id').on(t.bookId),
  }),
);

export type BookContributor = typeof bookContributors.$inferSelect;
export type NewBookContributor = typeof bookContributors.$inferInsert;
