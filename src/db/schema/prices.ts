import { pgTable, serial, integer, varchar, numeric, index } from 'drizzle-orm/pg-core';
import { books } from './books';

export const bookPrices = pgTable(
  'book_prices',
  {
    id: serial('id').primaryKey(),
    bookId: integer('book_id')
      .notNull()
      .references(() => books.id, { onDelete: 'cascade' }),
    priceType: varchar('price_type', { length: 2 }),    // 01=RRP excl tax, 02=RRP incl tax
    priceAmount: numeric('price_amount', { precision: 12, scale: 2 }),
    currencyCode: varchar('currency_code', { length: 3 }),
    taxRateCode: varchar('tax_rate_code', { length: 2 }),
    taxRatePercent: numeric('tax_rate_percent', { precision: 6, scale: 2 }),
  },
  (t) => ({
    bookIdIdx: index('idx_book_prices_book_id').on(t.bookId),
  }),
);

export type BookPrice = typeof bookPrices.$inferSelect;
export type NewBookPrice = typeof bookPrices.$inferInsert;
