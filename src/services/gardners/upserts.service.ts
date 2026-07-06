import { sql } from 'drizzle-orm';
import { db } from '../../db';
import { gardnersStock, NewGardnersStock } from '../../db/schema';
import { logger } from '../../lib/logger';

/**
 * Bulk upsert for gardners_stock, keyed on isbn13. Only overwrites an
 * existing row if the incoming data is at least as fresh as what's already
 * stored (stockUpdatedAt comparison) — protects against an in-flight daily
 * Inventory run racing an hourly Avail13 run and clobbering newer data with
 * stale data. See gardners-stock.ts's doc comment for the full rationale.
 */
async function upsertStockRows(
  rows: NewGardnersStock[],
): Promise<{ processed: number; failed: number }> {
  if (rows.length === 0) return { processed: 0, failed: 0 };

  try {
    await db
      .insert(gardnersStock)
      .values(rows)
      .onConflictDoUpdate({
        target: gardnersStock.isbn13,
        set: {
          rrpGbp: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.rrp_gbp ELSE gardners_stock.rrp_gbp END`,
          discountPercent: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.discount_percent ELSE gardners_stock.discount_percent END`,
          stockQty: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.stock_qty ELSE gardners_stock.stock_qty END`,
          reportCode: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.report_code ELSE gardners_stock.report_code END`,
          reportDate: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.report_date ELSE gardners_stock.report_date END`,
          source: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.source ELSE gardners_stock.source END`,
          sourceFileKey: sql`CASE WHEN excluded.stock_updated_at >= gardners_stock.stock_updated_at THEN excluded.source_file_key ELSE gardners_stock.source_file_key END`,
          stockUpdatedAt: sql`GREATEST(excluded.stock_updated_at, gardners_stock.stock_updated_at)`,
          updatedAt: new Date(),
        },
      });

    return { processed: rows.length, failed: 0 };
  } catch (err) {
    logger.error('Failed to upsert gardners_stock batch', {
      rowCount: rows.length,
      error: err instanceof Error ? err.message : String(err),
    });
    return { processed: 0, failed: rows.length };
  }
}

/**
 * Links gardners_stock rows to `books` by isbn13, scoped to just the ISBNs
 * from the batch that was just upserted — not a periodic full-table scan,
 * which would get more expensive every day as both tables grow into the
 * millions of rows. Only touches rows where book_id is still NULL, so
 * already-linked rows aren't rewritten on every sync.
 *
 * This is scoped-by-design, not perfect: an ISBN whose `books` row only
 * appears after this ISBN's last Inventory sync stays unlinked until that
 * ISBN is next touched by a future Inventory/Avail13 run. Acceptable
 * trade-off per the plan — a full-table catch-up would be periodically
 * expensive at scale for a gap that mostly closes itself day to day.
 */
async function backfillStockBookIds(isbns: string[]): Promise<void> {
  if (isbns.length === 0) return;

  try {
    const isbnArray = sql.join(
      isbns.map((isbn) => sql`${isbn}`),
      sql`, `,
    );
    await db.execute(sql`
      UPDATE gardners_stock
      SET book_id = books.id
      FROM books
      WHERE gardners_stock.isbn13 = books.isbn13
        AND gardners_stock.book_id IS NULL
        AND gardners_stock.isbn13 = ANY(ARRAY[${isbnArray}]::text[])
    `);
  } catch (err) {
    logger.error('Failed to backfill gardners_stock.book_id', {
      isbnCount: isbns.length,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

export const gardnersUpserts = {
  upsertStockRows,
  backfillStockBookIds,
};
