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

export const gardnersUpserts = {
  upsertStockRows,
};
