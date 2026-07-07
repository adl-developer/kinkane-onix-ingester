import { lt, sql } from 'drizzle-orm';
import { db } from '../../db';
import {
  gardnersStock,
  NewGardnersStock,
  gardnersPromotions,
  NewGardnersPromotion,
  gardnersFirmSale,
  NewGardnersFirmSale,
  gardnersIsbnSlips,
  NewGardnersIsbnSlip,
  gardnersMarketRestrictions,
  NewGardnersMarketRestriction,
  gardnersRegions,
  NewGardnersRegion,
} from '../../db/schema';
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
 * Links a Gardners table's rows to `books` by isbn13, scoped to just the
 * ISBNs from the batch that was just upserted — not a periodic full-table
 * scan, which would get more expensive every day as both tables grow into
 * the millions of rows. Only touches rows where book_id is still NULL, so
 * already-linked rows aren't rewritten on every sync. Shared across every
 * Gardners table that has a book_id column (all but gardners_isbn_slips,
 * which has none) since the shape of this update is identical — only the
 * table name differs.
 *
 * This is scoped-by-design, not perfect: an ISBN whose `books` row only
 * appears after this ISBN's last sync stays unlinked until that ISBN is
 * next touched by a future sync of the same feed. Acceptable trade-off per
 * the plan — a full-table catch-up would be periodically expensive at
 * scale for a gap that mostly closes itself day to day.
 */
async function backfillBookIds(tableName: string, isbns: string[]): Promise<void> {
  if (isbns.length === 0) return;

  try {
    const isbnArray = sql.join(
      isbns.map((isbn) => sql`${isbn}`),
      sql`, `,
    );
    const table = sql.raw(tableName);
    await db.execute(sql`
      UPDATE ${table}
      SET book_id = books.id
      FROM books
      WHERE ${table}.isbn13 = books.isbn13
        AND ${table}.book_id IS NULL
        AND ${table}.isbn13 = ANY(ARRAY[${isbnArray}]::text[])
    `);
  } catch (err) {
    logger.error(`Failed to backfill ${tableName}.book_id`, {
      isbnCount: isbns.length,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

const backfillStockBookIds = (isbns: string[]) => backfillBookIds('gardners_stock', isbns);
const backfillPromotionsBookIds = (isbns: string[]) => backfillBookIds('gardners_promotions', isbns);
const backfillFirmSaleBookIds = (isbns: string[]) => backfillBookIds('gardners_firm_sale', isbns);
const backfillMarketRestrictionsBookIds = (isbns: string[]) =>
  backfillBookIds('gardners_market_restrictions', isbns);

/**
 * Bulk upsert for gardners_promotions, keyed on isbn13. Unlike stock, only
 * this one feed ever writes to this table, so a plain unconditional
 * overwrite on conflict is correct — no freshness comparison needed.
 */
async function upsertPromotionRows(
  rows: NewGardnersPromotion[],
): Promise<{ processed: number; failed: number }> {
  if (rows.length === 0) return { processed: 0, failed: 0 };

  try {
    await db
      .insert(gardnersPromotions)
      .values(rows)
      .onConflictDoUpdate({
        target: gardnersPromotions.isbn13,
        set: {
          title: sql`excluded.title`,
          author: sql`excluded.author`,
          price: sql`excluded.price`,
          discountPercent: sql`excluded.discount_percent`,
          returnsFlag: sql`excluded.returns_flag`,
          finishDate: sql`excluded.finish_date`,
          sourceFileKey: sql`excluded.source_file_key`,
          syncedAt: sql`excluded.synced_at`,
          updatedAt: new Date(),
        },
      });

    return { processed: rows.length, failed: 0 };
  } catch (err) {
    logger.error('Failed to upsert gardners_promotions batch', {
      rowCount: rows.length,
      error: err instanceof Error ? err.message : String(err),
    });
    return { processed: 0, failed: rows.length };
  }
}

/** Full-replace mark-and-sweep: removes rows this run didn't touch. */
async function sweepStalePromotions(cutoff: Date): Promise<void> {
  await db.delete(gardnersPromotions).where(lt(gardnersPromotions.syncedAt, cutoff));
}

/** Bulk upsert for gardners_firm_sale, keyed on isbn13 (the table's PK). */
async function upsertFirmSaleRows(
  rows: NewGardnersFirmSale[],
): Promise<{ processed: number; failed: number }> {
  if (rows.length === 0) return { processed: 0, failed: 0 };

  try {
    await db
      .insert(gardnersFirmSale)
      .values(rows)
      .onConflictDoUpdate({
        target: gardnersFirmSale.isbn13,
        set: {
          reportCode: sql`excluded.report_code`,
          sourceFileKey: sql`excluded.source_file_key`,
          syncedAt: sql`excluded.synced_at`,
        },
      });

    return { processed: rows.length, failed: 0 };
  } catch (err) {
    logger.error('Failed to upsert gardners_firm_sale batch', {
      rowCount: rows.length,
      error: err instanceof Error ? err.message : String(err),
    });
    return { processed: 0, failed: rows.length };
  }
}

async function sweepStaleFirmSale(cutoff: Date): Promise<void> {
  await db.delete(gardnersFirmSale).where(lt(gardnersFirmSale.syncedAt, cutoff));
}

/** Bulk upsert for gardners_isbn_slips, keyed on oldIsbn13 (the table's PK). */
async function upsertIsbnSlipRows(
  rows: NewGardnersIsbnSlip[],
): Promise<{ processed: number; failed: number }> {
  if (rows.length === 0) return { processed: 0, failed: 0 };

  try {
    await db
      .insert(gardnersIsbnSlips)
      .values(rows)
      .onConflictDoUpdate({
        target: gardnersIsbnSlips.oldIsbn13,
        set: {
          newIsbn13: sql`excluded.new_isbn13`,
          sourceFileKey: sql`excluded.source_file_key`,
          syncedAt: sql`excluded.synced_at`,
        },
      });

    return { processed: rows.length, failed: 0 };
  } catch (err) {
    logger.error('Failed to upsert gardners_isbn_slips batch', {
      rowCount: rows.length,
      error: err instanceof Error ? err.message : String(err),
    });
    return { processed: 0, failed: rows.length };
  }
}

async function sweepStaleIsbnSlips(cutoff: Date): Promise<void> {
  await db.delete(gardnersIsbnSlips).where(lt(gardnersIsbnSlips.syncedAt, cutoff));
}

/**
 * Bulk upsert for gardners_market_restrictions, keyed on the (isbn13,
 * regionCode) composite unique index — there's no single-column PK since a
 * title can have multiple region rows.
 */
async function upsertRestrictionRows(
  rows: NewGardnersMarketRestriction[],
): Promise<{ processed: number; failed: number }> {
  if (rows.length === 0) return { processed: 0, failed: 0 };

  try {
    await db
      .insert(gardnersMarketRestrictions)
      .values(rows)
      .onConflictDoUpdate({
        target: [gardnersMarketRestrictions.isbn13, gardnersMarketRestrictions.regionCode],
        set: {
          flag: sql`excluded.flag`,
          sourceFileKey: sql`excluded.source_file_key`,
          syncedAt: sql`excluded.synced_at`,
        },
      });

    return { processed: rows.length, failed: 0 };
  } catch (err) {
    logger.error('Failed to upsert gardners_market_restrictions batch', {
      rowCount: rows.length,
      error: err instanceof Error ? err.message : String(err),
    });
    return { processed: 0, failed: rows.length };
  }
}

async function sweepStaleMarketRestrictions(cutoff: Date): Promise<void> {
  await db.delete(gardnersMarketRestrictions).where(lt(gardnersMarketRestrictions.syncedAt, cutoff));
}

/**
 * Wholesale upsert for the tiny gardners_regions lookup table — no
 * mark-and-sweep, a region code is never expected to disappear.
 */
async function upsertRegions(rows: NewGardnersRegion[]): Promise<void> {
  if (rows.length === 0) return;

  await db
    .insert(gardnersRegions)
    .values(rows)
    .onConflictDoUpdate({
      target: gardnersRegions.code,
      set: {
        name: sql`excluded.name`,
        syncedAt: sql`excluded.synced_at`,
      },
    });
}

export const gardnersUpserts = {
  upsertStockRows,
  backfillStockBookIds,
  upsertPromotionRows,
  backfillPromotionsBookIds,
  sweepStalePromotions,
  upsertFirmSaleRows,
  backfillFirmSaleBookIds,
  sweepStaleFirmSale,
  upsertIsbnSlipRows,
  sweepStaleIsbnSlips,
  upsertRestrictionRows,
  backfillMarketRestrictionsBookIds,
  sweepStaleMarketRestrictions,
  upsertRegions,
};
