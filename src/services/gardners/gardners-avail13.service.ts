import { NewGardnersStock } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { isValidIsbn13 } from './parsing-utils';

// data.gardners.com's hourly stock-quantity-only feed. `.000` is the
// start-of-day full snapshot (every ISBN currently in stock — an ISBN not
// listed implies zero stock); `.001` onwards are hourly update-only deltas
// (an item that just sold out appears with qty 0). No header/trailer
// framing, just one throwaway marker line at the top (`START,<timestamp>`
// or `UPDATE,<timestamp>`) followed by `ISBN13,QTY` rows.
const AVAIL13_FILENAME_RE = /^AV_\d{8}\.\d{3}$/i;

export const gardnersAvail13FeedConfig: FetchFeedConfig = {
  feed: 'avail13',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Avail13');
    return files.filter((f) => AVAIL13_FILENAME_RE.test(f.filename));
  },
};

export const AVAIL13_COLUMNS = ['isbn13', 'qty'];

/**
 * Maps one row to a gardners_stock row, or null to skip it. The leading
 * `START,...`/`UPDATE,...` marker line naturally gets skipped here too —
 * its first column ("START"/"UPDATE") fails isValidIsbn13 just like any
 * other non-ISBN row, so no separate marker-line handling is needed.
 */
export function mapAvail13Row(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; stockUpdatedAt: Date; source: 'avail13_full' | 'avail13_delta' },
): NewGardnersStock | null {
  const isbn13 = record.isbn13?.trim();
  if (!isValidIsbn13(isbn13)) return null;

  const qty = record.qty?.trim();
  if (qty === undefined || qty === '') return null;

  return {
    isbn13,
    rrpGbp: null,
    discountPercent: null,
    stockQty: Number(qty),
    reportCode: null,
    reportDate: null,
    source: ctx.source,
    sourceFileKey: ctx.sourceFileKey,
    stockUpdatedAt: ctx.stockUpdatedAt,
  };
}

// The sequence suffix (`.000`, `.001`, ...) distinguishes the full
// start-of-day snapshot from an hourly delta — needed so upsertStockRows
// can label provenance correctly (see gardners-stock.ts's `source` column).
export function avail13SourceForFilename(filename: string): 'avail13_full' | 'avail13_delta' {
  const seq = /\.(\d{3})$/.exec(filename)?.[1];
  return seq === '000' ? 'avail13_full' : 'avail13_delta';
}

async function sync(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersAvail13FeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Avail13 files found');
    return;
  }

  // Filename-ascending order (already guaranteed by listUnprocessedFiles)
  // matters here — hourly sequence files should be enqueued in order so
  // the single-concurrency file worker processes them chronologically.
  // Correctness doesn't strictly depend on this (stockUpdatedAt-gated
  // upserts resolve out-of-order writes correctly regardless), but it
  // avoids pointless churn from processing a later hour before an earlier
  // one.
  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('avail13', file);

    await gardnersFileQueue.add(`gardners-avail13-${file.filename}`, {
      feed: 'avail13',
      connection: 'genericSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners Avail13 file', { filename: file.filename, logId });
  }
}

export const gardnersAvail13Service = { sync };
