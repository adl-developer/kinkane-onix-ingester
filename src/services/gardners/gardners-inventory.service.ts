import { NewGardnersStock } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { isValidIsbn13, parseDdMmYyyy, pick } from './parsing-utils';

// edi.gardners.com's dedicated Bespoke Inventory account — full daily
// catalogue price+stock snapshot, gated on a sibling .DONE file. Filenames
// verified live: IV{MM}{DD}{YYYY}.TXT (e.g. IV07062026.TXT).
export const gardnersInventoryFeedConfig: FetchFeedConfig = {
  feed: 'inventory',
  connection: 'bespokeSftp',
  requiresDoneSentinel: true,
  listRemoteFiles: async (client) => {
    const files = await client.list('/Inventory');
    return files.filter((f) => /^IV\d{8}\.TXT$/i.test(f.filename));
  },
};

/**
 * Maps one CSV row to a gardners_stock row, or null to skip it. Column
 * names are matched by alias because the bespoke feed
 * (ISBN13,RRP_GBP,DISCOUNT,STOCK,REP_CODE,REP_DATE) and the older generic
 * feed (ISBN,EAN,RRP,DISCOUNT,QTY,REPORT,REPORT-DATE) use different header
 * names for the same logical fields.
 */
export function mapInventoryRow(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; stockUpdatedAt: Date },
): NewGardnersStock | null {
  const isbn13 = pick(record, 'ISBN13', 'EAN')?.trim();
  if (!isValidIsbn13(isbn13)) return null;

  const rrp = pick(record, 'RRP_GBP', 'RRP')?.trim();
  const discount = pick(record, 'DISCOUNT')?.trim();
  const stockQty = pick(record, 'STOCK', 'QTY')?.trim();
  const reportCode = pick(record, 'REP_CODE', 'REPORT')?.trim() || null;
  const reportDate = parseDdMmYyyy(pick(record, 'REP_DATE', 'REPORT-DATE'));

  return {
    isbn13,
    rrpGbp: rrp || null,
    discountPercent: discount || null,
    stockQty: stockQty ? Number(stockQty) : null,
    reportCode,
    reportDate,
    source: 'inventory',
    sourceFileKey: ctx.sourceFileKey,
    stockUpdatedAt: ctx.stockUpdatedAt,
  };
}

async function sync(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersInventoryFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Inventory files found');
    return;
  }

  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('inventory', file);

    await gardnersFileQueue.add(`gardners-inventory-${file.filename}`, {
      feed: 'inventory',
      connection: 'bespokeSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners Inventory file', { filename: file.filename, logId });
  }
}

export const gardnersInventoryService = { sync };
