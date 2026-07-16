import { NewGardnersPromotion } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { isValidIsbn13, parseDdMmYy } from './parsing-utils';

// data.gardners.com's daily promotions file — full replace, quoted CSV, no
// header/trailer framing. Verified live: some rows have a blank ISBN13
// (e.g. `"","(null)","","22.00","55.00","R","31/07/26"`) — skipped, not
// stored with an empty unique key.
export const gardnersPromotionsFeedConfig: FetchFeedConfig = {
  feed: 'promotions',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Prom');
    return files.filter((f) => f.filename === 'GARDPROM13.CSV');
  },
};

// Positional columns — GARDPROM13.CSV has no header row.
export const PROMOTIONS_COLUMNS = [
  'isbn13',
  'title',
  'author',
  'price',
  'discountPercent',
  'returnsFlag',
  'finishDate',
];

export function mapPromotionRow(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; syncedAt: Date },
): NewGardnersPromotion | null {
  const isbn13 = record.isbn13?.trim();
  if (!isValidIsbn13(isbn13)) return null;

  return {
    isbn13,
    title: record.title || null,
    author: record.author || null,
    price: record.price?.trim() || null,
    discountPercent: record.discountPercent?.trim() || null,
    returnsFlag: record.returnsFlag?.trim() || null,
    finishDate: parseDdMmYy(record.finishDate),
    sourceFileKey: ctx.sourceFileKey,
    syncedAt: ctx.syncedAt,
  };
}

async function sync(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersPromotionsFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Promotions files found');
    return;
  }

  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('promotions', file);

    await gardnersFileQueue.add(`gardners-promotions-${file.filename}`, {
      feed: 'promotions',
      connection: 'genericSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners Promotions file', { filename: file.filename, logId });
  }
}

export const gardnersPromotionsService = { sync };
