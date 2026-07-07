import { NewGardnersFirmSale } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { isValidIsbn13 } from './parsing-utils';

// data.gardners.com's firm-sale ISBN file — full replace (~6M rows, the
// largest of the CSV feeds), no header/trailer, two columns: ISBN13, an
// optional report code (often blank).
export const gardnersFirmSaleFeedConfig: FetchFeedConfig = {
  feed: 'firm_sale',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Firmsale');
    return files.filter((f) => f.filename === 'FIRMSALE13.CSV');
  },
};

export const FIRM_SALE_COLUMNS = ['isbn13', 'reportCode'];

export function mapFirmSaleRow(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; syncedAt: Date },
): NewGardnersFirmSale | null {
  const isbn13 = record.isbn13?.trim();
  if (!isValidIsbn13(isbn13)) return null;

  return {
    isbn13,
    reportCode: record.reportCode?.trim() || null,
    sourceFileKey: ctx.sourceFileKey,
    syncedAt: ctx.syncedAt,
  };
}

async function sync(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersFirmSaleFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Firm Sale files found');
    return;
  }

  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('firm_sale', file);

    await gardnersFileQueue.add(`gardners-firm-sale-${file.filename}`, {
      feed: 'firm_sale',
      connection: 'genericSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners Firm Sale file', { filename: file.filename, logId });
  }
}

export const gardnersFirmSaleService = { sync };
