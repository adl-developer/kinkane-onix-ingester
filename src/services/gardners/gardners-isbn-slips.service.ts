import { NewGardnersIsbnSlip } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { isValidIsbn13 } from './parsing-utils';

// data.gardners.com's weekly slipped-ISBN file — full replace, quoted CSV,
// no header/trailer, two columns: old ISBN13, new ISBN13.
export const gardnersIsbnSlipsFeedConfig: FetchFeedConfig = {
  feed: 'isbn_slips',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/isbnslip');
    return files.filter((f) => f.filename === 'ISBNSL13.CSV');
  },
};

export const ISBN_SLIPS_COLUMNS = ['oldIsbn13', 'newIsbn13'];

export function mapIsbnSlipRow(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; syncedAt: Date },
): NewGardnersIsbnSlip | null {
  const oldIsbn13 = record.oldIsbn13?.trim();
  const newIsbn13 = record.newIsbn13?.trim();
  if (!isValidIsbn13(oldIsbn13) || !isValidIsbn13(newIsbn13)) return null;

  return {
    oldIsbn13,
    newIsbn13,
    sourceFileKey: ctx.sourceFileKey,
    syncedAt: ctx.syncedAt,
  };
}

async function sync(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersIsbnSlipsFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners isbn-slips files found');
    return;
  }

  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('isbn_slips', file);

    await gardnersFileQueue.add(`gardners-isbn-slips-${file.filename}`, {
      feed: 'isbn_slips',
      connection: 'genericSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners isbn-slips file', { filename: file.filename, logId });
  }
}

export const gardnersIsbnSlipsService = { sync };
