import { NewGardnersMarketRestriction, NewGardnersRegion } from '../../db/schema';
import { logger } from '../../lib/logger';
import { gardnersFileQueue } from '../../queue';
import { parseGardnersCsv } from './csv.service';
import { FetchFeedConfig, gardnersFetcher } from './fetcher.service';
import { gardnersUpserts } from './upserts.service';
import { isValidIsbn13 } from './parsing-utils';

// data.gardners.com's market restrictions file — full daily replace, no
// header/trailer. Verified live: ONE region per row (`ISBN13,Y|N,REGION`),
// NOT the comma-list shape shown in Gardners' own I17 spec examples — the
// same ISBN repeats across multiple rows, once per region.
export const gardnersMarketRestrictionsFeedConfig: FetchFeedConfig = {
  feed: 'market_restrictions',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/mkres');
    return files.filter((f) => f.filename === 'RESTRICT.CSV');
  },
};

export const MARKET_RESTRICTIONS_COLUMNS = ['isbn13', 'flag', 'regionCode'];

export function mapRestrictionRow(
  record: Record<string, string>,
  ctx: { sourceFileKey: string; syncedAt: Date },
): NewGardnersMarketRestriction | null {
  const isbn13 = record.isbn13?.trim();
  const flag = record.flag?.trim();
  const regionCode = record.regionCode?.trim();
  if (!isValidIsbn13(isbn13) || (flag !== 'Y' && flag !== 'N') || !regionCode) return null;

  return {
    isbn13,
    flag,
    regionCode,
    sourceFileKey: ctx.sourceFileKey,
    syncedAt: ctx.syncedAt,
  };
}

async function syncRestrictions(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersMarketRestrictionsFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners market restrictions files found');
    return;
  }

  for (const file of files) {
    const logId = await gardnersFetcher.claimFile('market_restrictions', file);

    await gardnersFileQueue.add(`gardners-market-restrictions-${file.filename}`, {
      feed: 'market_restrictions',
      connection: 'genericSftp',
      file,
      logId,
    });

    logger.info('Enqueued Gardners market restrictions file', { filename: file.filename, logId });
  }
}

// REGIONS.CSV is tiny (~50 rows) — synced directly in one shot, no
// file/chunk queue needed. Wholesale upsert, no mark-and-sweep (a region
// code is never expected to disappear).
const regionsFeedConfig: FetchFeedConfig = {
  feed: 'regions',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/mkres');
    return files.filter((f) => f.filename === 'REGIONS.CSV');
  },
};

async function syncRegions(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(regionsFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners regions file found');
    return;
  }

  const [file] = files;
  const logId = await gardnersFetcher.claimFile('regions', file);

  try {
    const stream = await gardnersFetcher.openStreamForFile(regionsFeedConfig.connection, file);
    const syncedAt = file.modifiedAt ?? new Date();

    const regions: NewGardnersRegion[] = [];
    const generator = parseGardnersCsv<NewGardnersRegion>(stream, {
      framed: false,
      columns: ['code', 'name'],
      mapRow: (record) => {
        const code = record.code?.trim();
        const name = record.name?.trim();
        if (!code || !name) return null;
        return { code, name, syncedAt };
      },
    });

    for await (const batch of generator) {
      regions.push(...batch);
    }

    await gardnersUpserts.upsertRegions(regions);
    await gardnersFetcher.markFetchCompleted(logId, { rowCount: regions.length });
    logger.info('Synced Gardners regions', { count: regions.length });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    await gardnersFetcher.markFetchFailed(logId, error);
    throw error;
  }
}

export const gardnersMarketRestrictionsService = { syncRestrictions, syncRegions };
