import { createReadStream } from 'fs';
import { unlink } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import { PassThrough } from 'stream';
import { pipeline } from 'stream/promises';
import * as unzipper from 'unzipper';
import { config } from '../../config';
import { logger } from '../../lib/logger';
import { storageService } from '../storage.service';
import { FetchFeedConfig, gardnersFetcher, RemoteFileDescriptor } from './fetcher.service';

const DELTA_FILENAME_RE = /^GardnerspBookONIX3_wk(\d+)_Delta\.zip$/i;
const FULL_FILENAME_RE = /^GardnerspBookONIX3_wk(\d+)_Full\.zip$/i;

// data.gardners.com's genuine ONIX 3.1 feed — verified live to contain a
// single XML entry per zip. The legacy proprietary TAG/XML format also
// living under /Biblio (GARDBIB*, WeeklyExtract*) is NOT used; the existing
// onix_ingester parser already expects real ONIX 3.1, which is what these
// files contain.
export const gardnersBiblioDeltaFeedConfig: FetchFeedConfig = {
  feed: 'biblio_delta',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Biblio/Delta/Onix3');
    return files.filter((f) => DELTA_FILENAME_RE.test(f.filename));
  },
};

// The full catalogue reload (~1.7GB zipped) — not on an automatic cron (see
// cron/index.ts), triggered manually/rarely for an initial load or re-sync.
export const gardnersBiblioFullFeedConfig: FetchFeedConfig = {
  feed: 'biblio_full',
  connection: 'genericSftp',
  listRemoteFiles: async (client) => {
    const files = await client.list('/Biblio/ONIX');
    return files.filter((f) => FULL_FILENAME_RE.test(f.filename));
  },
};

function r2KeyFor(feed: 'biblio_delta' | 'biblio_full', file: RemoteFileDescriptor): string {
  const week = /wk(\d+)_/.exec(file.filename)?.[1] ?? 'unknown';
  const suffix = feed === 'biblio_full' ? 'full' : 'delta';
  // Distinct key naming purely for operator legibility in R2/Bull Board —
  // the R2 poll cron that picks this up doesn't care about the source,
  // only that it's a `.xml` file under R2_ONIX_PREFIX.
  return `${config.r2.onixPrefix}gardners-biblio-wk${week}-${suffix}.xml`;
}

/**
 * Downloads a remote ONIX zip to a local temp file (fastGet — see
 * downloadToLocalFile's doc comment for why plain streaming isn't used for
 * transfers this size), then unzips its single XML entry straight into R2,
 * without ever buffering the (up to ~1.7GB uncompressed) file in memory.
 * Deliberately does NOT call triggerIngestion() — the existing R2_POLL_CRON
 * cron picks up the landed .xml file on its own schedule via the unmodified
 * ingestion.service.ts/file.worker.ts/chunk.worker.ts path, exactly as it
 * would for a manually-uploaded file.
 */
async function fetchAndLandInR2(
  cfg: FetchFeedConfig,
  file: RemoteFileDescriptor,
): Promise<void> {
  const r2Key = r2KeyFor(cfg.feed as 'biblio_delta' | 'biblio_full', file);
  const logId = await gardnersFetcher.claimFile(cfg.feed, file);
  const localZipPath = join(tmpdir(), `gardners-${cfg.feed}-${logId}.zip`);

  try {
    await gardnersFetcher.downloadToLocalFile(cfg.connection, file, localZipPath);

    const zipStream = createReadStream(localZipPath);
    // unzipper.ParseOne() returns a Duplex that @aws-sdk/lib-storage's Body
    // type guard doesn't recognize as valid stream input — pipe through a
    // plain PassThrough to normalize it into an unambiguous Readable.
    const xmlEntry = new PassThrough();

    // Plain `.pipe()` chains don't propagate upstream errors. stream/promises'
    // pipeline() destroys the whole chain and rejects on any error. It races
    // against the upload reading from the same xmlEntry, since both must run
    // concurrently for a stream pipe.
    await Promise.all([
      pipeline(zipStream, unzipper.ParseOne(), xmlEntry),
      // Uncompressed size isn't known upfront (only the zip's compressed
      // size is), so this must go through the multipart uploader rather
      // than a plain Content-Length PUT.
      storageService.uploadStreamMultipart(r2Key, xmlEntry, 'application/xml'),
    ]);

    await gardnersFetcher.markFetchCompleted(logId, { r2Key });
    logger.info('Landed Gardners Biblio ONIX file in R2', {
      feed: cfg.feed,
      filename: file.filename,
      r2Key,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    await gardnersFetcher.markFetchFailed(logId, error);
    throw error;
  } finally {
    await unlink(localZipPath).catch(() => undefined);
  }
}

async function syncDelta(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersBiblioDeltaFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Biblio delta files found');
    return;
  }

  for (const file of files) {
    await fetchAndLandInR2(gardnersBiblioDeltaFeedConfig, file);
  }
}

async function syncFull(): Promise<void> {
  const files = await gardnersFetcher.listUnprocessedFiles(gardnersBiblioFullFeedConfig);

  if (files.length === 0) {
    logger.info('No new Gardners Biblio full files found');
    return;
  }

  // Only the single latest full file, even if more than one is somehow
  // unprocessed — this is an expensive, rarely-run operation, not something
  // to fan out across multiple ~1.7GB downloads in one call.
  const [file] = files;
  await fetchAndLandInR2(gardnersBiblioFullFeedConfig, file);
}

export const gardnersBiblioService = { syncDelta, syncFull };
