import { Readable } from 'stream';
import { parse } from 'csv-parse';

export interface ParseGardnersCsvOptions<T> {
  // Framed feeds (currently only the Inventory/.STK files) are wrapped in a
  // "HEADER,START" line, a real column-name header row, data rows, and a
  // trailing "TRAILER,<count>" row. Unframed feeds (Promotions, Firmsale,
  // isbn-slips, mkres) have no header at all — pass `columns` instead so
  // rows can still be mapped by name.
  framed: boolean;
  columns?: string[];
  // Return null to skip a row (e.g. an unparseable or non-ISBN row) without
  // failing the whole file.
  mapRow: (record: Record<string, string>) => T | null;
}

export interface ParseGardnersCsvSummary {
  totalRows: number;
  skippedRows: number;
  // Only set for framed feeds — the file's own claimed row count, checked
  // against totalRows as a sanity check (mismatches are logged by the
  // caller, not treated as fatal — a single off-by-one shouldn't block
  // ingestion of an otherwise-good file).
  trailerCount: number | null;
}

/**
 * Streams a Gardners CSV/TXT feed and yields mapped rows in batches. Mirrors
 * parser.service.ts's parseOnixStream shape (async generator yielding
 * batches) so callers can plug into the same chunk-queue pattern used for
 * ONIX files. Unlike the SAX parser, csv-parse's Transform stream is itself
 * async-iterable, so backpressure comes for free from `for await` semantics
 * — no manual pending-queue bookkeeping needed here.
 *
 * The generator's return value carries the row-count summary; access it via
 * manual `.next()` iteration rather than a `for await` loop if you need it.
 */
export async function* parseGardnersCsv<T>(
  stream: Readable,
  options: ParseGardnersCsvOptions<T>,
  batchSize = 1000,
): AsyncGenerator<T[], ParseGardnersCsvSummary> {
  const parser = stream.pipe(
    parse({ relax_column_count: true, skip_empty_lines: true, trim: true }),
  );

  let columns = options.framed ? undefined : options.columns;
  let sawFramingStart = !options.framed;
  let trailerCount: number | null = null;
  let totalRows = 0;
  let skippedRows = 0;
  let batch: T[] = [];

  for await (const rawRow of parser as AsyncIterable<string[]>) {
    // Skip the leading "**START"-style "HEADER,START" marker line.
    if (options.framed && !sawFramingStart) {
      sawFramingStart = true;
      continue;
    }

    // Next line after the marker is the real column-name header.
    if (options.framed && !columns) {
      columns = rawRow;
      continue;
    }

    // Trailing "TRAILER,<count>" row — not a data row.
    if (rawRow[0]?.trim() === 'TRAILER') {
      trailerCount = Number(rawRow[1]?.trim());
      continue;
    }

    const record: Record<string, string> = {};
    for (let i = 0; i < (columns?.length ?? 0); i++) {
      record[columns![i]] = rawRow[i] ?? '';
    }

    const mapped = options.mapRow(record);
    if (mapped === null) {
      skippedRows++;
      continue;
    }

    totalRows++;
    batch.push(mapped);
    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  if (batch.length > 0) {
    yield batch;
  }

  return { totalRows, skippedRows, trailerCount };
}
