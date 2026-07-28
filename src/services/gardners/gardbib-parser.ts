import { Readable } from 'stream';
import { createInterface } from 'readline';

/**
 * Parser for Gardners' proprietary TAG format (`/Biblio/GARDBIB.zip` →
 * GARDBIB.TXT, ~1.98M records). This is deliberately NOT the ONIX 3.1 parser
 * in parser.service.ts — the two feeds are different files in different
 * formats with different coverage:
 *
 *   ONIX 3.1 (/Biblio/ONIX/*_Full.zip) — 1,001,494 products, rich records.
 *   GARDBIB (/Biblio/GARDBIB.zip)      — 1,980,917 records, thinner records,
 *                                        but covers 97.5% of the ISBNs that
 *                                        actually appear in the stock feed.
 *
 * Verified live 2026-07-27: the ONIX full feed simply does not contain ~980K
 * of the titles Gardners stocks (e.g. 9781035906062, in stock with 31 units,
 * absent from the ONIX file entirely). GARDBIB is the only feed with
 * catalogue-wide coverage, which is why this parser exists at all.
 *
 * Format:
 *   **START                     <- file header
 *   IB 1035906066               <- two-char tag, space, value
 *   AU Ba, Mariama              <- repeatable tags appear multiple times
 *   TI So Long a Letter
 *   **                          <- record separator
 *   ...
 *
 * Lines are CRLF-terminated. (Stripping the \r matters more than it looks:
 * a `\r` left on the end of an ISBN silently turns every downstream join
 * into a zero-row result.)
 */

export interface GardbibContributor {
  role: string;
  personName: string;
  sequenceNumber: number;
}

export interface GardbibRecord {
  isbn13: string | null;
  title: string | null;
  subtitle: string | null;
  description: string | null;
  publisherName: string | null;
  publicationDate: string | null; // YYYY-MM-DD
  pageCount: number | null;
  heightMm: number | null;
  widthMm: number | null;
  thicknessMm: number | null;
  weightGr: number | null;
  productForm: string | null;
  contributors: GardbibContributor[];
  subjectCodes: string[];
}

// GARDBIB's BI field is free text, not an ONIX code, but `books.product_form`
// holds ONIX List 150 codes written by the ONIX pipeline. Mapping the handful
// of forms that cover ~99% of the file keeps the column consistent across
// both sources; anything unmapped is left null rather than guessed at.
const PRODUCT_FORM_BY_BINDING: Record<string, string> = {
  'paperback / softback': 'BC',
  paperback: 'BC',
  hardback: 'BB',
  'board book': 'BH',
  book: 'BA',
  'spiral bound': 'BE',
  'leather / fine binding': 'BG',
  pamphlet: 'BF',
  'loose-leaf': 'BD',
};

const MAX_TITLE = 2000;
const MAX_PUBLISHER = 500;
const MAX_PERSON_NAME = 500;
const MAX_SUBJECT_CODE = 50;

function truncate(value: string, max: number): string {
  return value.length > max ? value.slice(0, max) : value;
}

function parseNumber(raw: string | undefined): number | null {
  if (!raw) return null;
  const n = Number(raw.replace(/[^0-9.]/g, ''));
  return Number.isFinite(n) ? n : null;
}

// PD is YYYYMMDD. Gardners uses partial dates for unconfirmed pub dates
// (e.g. 20260000 for "sometime in 2026"), which Postgres rejects outright —
// snap those to the first of the month/year rather than dropping the record.
function parsePubDate(raw: string | undefined): string | null {
  if (!raw || !/^\d{8}$/.test(raw)) return null;
  const year = raw.slice(0, 4);
  const month = raw.slice(4, 6);
  const day = raw.slice(6, 8);
  if (year === '0000') return null;
  const safeMonth = month === '00' ? '01' : month;
  const safeDay = day === '00' ? '01' : day;
  const date = new Date(`${year}-${safeMonth}-${safeDay}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  return `${year}-${safeMonth}-${safeDay}`;
}

// DI is "height x width x thickness" in mm. Verified 2026-07-27 across the
// 29,751 sampled ISBNs that appear in both this feed and the ONIX one:
// 29,731 matched the ONIX record's height/width in this order, 2 were
// transposed, 18 disagreed outright. Worth stating explicitly because ~37%
// of the file has a second value larger than the first, which looks like a
// transposition bug until you check it against ONIX and find both feeds
// agree — those really are wider-than-tall products.
function parseDimensions(raw: string | undefined): {
  heightMm: number | null;
  widthMm: number | null;
  thicknessMm: number | null;
} {
  if (!raw) return { heightMm: null, widthMm: null, thicknessMm: null };
  const parts = raw.split('x').map((p) => parseNumber(p.trim()));
  return {
    heightMm: parts[0] ?? null,
    widthMm: parts[1] ?? null,
    thicknessMm: parts[2] ?? null,
  };
}

function buildRecord(fields: Map<string, string[]>): GardbibRecord {
  const first = (tag: string): string | undefined => fields.get(tag)?.[0];
  const all = (tag: string): string[] => fields.get(tag) ?? [];

  const dims = parseDimensions(first('DI'));
  const binding = first('BI')?.toLowerCase().trim();
  const title = first('TI');
  const subtitle = first('ST');
  const publisher = first('PU');

  const contributors: GardbibContributor[] = [];
  let sequenceNumber = 1;
  // A01 author / B01 editor / B06 translator — the same ONIX List 17 codes
  // the ONIX pipeline writes, so consumers don't have to care which feed a
  // contributor row came from.
  for (const [tag, role] of [
    ['AU', 'A01'],
    ['ED', 'B01'],
    ['TR', 'B06'],
  ] as const) {
    for (const name of all(tag)) {
      if (!name.trim()) continue;
      contributors.push({
        role,
        personName: truncate(name.trim(), MAX_PERSON_NAME),
        sequenceNumber: sequenceNumber++,
      });
    }
  }

  return {
    isbn13: first('I3')?.trim() ?? null,
    title: title ? truncate(title.trim(), MAX_TITLE) : null,
    subtitle: subtitle ? truncate(subtitle.trim(), MAX_TITLE) : null,
    description: first('DE')?.trim() ?? null,
    publisherName: publisher ? truncate(publisher.trim(), MAX_PUBLISHER) : null,
    publicationDate: parsePubDate(first('PD')),
    pageCount: parseNumber(first('NP')),
    heightMm: dims.heightMm,
    widthMm: dims.widthMm,
    thicknessMm: dims.thicknessMm,
    weightGr: parseNumber(first('WE')),
    productForm: binding ? (PRODUCT_FORM_BY_BINDING[binding] ?? null) : null,
    contributors,
    subjectCodes: all('BC')
      .map((c) => c.trim())
      .filter(Boolean)
      .map((c) => truncate(c, MAX_SUBJECT_CODE)),
  };
}

const TAG_LINE_RE = /^([A-Z0-9]{2}) (.*)$/;

/**
 * Streams records out of GARDBIB.TXT. Memory stays flat regardless of file
 * size — only one record's fields are held at a time.
 */
export async function* parseGardbibStream(input: Readable): AsyncGenerator<GardbibRecord> {
  const lines = createInterface({ input, crlfDelay: Infinity });

  let fields = new Map<string, string[]>();
  let hasFields = false;
  let lastTag: string | null = null;

  for await (const rawLine of lines) {
    const line = rawLine.replace(/\r$/, '');

    if (line.startsWith('**')) {
      if (hasFields) yield buildRecord(fields);
      fields = new Map();
      hasFields = false;
      lastTag = null;
      continue;
    }

    const match = TAG_LINE_RE.exec(line);
    if (!match) {
      // Not a tag line — a description (DE) that contains a hard line break
      // continues onto the next line. Append rather than drop it; a bare
      // blank line before a record separator is ignored.
      if (lastTag && line.trim()) {
        const values = fields.get(lastTag)!;
        values[values.length - 1] += ` ${line.trim()}`;
      }
      continue;
    }

    const [, tag, value] = match;
    const existing = fields.get(tag);
    if (existing) existing.push(value);
    else fields.set(tag, [value]);
    hasFields = true;
    lastTag = tag;
  }

  if (hasFields) yield buildRecord(fields);
}
