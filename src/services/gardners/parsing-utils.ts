// Shared row-mapping helpers used across all Gardners CSV feed mappers.

export function pick(record: Record<string, string>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    if (record[key] !== undefined) return record[key];
  }
  return undefined;
}

// Real EAN-13 book identifiers start with 978/979 — every Gardners feed
// carries some rows that aren't real ISBNs (internal SKU codes for
// non-book sundries, blank fields, etc.), so every mapper validates with
// this before accepting a row.
export function isValidIsbn13(value: string | undefined): value is string {
  if (!value) return false;
  return /^(978|979)\d{10}$/.test(value.trim());
}

// The Inventory feed's date format is DD/MM/YYYY, with '00' for an unknown
// day — treated the same as "no date" since a fictitious day-of-month isn't
// a valid DATE value.
export function parseDdMmYyyy(value: string | undefined): string | null {
  if (!value) return null;
  const match = /^(\d{2})\/(\d{2})\/(\d{4})$/.exec(value.trim());
  if (!match) return null;
  const [, dd, mm, yyyy] = match;
  if (dd === '00') return null;
  return `${yyyy}-${mm}-${dd}`;
}

// The Promotions feed's finish date uses a 2-digit year (DD/MM/YY per the
// I17 spec's own example, "01/01/20") — all Gardners data is recent enough
// that treating YY as 20YY is unambiguous.
export function parseDdMmYy(value: string | undefined): string | null {
  if (!value) return null;
  const match = /^(\d{2})\/(\d{2})\/(\d{2})$/.exec(value.trim());
  if (!match) return null;
  const [, dd, mm, yy] = match;
  return `20${yy}-${mm}-${dd}`;
}
