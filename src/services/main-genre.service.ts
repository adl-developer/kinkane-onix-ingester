import { sql } from 'drizzle-orm';
import { db } from '../db';
import { logger } from '../lib/logger';

/**
 * Keeps books.main_genre_id in step with a book's subjects and genres.
 *
 * Same rules as the server's one-off backfill (server/scripts/backfill-main-genre.ts):
 *
 *   1. The publisher's nomination — the scheme-93 subject flagged
 *      is_main_subject, matched to the genre sharing its subject_code.
 *   2. Otherwise, a book with exactly one genre has that one.
 *   3. Otherwise NULL. A book with several genres and no nomination is not
 *      guessed at.
 *
 * Unlike the backfill this also clears: a delta that drops a book's
 * nomination must not leave the old main genre behind. Call it after a book's
 * subjects and genres have been rewritten. The `IS DISTINCT FROM` guard means
 * books whose answer did not change are not written.
 *
 * The column belongs to the server's migration 0060, not to this service's
 * schema. If the ingester is deployed before that migration, this logs and
 * skips instead of failing the chunk, so ingestion keeps working and the
 * backfill fills the gap later.
 */
async function refreshForBooks(bookIds: number[]): Promise<number> {
  if (bookIds.length === 0) return 0;

  const ids = sql.join(bookIds.map((id) => sql`${id}`), sql`, `);
  try {
    const result = await db.execute(sql`
      UPDATE books b
      SET main_genre_id = r.genre_id
      FROM (
        SELECT t.id AS book_id,
               COALESCE(
                 (SELECT g.id
                  FROM book_subjects bs
                  JOIN genres g ON g.subject_code = bs.subject_code AND g.scheme_identifier = '93'
                  WHERE bs.book_id = t.id AND bs.is_main_subject AND bs.scheme_identifier = '93'
                  ORDER BY g.id
                  LIMIT 1),
                 (SELECT min(bg.genre_id)
                  FROM book_genres bg
                  WHERE bg.book_id = t.id
                  HAVING count(*) = 1)
               ) AS genre_id
        FROM books t
        WHERE t.id IN (${ids})
      ) r
      WHERE b.id = r.book_id
        AND b.main_genre_id IS DISTINCT FROM r.genre_id
    `);
    return (result as unknown as { count?: number }).count ?? 0;
  } catch (err) {
    // 42703 undefined_column: migration 0060 has not reached this database.
    if ((err as { code?: string }).code === '42703') {
      logger.warn('books.main_genre_id does not exist yet; skipping main genre refresh', { books: bookIds.length });
      return 0;
    }
    throw err;
  }
}

export const mainGenreService = { refreshForBooks };
