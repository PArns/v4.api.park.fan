import type { DataSource } from "typeorm";
import {
  DEFAULT_SEARCH_TYPE,
  type GscSearchType,
} from "./search-console.constants";

/** An import of one day, as `search_console_batches` holds it. */
export interface CurrentBatch {
  id: number;
  format: number;
  rows: number;
  clicks: number;
  impressions: number;
  positionSum: number;
}

/**
 * The newest batch of every day in the range for one type, by day: the day's
 * data. A day imported again is a newer batch, and nothing reads an older one.
 */
export async function currentBatches(
  db: Pick<DataSource, "query">,
  from: string,
  to: string,
  type: GscSearchType = DEFAULT_SEARCH_TYPE,
): Promise<Map<string, CurrentBatch>> {
  const rows: Array<{
    id: number;
    format: number;
    day: string;
    rows: number;
    clicks: number;
    impressions: number;
    position_sum: number;
  }> = await db.query(
    `SELECT DISTINCT ON (day) id, format, day::text AS day, rows, clicks, impressions, position_sum
       FROM search_console_batches
      WHERE day BETWEEN $1::date AND $2::date AND search_type = $3
      ORDER BY day, id DESC`,
    [from, to, type],
  );
  return new Map(
    rows.map((row) => [
      row.day,
      {
        id: row.id,
        format: row.format,
        rows: row.rows,
        clicks: row.clicks,
        impressions: row.impressions,
        positionSum: row.position_sum,
      },
    ]),
  );
}

/**
 * The newest day whose current batch has rows in the layout the reports
 * read: the day a rolling window ends on. Null before the first import.
 */
export async function lastDataDay(
  db: Pick<DataSource, "query">,
  format: number,
  type: GscSearchType = DEFAULT_SEARCH_TYPE,
): Promise<string | null> {
  const [row]: Array<{ day: string | null }> = await db.query(
    `SELECT MAX(day)::text AS day
       FROM (SELECT DISTINCT ON (day) day, rows, format
               FROM search_console_batches WHERE search_type = $2
              ORDER BY day, id DESC) b
      WHERE rows > 0 AND format >= $1`,
    [format, type],
  );
  return row?.day ?? null;
}
