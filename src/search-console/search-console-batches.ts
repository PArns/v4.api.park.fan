import type { DataSource } from "typeorm";

/** An import of one day, as `search_console_batches` holds it. */
export interface CurrentBatch {
  id: number;
  rows: number;
  clicks: number;
  impressions: number;
  positionSum: number;
}

/**
 * The newest batch of every day in the range, by day: the day's data. A day
 * imported again is a newer batch, and nothing reads an older one.
 */
export async function currentBatches(
  db: Pick<DataSource, "query">,
  from: string,
  to: string,
): Promise<Map<string, CurrentBatch>> {
  const rows: Array<{
    id: number;
    day: string;
    rows: number;
    clicks: number;
    impressions: number;
    position_sum: number;
  }> = await db.query(
    `SELECT DISTINCT ON (day) id, day::text AS day, rows, clicks, impressions, position_sum
       FROM search_console_batches
      WHERE day BETWEEN $1::date AND $2::date
      ORDER BY day, id DESC`,
    [from, to],
  );
  return new Map(
    rows.map((row) => [
      row.day,
      {
        id: row.id,
        rows: row.rows,
        clicks: row.clicks,
        impressions: row.impressions,
        positionSum: row.position_sum,
      },
    ]),
  );
}
