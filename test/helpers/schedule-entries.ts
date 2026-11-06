import { DataSource } from "typeorm";

/** One `schedule_entries` row as the e2e specs seed it. */
export interface ScheduleSeed {
  parkId: string;
  /** NULL for a park-level row (the park's opening hours). */
  attractionId: string | null;
  date: string;
  scheduleType: string;
  /** Tells otherwise identical rows apart after the statement under test. */
  description: string;
  /** Phase 1 of the dedup keeps the newest row, so the order has to be set. */
  updatedAt?: string;
}

/**
 * Inserts schedule rows with raw SQL, the way the statements under test read
 * them: no entity hooks, `updatedAt` exactly as given.
 */
export async function insertScheduleEntries(
  dataSource: DataSource,
  rows: ScheduleSeed[],
): Promise<void> {
  for (const row of rows) {
    await dataSource.query(
      `INSERT INTO schedule_entries
         (id, "parkId", "attractionId", date, "scheduleType", description, "createdAt", "updatedAt")
       VALUES (gen_random_uuid(), $1::uuid, $2::uuid, $3::date, $4, $5,
               NOW(), COALESCE($6::timestamptz, NOW()))`,
      [
        row.parkId,
        row.attractionId,
        row.date,
        row.scheduleType,
        row.description,
        row.updatedAt ?? null,
      ],
    );
  }
}

/**
 * A park's schedule as sorted `date type ride description` lines, the ride
 * written `park` for a park-level row. One readable diff instead of a count:
 * a count would stay green if the wrong one of two rows survived.
 */
export async function scheduleOf(
  dataSource: DataSource,
  parkId: string,
): Promise<string[]> {
  const rows: Array<{ line: string }> = await dataSource.query(
    `SELECT concat_ws(' ', to_char(date, 'YYYY-MM-DD'), "scheduleType",
                      COALESCE("attractionId"::text, 'park'), description) AS line
       FROM schedule_entries WHERE "parkId" = $1::uuid`,
    [parkId],
  );
  return rows.map((r) => r.line).sort();
}
