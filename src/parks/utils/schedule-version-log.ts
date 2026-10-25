import { EntityManager } from "typeorm";

/**
 * Append-only version log of the park-level day (`schedule_day_versions`,
 * PAR-1061, docs/architecture/covariate-source.md §3.2).
 *
 * The day state is derived from the park-level `schedule_entries` rows and
 * compared with the open version; only a change writes (close the open one,
 * insert the new one). A day that has no rows any more and had a version gets a
 * `NONE` version: a retraction is a version too, nothing is deleted.
 */

export type ScheduleVersionSource =
  "themeparks" | "wartezeiten" | "live_fallback" | "gap_fill";

export interface ScheduleDayState {
  day: string;
  state: "OPERATING" | "CLOSED" | "UNKNOWN" | "NONE";
  open_utc: string | null;
  close_utc: string | null;
  n_windows: number;
  ticketed_event: boolean;
  private_event: boolean;
  extra_hours: boolean;
  extra_open_utc: string | null;
  extra_close_utc: string | null;
}

/** A derived state plus, for a park's first versions, when it was last changed. */
export type DerivedDay = ScheduleDayState & { last_updated?: string | null };

const blank = (day: string): ScheduleDayState => ({
  day,
  state: "NONE",
  open_utc: null,
  close_utc: null,
  n_windows: 0,
  ticketed_event: false,
  private_event: false,
  extra_hours: false,
  extra_open_utc: null,
  extra_close_utc: null,
});

const FIELDS = [
  "state",
  "open_utc",
  "close_utc",
  "n_windows",
  "ticketed_event",
  "private_event",
  "extra_hours",
  "extra_open_utc",
  "extra_close_utc",
] as const;

/** pg hands timestamptz back as Date; compare instants, not representations. */
const norm = (v: unknown): unknown =>
  v instanceof Date
    ? v.getTime()
    : typeof v === "string" && /^\d{4}-\d{2}-\d{2}[T ]/.test(v)
      ? Date.parse(v)
      : v;

/**
 * The days whose derived state differs from the open version. A missing open
 * version and a missing derived day both read as the blank `NONE` day, so an
 * unpublished day writes nothing and a retracted one writes `NONE`.
 */
export function diffScheduleDays(
  open: ScheduleDayState[],
  current: DerivedDay[],
): { changed: DerivedDay[]; closeDays: string[] } {
  const openByDay = new Map(open.map((o) => [o.day, o]));
  const curByDay = new Map(current.map((c) => [c.day, c]));
  const changed: DerivedDay[] = [];
  const closeDays: string[] = [];
  for (const day of new Set([...openByDay.keys(), ...curByDay.keys()])) {
    const prev = openByDay.get(day) ?? blank(day);
    const cur = curByDay.get(day) ?? blank(day);
    if (FIELDS.every((f) => norm(prev[f]) === norm(cur[f]))) continue;
    if (openByDay.has(day)) closeDays.push(day);
    changed.push(cur);
  }
  return { changed, closeDays };
}

/** The park-level day as `fetch_windows` / `fetch_operating_windows` read it. */
const DERIVE_SQL = `
  SELECT se.date::text AS day,
         CASE WHEN bool_or(se."scheduleType" = 'OPERATING') THEN 'OPERATING'
              WHEN bool_or(se."scheduleType" = 'CLOSED') THEN 'CLOSED'
              WHEN bool_or(se."scheduleType" = 'UNKNOWN') THEN 'UNKNOWN'
              ELSE 'NONE' END AS state,
         min(se."openingTime") FILTER (WHERE se."scheduleType" = 'OPERATING') AS open_utc,
         max(se."closingTime") FILTER (WHERE se."scheduleType" = 'OPERATING') AS close_utc,
         (count(*) FILTER (WHERE se."scheduleType" = 'OPERATING'))::int AS n_windows,
         bool_or(se."scheduleType" = 'TICKETED_EVENT') AS ticketed_event,
         bool_or(se."scheduleType" = 'PRIVATE_EVENT') AS private_event,
         bool_or(se."scheduleType" = 'EXTRA_HOURS') AS extra_hours,
         min(se."openingTime") FILTER (WHERE se."scheduleType" = 'EXTRA_HOURS') AS extra_open_utc,
         max(se."closingTime") FILTER (WHERE se."scheduleType" = 'EXTRA_HOURS') AS extra_close_utc,
         max(se."updatedAt") AS last_updated
    FROM schedule_entries se
   WHERE se."parkId" = $1 AND se."attractionId" IS NULL
   GROUP BY se.date`;

/**
 * Log the park's current schedule state. Call it after the LAST writer of a
 * sync has finished with the park (save + gap fill + guards), never in
 * between, or the log fills with interim states no reader saw.
 *
 * A park's first call is the backfill: its versions take `known_from =
 * max(updatedAt)` of the day's rows and `asof_exact = false`, the conservative
 * proxy the design names (it can hide a window that was known earlier, never
 * show one that was not). Returns the number of versions written.
 */
export async function recordScheduleDayVersions(
  manager: EntityManager,
  parkId: string,
  source: ScheduleVersionSource,
): Promise<number> {
  return manager.transaction(async (tx) => {
    // One writer per park at a time, or two syncs both insert an open version.
    await tx.query(
      `SELECT pg_advisory_xact_lock(hashtext('schedule_day_versions:' || $1))`,
      [parkId],
    );
    const open: ScheduleDayState[] = await tx.query(
      `SELECT day::text AS day, state, open_utc, close_utc, n_windows,
              ticketed_event, private_event, extra_hours, extra_open_utc, extra_close_utc
         FROM schedule_day_versions
        WHERE park_id = $1 AND known_to IS NULL`,
      [parkId],
    );
    const firstLog =
      open.length === 0 &&
      (
        await tx.query(
          `SELECT 1 FROM schedule_day_versions WHERE park_id = $1 LIMIT 1`,
          [parkId],
        )
      ).length === 0;
    const current: DerivedDay[] = await tx.query(DERIVE_SQL, [parkId]);
    const { changed, closeDays } = diffScheduleDays(open, current);
    if (changed.length === 0) return 0;

    if (closeDays.length > 0) {
      await tx.query(
        `UPDATE schedule_day_versions SET known_to = now()
          WHERE park_id = $1 AND known_to IS NULL AND day = ANY($2::date[])`,
        [parkId, closeDays],
      );
    }
    await tx.query(
      `INSERT INTO schedule_day_versions
         (park_id, day, known_from, known_to, state, open_utc, close_utc, n_windows,
          ticketed_event, private_event, extra_hours, extra_open_utc, extra_close_utc,
          source, asof_exact)
       SELECT $1, r.day, CASE WHEN $3 THEN coalesce(r.last_updated, now()) ELSE now() END,
              NULL, r.state, r.open_utc, r.close_utc, r.n_windows,
              r.ticketed_event, r.private_event, r.extra_hours, r.extra_open_utc, r.extra_close_utc,
              CASE WHEN $3 THEN 'backfill' ELSE $4 END, NOT $3
         FROM jsonb_to_recordset($2::jsonb) AS r(
              day date, state text, open_utc timestamptz, close_utc timestamptz,
              n_windows smallint, ticketed_event boolean, private_event boolean,
              extra_hours boolean, extra_open_utc timestamptz, extra_close_utc timestamptz,
              last_updated timestamptz)`,
      [parkId, JSON.stringify(changed), firstLog, source],
    );
    return changed.length;
  });
}
