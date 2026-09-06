import { RECONCILIATION_SOURCE } from "./source-absent-status.util";

/**
 * Which `queue_data` rows an outage is allowed to be built from, in SQL and in
 * TypeScript, from one description.
 *
 * `queue_data` is a change log, not a sample: a row exists only when something
 * changed, when the park-local date rolled over, or when the hourly heartbeat
 * fired. Three of the writers put rows in that table that look like readings
 * and are not, and each of them, left alone, invents downtime:
 *
 * - **Reverse reconciliation** (`wait-times.processor`) writes `CLOSED` with
 *   `data_source = 'system-reconciliation'` for any ride no source has mentioned
 *   in 24 hours. That row records that our data stopped arriving. Filtering it
 *   out would be worse than reading it wrong: a ride that vanishes from every
 *   feed for twelve days would come back as one twelve-day outage, because the
 *   `DOWN` before the silence and the `DOWN` after it would join across it. So
 *   the row is kept and it **ends** the run.
 * - **The hourly heartbeat** copies the previous row's status *and* its
 *   `data_source`, so a carried `DOWN` is byte-identical to an observed one
 *   apart from `lastUpdated == timestamp`. It may confirm a state; it may never
 *   create a transition, and past a few carries in a row it stops being
 *   evidence at all (see `MAX_CARRIED_HEARTBEATS`).
 * - **The heartbeat's own invention**: for a ride with no STANDBY row in two
 *   days it writes `status = CLOSED, waitTime = 0` under
 *   `data_source = 'system-heartbeat'`. That is our bookkeeping, not a recovery,
 *   and it ends the run for the same reason reconciliation does.
 *
 * ## Two halves, changed together or not at all
 *
 * The SQL half is what a query over the hypertable uses; the TypeScript half is
 * what a test and any in-memory pass use. They are hand-maintained twins in the
 * pattern `season-window.sql.ts` and `scheduleRowSpeaksForToday()` already set,
 * and `outage-rows.sql.spec.ts` asserts that the two agree over every
 * combination of status and source.
 */

/** The only queue type an outage is read from. */
export const OUTAGE_QUEUE_TYPE = "STANDBY";

/** `dataSource` the hourly heartbeat stamps when it has nothing to carry. */
export const HEARTBEAT_SOURCE = "system-heartbeat";

/**
 * How far back the live "since when" question is asked.
 *
 * Seven days is the same bound the reconstruction uses to call a run a works
 * period rather than an outage. Past it the answer stops being a sentence a
 * visitor can act on, and a run still open at the edge is reported without a
 * start rather than with a made-up one.
 */
export const TRAILING_OUTAGE_LOOKBACK_DAYS = 7;

/**
 * How many consecutive carried heartbeat rows still count as the state holding.
 *
 * A heartbeat confirms a state; it does not prove it indefinitely. Four carries
 * is about four hours, long enough that a real recovery (which is a real delta
 * write) lands inside. Past it the state is unconfirmed and the interval is
 * right-censored rather than extended. Not used by the live query, which asks
 * only for a start, and load-bearing for the duration work that follows.
 */
export const MAX_CARRIED_HEARTBEATS = 4;

/** The sources whose rows are our own bookkeeping rather than an observation. */
export const SYNTHETIC_SOURCES = [
  RECONCILIATION_SOURCE,
  HEARTBEAT_SOURCE,
] as const;

/**
 * SQL: does this row end a `DOWN` run?
 *
 * True for anything that is not a `DOWN` reading, and for a `DOWN` that one of
 * our own writers put there. The caller supplies the alias its `queue_data` row
 * is bound to; nothing else is correlated.
 *
 * @param alias - Table alias the `queue_data` row carries in the caller's SQL.
 */
export function outageRunBreaks(alias: string): string {
  const synthetic = SYNTHETIC_SOURCES.map((source) => `'${source}'`).join(", ");
  // COALESCE rather than a bare IN: the column carries a default and is not
  // nullable on the entity, but a NULL here would make the whole expression
  // NULL, and a NULL passes neither `breaks` nor `NOT breaks` — the row would
  // drop out of the run AND out of the boundary, silently.
  return (
    `(${alias}.status <> 'DOWN'` +
    ` OR COALESCE(${alias}.data_source, '') IN (${synthetic}))`
  );
}

/** The TypeScript twin of {@link outageRunBreaks}. */
export function rowBreaksOutageRun(row: {
  status?: string | null;
  dataSource?: string | null;
}): boolean {
  if (row.status !== "DOWN") return true;
  return (SYNTHETIC_SOURCES as readonly string[]).includes(
    row.dataSource ?? "",
  );
}

/**
 * The start of the trailing `DOWN` run, per attraction, for a set of ids.
 *
 * Asked only for the ids that already read `DOWN` through the API's own status
 * chain, so this answers "since when", never "is it down".
 *
 * Shape notes that are easy to get wrong and were checked against the entities:
 *
 * - `queue_data."attractionId"` is **uuid** at DB level despite the
 *   `@Column({ type: "text" })` written beside the relation. `= ANY($1::uuid[])`,
 *   never a `::text` comparison. The damage report is in
 *   `plan-day.service.ts:857-869`: the cast raised
 *   `operator does not exist: text = uuid`, a catch swallowed it, and the set
 *   came back empty on every park, silently.
 * - **No cast on `timestamp` in the WHERE clause.** A half-open range on the raw
 *   column is what lets TimescaleDB prune chunks;
 *   `(qd.timestamp AT TIME ZONE tz)::date = …` hid the column from chunk
 *   exclusion and read all 254 chunks for nine rows.
 * - A run whose oldest row sits at the window edge has `startObserved = false`.
 *   Its start is older than the window and is not reported as the window's edge.
 * - An attraction whose newest row inside the window breaks the run returns no
 *   row at all. The caller renders nothing rather than a start it cannot place.
 */
export const TRAILING_OUTAGE_START_SQL = `
  WITH marked AS (
    SELECT qd."attractionId" AS aid,
           qd.timestamp      AS ts,
           ${outageRunBreaks("qd")} AS breaks
      FROM queue_data qd
     WHERE qd."attractionId" = ANY($1::uuid[])
       AND qd."queueType" = '${OUTAGE_QUEUE_TYPE}'
       AND qd.timestamp >= $2
       AND qd.timestamp <  $3
  ),
  bounds AS (
    SELECT aid,
           MAX(ts) FILTER (WHERE breaks) AS last_break
      FROM marked
     GROUP BY aid
  )
  SELECT m.aid                      AS "attractionId",
         MIN(m.ts)                  AS "startedAt",
         (b.last_break IS NOT NULL) AS "startObserved",
         COUNT(*)::int              AS "rowsInRun"
    FROM marked m
    JOIN bounds b ON b.aid = m.aid
   WHERE NOT m.breaks
     AND (b.last_break IS NULL OR m.ts > b.last_break)
   GROUP BY m.aid, b.last_break
`;
