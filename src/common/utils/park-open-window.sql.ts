/**
 * A park's operating windows as a disjoint union, in SQL.
 *
 * The question "was this park open at instant T" has no helper in this codebase
 * and could not be answered before this file existed. `isParkOpen()` and
 * `OPEN_PARKS_CTES` are hard-wired to NOW, `scheduleRowSpeaksForToday()` to
 * CURRENT_DATE ± 1, and attraction-level `schedule_entries` rows are never
 * written. Anything that normalises a ride's downtime by operating time needs
 * the general form, over history, for every park at once.
 *
 * Three things it has to do that a naive `JOIN schedule_entries` does not.
 *
 * ## 1. Repair the closing time in SQL
 *
 * `normalizeClosingTime()` (`operating-window.util.ts`) is a **write-path**
 * repair with no backfill, so the stored history still carries what the sources
 * sent: a close twelve hours BEFORE the open (ThemeParks.wiki stamps a
 * past-midnight close with the day's own date — Parque Warner Madrid read CLOSED
 * every day), a 34-hour day (SeaWorld San Diego) and a 3-year one (Busch Gardens
 * Williamsburg). Filtering those away would delete the evening from both the
 * numerator and the denominator; re-anchoring the clock time to the opening's
 * park-local date keeps it, which is what the write path decided and what this
 * hand-written twin repeats. Changed together with it or not at all.
 *
 * ## 2. Flatten to a DISJOINT union
 *
 * Two `OPERATING` rows on one day — an upstream re-publish, or a park that
 * publishes a morning and an evening block — otherwise count every overlapping
 * minute twice, so a day reports 660 open minutes where it has 480. Worse than
 * the arithmetic: the contiguity test that stitches an outage across a night
 * would see a gap between two overlapping windows and split one outage in two.
 *
 * ## 3. Anchor a segment to its WINDOW's day, not its own date
 *
 * The operating day of anything inside a window is the park-local date the
 * window OPENED on. Every park that closes after midnight otherwise loses its
 * evening from both sides: the 23:30 reading lands on one calendar day and the
 * 00:30 reading on the next, and neither matches the day the schedule row
 * describes. La Ronde does this every day of the season.
 *
 * Only `OPERATING` counts. `EXTRA_HOURS`, `TICKETED_EVENT` and `PRIVATE_EVENT`
 * are excluded on BOTH sides, so a ride that breaks during a hard-ticket
 * Halloween night contributes neither an outage nor exposure — the alternative
 * is a denominator that includes hours most visitors could not attend.
 */

/** Schedule types that count as the park being open to everybody. */
export const OPERATING_SCHEDULE_TYPE = "OPERATING";

/**
 * CTEs producing `park_tz(park_id, tz)` and `win(park_id, tz, opens_at,
 * closes_at, op_day)`, the flattened windows.
 *
 * Parameters the caller must bind: `$1` a `uuid[]` park filter or NULL, `$2` the
 * window start, `$3` the window end.
 *
 * `wikiOnly` restricts to parks that can emit `DOWN` at all. It is a capability
 * read from configuration, never from the outcome: a park with no
 * `wiki_entity_id` and a park with a quiet quarter are two different statements,
 * and only this column tells them apart.
 */
export function parkOpenWindowCtes(
  { wikiOnly }: { wikiOnly: boolean } = { wikiOnly: true },
): string {
  return `
  park_tz AS (
    SELECT pk.id AS park_id, pk.timezone AS tz
      FROM parks pk
     WHERE ($1::uuid[] IS NULL OR pk.id = ANY($1::uuid[]))
       ${wikiOnly ? "AND pk.wiki_entity_id IS NOT NULL" : ""}
       AND pk.timezone IS NOT NULL
  ),
  windows_raw AS (
    SELECT z.park_id, z.tz,
           se."openingTime" AS opens_at,
           ${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "z.tz")} AS closes_at
      FROM park_tz z
      JOIN schedule_entries se ON se."parkId" = z.park_id
     WHERE se."attractionId" IS NULL
       AND se."scheduleType" = '${OPERATING_SCHEDULE_TYPE}'
       AND se."openingTime" IS NOT NULL
       AND se."closingTime" IS NOT NULL
       -- Bounded on the OPENING, not the closing. A normalized window is at
       -- most 24 hours long, so two days of slack on the opening catches every
       -- window that reaches into the scan range — and unlike the closing, the
       -- opening is the one bound no source has been observed to misdate.
       AND se."openingTime" > $2::timestamptz - INTERVAL '2 days'
       AND se."openingTime" < $3::timestamptz
  ),
  w_ord AS (
    SELECT park_id, tz, opens_at, closes_at,
           MAX(closes_at) OVER (
             PARTITION BY park_id ORDER BY opens_at
             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS prev_max
      FROM windows_raw
     WHERE closes_at > opens_at
  ),
  w_grp AS (
    SELECT *,
           SUM(CASE WHEN prev_max IS NULL OR opens_at > prev_max THEN 1 ELSE 0 END)
             OVER (PARTITION BY park_id ORDER BY opens_at ROWS UNBOUNDED PRECEDING) AS grp
      FROM w_ord
  ),
  win AS (
    SELECT park_id, tz,
           MIN(opens_at)  AS opens_at,
           MAX(closes_at) AS closes_at,
           -- The window's OWN opening date in park-local time. Not the date of
           -- whatever instant is being placed inside it: a park closing at 02:00
           -- would otherwise split its evening across two operating days.
           (MIN(opens_at) AT TIME ZONE tz)::date AS op_day
      FROM w_grp
     GROUP BY park_id, tz, grp
  )`;
}

/**
 * The park's flattened `OPERATING` windows that are not over yet, ascending.
 *
 * The forward half of the same calendar `trailingOutageWithElapsedSql()` reads
 * backwards, and it goes through {@link parkOpenWindowCtes} for exactly that
 * reason: a second window definition is how a park ends up with two different
 * closing times depending on which question was asked. The repair, the merge and
 * the operating-day anchoring are all inherited.
 *
 * A window that is currently open is included — the bound is on the CLOSING, so
 * `[opens_at, closes_at)` reaching past `$2` qualifies however long ago it
 * opened. That is what lets a projection start counting inside today.
 *
 * Parameters: `$1` uuid[] park filter, `$2` the instant to look forward from,
 * `$3` the far end of the horizon.
 */
export function upcomingOperatingWindowsSql(): string {
  return `
  WITH ${parkOpenWindowCtes({ wikiOnly: true })}
  SELECT w.opens_at  AS "opensAt",
         w.closes_at AS "closesAt"
    FROM win w
   WHERE w.closes_at > $2::timestamptz
   ORDER BY w.opens_at
`;
}

/**
 * The SQL twin of `normalizeClosingTime()`.
 *
 * A plausible window (positive and at most 24 hours) is returned untouched,
 * including a degenerate zero-length one — rolling that forward would invent a
 * 24-hour operating day out of a source that reported nothing. Anything else has
 * its clock time re-anchored to the opening's park-local date and rolled forward
 * one day when that lands at or before the opening.
 *
 * The roll-forward goes through the timezone rather than adding 24 hours, so a
 * DST shift inside that night keeps the local closing time instead of moving it
 * by an hour.
 */
export function normalizedClosingSql(
  opensExpr: string,
  closesExpr: string,
  tzExpr: string,
): string {
  const anchored = `((${opensExpr} AT TIME ZONE ${tzExpr})::date + (${closesExpr} AT TIME ZONE ${tzExpr})::time) AT TIME ZONE ${tzExpr}`;
  const rolled = `((((${opensExpr} AT TIME ZONE ${tzExpr})::date + INTERVAL '1 day')::date + (${closesExpr} AT TIME ZONE ${tzExpr})::time) AT TIME ZONE ${tzExpr})`;
  return `CASE
            WHEN ${closesExpr} >= ${opensExpr}
             AND ${closesExpr} <= ${opensExpr} + INTERVAL '24 hours'
              THEN ${closesExpr}
            WHEN ${anchored} > ${opensExpr}
              THEN ${anchored}
            ELSE ${rolled}
          END`;
}

/**
 * Overlap in minutes between a half-open segment and a window.
 *
 * Half-open on both sides, so two adjacent segments never double-count the
 * instant they share.
 */
export function overlapMinutesSql(
  segStart: string,
  segEnd: string,
  winStart: string,
  winEnd: string,
): string {
  return `GREATEST(
            0,
            EXTRACT(EPOCH FROM (
              LEAST(${segEnd}, ${winEnd}) - GREATEST(${segStart}, ${winStart})
            )) / 60.0
          )`;
}
