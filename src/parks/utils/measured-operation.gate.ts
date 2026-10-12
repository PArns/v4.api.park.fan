/**
 * When measured ride activity is allowed to call a day an operating day.
 *
 * Reconstructing a past day's hours from ride activity is old
 * (`getDerivedHistoricalHours`), but it used to answer only for days the
 * schedule had nothing to say about (`UNKNOWN`). Since PAR-697 a day may also
 * be published against a park-level `CLOSED` entry — in the calendar and
 * therefore in every statistic that reads the shut-day rule
 * (`closed-park-days.sql.ts`). That is a statement against the operator's own
 * feed, so the bar it has to clear is higher than "some ride reported a wait".
 *
 * The old bar was one condition: at a 15-min slot, at least 10 % of the park's
 * rides (min 2, max 10) show activity. Measured against production over
 * 2025-12-24 … 2026-10-07, that bar alone would have opened 1 238 of the 6 174
 * park-level `CLOSED` days; 210 of them survive the conditions below. The ones
 * it loses are not noise, they are three named failure modes:
 *
 * 1. **A feed that reports one number on every ride.** Walibi Holland's
 *    2026-09-11 had 14 rides reporting the same wait at the same minute,
 *    jumping 15 → 45 → 70 → 36 → 16 → 44 → 80 in five-minute steps over 55
 *    minutes. Fourteen separate queues never agree to the minute, and a real
 *    queue does not jump like that. {@link MEASURED_OPERATION_MIN_DISTINCT_WAITS}
 *    and {@link MEASURED_OPERATION_MIN_BLOCK_HOURS} both reject it; across all
 *    parks the distinct-wait condition alone drops 196 days.
 * 2. **A feed stuck on its last reading.** 352 of 551 candidate days carried
 *    waits > 0 between 02:00 and 05:00 park-local, many across all 24 hours. No
 *    park runs at four in the morning, so
 *    {@link MEASURED_OPERATION_DEAD_HOURS_FROM} is a shape test rather than a
 *    business-hours test: it does not ask whether the park was open then, it
 *    asks whether the data could be a day at all.
 * 3. **A feed frozen on a stale timestamp.** PAR-748 measured the night-time
 *    readings this ticket started from: 3 992 rows, **none** of them a
 *    heartbeat, and 46.7 % of them carrying a `lastUpdated` hours old — the
 *    upstream feed kept serving the same reading and our writer kept storing
 *    it. {@link MEASURED_OPERATION_MAX_FEED_AGE_MINUTES} is the condition that
 *    separates those, and it exists only on the raw rows.
 *
 * ## Why the judgement is a function and not SQL
 *
 * Until 2026-10-07 this gate was a shared SQL fragment evaluated by every
 * reader. It was correct and it was unaffordable: the ML-accuracy query went
 * from 1.4 s to 7.4 s because the rule has to look at raw `queue_data` per
 * candidate day, and no reader has a place to hang a set-based pass. Patrick
 * decided to materialise the verdict instead — one row per (park, park-local
 * day) in `park_day_operations`, written by the nightly job, read by a primary
 * key lookup.
 *
 * So the gate now runs exactly once per day per park, in
 * `ParkDayOperationService`, over the aggregates of
 * {@link MEASURED_OPERATION_DAY_STATS_SQL}. Four numbers go in, a boolean comes
 * out, and the thresholds are testable against the two Walibi days without a
 * database.
 */

/**
 * How many different wait-time values a day needs across its qualifying rows.
 *
 * Counted over the whole day, not per slot: the failure mode is a feed with no
 * variety at all, and a day of real operation produces dozens of values.
 */
export const MEASURED_OPERATION_MIN_DISTINCT_WAITS = 3;

/**
 * The shortest and longest run, in hours, from a day's first qualifying reading
 * to its last.
 *
 * The floor rejects a burst — Walibi Holland's 55-minute artefact above. The
 * ceiling rejects a reading that never stops; a day that measures more than 14
 * hours of continuous activity is a stuck feed rather than a long season day,
 * because a park-local calendar day cannot hold a longer operating block than
 * its own evening.
 *
 * Both bounds are inclusive.
 */
export const MEASURED_OPERATION_MIN_BLOCK_HOURS = 4;
export const MEASURED_OPERATION_MAX_BLOCK_HOURS = 14;

/**
 * The park-local hours in which any qualifying reading disqualifies the day,
 * both bounds inclusive.
 *
 * 02:00–05:59 local is chosen because it is outside every operator's published
 * hours in our data, including the ones that close after midnight: 02:00 is
 * past the last of them.
 *
 * Those late hours do NOT count toward the day they started. The day is a
 * park-local calendar day, so readings between 00:00 and 01:59 land on the
 * next day, whose block then runs from just after midnight to its own evening
 * and exceeds {@link MEASURED_OPERATION_MAX_BLOCK_HOURS}. A park that operates
 * past midnight is therefore never reopened by this gate. That is the safe
 * direction (the operator's CLOSED stands); judging by operating day instead
 * of calendar day is PAR-722.
 */
export const MEASURED_OPERATION_DEAD_HOURS_FROM = 2;
export const MEASURED_OPERATION_DEAD_HOURS_TO = 5;

/**
 * How far a reading's `lastUpdated` may trail its own `timestamp` and still
 * count as a reading (PO, 2026-10-07, after PAR-748).
 *
 * A row whose `lastUpdated` is null is NOT a fresh reading for this purpose:
 * the column is nullable because it predates the current writers, so the
 * question is undecidable on that row — and a published statement against the
 * operator is not made on an undecidable row. The condition therefore sits in
 * the SQL rather than here, where it can be expressed as a `NULL`-rejecting
 * comparison.
 */
export const MEASURED_OPERATION_MAX_FEED_AGE_MINUTES = 60;

/**
 * One park-local day's qualifying readings, reduced to the four numbers the
 * gate judges. Produced by {@link MEASURED_OPERATION_DAY_STATS_SQL}; see
 * `measured-operation.sql.ts` for which rows qualify.
 */
export interface MeasuredOperationDayStats {
  /** Different `waitTime` values across the day. */
  distinctWaits: number;
  /** Qualifying readings inside the dead hours. Any one of them is fatal. */
  deadHourReadings: number;
  /**
   * Minutes from the day's first qualifying reading to its last. `null` when
   * the day has no qualifying reading at all.
   */
  blockMinutes: number | null;
}

/**
 * Does measured ride activity say the park operated on this day?
 *
 * Deliberately total: every day with a `CLOSED` entry gets a verdict row, and
 * `false` is a verdict rather than a missing one. A reader that finds no row at
 * all falls back to the operator's entry, which is the same outcome by a
 * different route — see `measuredOperationDayExists`.
 */
export function isMeasuredOperationDay(
  stats: MeasuredOperationDayStats,
): boolean {
  if (stats.blockMinutes === null) return false;
  if (stats.deadHourReadings > 0) return false;
  if (stats.distinctWaits < MEASURED_OPERATION_MIN_DISTINCT_WAITS) return false;

  return (
    stats.blockMinutes >= MEASURED_OPERATION_MIN_BLOCK_HOURS * 60 &&
    stats.blockMinutes <= MEASURED_OPERATION_MAX_BLOCK_HOURS * 60
  );
}
