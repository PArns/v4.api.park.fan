/**
 * When measured ride activity is allowed to call a day an operating day.
 *
 * Reconstructing a past day's hours from ride activity is old (`getDerivedHistoricalHours`),
 * but it used to answer only for days the schedule had nothing to say about
 * (`UNKNOWN`). Since PAR-697 the same reconstruction may also contradict a
 * park-level `CLOSED` entry, in the calendar and therefore in every statistic
 * that reads the calendar's shut-day rule (`closed-park-days.sql.ts`). That is a
 * published statement against the operator's own feed, so the bar it has to
 * clear is higher than "some ride reported a wait".
 *
 * The old bar was one condition: at a 15-min slot, at least 10 % of the park's
 * rides (min 2, max 10) show activity. Measured against production over
 * 2025-12-24 … 2026-10-04, that bar alone would have opened 1 227 of the 6 070
 * park-level `CLOSED` days; 193 of them survive the four conditions below. The
 * ones it loses are not noise, they are two named failure modes:
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
 *
 * The fourth condition is that the rows be observations rather than our own
 * bookkeeping (`observedReadingsSql`). It cannot be applied to every caller of
 * the gate: `attraction_hourly_history`, the rollup the hours are read from,
 * carries no `is_heartbeat` — so the three conditions here are evaluated
 * against the rollup, and the heartbeat-free counter-check runs as a second,
 * day-list-bounded query over raw `queue_data`
 * (`ParksService.getObservedOperationDays`) for the few days that are about to
 * overrule a `CLOSED` entry. Over the 199 days that clear the first three, the
 * counter-check confirms 193 and refuses 6.
 *
 * Numbers rather than SQL fragments on purpose. The two queries read different
 * shapes of the same day — one park-local `"HH:MM"` text slots out of a JSONB
 * rollup, one `timestamptz` rows out of a hypertable — so there is no predicate
 * text both could share. What must not drift is the thresholds, and those are
 * here.
 */

/**
 * How many different wait-time values a day needs across its qualifying slots.
 *
 * Counted over the whole day, not per slot: the failure mode is a feed with no
 * variety at all, and a day of real operation produces dozens of values.
 */
export const MEASURED_OPERATION_MIN_DISTINCT_WAITS = 3;

/**
 * The shortest and longest run, in hours, from a day's first qualifying slot to
 * its last.
 *
 * The floor rejects a burst — Walibi Holland's 55-minute artefact above. The
 * ceiling rejects a reading that never stops; a day that measures 15 hours of
 * continuous activity is a stuck feed rather than a long season day, because
 * a park-local calendar day cannot hold a longer operating block than its own
 * evening.
 *
 * Both bounds are inclusive.
 */
export const MEASURED_OPERATION_MIN_BLOCK_HOURS = 4;
export const MEASURED_OPERATION_MAX_BLOCK_HOURS = 14;

/**
 * The park-local hours in which any qualifying activity disqualifies the day,
 * both bounds inclusive.
 *
 * 02:00–05:59 local is chosen because it is outside every operator's published
 * hours in our data, including the ones that close after midnight — their late
 * hours fall on the calendar day they started, and 02:00 is past the last of
 * them.
 */
export const MEASURED_OPERATION_DEAD_HOURS_FROM = 2;
export const MEASURED_OPERATION_DEAD_HOURS_TO = 5;
