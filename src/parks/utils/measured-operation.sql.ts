import { observedReadingsSql } from "../../common/utils/closure-gap.sql";
import {
  MEASURED_OPERATION_DEAD_HOURS_FROM,
  MEASURED_OPERATION_DEAD_HOURS_TO,
  MEASURED_OPERATION_MAX_FEED_AGE_MINUTES,
} from "./measured-operation.gate";

/**
 * The minimum wait a reading has to show to count as queueing.
 *
 * Same floor the hourly-history rollup uses for its qualifying samples
 * (`computeParkHourlyHistoryForDate`), so the derived hours stored beside the
 * verdict are read off the same population the verdict was taken from.
 */
export const MEASURED_OPERATION_MIN_WAIT_MINUTES = 5;

/**
 * One park-local day's qualifying readings, reduced to the numbers
 * `isMeasuredOperationDay` judges. Returns exactly one row; every column is
 * `NULL`/0 when the day has no qualifying reading.
 *
 * `$1` park id, `$2` the park-local day as `YYYY-MM-DD`, `$3` the park's IANA
 * timezone.
 *
 * ## Which rows qualify
 *
 * An OPERATING STANDBY reading of at least
 * {@link MEASURED_OPERATION_MIN_WAIT_MINUTES} minutes that is an observation
 * rather than our own bookkeeping (`observedReadingsSql`) and whose upstream
 * `lastUpdated` trails its `timestamp` by no more than
 * {@link MEASURED_OPERATION_MAX_FEED_AGE_MINUTES} minutes. The last condition
 * rejects `lastUpdated IS NULL` along with the stale rows, which is deliberate
 * — see the constant.
 *
 * Retired rides are NOT excluded. The question is whether the park operated on
 * a day that has already happened, and a ride retired since still answers it.
 *
 * ## Why the day is a timestamp range and never a day cast
 *
 * `(qd.timestamp AT TIME ZONE tz)::date = $2` is the obvious way to write "this
 * park-local day" and it is the expensive one: the cast is not sargable, so
 * TimescaleDB cannot exclude chunks and each evaluation reads every chunk the
 * park has rows in. Measured against production on the read-path version of
 * this rule, the difference was 4 523–8 346 ms with the cast against 135–161 ms
 * with the bounds below, at identical verdicts (G-146).
 */
export const MEASURED_OPERATION_DAY_STATS_SQL = `
  SELECT
    COUNT(DISTINCT qd."waitTime")::int AS distinct_waits,
    COUNT(*) FILTER (
      WHERE EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $3)
            BETWEEN ${MEASURED_OPERATION_DEAD_HOURS_FROM}
                AND ${MEASURED_OPERATION_DEAD_HOURS_TO}
    )::int AS dead_hour_readings,
    (EXTRACT(EPOCH FROM (MAX(qd.timestamp) - MIN(qd.timestamp))) / 60)::int
      AS block_minutes
  FROM queue_data qd
  JOIN attractions a ON a.id = qd."attractionId"
  WHERE a."parkId" = $1::uuid
    AND qd.timestamp >= ($2::text || ' 00:00')::timestamp AT TIME ZONE $3
    AND qd.timestamp <  (($2::date + 1)::text || ' 00:00')::timestamp
                        AT TIME ZONE $3
    AND qd.status = 'OPERATING'
    AND qd."queueType" = 'STANDBY'
    AND qd."waitTime" IS NOT NULL
    AND qd."waitTime" >= ${MEASURED_OPERATION_MIN_WAIT_MINUTES}
    AND qd.timestamp - qd."lastUpdated"
        <= INTERVAL '${MEASURED_OPERATION_MAX_FEED_AGE_MINUTES} minutes'
    AND ${observedReadingsSql("qd")}
`;
