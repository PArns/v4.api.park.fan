import { observedReadingsSql } from "./closure-gap.sql";
import { parkOpenWindowCtes } from "./park-open-window.sql";

/**
 * How far back the last run of a ride is looked for, in days.
 *
 * Seven, for the reason the outage line names its start by weekday: inside a
 * week a weekday is unambiguous, and the page states the instant as „Sonntag,
 * 18:00" rather than as a date. A ride that has not run for longer than that is
 * not „not running yet today" — it is closed for something longer than a day,
 * and saying „noch" about it would promise an opening nobody announced. Such a
 * ride gets no line at all.
 */
export const NOT_RUN_TODAY_LOOKBACK_DAYS = 7;

/**
 * Minutes the park must have been open before a ride that has not run yet is
 * said not to have run yet.
 *
 * Rides open over the first minutes of the day, poll by poll. Without a grace
 * period every ride in the park would carry the line for the first five or ten
 * minutes after the gates open, which is true and says nothing.
 */
export const NOT_RUN_TODAY_GRACE_MINUTES = 15;

/**
 * The rides that are closed while their park is open and have not run at all
 * since the park last closed — and when they last did.
 *
 * ## What it says, and what it does not
 *
 * A ride that ran this morning and stopped is a closure, and
 * `CURRENT_CLOSURE_GAP_SQL` answers for it with „steht seit … still". A ride
 * that has not run at all today is a different statement and a weaker one: a
 * water ride on a cold day, a maintenance day, a ride that opens at 11:00 in a
 * park that opens at 09:00, a fault from yesterday evening. The feed does not
 * say which, and the closure signal deliberately refuses all of them for that
 * reason. What can be said about every one of them is true and neutral: it has
 * not run today, and it last ran on Sunday at 18:00.
 *
 * ## "Today" starts when the park last closed
 *
 * Not at today's opening time and not at local midnight. A ride that runs
 * during an early-entry hour before the published opening has run today; a ride
 * whose feed never flips it to CLOSED at night and flips it in the morning has
 * not. Both are answered by asking for any OPERATING reading after the end of
 * the park's previous window.
 *
 * ## When it last ran, clipped to the park's hours
 *
 * The run ended at the reading that followed its last OPERATING one. A feed
 * that leaves a ride OPERATING overnight and flips it at 09:00 would make that
 * „Montag, 09:00" — a run nobody could have ridden, in a sentence that also
 * says the ride has not run today. So the end is clipped to the close of the
 * window the last OPERATING reading fell in, unless that reading itself came
 * after the close (an evening event), in which case the reading is the latest
 * instant that can be vouched for.
 *
 * Parameters: `$1` uuid[] attraction ids, `$2` the park's timezone, `$3`
 * as-of, `$4` the park id — the same four `CURRENT_CLOSURE_GAP_SQL` takes, so a
 * caller holds one parameter list for both.
 */
export const NOT_RUN_TODAY_SQL = `
  WITH ${parkOpenWindowCtes({
    parkTz: `SELECT $4::uuid AS park_id, $2::text AS tz`,
    from: `$3::timestamptz - INTERVAL '${NOT_RUN_TODAY_LOOKBACK_DAYS + 1} days'`,
    to: `$3::timestamptz + INTERVAL '1 day'`,
  })},
  -- The window the park is in now, open long enough for the grace period. No
  -- row means a shut park, and a CLOSED ride in a shut park is a shut park.
  park_open AS (
    SELECT w.opens_at
      FROM win w
     WHERE w.opens_at <= $3::timestamptz
           - INTERVAL '${NOT_RUN_TODAY_GRACE_MINUTES} minutes'
       AND w.closes_at > $3::timestamptz
     ORDER BY w.closes_at DESC
     LIMIT 1
  ),
  -- Where "today" begins: the end of the park's previous window, or today's
  -- opening when the lookback holds none.
  day_start AS (
    SELECT COALESCE(
             (SELECT MAX(w.closes_at) FROM win w WHERE w.closes_at <= po.opens_at),
             po.opens_at
           ) AS at
      FROM park_open po
  ),
  readings AS (
    SELECT qd."attractionId" AS aid,
           qd.timestamp      AS ts,
           qd.status::text   AS st,
           lead(qd.timestamp) OVER (PARTITION BY qd."attractionId"
                                    ORDER BY qd.timestamp) AS next_ts
      FROM queue_data qd
     WHERE EXISTS (SELECT 1 FROM park_open)
       AND qd."attractionId" = ANY($1::uuid[])
       AND qd."queueType" = 'STANDBY'
       AND ${observedReadingsSql("qd")}
       AND qd.timestamp >= $3::timestamptz
           - INTERVAL '${NOT_RUN_TODAY_LOOKBACK_DAYS} days'
       AND qd.timestamp <= $3::timestamptz
  ),
  newest AS (
    SELECT DISTINCT ON (aid) aid, st
      FROM readings
     ORDER BY aid, ts DESC
  ),
  last_run AS (
    SELECT DISTINCT ON (aid) aid, ts AS last_operating, next_ts AS ended_at
      FROM readings
     WHERE st = 'OPERATING'
     ORDER BY aid, ts DESC
  )
  SELECT n.aid AS "attractionId",
         LEAST(
           lr.ended_at,
           GREATEST(
             lr.last_operating,
             (SELECT w.closes_at FROM win w
               WHERE w.opens_at <= lr.last_operating
               ORDER BY w.opens_at DESC
               LIMIT 1)
           )
         ) AS "lastRunAt"
    FROM newest n
    JOIN last_run lr ON lr.aid = n.aid
   CROSS JOIN day_start d
   WHERE n.st = 'CLOSED'
     AND lr.last_operating < d.at
`;
