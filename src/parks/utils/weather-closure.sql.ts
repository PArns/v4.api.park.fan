import { parkOpenWindowCtes } from "../../common/utils/park-open-window.sql";

/** Both statements are about one park whose timezone the caller holds. */
const ONE_PARK = "SELECT $1::uuid AS park_id, $4::text AS tz";

/**
 * Every ride of one park in every whole hour the park was open, with whether
 * it was shut in that hour.
 *
 * "Shut" is CLOSED or DOWN with no OPERATING reading in the hour. An hour with
 * both is a ride that ran, and an hour with neither (refurbishment, no
 * reading) is left out rather than counted either way. Only whole hours inside
 * a window count: the first, partial hour of a day is when rides open one poll
 * at a time, and it would read every ride as shut by the cold at 09:00.
 *
 * Parameters: `$1` park id, `$2` range start, `$3` range end, `$4` the park's
 * timezone.
 */
export const WEATHER_CLOSURE_HOURS_SQL = `
  WITH ${parkOpenWindowCtes({ parkTz: ONE_PARK })},
  open_hours AS (
    SELECT DISTINCT gs AS hour_start
      FROM win,
           generate_series(
             date_trunc('hour', win.opens_at + INTERVAL '1 hour' - INTERVAL '1 millisecond'),
             win.closes_at - INTERVAL '1 hour',
             INTERVAL '1 hour'
           ) gs
     WHERE win.opens_at >= $2::timestamptz
       AND win.closes_at <= $3::timestamptz
  ),
  ride_hours AS (
    SELECT q."attractionId" AS attraction_id,
           date_trunc('hour', q."timestamp") AS hour_start,
           bool_or(q.status = 'OPERATING') AS ran,
           bool_or(q.status IN ('CLOSED', 'DOWN')) AS shut
      FROM queue_data q
      JOIN attractions a ON a.id = q."attractionId"
     WHERE a."parkId" = $1::uuid
       AND q."queueType" = 'STANDBY'
       AND q."timestamp" >= $2::timestamptz
       AND q."timestamp" < $3::timestamptz
     GROUP BY 1, 2
  )
  SELECT r.attraction_id AS "attractionId",
         r.hour_start    AS "hourStart",
         (r.shut AND NOT r.ran) AS closed
    FROM ride_hours r
    JOIN open_hours o USING (hour_start)
   WHERE r.ran OR r.shut
`;

/**
 * The closing time of the park's window that is open at `$2`, if one is.
 *
 * Through the shared window definition rather than today's schedule row, so a
 * closing past midnight and two overlapping rows read as they do everywhere
 * else. Parameters: `$1` park id, `$2` the instant, `$3` the instant plus a
 * minute (the CTEs bound the opening by it), `$4` the park's timezone.
 */
export const WEATHER_CLOSURE_CURRENT_WINDOW_SQL = `
  WITH ${parkOpenWindowCtes({ parkTz: ONE_PARK })}
  SELECT w.closes_at AS "closesAt"
    FROM win w
   WHERE w.opens_at <= $2::timestamptz
     AND w.closes_at > $2::timestamptz
   ORDER BY w.closes_at DESC
   LIMIT 1
`;
