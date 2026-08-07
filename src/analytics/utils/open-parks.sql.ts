/**
 * SINGLE SOURCE OF TRUTH for "which parks are open right now" in the analytics
 * queries — the SQL counterpart of `isParkOpen()` (common/utils/status-calculator.util).
 *
 * Every analytics endpoint that answers "how many parks are open" has to start from
 * the same set, or the homepage contradicts itself: the hero badge, the world map and
 * the ticker each used to carry their own copy of these CTEs.
 *
 * Two ways in, mirroring `isParkOpen()`:
 *   1. schedule_open_parks — the park publishes an OPERATING schedule covering now.
 *   2. ride_open_parks     — the fallback for parks with NO schedule integration,
 *                            decided from live ride data alone.
 *
 * ## Why the ride fallback needs a movement test
 *
 * Some upstream feeds never close. queue-times keeps serving a park's last snapshot
 * after closing time — the same wait times, the same `is_open: true` per ride, with a
 * freshly stamped `last_updated`. Measured on 2026-08-07: Cinecittà World served 29
 * rides "OPERATING @ 15 min" unchanged for 24 h straight, so every rule of the shape
 * "a ride is OPERATING and has a plausible wait" called it open at 03:00 local.
 *
 * Neither a wait-time floor nor a staleness check separates that from a real park:
 * the placeholder waits are well above any floor, and the row timestamps are current.
 * What does separate them is MOVEMENT — a live feed's numbers change, a frozen
 * snapshot's do not. Hence `has_moved` below.
 *
 * ## Why the windows differ (2 h freshness, 4 h movement)
 *
 * Freshness stays at 2 h: that is "is this park still reporting at all".
 * Movement looks back 4 h, because a real park's queues can settle for an hour or two
 * near closing time — a 2 h window drops the last hours of the day.
 *
 * Replaying the rule hourly over 7 days against production, in park-hours judged open
 * per local hour of day (168 samples per hour, 18 fallback parks):
 *
 *   local hour   00  02  04  06  08  10  12  14  16  18  20  22
 *   before        21  22  20  20  20  18  49  60  60  55  40  23
 *   after          1   0   0   1   2   4  41  53  50  43  27   4
 *
 * Before, the count sits flat at ~20 through the entire night — that is the bug.
 * After, the night empties and what is left is an opening-hours curve peaking at
 * 14:00-16:00 local. Per park over those 7 days: Cinecittà World 169 h -> 4 h and
 * Le Parc du Petit Prince 22 h -> 7 h (the two frozen feeds), Adventureland 152 -> 71,
 * Lake Compounce 132 -> 65, Le Pal 91 -> 61. Parks that were already reported
 * correctly lose nothing at all: Gröna Lund 63 -> 63, Beto Carrero 43 -> 43,
 * Hellendoorn 32 -> 32, Fårup 27 -> 27, Universal Studios Orlando 75 -> 75.
 *
 * ## What this deliberately does NOT do
 *
 * There is no "no park is open at 04:00 local" clock guard. Movement cannot catch a
 * feed that keeps *changing* through the night, so one was measured — and over these
 * 7 days it removed 2 park-hours, both at 05:00-06:00 local. A magic constant that
 * buys 2 park-hours a week is not worth the maintenance question it raises; if a feed
 * ever does stay noisy overnight, revisit it with that park's numbers in hand.
 */

/** Ride data older than this no longer counts as the park "still reporting". */
const FRESHNESS_WINDOW = "2 hours";

/** How far back to look for a changed value before calling the feed frozen. */
const MOVEMENT_WINDOW = "4 hours";

/** Minimum number of fresh wait-time samples before the fallback may fire at all. */
const MIN_FRESH_SAMPLES = 3;

/** Share of fresh samples that must carry a real queue, in percent. */
const MIN_PCT_WITH_QUEUE = 25;

/** A sample at or above this many minutes counts as a real queue. */
const REAL_QUEUE_MINUTES = 10;

/**
 * The CTEs defining the open-park set. Interpolate directly after `WITH` and append
 * the caller's own CTEs:
 *
 * ```ts
 * const rows = await repo.query(`
 *   WITH ${OPEN_PARKS_CTES},
 *   latest_updates AS ( ... JOIN park_status ps ON ps."parkId" = a."parkId" ... )
 *   SELECT ...
 * `);
 * ```
 *
 * Exposes `park_status("parkId")` — one row per currently open park.
 */
export const OPEN_PARKS_CTES = `
      schedule_open_parks AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      ride_open_parks AS (
        -- Parks with no schedule integration, judged open from live ride data.
        -- Scans ${MOVEMENT_WINDOW} so the movement test has history to compare against;
        -- the sample-count and queue-share tests stay on the ${FRESHNESS_WINDOW} window
        -- via FILTER.
        SELECT a."parkId"
        FROM attractions a
        JOIN queue_data qd ON qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '${MOVEMENT_WINDOW}'
          AND qd."waitTime" IS NOT NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM schedule_entries se
          WHERE se."parkId" = a."parkId" AND se."scheduleType" = 'OPERATING'
        )
        AND NOT EXISTS (
          SELECT 1 FROM schedule_entries se
          WHERE se."parkId" = a."parkId"
            AND se."scheduleType" = 'CLOSED'
            AND se.date = CURRENT_DATE
        )
        GROUP BY a."parkId"
        HAVING COUNT(*) FILTER (
                 WHERE qd.timestamp > NOW() - INTERVAL '${FRESHNESS_WINDOW}'
               ) >= ${MIN_FRESH_SAMPLES}
          AND 100.0 * COUNT(*) FILTER (
                WHERE qd."waitTime" >= ${REAL_QUEUE_MINUTES}
                  AND qd.timestamp > NOW() - INTERVAL '${FRESHNESS_WINDOW}'
              ) / NULLIF(COUNT(*) FILTER (
                WHERE qd.timestamp > NOW() - INTERVAL '${FRESHNESS_WINDOW}'
              ), 0) >= ${MIN_PCT_WITH_QUEUE}
          -- has_moved: at least one ride reported more than one distinct
          -- (waitTime, status) over the movement window. A frozen feed reports
          -- exactly one state per ride, so the two counts come out equal.
          AND COUNT(DISTINCT (a.id, qd."waitTime", qd.status)) FILTER (
                WHERE qd."queueType" = 'STANDBY'
              ) > COUNT(DISTINCT a.id) FILTER (
                WHERE qd."queueType" = 'STANDBY'
              )
      ),
      park_status AS (
        SELECT "parkId" FROM schedule_open_parks
        UNION
        SELECT "parkId" FROM ride_open_parks
      )`;
