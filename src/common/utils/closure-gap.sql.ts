import { MIN_BLIND_EVIDENCE_HOURS } from "../../analytics/entities/park-downtime-coverage.entity";
/**
 * A fault read from a closure, for the parks whose feed never says DOWN.
 *
 * ## Why this exists at all
 *
 * `docs/analytics/ride-downtime.md` §1 excludes `CLOSED` while the park is open,
 * and that exclusion is right for the reason it gives: it is the class that
 * killed ML's `unexpected_closure`, where 534 of 602 anomalies were genuine
 * closures. But it was applied to a raw status, and a raw status is not what a
 * visitor sees. **102 of 182 scheduled parks never emit a single DOWN** —
 * Phantasialand, Energylandia, Alton Towers — so for them the choice is not
 * between a strong signal and a weak one. It is between a weak one and silence.
 *
 * ## The definition, and why each clause is load-bearing
 *
 * A closure gap is a ride that **was OPERATING earlier the same park-local
 * operating day**, went `CLOSED` inside opening hours, and **came back to
 * OPERATING the same day**. Each of the filters below removes a class of
 * non-fault that measurement showed dominates the raw signal:
 *
 * | Filter | What it removes | Measured |
 * | --- | --- | --- |
 * | came back the same day | the park shutting for the night | a closing park does not reopen |
 * | `MAX_SIMULTANEOUS_CLOSERS` | park-wide events (weather, closing time) | **61.2 %** of raw transitions are 5+ rides in one minute |
 * | MAX_REGULAR_DAYS | a ride with its own shorter hours | 130 ride-hour pairs close on 10+ days at the same hour |
 * | MAX_GAP_DAY_SHARE | a show or duty cycle, which closes at a different time each day | 71 rides carry a gap on 60 %+ of their operating days |
 * | `MIN_GAP_MINUTES` | one poll of noise | 27.1 % of raw gaps are exactly one 5-minute cycle |
 *
 * Raw transitions over 21 days: 25 759. After all four: **3618**, over 1137
 * rides in 121 parks, with quartiles 15/20/35 minutes — close to the DOWN
 * signal's own 10/25/50 and unlike anything a scheduled closure looks like.
 *
 * ## Only where DOWN is absent
 *
 * Restricted to parks that have never emitted a DOWN. In parks that do emit it,
 * a fault is already reported as DOWN (45.74 per 1000 operating hours against
 * 2.37 for closure gaps), so adding this there would mix two populations for a
 * 5 % gain and make every published figure mean two things at once.
 *
 * ## What it is NOT
 *
 * It is not a reported outage, and no sentence built on it may say „gemeldet".
 * The operator never told us the ride was broken; we noticed it stopped and came
 * back. That distinction lives in `attraction_outages.signal` and travels all
 * the way to the wording on the page.
 */

/** Rides closing in the same minute, above which it is the park and not a ride. */
export const MAX_SIMULTANEOUS_CLOSERS = 2;

/**
 * Share of a ride's operating days that may carry a gap before it is read as a
 * cycle rather than a fault.
 *
 * The hour-based filter below only catches a ride that closes at the SAME hour.
 * A show, a character meet or a ride on a duty cycle closes at a different time
 * every day and sails straight through it. Measured over 21 days across the
 * blind parks: **71 rides carry a gap on 60 % or more of their operating days**
 * (1433 gaps), against 883 rides under 10 % (1176 gaps). The first group is
 * Futuroscope's cinemas, Nigloland's character meets and a handful of coasters
 * on what is plainly a duty cycle — 17 gap-days out of 21 operating days is not
 * a run of bad luck.
 *
 * Set at half: a ride that stops on more days than not is describing its
 * timetable, not its reliability. It is the conservative direction — a genuinely
 * unreliable ride is withheld rather than a timetable published as faults.
 * Applying it drops the 21-day population from 2817 to 1757 and leaves the
 * median untouched at 25 minutes.
 */
export const MAX_GAP_DAY_SHARE = 0.5;

/** Operating days a ride needs before that share means anything. */
export const MIN_DAYS_FOR_CYCLE_TEST = 5;

/**
 * Days a ride may close at the same hour before it is read as its own schedule.
 *
 * A ride that shuts at 16:00 in a park open until 18:00 is not failing daily; it
 * has shorter hours. Five days inside the window is already a pattern.
 */
export const MAX_REGULAR_DAYS = 5;

/** Minutes a gap must span. One poll cycle is not an outage, it is a reading. */
export const MIN_GAP_MINUTES = 10;

/**
 * Share of days a ride may end before the park does, before that IS its day.
 *
 * The filter that finally separates a cinema from a broken coaster, and the one
 * the same-hour test could not do because these rides stop at a different time
 * every day. Measured over 21 days:
 *
 * | Ride | days ending early |
 * | --- | ---: |
 * | KinéMAX, Cosmic Collisions, T. Rex (Futuroscope) | **100 %** |
 * | The Extraordinary Journey | 57 % |
 * | Arthur, the 4D adventure | 38 % |
 * | Journey to Atlantis | 15 % |
 * | Steel Eel (a real fault) | **8 %** |
 *
 * A ride that has ended before the park on every single one of the last 21 days
 * is not breaking daily at a different time; that is its timetable. Set at half,
 * matching MAX_GAP_DAY_SHARE, and the same conservative direction: a genuinely
 * unreliable ride is withheld rather than a timetable published as a fault.
 */
export const MAX_EARLY_END_SHARE = 0.5;

/**
 * Park minutes that must still remain when a ride shuts, for it to be a fault.
 *
 * A ride closing in the last hour of the day is winding down with the park, not
 * breaking. This is the user-stated rule ("kurz vor Parkschluss ist keine
 * Störung") and it is cheap, because the closing time is already joined.
 *
 * It does NOT catch everything it might look like it does. Measured at
 * Futuroscope: the cinemas shut with 80 to 210 minutes still on the clock, so a
 * threshold that caught them would suppress genuine late-afternoon faults too.
 * See the note on the live statement about what this signal cannot separate.
 */
export const MIN_PARK_MINUTES_LEFT = 60;

/** How far a closure may reach before it stops being a same-day gap. */
export const MAX_GAP_HOURS = 12;

/**
 * Closure gaps as intervals, for one park filter and window.
 *
 * Parameters: `$1` uuid[] park filter or NULL, `$2` window start, `$3` window
 * end. Deliberately the same shape as `OUTAGE_INTERVALS_SQL` so the processor
 * can run both and write one table.
 *
 * The window functions do all the work in one pass; an earlier version used
 * correlated subqueries for "was it OPERATING before" and "did it come back",
 * and it was killed by the server at 30 days.
 */
export const CLOSURE_GAP_INTERVALS_SQL = `
  WITH blind_parks AS (
    -- Resolved ONCE per park, not once per row. As a correlated NOT EXISTS
    -- inside the row scan this took 70 s over 21 days; hoisted out it is a
    -- single grouped pass and the whole statement runs in about 6.
    --
    -- Derived here rather than read from park_downtime_coverage, and the
    -- reason is ordering, not oversight: the nightly job runs
    -- reconstruction -> profiles -> curves, and the coverage table is written
    -- by the profile step. Reading it from inside the reconstruction would use
    -- yesterday's regime to decide what today's intervals mean, and it cannot
    -- be reordered because the regime depends on attraction_exposure_days,
    -- which this step produces.
    --
    -- The LIVE statement does read the table: it runs all day, long after the
    -- job settled, and proving a negative over an unbounded history is not
    -- something to do on a page render.
    --
    -- Both must agree, so both use MIN_BLIND_EVIDENCE_HOURS and both require
    -- never-DOWN. Without the floor the two disagree for the 11 parks the
    -- coverage entity describes as having "too little observation to say":
    -- reports at the profile gate, blind here, collecting inferred intervals
    -- that their own profile would count as reported ones.
    SELECT p.id AS pid
      FROM parks p
     WHERE p.wiki_entity_id IS NOT NULL
       AND ($1::uuid[] IS NULL OR p.id = ANY($1::uuid[]))
       AND NOT EXISTS (
         SELECT 1 FROM queue_data d
           JOIN attractions da ON da.id = d."attractionId"
          WHERE da."parkId" = p.id
            AND d."queueType" = 'STANDBY'
            AND d.status = 'DOWN'
       )
       AND COALESCE((
         SELECT SUM(ed.operating_minutes) / 60.0
           FROM attraction_exposure_days ed
           JOIN attractions ea ON ea.id = ed."attractionId"
          WHERE ea."parkId" = p.id
       ), 0) >= ${MIN_BLIND_EVIDENCE_HOURS}
  ),
  src AS (
    SELECT a.id AS aid, a."parkId" AS pid, p.timezone AS tz,
           qd.timestamp AS ts, qd.status::text AS st
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      JOIN blind_parks b ON b.pid = a."parkId"
      JOIN parks p ON p.id = a."parkId"
     WHERE qd."queueType" = 'STANDBY'
       AND a.retired_at IS NULL
       AND (a.last_merged_at IS NULL OR a.last_merged_at < $2::timestamptz)
       -- Our own bookkeeping rows are not observations of anything.
       AND COALESCE(qd.data_source, '') NOT IN
           ('system-reconciliation', 'system-heartbeat')
       -- And neither is a CARRIED row, which is the same trap one level down:
       -- writeHourlyHeartbeats copies the previous row's data_source, so a
       -- carried CLOSED is indistinguishable from an observed one by source
       -- alone. It matters more here than anywhere else because the gap is
       -- recognised by an exact OPERATING/CLOSED/OPERATING triple: one carried
       -- row in the middle turns next_st into CLOSED and drops the gap
       -- entirely, which silently truncated this population at ~65 minutes.
       -- NULL means "written before the column existed" and falls back to the
       -- old heuristic, exactly as the reconstruction does.
       AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
       AND qd.timestamp >= $2::timestamptz
       AND qd.timestamp <  $3::timestamptz
  ),
  seq AS (
    SELECT *,
           lag(st)  OVER w AS prev_st,
           lead(st) OVER w AS next_st,
           lead(ts) OVER w AS next_ts
      FROM src
    WINDOW w AS (PARTITION BY aid ORDER BY ts)
  ),
  raw_gaps AS (
    SELECT aid, pid, tz,
           ts        AS started_at,
           next_ts   AS ended_at,
           (ts AT TIME ZONE tz)::date            AS op_day,
           EXTRACT(HOUR FROM ts AT TIME ZONE tz)::int AS closed_hour,
           EXTRACT(EPOCH FROM (next_ts - ts)) / 60.0  AS gap_min
      FROM seq
     WHERE st = 'CLOSED'
       AND prev_st = 'OPERATING'
       AND next_st = 'OPERATING'
       AND next_ts IS NOT NULL
       AND next_ts < ts + INTERVAL '${MAX_GAP_HOURS} hours'
       -- Back the same operating day. A park shutting does not reopen.
       AND (next_ts AT TIME ZONE tz)::date = (ts AT TIME ZONE tz)::date
  ),
  simultaneity AS (
    SELECT pid, date_trunc('minute', started_at) AS minute, count(*) AS closers
      FROM raw_gaps GROUP BY 1, 2
  ),
  regularity AS (
    SELECT aid, closed_hour, count(DISTINCT op_day) AS days
      FROM raw_gaps GROUP BY 1, 2
  ),
  -- A ride that stops on more days than not is describing its timetable.
  -- The hour filter above cannot see this: a show closes at a different time
  -- every day. Measured over the blind parks, 71 rides carry a gap on 60 %+ of
  -- their operating days.
  -- Operating days per ride, resolved ONCE as a grouped scan. As a correlated
  -- subquery inside cycle this ran past two minutes; the same mistake, and
  -- the same fix, as the blind-park check above.
  active AS (
    SELECT e."attractionId" AS aid,
           count(*) FILTER (WHERE e.operating_minutes > 0)::numeric AS active_days
      FROM attraction_exposure_days e
     WHERE e.op_day >= ($2::timestamptz)::date
     GROUP BY e."attractionId"
  ),
  cycle AS (
    SELECT g.aid,
           count(DISTINCT g.op_day)::numeric AS gap_days,
           COALESCE(MAX(ac.active_days), 0)  AS active_days
      FROM raw_gaps g
      LEFT JOIN active ac ON ac.aid = g.aid
     GROUP BY g.aid
  )
  SELECT g.aid                                  AS "attractionId",
         g.pid                                  AS "parkId",
         g.started_at                           AS "startedAt",
         g.ended_at                             AS "endedAt",
         g.op_day                               AS "startOpDay",
         ROUND(g.gap_min)::int                  AS "wallMinutes"
    FROM raw_gaps g
    JOIN simultaneity s
      ON s.pid = g.pid AND s.minute = date_trunc('minute', g.started_at)
    JOIN regularity r
      ON r.aid = g.aid AND r.closed_hour = g.closed_hour
    JOIN cycle c ON c.aid = g.aid
   WHERE s.closers <= ${MAX_SIMULTANEOUS_CLOSERS}
     AND r.days    <  ${MAX_REGULAR_DAYS}
     AND g.gap_min >= ${MIN_GAP_MINUTES}
     -- Not a duty cycle. Below the day floor there is not enough to judge, and
     -- the ride is kept.
     AND (c.active_days < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR c.gap_days / c.active_days <= ${MAX_GAP_DAY_SHARE})
   ORDER BY g.pid, g.aid, g.started_at
`;

/**
 * The closure a ride is sitting in **right now**, for a blind park.
 *
 * The historical statement above needs the ride to have come back; this one
 * cannot wait for that, so it substitutes the checks that are available at the
 * instant:
 *
 * - the ride reads `CLOSED` on its newest reading, and
 * - it was `OPERATING` earlier the same park-local operating day — which is the
 *   whole of the user's rule and the thing a seasonal or all-day closure can
 *   never satisfy, and
 * - **it did not close together with the rest of the park**, counted over the
 *   same minute. This is the filter that matters live: at Phantasialand every
 *   ride flipped to CLOSED at 18:10, and without it the page would have
 *   announced forty simultaneous faults at closing time.
 *
 * The duration floor and the simultaneity filter DO apply here — the latter
 * counted over the park rather than over the ids passed in, because on a ride
 * page that list is one entry long and the count would always be one.
 *
 * The duty-cycle filter DOES apply here, and it has to. It is what separates a
 * cinema between showings from a broken ride, and the first thing the repaired
 * endpoint returned was seven Futuroscope cinemas. It costs one indexed lookup
 * per candidate — and the candidates are only the rides currently sitting in a
 * closure, which is a handful.
 *
 * The same-hour regularity filter is deliberately NOT checked here. It needs
 * weeks of history per ride AND hour, it is the weakest of the filters (130
 * ride-hour pairs against the simultaneity filter's 61 % of raw transitions),
 * and the nightly reconstruction applies it to the stored record anyway. A ride
 * with its own shorter hours may therefore show a live line for one closing; it
 * will not enter the history.
 *
 * ## What this signal cannot separate
 *
 * A ride that shuts mid-afternoon and does not come back looked, at first, like
 * something no available signal could separate from a fault: the Futuroscope
 * cinemas shut with 80 to 210 minutes of park time left, at a different hour
 * every day, so neither the same-hour test nor a closing-time margin caught
 * them. What does catch them is asking whether the ride's DAY habitually ends
 * before the park's — see MAX_EARLY_END_SHARE, where they score 100 % against a
 * real fault's 8 %.
 *
 * What remains unseparated is a ride whose early finish is irregular: closing at
 * 16:00 on a third of its days is neither a timetable nor obviously a fault.
 * Those still surface, which is why the wording is „Steht seit … still" and
 * never „Störung" — a sentence true of a cinema after its last showing and of a
 * broken ride alike. Anything stronger would need the operator's own showtimes.
 *
 * It also stops at closing time. A ride reading CLOSED in a shut park is not a
 * fault, it is a shut park, so the whole statement returns nothing unless `$3`
 * falls inside a published OPERATING window. Without that, Phantasialand's
 * River Quest — genuinely broken from 17:00 — would have kept its line all
 * night and into the morning.
 *
 * Parameters: `$1` uuid[] attraction ids, `$2` the park's timezone, `$3` as-of,
 * `$4` the park id.
 */
export const CURRENT_CLOSURE_GAP_SQL = `
  WITH blind AS (
    -- Read from the table that OWNS this answer, not re-derived beside it.
    --
    -- park_downtime_coverage.regime is rebuilt nightly and already applies
    -- both halves of the test (never-DOWN, and MIN_BLIND_EVIDENCE_HOURS of
    -- observation). Re-deriving it here meant two sources of truth for one
    -- question, kept in step only by a comment — the shape CLAUDE.md §4 names,
    -- where the answer has an owner and the serving path computes its own.
    --
    -- It was also expensive in the worst place. Proving NOT EXISTS (… status =
    -- 'DOWN') with no time bound means visiting every chunk this park's
    -- attractions touch on a compressed hypertable — on the park-page render
    -- path, per cache fill, inside a try/catch that turns a timeout into
    -- silence — to compute something that changes at most once in a park's
    -- lifetime.
    SELECT 1 AS ok
      FROM park_downtime_coverage c
     WHERE c."parkId" = $4::uuid
       AND c.regime = 'never_reports'
  ),
  park_open AS (
    -- Is the park open at this instant, and when does it shut? No rows
    -- short-circuits everything below, because a CLOSED ride in a shut park is
    -- a shut park.
    SELECT se."closingTime" AS closes_at
      FROM schedule_entries se
     WHERE se."parkId" = $4::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'OPERATING'
       AND se."openingTime" <= $3::timestamptz
       AND se."closingTime" >  $3::timestamptz
     ORDER BY se."closingTime" DESC
     LIMIT 1
  ),
  recent AS (
    SELECT qd."attractionId" AS aid,
           qd.timestamp      AS ts,
           qd.status::text   AS st,
           row_number() OVER (PARTITION BY qd."attractionId"
                              ORDER BY qd.timestamp DESC) AS rn
      FROM queue_data qd
     WHERE qd."attractionId" = ANY($1::uuid[])
       AND qd."queueType" = 'STANDBY'
       AND COALESCE(qd.data_source, '') NOT IN
           ('system-reconciliation', 'system-heartbeat')
       AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
       -- One operating day back is enough: the rule is "was open earlier
       -- today", and a longer window would let yesterday's OPERATING qualify a
       -- ride that has been shut since.
       AND qd.timestamp >= $3::timestamptz - INTERVAL '${MAX_GAP_HOURS} hours'
       AND qd.timestamp <= $3::timestamptz
  ),
  newest AS (SELECT * FROM recent WHERE rn = 1 AND st = 'CLOSED'),
  -- When the current CLOSED run started: the oldest CLOSED reading with no
  -- OPERATING after it.
  run_start AS (
    SELECT n.aid, MIN(r.ts) AS started_at
      FROM newest n
      JOIN recent r ON r.aid = n.aid AND r.st = 'CLOSED'
     WHERE NOT EXISTS (
       SELECT 1 FROM recent o
        WHERE o.aid = n.aid AND o.st = 'OPERATING' AND o.ts > r.ts
     )
     GROUP BY n.aid
  ),
  -- Was it OPERATING earlier the same park-local day?
  open_today AS (
    SELECT DISTINCT r.aid
      FROM recent r
      JOIN run_start s ON s.aid = r.aid
     WHERE r.st = 'OPERATING'
       AND r.ts < s.started_at
       AND (r.ts AT TIME ZONE $2)::date = (s.started_at AT TIME ZONE $2)::date
  ),
  -- Is this ride on a duty cycle rather than broken?
  --
  -- Counted over the same window and the same threshold the nightly statement
  -- uses, so live and history agree about what a fault is. Restricted to the
  -- rides actually sitting in a closure right now, which is why a per-ride
  -- scan is affordable here and is not in the historical statement.
  cycle AS (
    -- Days carrying a real GAP, which is the same triple the nightly statement
    -- recognises: OPERATING, then CLOSED, then OPERATING again.
    --
    -- Counting days with any CLOSED row instead is wrong and was: every ride
    -- shuts at closing time, so every ride scored 22 of 22 and the filter
    -- suppressed the entire signal. The tell was a share above 1.0.
    SELECT d.aid, count(DISTINCT d.op_day)::numeric AS gap_days
      FROM (
        SELECT aid, (ts AT TIME ZONE $2)::date AS op_day
          FROM (
            SELECT qd."attractionId" AS aid,
                   qd.timestamp      AS ts,
                   qd.status::text   AS st,
                   lag(qd.status::text)  OVER w AS prev_st,
                   lead(qd.status::text) OVER w AS next_st
              FROM queue_data qd
             WHERE qd."attractionId" = ANY($1::uuid[])
               AND qd."queueType" = 'STANDBY'
               AND COALESCE(qd.data_source, '') NOT IN
                   ('system-reconciliation', 'system-heartbeat')
               AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
               AND qd.timestamp >= $3::timestamptz - INTERVAL '21 days'
               AND qd.timestamp <  $3::timestamptz
            WINDOW w AS (PARTITION BY qd."attractionId" ORDER BY qd.timestamp)
          ) f
         WHERE f.st = 'CLOSED'
           AND f.prev_st = 'OPERATING'
           AND f.next_st = 'OPERATING'
      ) d
     GROUP BY d.aid
  ),
  active AS (
    SELECT e."attractionId" AS aid,
           count(*) FILTER (WHERE e.operating_minutes > 0)::numeric AS active_days
      FROM attraction_exposure_days e
     WHERE e."attractionId" = ANY($1::uuid[])
       AND e.op_day >= ($3::timestamptz - INTERVAL '21 days')::date
     GROUP BY e."attractionId"
  ),
  -- Does this ride habitually end its day before the park does?
  --
  -- Restricted to the candidates, so it is one indexed scan each. Compares each
  -- day's last OPERATING reading against that day's published closing time.
  early_end AS (
    SELECT d.aid,
           count(*)::numeric AS days,
           count(*) FILTER (
             WHERE d.last_operating < d.closes_at
                   - INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'
           )::numeric AS early_days
      FROM (
        SELECT qd."attractionId" AS aid,
               (qd.timestamp AT TIME ZONE $2)::date AS d,
               max(qd.timestamp) FILTER (WHERE qd.status = 'OPERATING')
                 AS last_operating,
               max(w.closes_at) AS closes_at
          FROM queue_data qd
          JOIN LATERAL (
            SELECT se."closingTime" AS closes_at
              FROM schedule_entries se
             WHERE se."parkId" = $4::uuid
               AND se."attractionId" IS NULL
               AND se."scheduleType" = 'OPERATING'
               AND (se."openingTime" AT TIME ZONE $2)::date
                   = (qd.timestamp AT TIME ZONE $2)::date
             LIMIT 1
          ) w ON TRUE
         WHERE qd."attractionId" = ANY($1::uuid[])
           AND qd."queueType" = 'STANDBY'
           AND COALESCE(qd.data_source, '') NOT IN
               ('system-reconciliation', 'system-heartbeat')
           AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
           AND qd.timestamp >= $3::timestamptz - INTERVAL '21 days'
           AND qd.timestamp <  $3::timestamptz
         GROUP BY 1, 2
      ) d
     WHERE d.last_operating IS NOT NULL
     GROUP BY d.aid
  ),
  -- How many rides in the PARK went CLOSED in that same minute.
  --
  -- Over $1 alone this count is whatever the caller happened to pass — one,
  -- on a ride detail page — so the filter that separates a park-wide closing
  -- from a single fault would always pass. It has to see the park.
  park_closers AS (
    SELECT date_trunc('minute', qd.timestamp) AS minute, count(*) AS closers
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
     WHERE a."parkId" = $4::uuid
       AND a.retired_at IS NULL
       AND qd."queueType" = 'STANDBY'
       AND qd.status = 'CLOSED'
       AND COALESCE(qd.data_source, '') NOT IN
           ('system-reconciliation', 'system-heartbeat')
       AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
       AND qd.timestamp >= $3::timestamptz - INTERVAL '${MAX_GAP_HOURS} hours'
       AND qd.timestamp <= $3::timestamptz
     GROUP BY 1
  )
  SELECT s.aid                                   AS "attractionId",
         s.started_at                            AS "startedAt",
         sm.closers::int                         AS "simultaneousClosers"
    FROM run_start s
    JOIN open_today o ON o.aid = s.aid
    JOIN park_closers sm ON sm.minute = date_trunc('minute', s.started_at)
    LEFT JOIN cycle cy ON cy.aid = s.aid
    LEFT JOIN active ac ON ac.aid = s.aid
    LEFT JOIN early_end ee ON ee.aid = s.aid
   CROSS JOIN park_open po
   CROSS JOIN blind
   WHERE sm.closers <= ${MAX_SIMULTANEOUS_CLOSERS}
     -- Winding down with the park is not breaking.
     AND po.closes_at >= s.started_at
         + INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'
     -- Not a duty cycle. Below the day floor there is not enough to judge and
     -- the ride is kept, same as the nightly statement.
     AND (COALESCE(ac.active_days, 0) < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR COALESCE(cy.gap_days, 0) / ac.active_days <= ${MAX_GAP_DAY_SHARE})
     -- Not a ride whose day simply ends earlier than the park's.
     AND (COALESCE(ee.days, 0) < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR ee.early_days / ee.days <= ${MAX_EARLY_END_SHARE})
     -- The same floor the historical statement applies. Without it a ride that
     -- flips CLOSED two minutes before the page renders is announced as a
     -- fault, and 27.1 % of raw gaps are exactly one poll cycle.
     AND $3::timestamptz >= s.started_at + INTERVAL '${MIN_GAP_MINUTES} minutes'
`;
