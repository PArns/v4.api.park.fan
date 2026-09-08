import { MIN_BLIND_EVIDENCE_HOURS } from "../../analytics/entities/park-downtime-coverage.entity";
import { normalizedClosingSql } from "./park-open-window.sql";
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
 *
 * The live statement applies one more, `MAX_EARLY_END_SHARE`, which this one
 * deliberately does not — see below.
 * | `MIN_GAP_MINUTES` | one poll of noise | 27.1 % of raw gaps are exactly one 5-minute cycle |
 *
 * Raw transitions over 21 days: 25 759. After all four: **3618**, over 1137
 * rides in 121 parks, with quartiles 15/20/35 minutes — close to the DOWN
 * signal's own 10/25/50 and unlike anything a scheduled closure looks like.
 *
 * ## Why the early-end filter is live-only
 *
 * `MAX_EARLY_END_SHARE` catches a ride whose DAY habitually ends before the
 * park's — Futuroscope's cinemas score 100 % against a real fault's 8 %. It
 * needs, per ride and per day, the last OPERATING reading against that day's
 * published closing time, and over a 21-day reconstruction that comparison does
 * not finish: measured at 61 s over seven days as a lateral, and past 110 s
 * over 21 even hoisted into a grouped CTE and scoped to candidates.
 *
 * It is live-only on purpose, and the asymmetry is safe today because nothing
 * published reads these rows: the profile and coverage queries filter
 * `signal = 'down'`, and so does the recovery curve. What the nightly job
 * stores for `closed_gap` is a record, not a figure.
 *
 * **This becomes a real defect the moment anything publishes `closed_gap`
 * history** — the stored rows would carry cinema noise the live line refuses to
 * show. `todo.md` carries it.
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

/**
 * How far the LIVE statement looks back for "was this ride open earlier today".
 *
 * Deliberately NOT `MAX_GAP_HOURS`, which it used to reuse. The two want
 * opposite things: that one is a ceiling on how long a gap may span and is kept
 * short to exclude an overnight reopening, while this one must cover the whole
 * operating day or the morning falls out of the window.
 *
 * Measured: 18 blind parks had 399 operating days longer than 12 hours in the
 * last three weeks, the longest running 24. Under the old 12-hour bound a ride
 * that broke at 09:15 in a park open until 23:00 dropped off the live line once
 * the outage passed twelve hours — the failure ran backwards, with the longest
 * and most certain faults disappearing first.
 *
 * 26 hours covers a 24-hour operating day with slack for the poll that opened
 * it.
 */
export const LIVE_LOOKBACK_HOURS = 26;

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
    SELECT p.id AS pid, p.timezone AS tz
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
    -- The timezone comes from blind_parks, which now carries it. This used to
    -- join parks a second time for the same column of the same row, per row of
    -- the widest scan in the statement.
    SELECT a.id AS aid, a."parkId" AS pid, b.tz AS tz,
           qd.timestamp AS ts, qd.status::text AS st
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
      JOIN blind_parks b ON b.pid = a."parkId"
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
  -- Bounded on BOTH sides, in PARK-local days, over the rides in scope.
  --
  -- It had none of the three. The lower bound alone let a replay count every
  -- operating day from $2 through to today -- the rule this codebase already
  -- wrote down after a leak of exactly that shape
  -- (reference_sql_rule_replay_windowing: "> t - interval without <= t leaks
  -- all future data"). And with no park filter it computed the figure for every
  -- attraction in the database in order to use it for the blind ones.
  --
  -- The replay is the real case; a short-window run is not. The processor's
  -- DEFAULT_WINDOW_DAYS is 30 and OUTAGE_SCAN_START_SQL can only widen it, so
  -- the "scanStart is hours old, active_days is 0-2, the filter never fires"
  -- story does not describe a run that happens -- and the bounds would not fix
  -- it if it did, because a two-day window is under MIN_DAYS_FOR_CYCLE_TEST
  -- either way. Written down because it was asserted here before it was
  -- checked.
  --
  -- Park-local because op_day is: casting $2 to ::date takes the SESSION zone,
  -- UTC in production, and the two disagree for 56 of the 91 blind parks at any
  -- instant. One day, against a gate that sits at 5. The live twin says it uses
  -- "the same window and the same threshold" -- that sentence was false in both
  -- directions until this matched it.
  --
  -- The lower bound is the NUMERATOR's own edge, not a day of slack in front
  -- of it. A slack day looked like the DST fix park_day_close needs and is not
  -- the same thing: park_day_close is JOINED to days that exist, so an unused
  -- extra day costs a row, while this one is COUNTED. Measured over 21 days
  -- across five blind parks, a single day of slack moved the nightly statement
  -- from 330 intervals over 112 rides to 370 over 120 -- and the exact bound
  -- reproduces 330/112, the pre-fix number, exactly. The whole difference had
  -- been credited to the timezone fix; all of it was the slack diluting the
  -- denominator, in the direction that publishes a timetable as a fault.
  --
  -- Taking the numerator's expression handles the DST case for free: on an
  -- ordinary day it is local_date($2), and after a shift it is whatever day the
  -- oldest reading actually landed on, because it is the same conversion.
  active AS (
    SELECT e."attractionId" AS aid,
           count(*) FILTER (WHERE e.operating_minutes > 0)::numeric AS active_days
      FROM attraction_exposure_days e
      -- Joined on e."parkId", not through attractions. The table carries the
      -- column and is indexed on ("parkId", op_day), which is this predicate
      -- exactly; going via attractions costs two more joins and forgoes that
      -- index. It also changed behaviour nobody asked for: after a merge or a
      -- hard delete, exposure rows whose attraction no longer resolves would
      -- drop out of the denominator and inflate that ride's gap share.
      JOIN blind_parks b ON b.pid = e."parkId"
     WHERE e.op_day >= ($2::timestamptz AT TIME ZONE b.tz)::date
       AND e.op_day <= ($3::timestamptz AT TIME ZONE b.tz)::date
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
     -- Not inside a declared works period. The DOWN reconstruction excludes
     -- this window and the guarantee in curated-out-of-service.util carries no
     -- signal qualifier: "inside it nothing is reported". A ride mid-rebuild
     -- cycles OPERATING/CLOSED during testing, which is exactly the shape this
     -- statement recognises, so without this it would accumulate inferred
     -- outages for the whole declared period — in the blind parks, which are
     -- the only ones it serves.
     AND NOT EXISTS (
       SELECT 1 FROM attractions ca
        WHERE ca.id = g.aid
          AND (ca.curated_out_of_service_from IS NOT NULL
               OR ca.curated_out_of_service_to IS NOT NULL)
          AND (ca.curated_out_of_service_from IS NULL
               OR (g.started_at AT TIME ZONE g.tz)::date
                  >= ca.curated_out_of_service_from)
          AND (ca.curated_out_of_service_to IS NULL
               OR (g.started_at AT TIME ZONE g.tz)::date
                  <= ca.curated_out_of_service_to)
     )
     AND g.gap_min >= ${MIN_GAP_MINUTES}
     -- Not a duty cycle. Below the day floor there is not enough to judge, and
     -- the ride is kept.
     --
     -- NULLIF, because the left arm is not a guard. SQL does not promise to
     -- evaluate OR left to right, and this CTE COALESCEs active_days to 0 two
     -- dozen lines above — so a ride whose exposure rows all carry zero
     -- operating minutes (shut for the whole window, then reopening) can reach
     -- 0 / 0 and raise. NULL divides to NULL, the comparison is NULL, and the
     -- left arm still carries the OR.
     AND (c.active_days < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR c.gap_days / NULLIF(c.active_days, 0) <= ${MAX_GAP_DAY_SHARE})
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
  WITH park_open AS (
    -- Is the park open at this instant, and when does it shut? No rows
    -- short-circuits everything below, because a CLOSED ride in a shut park is
    -- a shut park — and this CTE is CROSS JOINed, so getting it wrong silences
    -- the whole park rather than one ride.
    --
    -- Through normalizedClosingSql(), like every other query in this feature.
    -- The write-path repair has no backfill, so stored history still carries
    -- what the sources sent: a past-midnight close stamped with the opening's
    -- own calendar date. La Ronde does that every day of its season. Read raw,
    -- such a row is already "in the past" at 00:30, this CTE returns nothing,
    -- and every ride in that park loses its line for the rest of the night.
    SELECT ${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "p.timezone")} AS closes_at
      FROM schedule_entries se
      JOIN parks p ON p.id = se."parkId"
     WHERE se."parkId" = $4::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'OPERATING'
       AND se."openingTime" <= $3::timestamptz
       AND ${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "p.timezone")} > $3::timestamptz
     ORDER BY 1 DESC
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
       -- Far enough back to cover the whole operating day. open_today below
       -- still requires the OPERATING reading to fall on the same park-local
       -- day, so a longer window cannot let yesterday qualify a ride that has
       -- been shut since — the date check does that work, not the bound.
       AND qd.timestamp >= $3::timestamptz - INTERVAL '${LIVE_LOOKBACK_HOURS} hours'
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
  -- uses, so live and history agree about what a fault is.
  --
  -- The three historical CTEs from here down read run_start, not $1, and that
  -- is the difference between judging the rides in a closure and judging the
  -- whole park. Each is consumed through a LEFT JOIN against run_start alone
  -- and each groups by ride with no cross-ride term, so narrowing them changes
  -- no row that is read. Their own comments already claimed it ("restricted to
  -- the rides actually sitting in a closure right now", "restricted to the
  -- candidates") -- but the candidates are the WHOLE PARK, because
  -- park_closers needs the roster and every CTE was handed the same $1. A
  -- 96-ride park scanned 21 days of queue_data 96 times over and threw almost
  -- all of it away at a join it never reached.
  --
  -- ANY(ARRAY(...)) rather than IN (...) on purpose. The array is an InitPlan,
  -- so it stays an indexable predicate with runtime chunk exclusion on the
  -- hypertable; a semi-join would have to scan to find out there is nothing to
  -- match. Empty is the normal case and has to be the cheap one -- and it is
  -- not the rare one: run_start is empty in every ordinary open park, though
  -- in a park that has shut for the night it IS the roster again.
  -- The 21 days of readings both historical CTEs judge, read once.
  --
  -- cycle and early_end had six byte-identical predicates over the same slice
  -- of the same hypertable, so every call decompressed the same chunks twice —
  -- the shape this whole change exists to remove, reintroduced by narrowing
  -- both to the same population. Materialised here because it is referenced
  -- more than once, which is what makes it read once.
  --
  -- Keyed on open_today rather than run_start, and that is not a nicety.
  -- open_today is by construction a subset (it joins run_start), and the final
  -- select INNER JOINs it, so every ride in run_start \\ open_today has its 21
  -- days computed and then discarded at that join. The difference is the whole
  -- roster in a park that has been shut all day: run_start is every ride,
  -- open_today is empty.
  run_readings AS (
    SELECT qd."attractionId" AS aid,
           qd.timestamp      AS ts,
           qd.status::text   AS st,
           lag(qd.status::text)  OVER w AS prev_st,
           lead(qd.status::text) OVER w AS next_st,
           lead(qd.timestamp)    OVER w AS next_ts
      FROM queue_data qd
     WHERE qd."attractionId" = ANY(ARRAY(SELECT aid FROM open_today))
       AND qd."queueType" = 'STANDBY'
       AND COALESCE(qd.data_source, '') NOT IN
           ('system-reconciliation', 'system-heartbeat')
       AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
       AND qd.timestamp >= $3::timestamptz - INTERVAL '21 days'
       AND qd.timestamp <  $3::timestamptz
    WINDOW w AS (PARTITION BY qd."attractionId" ORDER BY qd.timestamp)
  ),
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
          FROM run_readings f
         WHERE f.st = 'CLOSED'
           AND f.prev_st = 'OPERATING'
           AND f.next_st = 'OPERATING'
           -- The SAME bounds raw_gaps applies, and the comment above claimed
           -- these were already here. Without them a ride that shuts at night
           -- and opens next morning satisfies the triple, so every ordinary
           -- operating day counts as a gap day and the ratio converges on 1.0 —
           -- which would suppress the live line for exactly the rides that have
           -- been reliably open. Measured today the two counts agree on all
           -- 2132 rides, because overnight rows are broken up by our own
           -- bookkeeping writes; that is the data being kind, not the query
           -- being right.
           AND f.next_ts < f.ts + INTERVAL '${MAX_GAP_HOURS} hours'
           AND (f.next_ts AT TIME ZONE $2)::date = (f.ts AT TIME ZONE $2)::date
      ) d
     GROUP BY d.aid
  ),
  active AS (
    SELECT e."attractionId" AS aid,
           count(*) FILTER (WHERE e.operating_minutes > 0)::numeric AS active_days
      FROM attraction_exposure_days e
     WHERE e."attractionId" = ANY(ARRAY(SELECT aid FROM open_today))
       -- Park-local, because op_day is. Casting $3 - 21 days to ::date takes
       -- the SESSION zone (UTC in production), and the two answers disagree
       -- for 56 of the 91 blind parks at any given instant — one operating day
       -- more or less. That day drives both the MIN_DAYS_FOR_CYCLE_TEST gate
       -- and the gap_days/active_days ratio, and the gate sits at 5, so a
       -- ride can cross it on the cast alone.
       --
       -- The numerator's own edge, converted the same way, rather than a fixed
       -- offset in front of it. The cycle CTE reads readings cut at $3 minus 21
       -- days in UTC and buckets them park-local, so on an ordinary day its
       -- oldest gap day is local day 21 and after a DST shift it is 22 -- and
       -- this expression is whichever of those it actually was.
       --
       -- Writing "- 22" instead looked like the same fix park_day_close needs
       -- and is not: that CTE is JOINED to days that exist, so an unused extra
       -- day costs a row, while this one is COUNTED. Measured on the nightly
       -- twin, one day of slack moved 330 intervals over 112 rides to 370 over
       -- 120, all of it dilution in the direction that publishes a timetable
       -- as a fault.
       AND e.op_day >= (($3::timestamptz - INTERVAL '21 days')
                        AT TIME ZONE $2)::date
       -- And an upper bound, which this had no more than its nightly twin did.
       -- $3 is now() in production so nothing lies beyond it today, but a
       -- pinned as-of — a spec, a replay — would count operating days from
       -- after the instant being judged.
       AND e.op_day <= ($3::timestamptz AT TIME ZONE $2)::date
     GROUP BY e."attractionId"
  ),
  -- When the park shut, once per day it published hours for.
  --
  -- This used to be a LATERAL inside early_end, and that put one schedule
  -- lookup on every single queue_data row it read: measured at Alton Towers,
  -- 28 485 executions of one bitmap index scan, 21.5 s of a 24 s statement,
  -- for a table with at most 23 rows to offer. The park's closing time does
  -- not vary by ride or by reading, so it is resolved once per day here and
  -- joined.
  --
  -- Verified equivalent rather than assumed: the LATERAL took LIMIT 1 with no
  -- ordering and the caller wrapped it in max(), which are the same value only
  -- while a park-day has one entry. Measured 2026-09-08 over 365 days: all
  -- 36 226 park-days across every park have exactly one OPERATING entry, and
  -- 15 589 of them are in the blind parks this statement serves. Not one
  -- park-day anywhere has two. max() is then the deterministic reading of what
  -- LIMIT 1 was picking arbitrarily.
  --
  -- If that ever stops holding, max() is the wrong answer rather than a
  -- different one: a park publishing a morning and an evening block would have
  -- every morning-only ride score as ending early. park-open-window.sql builds
  -- the disjoint-union flattener for exactly that, and todo.md carries the
  -- reasons this statement does not use it yet.
  --
  -- Bounded by park-local DATE, not by timestamp. A timestamp bound of $3 would
  -- drop today's entry whenever the page renders before the park opens, which
  -- is precisely when a ride's morning readings are being judged against it.
  --
  -- The lower bound is 22 days for a 21-day window of readings, and the extra
  -- day is not slack. The readings are cut at $3 - 21 days in UTC, but their
  -- day is taken in the park's zone, so a row at the edge of the window lands
  -- on local day 22 whenever a DST shift has moved it across local midnight.
  -- Measured over every half hour of a winter in four park zones: 42 such
  -- instants each -- a one-hour band on each of the 21 days after the shift,
  -- in Europe/Berlin, Europe/London, America/New_York and Australia/Sydney
  -- alike. The join is an INNER one, so on the 21-day bound those readings are
  -- dropped rather than counted, which moves early_days/days in the direction
  -- of a ride looking more regular than it is. A day nothing references costs
  -- one row of a CTE that has at most 23 (the bounds are inclusive on both
  -- ends: local_date($3) - 22 through local_date($3)).
  park_day_close AS (
    -- Normalized for the same reason park_open is: a raw past-midnight close
    -- would make every day look like it ended early, and this feeds the filter
    -- that decides a ride is on a timetable.
    SELECT (se."openingTime" AT TIME ZONE $2)::date AS d,
           max(${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "$2")})
             AS closes_at
      FROM schedule_entries se
     WHERE se."parkId" = $4::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'OPERATING'
       -- Both guards are defensive and both are no-ops today: 0 of 10 600
       -- OPERATING park rows in the blind parks carry a null close.
       --
       -- The closing guard is not a free win in either direction, and the
       -- comment used to claim it was. Without it a null-close day joins with
       -- a NULL closes_at, counts in the days denominator and can never count
       -- as early, biasing the ratio towards publishing a timetable as a fault.
       -- With it the day leaves the sample entirely, which can drop a ride
       -- under MIN_DAYS_FOR_CYCLE_TEST and switch the early-end filter off
       -- altogether -- the filter that separates a cinema at 100 % from a
       -- broken coaster at 8 %. Dropping is chosen because a day with no
       -- published close cannot answer the question at all, and because
       -- parkOpenWindowCtes chose the same; it is not chosen because it is
       -- harmless.
       AND se."closingTime" IS NOT NULL
       -- The opening guard is separate and belongs here rather than being left
       -- to NULL-propagation through the timestamp bounds below, whose stated
       -- job is index pruning. A row with a null opening would be dropped by a
       -- predicate documented as never excluding anything.
       AND se."openingTime" IS NOT NULL
       -- The local-date pair is the authority; this pair only lets an index
       -- prune. Wrapping openingTime in AT TIME ZONE ... ::date is not
       -- sargable, so without it the scan reads every OPERATING row the park
       -- has ever published (276 at Alton Towers, 616 at the worst park, 189
       -- on average) to keep at most 23 — and grows with how far ahead the
       -- park publishes, which has nothing to do with this question.
       --
       -- Two days of slack on each side of what the date pair can select, so
       -- it can never be the predicate that excludes a row: no zone is further
       -- than 14 hours from UTC, and se.date is deliberately not used here
       -- because 885 rows carry a date that does not match their opening's
       -- park-local day (npm run repair:schedule-dates).
       AND se."openingTime" >= $3::timestamptz - INTERVAL '24 days'
       AND se."openingTime" <  $3::timestamptz + INTERVAL '2 days'
       AND (se."openingTime" AT TIME ZONE $2)::date
           >= ($3::timestamptz AT TIME ZONE $2)::date - 22
       AND (se."openingTime" AT TIME ZONE $2)::date
           <= ($3::timestamptz AT TIME ZONE $2)::date
     GROUP BY 1
  ),
  -- Does this ride habitually end its day before the park does?
  --
  -- Compares each day's last OPERATING reading against that day's published
  -- closing time. The join to park_day_close is an INNER one, as the LATERAL
  -- was: a day the park published no hours for cannot say whether a ride
  -- finished early, so it is not counted either way.
  early_end AS (
    SELECT q.aid,
           count(*)::numeric AS days,
           count(*) FILTER (
             WHERE q.last_operating < pdc.closes_at
                   - INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'
           )::numeric AS early_days
      FROM (
        -- The same slice cycle reads, from run_readings, so the hypertable is
        -- visited once for both.
        SELECT r.aid,
               (r.ts AT TIME ZONE $2)::date AS d,
               max(r.ts) FILTER (WHERE r.st = 'OPERATING') AS last_operating
          FROM run_readings r
         GROUP BY 1, 2
      ) q
      JOIN park_day_close pdc ON pdc.d = q.d
     WHERE q.last_operating IS NOT NULL
     GROUP BY q.aid
  ),
  -- How many rides in the PARK went CLOSED in that same minute.
  --
  -- Over $1 alone this count is whatever the caller happened to pass — one,
  -- on a ride detail page — so the filter that separates a park-wide closing
  -- from a single fault would always pass. It has to see the park.
  --
  -- The last heavy CTE that had no guard of its own. It cannot be narrowed to
  -- the candidates for the reason above, and unlike the other three it is an
  -- INNER JOIN input in the FROM clause rather than a correlated one — so
  -- whether it runs at all when there is nothing to join against comes down to
  -- which side the planner builds first. The EXISTS makes the empty case free
  -- unconditionally, and empty is the normal case: no ride sitting in a closure
  -- means no row can come out of this statement anyway.
  park_closers AS (
    SELECT date_trunc('minute', qd.timestamp) AS minute, count(*) AS closers
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
     WHERE EXISTS (SELECT 1 FROM open_today)
       AND a."parkId" = $4::uuid
       AND a.retired_at IS NULL
       AND qd."queueType" = 'STANDBY'
       AND qd.status = 'CLOSED'
       AND COALESCE(qd.data_source, '') NOT IN
           ('system-reconciliation', 'system-heartbeat')
       AND NOT COALESCE(qd.is_heartbeat, qd."lastUpdated" = qd.timestamp)
       AND qd.timestamp >= $3::timestamptz - INTERVAL '${LIVE_LOOKBACK_HOURS} hours'
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
   -- Only in a park whose feed never says DOWN, and this has to be an EXISTS.
   --
   -- Read from the table that OWNS this answer, not re-derived beside it.
   -- park_downtime_coverage.regime is rebuilt nightly and already applies both
   -- halves of the test (never-DOWN, and MIN_BLIND_EVIDENCE_HOURS of
   -- observation). Re-deriving it here meant two sources of truth for one
   -- question, kept in step only by a comment — the shape CLAUDE.md §4 names.
   --
   -- It used to be a CTE brought in with CROSS JOIN, and the service said
   -- beside the call site that this "costs one indexed lookup" in a park that
   -- reports DOWN. A CROSS JOIN is a join node: the planner has no reason to
   -- evaluate that arm first, so every historical CTE below ran to completion
   -- and the test was applied last. Measured on production, 70 % of this
   -- statement's calls were for the 122 parks that cannot produce a row, and
   -- the statement was 79 % of all database CPU.
   --
   -- As an EXISTS referencing only a parameter it is uncorrelated, so
   -- PostgreSQL evaluates it once as an InitPlan, the qual becomes a
   -- pseudoconstant, and it emits a One-Time Filter that never demands the
   -- subtree. Measured at Europa-Park (regime 'reports'): 939.8 ms with the
   -- CROSS JOIN against 2.3 ms with the EXISTS, 132 nodes never executed.
   -- Alton Towers, which is blind, is unchanged at 531 ms — it still has to do
   -- the work.
   --
   -- Keep it a bare EXISTS on $4 alone. Correlate it with anything from the
   -- rows above and it stops being an InitPlan, which is the whole mechanism.
   -- The same argument a second time, and it is the one that costs most.
   --
   -- park_open is CROSS JOINed just above, so a shut park already returns
   -- nothing — but a CROSS JOIN is a join node, not a guard, which is the whole
   -- point of the paragraph below. And a shut park is the expensive case: every
   -- ride's newest reading is CLOSED, so run_start IS the roster (45 of 54 at
   -- Alton Towers) and the three historical CTEs scan 21 days of queue_data for
   -- all of them, to be discarded by an empty join.
   --
   -- That was the peak the old statement was measured at: Futuroscope read
   -- 31 655 ms at closing time and 2.8 ms the same evening, and it was the
   -- park having shut rather than any cache. As a pseudoconstant qual:
   -- Alton Towers 586.4 ms to 10.2, Phantasialand 707.6 to 3.8, both shut for
   -- the night, 127 nodes never executed. SeaWorld Orlando, still open, is
   -- unchanged at ~20 ms because it still has the work to do.
   --
   -- Redundant with the CROSS JOIN by construction, never instead of it: an
   -- empty park_open makes the join produce nothing and this produce false.
   -- Cheapest first, which is the same lesson one level down. The regime test
   -- is a single primary-key probe and it rejects the 122 parks that make 70 %
   -- of the calls; park_open is a schedule scan plus normalizedClosingSql over
   -- what it finds. Both are pseudoconstants either way, so this only decides
   -- which InitPlan runs first — but writing the expensive one in front of the
   -- cheap one is the shape this whole change exists to remove.
   WHERE EXISTS (
           SELECT 1
             FROM park_downtime_coverage c
            WHERE c."parkId" = $4::uuid
              AND c.regime = 'never_reports'
         )
     AND EXISTS (SELECT 1 FROM park_open)
     AND sm.closers <= ${MAX_SIMULTANEOUS_CLOSERS}
     -- Winding down with the park is not breaking.
     AND po.closes_at >= s.started_at
         + INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'
     -- Not a duty cycle. Below the day floor there is not enough to judge and
     -- the ride is kept, same as the nightly statement — and NULLIF for the
     -- same reason: the left arm is not a guard, because SQL does not promise
     -- to evaluate OR left to right. A ride whose exposure rows all carry zero
     -- operating minutes reaches 0 / 0, and one raised error here costs EVERY
     -- ride in the park its closure line, not just this one — addClosureGaps
     -- catches and returns.
     AND (COALESCE(ac.active_days, 0) < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR COALESCE(cy.gap_days, 0)
             / NULLIF(ac.active_days, 0) <= ${MAX_GAP_DAY_SHARE})
     -- Not a ride whose day simply ends earlier than the park's. No NULLIF: ee
     -- is a GROUP BY over count(*), so a row that exists has days >= 1, and an
     -- absent row divides NULL by NULL.
     AND (COALESCE(ee.days, 0) < ${MIN_DAYS_FOR_CYCLE_TEST}
          OR ee.early_days / ee.days <= ${MAX_EARLY_END_SHARE})
     -- The same floor the historical statement applies. Without it a ride that
     -- flips CLOSED two minutes before the page renders is announced as a
     -- fault, and 27.1 % of raw gaps are exactly one poll cycle.
     AND $3::timestamptz >= s.started_at + INTERVAL '${MIN_GAP_MINUTES} minutes'
`;
