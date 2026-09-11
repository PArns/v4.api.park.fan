import { MIN_BLIND_EVIDENCE_HOURS } from "../../analytics/entities/park-downtime-coverage.entity";
import {
  normalizedClosingSql,
  parkOpenWindowCtes,
} from "./park-open-window.sql";
import { RECONCILIATION_SOURCE } from "./source-absent-status.util";
import { HEARTBEAT_SOURCE } from "./outage-rows.sql";
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

/**
 * The predicate that separates an observation from our own bookkeeping.
 *
 * Two halves, and both are load-bearing. `data_source` catches the rows the
 * reconciliation and heartbeat writers stamp as theirs. `is_heartbeat` catches
 * the ones they CARRY, which is the same trap one level down:
 * `writeHourlyHeartbeats` copies the previous row's `data_source`, so a carried
 * CLOSED is indistinguishable from an observed one by source alone.
 *
 * That second half decides what a gap IS. The interval is recognised by an
 * exact OPERATING/CLOSED/OPERATING triple, so one carried row in the middle
 * turns `next_st` into CLOSED and drops the gap entirely — which silently
 * truncated this population at ~65 minutes until it was found. NULL means
 * "written before the column existed" and falls back to the old heuristic.
 *
 * It lived as five hand-copied blocks across the two statements, with that
 * explanation attached to exactly one of them. A predicate whose reason is
 * written down once and applied five times is the drift this file keeps
 * writing shared helpers to prevent.
 *
 * The two source names come from the writers' own exports rather than being
 * typed again here. Hard-coding them re-created, one level down, exactly the
 * drift this helper was extracted to end: rename either writer's data_source
 * and the predicate silently stops matching.
 *
 * Parenthesised, so it composes. Unbracketed it is `A AND B`, and the first
 * caller writing `NOT ${'${observedReadingsSql("qd")}'}` would negate only A —
 * silently admitting exactly the carried rows the paragraph above says
 * truncated this population at ~65 minutes.
 */
export function observedReadingsSql(alias: string): string {
  return `(COALESCE(${alias}.data_source, '') NOT IN
            ('${RECONCILIATION_SOURCE}', '${HEARTBEAT_SOURCE}')
        AND NOT COALESCE(${alias}.is_heartbeat, ${alias}."lastUpdated" = ${alias}.timestamp))`;
}

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
 * How far back both share tests look, in days.
 *
 * The window is not a period of interest; it is a **sample size**. Both
 * `MAX_GAP_DAY_SHARE` and `MAX_EARLY_END_SHARE` are shares over a ride's
 * OPERATING days, so what a calendar window really decides is how many of those
 * the denominator gets — and a park open at weekends contributes a third of what
 * a park open daily does over the same span.
 *
 * It was 21 days live against the nightly job's whole scan window
 * (`DEFAULT_WINDOW_DAYS` 30, wider when a running outage pushes the scan back),
 * and the live comment claimed the two matched. Measured over the blind parks
 * on 2026-09-08, of 2176 rides judged by both windows **54 disagree** — 32
 * dropped at 21 days that 30 days keeps, 22 the other way — so live and history
 * could publish opposite verdicts about one ride.
 *
 * 30, and the decisive number is not that disagreement. It is
 * `MIN_DAYS_FOR_CYCLE_TEST`: **49 rides hold fewer than five operating days in
 * 21 and at least five in 30**, so for them the duty-cycle filter does not fire
 * at all today and their timetable is published as faults — the exact failure
 * the filter exists to prevent, and larger than the disagreement in either
 * direction. A 21-day window holds only 11 to 14 operating days in a seasonal
 * park (LEGOLAND California's Dragon Coaster: the same 9 gap days, a denominator
 * of 14 against 23), so the short window is harshest on precisely the parks with
 * the thinnest calendar. Rides with under ten samples fall from 500 to 421.
 *
 * The threshold survives the move, which had to be checked because
 * `MAX_GAP_DAY_SHARE` was calibrated on a 21-day measurement. The share
 * distribution has the same shape in both windows — mass at 0.2–0.4, and the
 * cliff between the 0.4 bucket (269 rides at 21 days, 216 at 30) and the 0.5
 * bucket (90 either way). 0.5 still sits in the gap.
 *
 * The median ride barely notices: 20 operating days at 21, 28 at 30. The whole
 * gain lands where the sample was too thin to mean anything.
 */
export const CYCLE_WINDOW_DAYS = 30;

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
  WITH ${parkOpenWindowCtes()},
  blind_parks AS (
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
    -- Through park_tz rather than a second scan of parks, now that
    -- parkOpenWindowCtes() is in this statement for the operating day. It
    -- already applies both halves of what this WHERE used to spell out again
    -- ($1 and wiki_entity_id) plus timezone IS NOT NULL — which was implicit
    -- here anyway: AT TIME ZONE NULL is strict, so a park without a zone
    -- produced a NULL op_day and every one of its rows fell out at the
    -- same-day comparison below. Two copies of one park filter is the drift
    -- this file keeps extracting helpers to prevent.
    SELECT z.park_id AS pid, z.tz AS tz
      FROM park_tz z
     WHERE NOT EXISTS (
         SELECT 1 FROM queue_data d
           JOIN attractions da ON da.id = d."attractionId"
          WHERE da."parkId" = z.park_id
            AND d."queueType" = 'STANDBY'
            AND d.status = 'DOWN'
       )
       AND COALESCE((
         SELECT SUM(ed.operating_minutes) / 60.0
           FROM attraction_exposure_days ed
           JOIN attractions ea ON ea.id = ed."attractionId"
          WHERE ea."parkId" = z.park_id
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
       AND ${observedReadingsSql("qd")}
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
  -- The operating day of each edge of a candidate gap, from the WINDOW that
  -- contains it rather than from the calendar.
  --
  -- The operating day of anything inside a window is the park-local date the
  -- window OPENED on — park-open-window.sql §3, which the two DOWN statements
  -- of this same nightly job already key on. Read off the calendar instead, a
  -- park that closes after midnight has its evening split in two: the 23:30
  -- reading lands on one date and the 00:30 reading on the next, the same-day
  -- test below fails, and the gap is dropped. La Ronde does that every day of
  -- its season, and it is the case the comment on park_open in the live
  -- statement already names.
  --
  -- Three things this is deliberately NOT:
  --
  -- 1. **Not an INNER join.** A gap outside every published window keeps the
  --    calendar day it had, through the COALESCE. An inner join would silently
  --    narrow the population to rides that broke inside opening hours — a
  --    change to what this statement measures, which is a different question
  --    from where its days come from.
  -- 2. **Not a second window definition.** win comes from
  --    parkOpenWindowCtes(), so the closing-time repair, the disjoint-union
  --    flattening and the day anchoring are inherited rather than rewritten.
  -- 3. **Not on the wide scan.** It hangs off the candidate transitions
  --    (~25 000 over 21 days), not off src, which reads every STANDBY reading
  --    the blind parks produced in the window. The st/prev_st/next_st filters
  --    are quals on the outer side of both LEFT JOINs, so they still prune
  --    first.
  --
  -- win is disjoint per park, so each join matches at most one row. Two
  -- windows CAN share an op_day (a park publishing a morning and an evening
  -- block, which the flattener merges only when they overlap) — and comparing
  -- the DAY rather than the window identity is what makes that one operating
  -- day, which is the rule as written.
  --
  -- ## What the fallback does at a window's edge, and why it is left there
  --
  -- An edge outside every window takes the calendar day, so a gap that STARTS
  -- inside opening hours and ENDS after the close is judged by comparing the
  -- start's operating day against the end's calendar day. In an ordinary park
  -- those two are the same date and the gap is kept — which is exactly what
  -- the old code did, by coincidence rather than by rule. In a park that
  -- closes after midnight they differ, so a gap opening at 01:50 and
  -- recovering at 02:10 past a 02:00 close is now dropped where the old
  -- comparison kept it.
  --
  -- Deliberate, and in the direction this file always takes: the ride shut ten
  -- minutes before the end of the operating day and "came back" after it,
  -- which is the shape MIN_PARK_MINUTES_LEFT refuses live. Withholding a
  -- doubtful fault beats publishing a timetable as one.
  --
  -- The alternative was to define the operating day as [opens_at, NEXT
  -- window's opens_at) rather than [opens_at, closes_at), which would have
  -- kept it — and would also have admitted an ordinary park's overnight
  -- closure (shut 17:50, an OPERATING reading at 03:50, ten hours, inside
  -- MAX_GAP_HOURS), i.e. precisely what the same-day filter exists to remove.
  -- Containment is the conservative half of that trade.
  --
  -- The value leaves as "startOpDay". Nothing reads that column for a closure
  -- gap today — the processor builds its starts map from statement 1 only —
  -- but statement 1's own startOpDay IS keyed into attraction_exposure_days,
  -- whose op_day has always come from win, and this is the same quantity in
  -- the same units as that one. A calendar day here was a second notion of
  -- "the day of an outage" in a job that writes both.
  gap_edges AS (
    SELECT s.aid, s.pid, s.tz, s.ts, s.next_ts,
           COALESCE(wo.op_day, (s.ts AT TIME ZONE s.tz)::date)
             AS start_op_day,
           COALESCE(wb.op_day, (s.next_ts AT TIME ZONE s.tz)::date)
             AS end_op_day
      FROM seq s
      LEFT JOIN win wo
        ON wo.park_id = s.pid
       AND s.ts >= wo.opens_at
       AND s.ts <  wo.closes_at
      LEFT JOIN win wb
        ON wb.park_id = s.pid
       AND s.next_ts >= wb.opens_at
       AND s.next_ts <  wb.closes_at
     WHERE s.st = 'CLOSED'
       AND s.prev_st = 'OPERATING'
       AND s.next_st = 'OPERATING'
       AND s.next_ts IS NOT NULL
       AND s.next_ts < s.ts + INTERVAL '${MAX_GAP_HOURS} hours'
  ),
  raw_gaps AS (
    SELECT aid, pid, tz,
           ts        AS started_at,
           next_ts   AS ended_at,
           start_op_day                              AS op_day,
           EXTRACT(HOUR FROM ts AT TIME ZONE tz)::int AS closed_hour,
           EXTRACT(EPOCH FROM (next_ts - ts)) / 60.0  AS gap_min
      FROM gap_edges
     -- Back the same operating day. A park shutting does not reopen.
     WHERE start_op_day = end_op_day
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
  -- The lowest operating day the numerator can reach, resolved ONCE per park.
  --
  -- A grouped CTE rather than a scalar subquery in the WHERE below, and that is
  -- not tidiness: correlated on the park, it would be a SubPlan re-executed for
  -- every attraction_exposure_days row the scan touches — roughly 6800 a day —
  -- against a CTE the planner cannot index. This file has paid that bill twice
  -- already and written both receipts a few lines apart: the blind-park check
  -- at 70 s as a correlated NOT EXISTS, and the operating-day count past two
  -- minutes as a correlated subquery inside cycle. Same mistake, same fix.
  --
  -- Cheap here: blind_parks LEFT JOIN win is about 91 parks against the
  -- window's schedule rows, grouped straight back down to one row per park.
  --
  -- LEFT, so a park that published no hours in the window still gets a floor.
  -- An inner join would drop it from this CTE, the join below would drop its
  -- exposure rows with it, and every one of its rides would sit at
  -- active_days = 0 — which passes the duty-cycle arm unconditionally and
  -- stores its whole history as faults.
  --
  -- Why from_day is two candidates with the lowest winning. op_day stopped
  -- being local_date($2) when raw_gaps moved onto the window's opening date:
  -- in a park that closes after midnight a gap read just after $2 can carry
  -- the PREVIOUS local day, so a bare local_date($2) floor would exclude an
  -- operating day the numerator counted and push the gap-share ratio up — the
  -- direction that suppresses a real fault as a duty cycle. The numerator has
  -- two sources for a day: a reading inside a window contributes that window's
  -- op_day (and the earliest window a reading at or after $2 can fall in is the
  -- earliest one still running at $2, MIN(op_day) FILTER closes_at > $2), a
  -- reading outside every window contributes its calendar day (earliest
  -- local_date($2)). For every park that closes before midnight the two are
  -- the same date, so the bound is unchanged in value and the measured 330/112
  -- stands.
  --
  -- LEAST, not the filtered MIN alone: when the park is shut at $2 the earliest
  -- window still to come opens LATER than $2, and its op_day would move the
  -- floor forward rather than back. And COALESCE inside, redundant in
  -- PostgreSQL's NULL-skipping LEAST but kept: the same function returns NULL
  -- on a NULL argument in other dialects, and a floor that silently became
  -- NULL would drop every denominator row and pass the gap-share test for
  -- every ride — too quiet a failure to rest on which engine reads the word.
  active_floor AS (
    SELECT b.pid,
           LEAST(
             ($2::timestamptz AT TIME ZONE b.tz)::date,
             COALESCE(
               MIN(w.op_day) FILTER (WHERE w.closes_at > $2::timestamptz),
               ($2::timestamptz AT TIME ZONE b.tz)::date
             )
           ) AS from_day
      FROM blind_parks b
      LEFT JOIN win w ON w.park_id = b.pid
     GROUP BY b.pid, b.tz
  ),
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
      JOIN active_floor f ON f.pid = e."parkId"
     -- The SCAN window, which is what this statement's numerator spans.
     --
     -- One rule governs both statements: the denominator covers the same span
     -- as the gaps it judges. Live that is CYCLE_WINDOW_DAYS, because
     -- run_readings is; here it is scanStart to asOf, because raw_gaps is. They
     -- coincide on an ordinary run (DEFAULT_WINDOW_DAYS is 30) and diverge only
     -- when a running outage pushes scanStart back -- exactly when this
     -- statement has older gaps to judge and needs the operating days to judge
     -- them by.
     --
     -- Capping this at CYCLE_WINDOW_DAYS instead, to make the two spans equal
     -- by fiat, was a regression: a ride whose duty cycle ran 60 to 31 days ago
     -- and has been shut since gets active_days = 0, the
     -- MIN_DAYS_FOR_CYCLE_TEST arm passes unconditionally, and every one of its
     -- historical gaps is stored as a fault. The equal-span rule is the real
     -- invariant; equal LENGTHS were a proxy for it that breaks whenever the
     -- inputs differ.
     --
     -- The lower bound is active_floor.from_day, the numerator's own lowest
     -- reachable operating day, resolved once per park above — see that CTE for
     -- why it is two candidates and not a bare local_date($2).
     WHERE e.op_day >= f.from_day
       -- Both edges from the numerator's own instants, for the reason the
       -- lower one carries. src reads qd.timestamp < $3, so its last possible
       -- day is the local day of the instant just before $3 -- which is the
       -- day BEFORE local_date($3) whenever $3 lands on that park's local
       -- midnight. Across 24 zones and a 91-park sweep some park sits there
       -- routinely, and counting a day the numerator cannot reach is the same
       -- ~4.5 % dilution the slack day was, in the same direction.
       --
       -- This edge needs no window treatment, unlike the lower one. An
       -- instant's op_day is the date of a window that opened at or BEFORE it,
       -- so it can only ever be at or below that instant's local date — the
       -- bound can therefore not exclude a day the numerator reaches, whatever
       -- the park's closing hour. It can be one day loose in a wrap park, and
       -- loose here dilutes rather than suppresses, which is the safe side of
       -- this particular ratio.
       AND e.op_day <= (($3::timestamptz - INTERVAL '1 microsecond')
                        AT TIME ZONE b.tz)::date
     GROUP BY e."attractionId"
  ),
  cycle AS (
    -- The numerator over the same span as the denominator above -- which is
    -- the scan window for both, so no filter is needed here. Bounding this to
    -- CYCLE_WINDOW_DAYS while raw_gaps still spanned the scan left old gaps
    -- divided by a recent-only denominator.
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
          -- g.op_day, not a third conversion of the same instant. It is the
          -- operating day the gap belongs to, and after the fix above the two
          -- differ for exactly the parks this change is about: a 00:30 gap in
          -- a midnight-wrap park is part of the previous day's operation, so a
          -- works period declared for that day covers it.
          AND (ca.curated_out_of_service_from IS NULL
               OR g.op_day >= ca.curated_out_of_service_from)
          AND (ca.curated_out_of_service_to IS NULL
               OR g.op_day <= ca.curated_out_of_service_to)
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
    -- a shut park. It is CROSS JOINed into open_today, so getting it wrong
    -- silences the whole park rather than one ride.
    --
    -- Through normalizedClosingSql(), like every other query in this feature.
    -- The write-path repair has no backfill, so stored history still carries
    -- what the sources sent: a past-midnight close stamped with the opening's
    -- own calendar date. La Ronde does that every day of its season. Read raw,
    -- such a row is already "in the past" at 00:30, this CTE returns nothing,
    -- and every ride in that park loses its line for the rest of the night.
    -- $2, not a join to parks. It used to join for parks.timezone, which is a
    -- second source for a value the caller already passed -- and park_day_close
    -- below normalizes with $2, so one park's day end was being resolved in two
    -- zones by two CTEs of the same statement. Both callers pass the park's own
    -- timezone through different load paths, so the day they disagree nothing
    -- would detect it.
    SELECT ${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "$2")} AS closes_at
      FROM schedule_entries se
     WHERE se."parkId" = $4::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'OPERATING'
       AND se."openingTime" <= $3::timestamptz
       AND ${normalizedClosingSql('se."openingTime"', 'se."closingTime"', "$2")} > $3::timestamptz
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
       AND ${observedReadingsSql("qd")}
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
  -- How many rides in the PARK went CLOSED in that same minute.
  --
  -- Over $1 alone this count is whatever the caller happened to pass — one,
  -- on a ride detail page — so the filter that separates a park-wide closing
  -- from a single fault would always pass. It has to see the park.
  --
  -- It cannot be narrowed to the candidates for the reason above. The EXISTS
  -- makes the empty case free rather than leaving it to join order, and empty
  -- is the normal case: no ride sitting in a closure means no row can come out
  -- of this statement at all.
  --
  -- Gated on run_start rather than open_today, and defined ABOVE it, because
  -- open_today applies this CTE's threshold. Gating it on open_today would be
  -- circular, which is the only reason it used to sit below.
  park_closers AS (
    SELECT date_trunc('minute', qd.timestamp) AS minute, count(*) AS closers
      FROM queue_data qd
      JOIN attractions a ON a.id = qd."attractionId"
     WHERE EXISTS (SELECT 1 FROM run_start)
       AND a."parkId" = $4::uuid
       AND a.retired_at IS NULL
       AND qd."queueType" = 'STANDBY'
       AND qd.status = 'CLOSED'
       AND ${observedReadingsSql("qd")}
       AND qd.timestamp >= $3::timestamptz - INTERVAL '${LIVE_LOOKBACK_HOURS} hours'
       AND qd.timestamp <= $3::timestamptz
     GROUP BY 1
  ),
  -- Was it OPERATING earlier the same park-local day?
  --
  -- The two per-ride gates below used to sit in the final WHERE, after the
  -- three historical CTEs had read the whole window for every ride that got
  -- this far.
  -- Both are functions of s.started_at and po.closes_at alone, so they belong
  -- where they can still remove a ride cheaply. It is the same cheap-test-last
  -- shape as the two pseudoconstants, one level down, and it is worst at the
  -- moment a blind park is closing: every ride winds down, enters run_start
  -- and open_today, and has the window materialised for it a moment before
  -- MIN_PARK_MINUTES_LEFT throws it away.
  open_today AS (
    SELECT DISTINCT r.aid
      FROM recent r
      JOIN run_start s ON s.aid = r.aid
      CROSS JOIN park_open po
      -- The most selective gate in the statement, and it used to sit in the
      -- final WHERE below three CTEs that had already read the window for every
      -- ride. It is the PR's own headline scenario: Phantasialand flips 40
      -- rides to CLOSED at 18:10 with the park open until 20:00, so the two
      -- duration gates pass, all 40 are materialised, and every row is thrown
      -- away at closers = 40. Materialisation happens for the whole CTE the
      -- moment it is first demanded, so no join order rescues it.
      JOIN park_closers sm
        ON sm.minute = date_trunc('minute', s.started_at)
       AND sm.closers <= ${MAX_SIMULTANEOUS_CLOSERS}
     WHERE r.st = 'OPERATING'
       AND r.ts < s.started_at
       -- STILL THE CALENDAR DAY, where the nightly twin now takes the day from
       -- the window that contains the reading. A park closing after midnight
       -- therefore keeps this gate shut all night: park_open normalizes and
       -- returns a row at 00:30, and this line throws the ride out anyway
       -- because its last OPERATING reading carries yesterday's date. La Ronde,
       -- every night of its season.
       --
       -- Not fixed here on purpose, and the reason is not the two lines. This
       -- statement needs the operating day of an ARBITRARY instant across 30
       -- days rather than of now, so park_day_close has to carry the opening
       -- and open_today, cycle and early_end all move with it — inside the
       -- statement that measured 79 % of the database's CPU, whose plan rests
       -- on InitPlans and one materialised CTE. That is an EXPLAIN ANALYZE
       -- against real data, not an edit. PAR-129 carries it, with the
       -- measurements it owes.
       AND (r.ts AT TIME ZONE $2)::date = (s.started_at AT TIME ZONE $2)::date
       -- Winding down with the park is not breaking.
       AND po.closes_at >= s.started_at
           + INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'
       -- One poll of noise is a reading, not an outage. 27.1 % of raw gaps are
       -- exactly one 5-minute cycle.
       AND $3::timestamptz >= s.started_at
           + INTERVAL '${MIN_GAP_MINUTES} minutes'
  ),
  -- Is this ride on a duty cycle rather than broken?
  --
  -- The same threshold, the same triple, and now the same window: both take
  -- CYCLE_WINDOW_DAYS rather than one taking a fixed 21 days while the other
  -- took however far the nightly scan happened to reach. The comment here
  -- claimed they matched long before they did -- see that constant for the
  -- measurement that chose 30, and for why the number that decided it was not
  -- the disagreement between the two windows but the 49 rides the shorter one
  -- left without a duty-cycle test at all.
  --
  -- The three historical CTEs from here down read run_start, not $1, and that
  -- is the difference between judging the rides in a closure and judging the
  -- whole park. Each is consumed through a LEFT JOIN against run_start alone
  -- and each groups by ride with no cross-ride term, so narrowing them changes
  -- no row that is read. Their own comments already claimed it ("restricted to
  -- the rides actually sitting in a closure right now", "restricted to the
  -- candidates") -- but the candidates are the WHOLE PARK, because
  -- park_closers needs the roster and every CTE was handed the same $1. A
  -- 96-ride park scanned the window's queue_data 96 times over and threw almost
  -- all of it away at a join it never reached.
  --
  -- ANY(ARRAY(...)) rather than IN (...) on purpose. The array is an InitPlan,
  -- so it stays an indexable predicate with runtime chunk exclusion on the
  -- hypertable; a semi-join would have to scan to find out there is nothing to
  -- match. Empty is the normal case and has to be the cheap one -- and it is
  -- not the rare one: run_start is empty in every ordinary open park, though
  -- in a park that has shut for the night it IS the roster again.
  -- The CYCLE_WINDOW_DAYS of readings both historical CTEs judge, read once.
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
  run_readings AS MATERIALIZED (
    SELECT qd."attractionId" AS aid,
           qd.timestamp      AS ts,
           qd.status::text   AS st,
           lag(qd.status::text)  OVER w AS prev_st,
           lead(qd.status::text) OVER w AS next_st,
           lead(qd.timestamp)    OVER w AS next_ts
      FROM queue_data qd
     WHERE qd."attractionId" = ANY(ARRAY(SELECT aid FROM open_today))
       AND qd."queueType" = 'STANDBY'
       AND ${observedReadingsSql("qd")}
       AND qd.timestamp >= $3::timestamptz
           - INTERVAL '${CYCLE_WINDOW_DAYS} days'
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
        -- Every day but today, matching early_end and matching what the
        -- denominator can actually see: today's attraction_exposure_days row
        -- was written by last night's reconstruction, before the park opened,
        -- so it carries operating_minutes = 0 and the active FILTER skips it.
        -- Counting today's gap against a denominator that cannot count today
        -- inflates the share by ~1/30, in the direction that suppresses a
        -- genuine fault.
        SELECT aid, (ts AT TIME ZONE $2)::date AS op_day
          FROM run_readings f
         WHERE f.st = 'CLOSED'
           AND f.prev_st = 'OPERATING'
           AND f.next_st = 'OPERATING'
           -- The same bounds raw_gaps applies — with one exception since the
           -- nightly statement moved to the operating day: raw_gaps compares
           -- window-derived days here, this still compares calendar dates. The
           -- two agree for every park that closes before midnight, which is
           -- almost all of them, and diverge for the rest — so the same ride
           -- can count a different number of gap_days against the same
           -- MAX_GAP_DAY_SHARE on the two sides. PAR-129, together with the
           -- open_today gate above; the note is here so the divergence is
           -- written down rather than inferred from a diff.
           --
           -- The comment above claimed these bounds were already here before
           -- they were. Without them a ride that shuts at night
           -- and opens next morning satisfies the triple, so every ordinary
           -- operating day counts as a gap day and the ratio converges on 1.0 —
           -- which would suppress the live line for exactly the rides that have
           -- been reliably open. Measured today the two counts agree on all
           -- 2132 rides, because overnight rows are broken up by our own
           -- bookkeeping writes; that is the data being kind, not the query
           -- being right.
           AND f.next_ts < f.ts + INTERVAL '${MAX_GAP_HOURS} hours'
           AND (f.next_ts AT TIME ZONE $2)::date = (f.ts AT TIME ZONE $2)::date
           AND (f.ts AT TIME ZONE $2)::date
               < ($3::timestamptz AT TIME ZONE $2)::date
      ) d
     GROUP BY d.aid
  ),
  active AS (
    SELECT e."attractionId" AS aid,
           count(*) FILTER (WHERE e.operating_minutes > 0)::numeric AS active_days
      FROM attraction_exposure_days e
     WHERE e."attractionId" = ANY(ARRAY(SELECT aid FROM open_today))
       -- Park-local, because op_day is. Casting the window's start to ::date takes
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
       AND e.op_day >= (($3::timestamptz
                         - INTERVAL '${CYCLE_WINDOW_DAYS} days')
                        AT TIME ZONE $2)::date
       -- And an upper bound, which this had no more than its nightly twin did.
       -- $3 is now() in production so nothing lies beyond it today, but a
       -- pinned as-of — a spec, a replay — would count operating days from
       -- after the instant being judged.
       -- The numerator's last reachable day, not local_date($3): run_readings
       -- reads qd.timestamp < $3, so when $3 is this park's local midnight the
       -- newest reading it can see belongs to the day before. Same asymmetry
       -- as the lower edge, same direction.
       AND e.op_day <= (($3::timestamptz - INTERVAL '1 microsecond')
                        AT TIME ZONE $2)::date
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
  -- The lower bound is one day WIDER than the reading window, and that day is
  -- not slack. The readings are cut at $3 minus CYCLE_WINDOW_DAYS in UTC, but
  -- their day is taken in the park's zone, so a row at the edge of the window
  -- lands a local day earlier whenever a DST shift has moved it across local
  -- midnight. Measured over every half hour of a winter in four park zones: 42
  -- such instants each -- a one-hour band on each of the days after the shift,
  -- in Europe/Berlin, Europe/London, America/New_York and Australia/Sydney
  -- alike. The join is an INNER one, so without the extra day those readings
  -- are dropped rather than counted, which moves early_days/days in the
  -- direction of a ride looking more regular than it is.
  --
  -- Unlike the active denominator, an unused day here costs a row rather than
  -- diluting anything: this CTE is JOINED to days that exist. The bounds are
  -- inclusive on both ends, so it holds at most CYCLE_WINDOW_DAYS + 2 rows.
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
       -- No guard on the CLOSING time, deliberately, and it was here for one
       -- revision. A null close makes closes_at NULL, so the day counts in the
       -- days denominator and can never count as early -- which is what the
       -- LATERAL this replaced did, and the behaviour that has shipped for
       -- months. Excluding such a day instead looked tidier and is strictly
       -- riskier: it shrinks the sample, and a park publishing a run of
       -- null-close days would drop a ride under MIN_DAYS_FOR_CYCLE_TEST and
       -- switch the early-end filter off altogether -- the filter that
       -- separates a cinema at 100 % from a broken coaster at 8 %. Measured
       -- benefit of the change: none, 0 of 10 600 rows.
       --
       -- The OPENING guard stays, because it is not a behaviour change: a null
       -- opening makes the grouping key NULL, so the row can never join a day.
       -- Written out rather than left to NULL-propagation through the
       -- timestamp bounds below, whose stated job is index pruning.
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
       AND se."openingTime" >= $3::timestamptz
           - INTERVAL '${CYCLE_WINDOW_DAYS + 3} days'
       AND se."openingTime" <  $3::timestamptz + INTERVAL '2 days'
       AND (se."openingTime" AT TIME ZONE $2)::date
           >= ($3::timestamptz AT TIME ZONE $2)::date
              - ${CYCLE_WINDOW_DAYS + 1}
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
        -- Every day BUT the one being judged. A ride standing still right now
        -- has today's last OPERATING reading well before today's close, so it
        -- counts itself as having ended early -- and on the fourth day of an
        -- unrecovered outage it has manufactured four such days against a
        -- denominator that is only days-with-published-hours. The longest and
        -- most certain closures were the first to be suppressed as timetables.
        SELECT r.aid,
               (r.ts AT TIME ZONE $2)::date AS d,
               max(r.ts) FILTER (WHERE r.st = 'OPERATING') AS last_operating
          FROM run_readings r
         WHERE (r.ts AT TIME ZONE $2)::date
               < ($3::timestamptz AT TIME ZONE $2)::date
         GROUP BY 1, 2
      ) q
      JOIN park_day_close pdc ON pdc.d = q.d
     WHERE q.last_operating IS NOT NULL
     GROUP BY q.aid
  )
  SELECT s.aid                                   AS "attractionId",
         s.started_at                            AS "startedAt"
    FROM run_start s
    JOIN open_today o ON o.aid = s.aid
    -- No re-join to park_closers. It used to be here to carry a
    -- simultaneousClosers column that nothing ever read -- addClosureGaps types
    -- its rows as { attractionId, startedAt } -- so the join's only effect was
    -- one more CTE scan per call on the path this statement exists to make
    -- cheap. The threshold itself is applied in open_today.
    LEFT JOIN cycle cy ON cy.aid = s.aid
    LEFT JOIN active ac ON ac.aid = s.aid
    LEFT JOIN early_end ee ON ee.aid = s.aid
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
   -- park_open is CROSS JOINed into open_today, so a shut park already yields
   -- no candidates — but a CROSS JOIN is a join node, not a guard, which is the
   -- whole point of the paragraph below. And a shut park is the expensive case: every
   -- ride's newest reading is CLOSED, so run_start IS the roster (45 of 54 at
   -- Alton Towers) and the three historical CTEs scan the window's queue_data for
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
     -- MAX_SIMULTANEOUS_CLOSERS, MIN_PARK_MINUTES_LEFT and MIN_GAP_MINUTES are
     -- all applied in open_today, above the CTEs that read history rather than
     -- below them. Repeating them here would be free and is left out on
     -- purpose: two copies of a filter is how this file's predicates drifted
     -- before. park_open is not in this FROM clause at all: both its uses moved
     -- into open_today, and a CROSS JOIN kept here only as a guard would
     -- duplicate the EXISTS above while risking a row multiplication if LIMIT 1
     -- ever leaves that CTE.
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
`;
