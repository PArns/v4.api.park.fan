import { Injectable, Logger } from "@nestjs/common";
import { DataSource } from "typeorm";
import { observedReadingsSql } from "../common/utils/closure-gap.sql";
import { PARK_FEED_SILENT_DAYS } from "../common/utils/no-live-data-status.util";
import { ABSENT_UPSTREAM_REASON } from "../attractions/services/attraction-retirement.service";
import { reissueNamesMatch } from "../attractions/utils/attraction-match.util";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { parseExpression } from "cron-parser";
import { BULL_QUEUE_REGISTRATIONS } from "../queues/queue-registrations";

/**
 * Three detectors for the ways this system went quietly wrong for weeks.
 *
 * Both incidents were found by accident on 2026-08-15, while looking at
 * something else, and neither was visible in any existing check:
 *
 *   - `detect-seasonal` threw a SQL syntax error on **every** run from
 *     2026-06-03. It was scheduled, it ran, it died. 73 days.
 *   - ThemeParks.wiki dropped 44 Europa-Park attractions from its live feed on
 *     2026-06-07 (and clusters at nine other parks). 10 weeks.
 *
 * `SystemHealthService.freshness()` could not have caught either: it reads
 * `MAX(timestamp)` across all of queue_data and the row count for the last
 * hour. Both stayed perfectly healthy while 140 attractions were dead — an
 * aggregate cannot see a subset go silent. The boot-time `hasRepeatableJob`
 * check could not have caught the first either: it detects jobs that were never
 * scheduled, and this one was scheduled and running.
 *
 * The third was added on 2026-09-16 for a failure the first two are structurally
 * blind to: not a subset of a park going silent, but the whole park, while its
 * schedule keeps publishing operating days. La Ronde had been in that state for
 * 84 days and was found the same way as the other two — by accident, while
 * verifying something else. See `findScheduledButSilentParks`.
 */

export interface SilencedCluster {
  parkId: string;
  parkName: string;
  attractionCount: number;
  lastOperating: string;
  sampleNames: string[];
  /**
   * Every ride of the cluster, so the admin can answer it in one step — a
   * season window on all of them when it is a section closing for the season
   * (PAR-695). `sampleNames` stays for the log line.
   */
  attractions: Array<{ attractionId: string; name: string }>;
}

export interface ScheduledButSilentPark {
  parkId: string;
  parkName: string;
  attractionCount: number;
  /** Park-local date of the last observed reading, or null if there is none in 400 days. */
  lastReading: string | null;
  /** Operating days published inside `SILENT_PARK_LOOKAHEAD_DAYS`. */
  operatingDaysAhead: number;
  /** The last operating day on the calendar, however far out it runs. */
  lastScheduledDay: string;
}

/**
 * How far ahead the schedule has to claim an operating day.
 *
 * A window rather than "any future day", because without one the detector is a
 * seasonal-park alarm: a park shut for the winter with next summer already
 * published has future operating days and an empty feed, and is neither a fault
 * nor news. Seven days makes the report's claim the narrow one — *this park is
 * supposed to be open this week and nobody is reading it.*
 *
 * Measured 2026-09-16: at 7, 30 and unbounded the query returns the same five
 * parks, so the window costs nothing today and is the gate that keeps January
 * from filling the log.
 */
export const SILENT_PARK_LOOKAHEAD_DAYS = 7;

/**
 * A ride the children sync retired for being absent from ThemeParks.wiki's
 * `/children`, while nobody has said whether it is seasonal (PAR-684).
 *
 * The absence step skips a row whose season is known, but the detector cannot
 * know one before `MIN_OBSERVED_DAYS` of watching — so a maze in its first year
 * is retired like a ride that was torn down. This is the list a human answers:
 * a season window in the admin (`curated_is_seasonal` true) or "not seasonal"
 * (false), and either answer takes the row off it.
 */
export interface AbsenceRetiredUnreviewed {
  attractionId: string;
  name: string;
  slug: string;
  parkId: string;
  parkName: string;
  retiredAt: string;
  /** Park-local date of the last observed reading, or null if there is none. */
  lastReading: string | null;
}

/** One row of a re-issue candidate pair, as the admin needs it to decide. */
export interface ReissueCandidateSide {
  attractionId: string;
  name: string;
  slug: string;
  externalId: string | null;
  createdAt: string;
  /** Park-local date of the last observed reading in 400 days, or null. */
  lastReading: string | null;
}

/**
 * A ride the absence step retired, next to a younger live ride within
 * `REISSUE_CANDIDATE_METERS` of it — possibly the same attraction the feed
 * re-issued under a new id AND a new name (PAR-686), which the sync's name
 * match cannot claim. Shown to a human with a merge and a dismiss button;
 * nothing here merges by itself (PO decision 2026-10-04, option B).
 */
export interface ReissueCandidate {
  parkId: string;
  parkName: string;
  /** The retired row: history and the old slug. The natural survivor. */
  previous: ReissueCandidateSide;
  /** The live row the feed lists now. */
  current: ReissueCandidateSide;
  meters: number;
  /** `reissueNamesMatch` — a hint, never the decision. */
  namesMatch: boolean;
}

/**
 * How close two rows have to be to be shown as a re-issue candidate.
 *
 * Measured 2026-10-04: every confirmed different-name re-issue lay within 26 m
 * (Movie Park 5–12 m, Six Flags Great America 16–25 m). Seasonal overlays sit
 * just as close — Walibi Belgium's 4D films at 0 m — which is why distance
 * only decides what is shown, never what is merged.
 */
export const REISSUE_CANDIDATE_METERS = 30;

export interface FailingJob {
  queue: string;
  jobName: string;
  failures: number;
  lastReason: string;
  lastFailedAt: string | null;
}

/**
 * Registered queues whose failures are deliberately NOT reported, each with
 * the reason. Empty today: every queue the app registers runs work whose
 * failure somebody needs to hear about. An entry here is the only way to leave
 * a queue out — the spec fails for a registered queue that is neither
 * monitored nor named here, and for an entry here that names no registered
 * queue or gives no reason.
 */
export const UNMONITORED_QUEUES: Readonly<Record<string, string>> = {};

/**
 * Queues whose failures the nightly sweep reports: every registered queue
 * minus the opt-outs above.
 *
 * Derived rather than written out. The hand-written list this replaced
 * (PAR-821) claimed to be "every queue this app registers" and held fourteen
 * of thirty-two — downtime, p50-baseline, push-notifications and fifteen
 * more were never read, and four days of `downtime` failures passed without a
 * line in the log.
 */
export const MONITORED_QUEUES: readonly string[] = BULL_QUEUE_REGISTRATIONS.map(
  (q) => q.name as string,
).filter((name) => !(name in UNMONITORED_QUEUES));

/**
 * How far back a failure counts when its job has no slower schedule and no
 * previous sweep is on record: the sweep runs once a day (06:45 UTC), so 26 h
 * covers the gap between two sweeps with slack for a late run. The nightly
 * sweep normally anchors on its own previous run instead
 * (`FAILING_JOB_LAST_SWEEP_KEY`), so a failure at 06:00 is not reported on two
 * nights; this is the fail-safe when that key is missing, and the window the
 * admin page reads.
 */
export const FAILING_JOB_DEFAULT_WINDOW_MS = 26 * 60 * 60 * 1000;

/**
 * Redis key holding the start time (epoch ms) of the last nightly failing-job
 * sweep. Not under the Bull prefix: it is not Bull's, and losing it (a cache
 * flush, eviction — it carries a TTL) only falls back to the 26 h default.
 */
export const FAILING_JOB_LAST_SWEEP_KEY =
  "data-quality:failing-jobs:last-sweep";

/** An anchor older than this is not trusted; the window is capped here. */
const FAILING_JOB_MAX_ANCHOR_AGE_MS = 7 * 24 * 60 * 60 * 1000;

/** Slack before a repeatable job's previous fire time — a run can start late. */
const FAILING_JOB_FIRE_SLACK_MS = 2 * 60 * 60 * 1000;

/**
 * Earliest finish time at which a failure of each repeatable job still counts,
 * read from Bull's `<prefix>:<queue>:repeat` ZSET.
 *
 * A failure counts until the job has had its next scheduled chance to run, so
 * the window follows the job's own cadence instead of one number for every
 * queue: a five-minute or daily job is judged on the last 26 h (the sweep's own
 * cadence), the weekly Six Flags heights sync on its last Monday run, the monthly
 * holiday sync on its run on the 1st — otherwise a failed monthly run would be
 * reported for one night and then hidden for a month.
 *
 * Members are Bull's repeat keys, `name:jobId:endDate:tz:cron` (bull@4.16.5
 * lib/repeatable.js; production reads
 * `calculate-percentiles:percentiles-cron:::0 2 * * *`). Parsed from the
 * right, because the right-hand fields are the fixed ones: the cron is the
 * last segment (a cron string has no colon), the tz the one before it, the
 * endDate the one before that, then the jobId (a fixed `…-cron` id here);
 * whatever remains on the left is the job name, colons and all. A purely
 * numeric last segment is an `every` interval in ms. A key that does not parse
 * gets no entry, so its job falls back to `defaultStart`.
 */
export function failureWindowStarts(
  repeatKeys: string[],
  now: number,
  defaultStart = now - FAILING_JOB_DEFAULT_WINDOW_MS,
): Map<string, number> {
  const starts = new Map<string, number>();
  for (const key of repeatKeys) {
    const parts = key.split(":");
    if (parts.length < 5) continue;
    const spec = parts[parts.length - 1];
    const tz = parts[parts.length - 2] || undefined;
    const name = parts.slice(0, parts.length - 4).join(":");
    if (!name || !spec.trim()) continue;

    let previousFire: number;
    try {
      if (/^\d+$/.test(spec)) {
        const every = Number(spec);
        if (every <= 0) continue;
        previousFire = Math.floor(now / every) * every;
      } else {
        previousFire = parseExpression(spec, {
          currentDate: new Date(now),
          tz,
        })
          .prev()
          .getTime();
      }
    } catch {
      continue; // an unparseable entry falls back to the default window
    }

    const start = Math.min(
      defaultStart,
      previousFire - FAILING_JOB_FIRE_SLACK_MS,
    );
    starts.set(name, Math.min(starts.get(name) ?? start, start));
  }
  return starts;
}

@Injectable()
export class DataQualityMonitorService {
  private readonly logger = new Logger(DataQualityMonitorService.name);

  constructor(
    private readonly dataSource: DataSource,
    @InjectQueue("analytics") private readonly analyticsQueue: Queue,
  ) {}

  /**
   * Parks where a block of attractions stopped reporting on the same day.
   *
   * The window is the whole design. `lastOperating` must fall between
   * `now - windowDays` and `now - minDaysSilent`, so the detector speaks up
   * within a few days of a drop and **goes quiet again by itself** afterwards.
   * There is no acknowledgement table and no state: a warning that fires every
   * night forever is one people learn to scroll past, which is exactly how the
   * ride-profile term audit was designed too.
   *
   * Consequences worth knowing before reading the output:
   *   - Today's known clusters (Europa-Park, Rulantica, Universal Studios
   *     Singapore, Wet'n'Wild) are months old, so the DEFAULT window does not
   *     report them. Call it with a large `windowDays` to see them — that is
   *     the check that this query finds real incidents.
   *   - It deliberately does NOT catch attractions retiring one at a time on
   *     different dates (Magic Kingdom's ending meet-and-greets, Universal
   *     Studios Japan's closed 4-D theatres). Those are real closures, not a
   *     feed fault, and no single-day cluster exists to key on.
   *   - **A hit is an event, not a verdict.** A water-park section closing for
   *     the season looks identical to a dropped feed: on the first run this
   *     reported 19 rides at Schlitterbahn, 15 at Six Flags Over Georgia and 13
   *     at Kings Dominion all falling silent on 2026-08-09, which is far more
   *     likely to be US water parks shutting after the school year than four
   *     simultaneous upstream faults. Deciding which is a human's job. The
   *     point is that today nobody could even see the event.
   */
  async findSilencedClusters(
    windowDays = 14,
    minDaysSilent = 3,
    minClusterSize = 5,
  ): Promise<SilencedCluster[]> {
    const rows: Array<{
      park_id: string;
      park_name: string;
      last_op: string;
      n: string;
      names: string[];
      rides: Array<{ id: string; name: string }> | null;
    }> = await this.dataSource.query(
      `
      WITH activity AS (
        SELECT q."attractionId" AS aid,
               max(q.timestamp) FILTER (WHERE q.status = 'OPERATING') AS last_op,
               max(q.timestamp) AS last_row
          FROM queue_data q
         WHERE q.timestamp > now() - ($1::int + 30) * INTERVAL '1 day'
         GROUP BY 1
      ),
      -- Only trust parks whose feed demonstrably still works, or a park simply
      -- closing for the season looks exactly like a dropped feed: everything
      -- goes silent on one day there too.
      --
      -- An absolute floor, NOT a ratio. A ratio is self-defeating here: the
      -- very incident this looks for drags the park below it. Europa-Park lost
      -- 44 of ~96 attractions, leaving 45% live — a 70% gate would have hidden
      -- the largest cluster in the data. What actually separates the two cases
      -- is that a closed park has NOBODY operating, while a park with a dropped
      -- feed subset still has plenty.
      park_health AS (
        SELECT a."parkId"
          FROM activity act
          JOIN attractions a ON a.id = act.aid
         GROUP BY a."parkId"
        HAVING count(*) FILTER (WHERE act.last_op > now() - INTERVAL '2 days') >= 3
      )
      SELECT a."parkId" AS park_id,
             p.name AS park_name,
             (act.last_op AT TIME ZONE p.timezone)::date::text AS last_op,
             count(*)::text AS n,
             (array_agg(a.name ORDER BY a.name))[1:4] AS names,
             json_agg(
               json_build_object('id', a.id, 'name', COALESCE(a.curated_name, a.name))
               ORDER BY a.name
             ) AS rides
        FROM activity act
        JOIN attractions a ON a.id = act.aid
        JOIN parks p ON p.id = a."parkId"
        JOIN park_health ph ON ph."parkId" = a."parkId"
       WHERE act.last_op IS NOT NULL
         AND act.last_op < now() - ($2::int * INTERVAL '1 day')
         AND act.last_op > now() - ($1::int * INTERVAL '1 day')
         -- still receiving rows, so this is a silence rather than a deletion
         AND act.last_row > now() - INTERVAL '2 days'
         AND NOT a.open_with_park
         -- A ride with a known season going quiet is the season ending, which
         -- is the answer this card asks for. Without this, the admin's
         -- "season ends" button would leave the card standing (PAR-695).
         AND NOT COALESCE(a.curated_is_seasonal, a.is_seasonal)
         AND a.retired_at IS NULL
       GROUP BY a."parkId", p.name, (act.last_op AT TIME ZONE p.timezone)::date
      HAVING count(*) >= $3::int
       ORDER BY count(*) DESC
      `,
      [windowDays, minDaysSilent, minClusterSize],
    );

    return rows.map((r) => ({
      parkId: r.park_id,
      parkName: r.park_name,
      attractionCount: Number(r.n),
      lastOperating: r.last_op,
      sampleNames: r.names ?? [],
      attractions: (r.rides ?? []).map((ride) => ({
        attractionId: ride.id,
        name: ride.name,
      })),
    }));
  }

  /**
   * Parks the schedule says are open and the feed says nothing about.
   *
   * The exact complement of `findSilencedClusters` above, and the reason that
   * one cannot be widened to cover it: its `park_health` CTE demands at least
   * three attractions with an OPERATING reading in the last two days, precisely
   * so a park closing for the season is not mistaken for a dropped feed. A park
   * where EVERY ride is silent fails that gate by construction, so the detector
   * built to find dropped feeds is blind to the largest drop there is.
   *
   * What separates the two cases here is not the feed — it is the schedule, read
   * over the next `SILENT_PARK_LOOKAHEAD_DAYS`. A park shut for the winter is
   * not scheduled open this week, whatever it has published for next summer. A
   * park scheduled open tomorrow with no reading since June is an unresolved
   * contradiction between two of our own sources, and one of them is wrong.
   *
   * Found on 2026-09-16 (PAR-192): La Ronde, silent since 2026-06-24 with a
   * schedule running to 2027-08-31, plus four parks that have never produced a
   * reading at all — Paradise Country, Movieland The Hollywood Park, Adventure
   * Island Tampa, Water Country USA. 110 attractions between them.
   *
   * Unlike the cluster detector this one has no window and does NOT go quiet by
   * itself, which is deliberate and is the whole difference in kind. A silenced
   * cluster resolves into a judgement — "that was the water park closing" — and
   * the judgement leaves no trace in the data, so a detector that kept firing
   * would be asking the same answered question every night. This state has only
   * two exits and both are edits: the feed returns, or the schedule stops
   * claiming days nobody can confirm. Until one of them happens the
   * contradiction is still there, and five WARN lines a day is what it costs.
   *
   * A third exit exists and this query does not know about it: a park written
   * into `PARKS_WITHOUT_LIVE_WAIT_TIMES` is explained rather than broken, and
   * would still be reported here every night. No park in that list is silent
   * today — Hansa-Park's upstream publishes a row per attraction, so it never
   * reaches this — and reading a curated TypeScript array from SQL is a worse
   * trade than the line it would save. It is the first thing to change if the
   * list grows a park whose feed also stops.
   *
   * @param minDaysSilent Days without an observed reading before a park counts.
   * @param lookaheadDays How far ahead an operating day has to be published.
   */
  async findScheduledButSilentParks(
    minDaysSilent = PARK_FEED_SILENT_DAYS,
    lookaheadDays = SILENT_PARK_LOOKAHEAD_DAYS,
  ): Promise<ScheduledButSilentPark[]> {
    const rows: Array<{
      park_id: string;
      park_name: string;
      n: string;
      last_reading: string | null;
      days_ahead: string;
      last_day: string;
    }> = await this.dataSource.query(
      `
      WITH catalog AS (
        SELECT a."parkId", count(*)::int AS rides
          FROM attractions a
         WHERE a.retired_at IS NULL
         GROUP BY 1
      ),
      -- Future only. Today's entry is not enough on its own: a park can be
      -- scheduled open today and have nothing after it, which is a schedule
      -- running out rather than a schedule nobody can confirm.
      --
      -- attractionId IS NULL, because schedule_entries holds the park's own
      -- opening hours and per-ride rows in the same table. Every other reader
      -- of a park's hours filters it and there is a partial index for it
      -- (idx_schedule_park_date_no_attraction); PAR-246 is what the same blind
      -- key cost on the cleanup path. Production holds no future per-ride
      -- OPERATING row today (0 of 10.853, measured 2026-09-16), so this is the
      -- guard being right rather than the guard being needed.
      --
      -- count(DISTINCT se.date) for the neighbouring reason: the dedup jobs run
      -- periodically, so two rows for one day are possible between passes and a
      -- plain count(*) would report a park as open more days than the calendar
      -- has.
      future_schedule AS (
        SELECT se."parkId",
               count(DISTINCT se.date) FILTER (
                 WHERE se.date <= (now() AT TIME ZONE p.timezone)::date
                                  + ($2::int * INTERVAL '1 day')
               )::int AS days_ahead,
               max(se.date)::text AS last_day
          FROM schedule_entries se
          JOIN parks p ON p.id = se."parkId"
         WHERE se."scheduleType" = 'OPERATING'
           AND se."attractionId" IS NULL
           AND se.date > (now() AT TIME ZONE p.timezone)::date
         GROUP BY 1
        HAVING count(DISTINCT se.date) FILTER (
                 WHERE se.date <= (now() AT TIME ZONE p.timezone)::date
                                  + ($2::int * INTERVAL '1 day')
               ) > 0
      ),
      -- Bounded on purpose, and the bound is what makes the NULL meaningful: a
      -- park with no row here has not been observed inside the window, which is
      -- the condition itself. An unbounded max() would plan against every chunk
      -- of the hypertable to produce a date nobody reads.
      last_seen AS (
        SELECT a."parkId", max(qd.timestamp) AS ts
          FROM queue_data qd
          JOIN attractions a ON a.id = qd."attractionId"
         WHERE a.retired_at IS NULL
           AND qd.timestamp > now() - ($1::int * INTERVAL '1 day')
           -- COALESCE for the same reason hasObservedReadingWithin uses one:
           -- the predicate is NULL, not false, for a row with neither
           -- is_heartbeat nor lastUpdated, and an unclassifiable row must not
           -- put a park on a warning list.
           AND COALESCE(${observedReadingsSql("qd")}, true)
         GROUP BY 1
      )
      SELECT p.id AS park_id,
             p.name AS park_name,
             c.rides::text AS n,
             (SELECT max(qd2.timestamp AT TIME ZONE p.timezone)::date::text
                FROM queue_data qd2
                JOIN attractions a2 ON a2.id = qd2."attractionId"
               WHERE a2."parkId" = p.id
                 AND a2.retired_at IS NULL
                 AND qd2.timestamp > now() - INTERVAL '400 days'
                 AND COALESCE(${observedReadingsSql("qd2")}, true)) AS last_reading,
             f.days_ahead::text AS days_ahead,
             f.last_day
        FROM future_schedule f
        JOIN parks p ON p.id = f."parkId"
        JOIN catalog c ON c."parkId" = p.id
        LEFT JOIN last_seen ls ON ls."parkId" = p.id
       WHERE ls.ts IS NULL
       ORDER BY c.rides DESC
      `,
      [minDaysSilent, lookaheadDays],
    );

    return rows.map((r) => ({
      parkId: r.park_id,
      parkName: r.park_name,
      attractionCount: Number(r.n),
      lastReading: r.last_reading,
      operatingDaysAhead: Number(r.days_ahead),
      lastScheduledDay: r.last_day,
    }));
  }

  /**
   * Rides the absence step retired while nobody has said whether they are
   * seasonal — the "season or gone?" list (PAR-684, option B).
   *
   * `retireAbsentAttractions` leaves a row alone when its season is known, but
   * the detector needs `MIN_OBSERVED_DAYS` before it can know one, so every
   * seasonal attraction in its first year is retired like a ride that was torn
   * down. On 2026-10-03 that was 17 mazes and walk-throughs (PAR-684); the
   * retirement itself is right for the rest of the list, the 22 Wet'n'Wild
   * facilities among them.
   *
   * Like `findScheduledButSilentParks` it does not go quiet by itself: a row
   * leaves the list when an editor writes `curated_is_seasonal` — true puts a
   * season window on it and the sync lifts the retirement when the wiki lists
   * the attraction again, under its old id or a new one; false says "not
   * seasonal" and the retirement stands. A row the detector already calls
   * seasonal is not listed: the absence step would not have retired it unless a
   * twin replaced it, and that is a merge question, not a season question.
   */
  async findAbsenceRetiredUnreviewed(): Promise<AbsenceRetiredUnreviewed[]> {
    const rows: Array<{
      id: string;
      name: string;
      slug: string;
      park_id: string;
      park_name: string;
      retired_at: Date;
      last_reading: string | null;
    }> = await this.dataSource.query(
      `
      SELECT a.id,
             COALESCE(a.curated_name, a.name) AS name,
             a.slug,
             p.id AS park_id,
             p.name AS park_name,
             a.retired_at,
             -- Bounded like the silent-park query: an unbounded max() plans
             -- against every chunk of the hypertable, once per row.
             (SELECT max(qd.timestamp AT TIME ZONE p.timezone)::date::text
                FROM queue_data qd
               WHERE qd."attractionId" = a.id
                 AND qd.timestamp > now() - INTERVAL '400 days'
                 AND COALESCE(${observedReadingsSql("qd")}, true)) AS last_reading
        FROM attractions a
        JOIN parks p ON p.id = a."parkId"
       WHERE a.retired_reason = $1
         AND a.curated_is_seasonal IS NULL
         AND NOT a.is_seasonal
       ORDER BY p.name, a.name
      `,
      [ABSENT_UPSTREAM_REASON],
    );

    return rows.map((r) => ({
      attractionId: r.id,
      name: r.name,
      slug: r.slug,
      parkId: r.park_id,
      parkName: r.park_name,
      retiredAt: new Date(r.retired_at).toISOString(),
      lastReading: r.last_reading,
    }));
  }

  /**
   * Retired rows with a younger live row right beside them — candidates for a
   * re-issue under a changed name (PAR-686).
   *
   * The sync claims a re-issued row only when the name matches exactly
   * (PAR-682). The wiki often renames on the way — `HAUNTED HOUSE: SAW: Legacy
   * of Terror` came back as `SAW Legacy of Terror`, Movie Park's mazes came
   * back in Dutch — and then the old row stays retired beside a new one with no
   * history. Distance and a name hint put such pairs in front of a human; the
   * admin page merges (`adoptLoserExternalId`, so the survivor takes the listed
   * id) or dismisses with a `not_a_duplicate` mark, which keeps the pair off
   * this list for good.
   *
   * Every pair within the radius is returned, matching name or not: Movie
   * Park's language pairs were real and match nothing, Walibi Belgium's 4D
   * films are 0 m apart and are three films. `namesMatch` sorts and hints; the
   * human decides.
   */
  async findReissueCandidates(): Promise<ReissueCandidate[]> {
    const rows: Array<{
      park_id: string;
      park_name: string;
      o_id: string;
      o_name: string;
      o_slug: string;
      o_ext: string | null;
      o_created: Date;
      o_last: string | null;
      n_id: string;
      n_name: string;
      n_slug: string;
      n_ext: string | null;
      n_created: Date;
      n_last: string | null;
      meters: string;
    }> = await this.dataSource.query(
      `
      WITH pairs AS (
        SELECT o.id AS o_id, n.id AS n_id, p.id AS park_id, p.name AS park_name,
               p.timezone,
               6371000 * 2 * asin(sqrt(
                 power(sin(radians(n.latitude::float8 - o.latitude::float8) / 2), 2) +
                 cos(radians(o.latitude::float8)) * cos(radians(n.latitude::float8)) *
                 power(sin(radians(n.longitude::float8 - o.longitude::float8) / 2), 2)
               )) AS meters
          FROM attractions o
          JOIN attractions n
            ON n."parkId" = o."parkId"
           AND n.id <> o.id
           AND n.retired_at IS NULL
           AND n."createdAt" > o."createdAt"
          JOIN parks p ON p.id = o."parkId"
         WHERE o.retired_reason = $1
           AND o.latitude IS NOT NULL AND n.latitude IS NOT NULL
           -- A pair somebody already looked at and called two things.
           AND NOT EXISTS (
             SELECT 1 FROM attraction_review_marks m
              WHERE m.kind = 'not_a_duplicate'
                AND m.attraction_id = LEAST(o.id, n.id)
                AND m.other_attraction_id = GREATEST(o.id, n.id)
           )
      )
      SELECT pr.park_id, pr.park_name, pr.meters::text AS meters,
             o.id AS o_id, COALESCE(o.curated_name, o.name) AS o_name, o.slug AS o_slug,
             o."externalId" AS o_ext, o."createdAt" AS o_created,
             (SELECT max(qd.timestamp AT TIME ZONE pr.timezone)::date::text
                FROM queue_data qd
               WHERE qd."attractionId" = o.id
                 AND qd.timestamp > now() - INTERVAL '400 days'
                 AND COALESCE(${observedReadingsSql("qd")}, true)) AS o_last,
             n.id AS n_id, COALESCE(n.curated_name, n.name) AS n_name, n.slug AS n_slug,
             n."externalId" AS n_ext, n."createdAt" AS n_created,
             (SELECT max(qd.timestamp AT TIME ZONE pr.timezone)::date::text
                FROM queue_data qd
               WHERE qd."attractionId" = n.id
                 AND qd.timestamp > now() - INTERVAL '400 days'
                 AND COALESCE(${observedReadingsSql("qd")}, true)) AS n_last
        FROM pairs pr
        JOIN attractions o ON o.id = pr.o_id
        JOIN attractions n ON n.id = pr.n_id
       WHERE pr.meters <= $2
       ORDER BY pr.park_name, pr.meters
      `,
      [ABSENT_UPSTREAM_REASON, REISSUE_CANDIDATE_METERS],
    );

    const side = (
      id: string,
      name: string,
      slug: string,
      externalId: string | null,
      createdAt: Date,
      lastReading: string | null,
    ): ReissueCandidateSide => ({
      attractionId: id,
      name,
      slug,
      externalId,
      createdAt: new Date(createdAt).toISOString(),
      lastReading,
    });

    return rows
      .map((r) => ({
        parkId: r.park_id,
        parkName: r.park_name,
        previous: side(
          r.o_id,
          r.o_name,
          r.o_slug,
          r.o_ext,
          r.o_created,
          r.o_last,
        ),
        current: side(
          r.n_id,
          r.n_name,
          r.n_slug,
          r.n_ext,
          r.n_created,
          r.n_last,
        ),
        meters: Math.round(Number(r.meters) * 10) / 10,
        namesMatch: reissueNamesMatch(r.o_name, r.n_name),
      }))
      .sort(
        (a, b) =>
          Number(b.namesMatch) - Number(a.namesMatch) ||
          a.parkName.localeCompare(b.parkName) ||
          a.meters - b.meters,
      );
  }

  /**
   * Jobs that are scheduled, run, and throw.
   *
   * Complementary to the boot-time `hasRepeatableJob` check in
   * QueueSchedulerService, which catches the opposite failure: a repeatable job
   * that stopped being scheduled at all. Neither covers the other, and
   * detect-seasonal was the second kind for 73 days.
   *
   * Read straight off Bull's Redis keys rather than by injecting every queue:
   * `<prefix>:<queue>:failed` is a ZSET of job ids scored by the time the job
   * failed (repeatable runs appear as `repeat:<hash>:<millis>`), and each id
   * resolves to a hash holding `name`, `failedReason` and `finishedOn`.
   * Verified against production, where the detect-seasonal corpse still reads
   * `syntax error at or near "attr_activity"`.
   *
   * Only failures inside the job's window count (see `failureWindowStarts`).
   * Bull keeps the last 500 failures per queue for as long as it takes 500 more
   * to happen, which for a quiet queue is forever — without the window a bug
   * fixed in July is reported every night until December, and a report that is
   * red every night is one people learn to ignore. Until PAR-821 the boot-time
   * wipe hid that by deleting the evidence on every deploy, for six queues.
   *
   * And a job whose latest run succeeded is not failing: a job name drops out
   * when the same queue's `completed` set holds a run of that name that
   * finished after its newest failure — a manual rerun of a weekly sync clears
   * it the same night instead of a week later. The completed set keeps the
   * last 100 runs per queue, so on a busy queue an old success may have been
   * trimmed; that errs towards reporting, never towards hiding.
   *
   * `since` replaces the 26 h default window (the nightly sweep passes its own
   * previous run, see `findFailingJobsSinceLastSweep`).
   */
  async findFailingJobs(
    perQueueLimit = 100,
    now = Date.now(),
    since?: number,
  ): Promise<FailingJob[]> {
    const client = this.analyticsQueue.client;
    const prefix = process.env.BULL_PREFIX || "parkfan";
    const results: FailingJob[] = [];

    for (const queueName of MONITORED_QUEUES) {
      try {
        const scored: string[] = await client.zrange(
          `${prefix}:${queueName}:failed`,
          -perQueueLimit,
          -1,
          "WITHSCORES",
        );
        if (scored.length === 0) continue;

        const repeatKeys: string[] = await client.zrange(
          `${prefix}:${queueName}:repeat`,
          0,
          -1,
        );
        const defaultStart = since ?? now - FAILING_JOB_DEFAULT_WINDOW_MS;
        const windowStarts = failureWindowStarts(repeatKeys, now, defaultStart);
        const oldestStart = Math.min(defaultStart, ...windowStarts.values());

        const byJobName = new Map<string, FailingJob>();
        const newestFailure = new Map<string, number>();
        for (let i = 0; i < scored.length; i += 2) {
          const id = scored[i];
          const score = Number(scored[i + 1]);
          // Cheap pre-filter on the ZSET score before reading the hash.
          if (!(score >= oldestStart)) continue;

          const [name, failedReason, finishedOn] = await client.hmget(
            `${prefix}:${queueName}:${id}`,
            "name",
            "failedReason",
            "finishedOn",
          );
          const jobName = name ?? "unknown";
          if (score < (windowStarts.get(jobName) ?? defaultStart)) continue;

          const failedAtMs = finishedOn ? Number(finishedOn) : score;
          const failedAt = new Date(failedAtMs).toISOString();
          newestFailure.set(
            jobName,
            Math.max(newestFailure.get(jobName) ?? 0, failedAtMs),
          );
          const existing = byJobName.get(jobName);

          if (!existing) {
            byJobName.set(jobName, {
              queue: queueName,
              jobName,
              failures: 1,
              lastReason: this.firstLine(failedReason),
              lastFailedAt: failedAt,
            });
            continue;
          }

          existing.failures++;
          if (!existing.lastFailedAt || failedAt > existing.lastFailedAt) {
            existing.lastFailedAt = failedAt;
            existing.lastReason = this.firstLine(failedReason);
          }
        }

        if (byJobName.size === 0) continue;

        // A later success clears the failure. Only completions newer than the
        // oldest reported failure can matter, so read just those.
        const earliest = Math.min(...newestFailure.values());
        const completed: string[] = await client.zrangebyscore(
          `${prefix}:${queueName}:completed`,
          `(${earliest}`,
          "+inf",
          "WITHSCORES",
        );
        const newestSuccess = new Map<string, number>();
        for (let i = 0; i < completed.length; i += 2) {
          const doneAt = Number(completed[i + 1]);
          const [name] = await client.hmget(
            `${prefix}:${queueName}:${completed[i]}`,
            "name",
          );
          const jobName = name ?? "unknown";
          newestSuccess.set(
            jobName,
            Math.max(newestSuccess.get(jobName) ?? 0, doneAt),
          );
        }

        for (const [jobName, job] of byJobName) {
          const failedAt = newestFailure.get(jobName) ?? 0;
          if ((newestSuccess.get(jobName) ?? 0) > failedAt) continue;
          results.push(job);
        }
      } catch (e) {
        this.logger.debug(
          `Could not read failures for queue ${queueName}: ${(e as Error)?.message ?? e}`,
        );
      }
    }

    return results.sort((a, b) => b.failures - a.failures);
  }

  /**
   * The nightly sweep's entry point: failures since the previous sweep's run,
   * so a failure is reported on exactly one night (or, for a slower cron, until
   * its next run). Records this sweep's start only after the read, so a
   * missing or unreadable anchor falls back to the 26 h default.
   */
  async findFailingJobsSinceLastSweep(now = Date.now()): Promise<FailingJob[]> {
    const client = this.analyticsQueue.client;
    let since: number | undefined;
    try {
      const raw = await client.get(FAILING_JOB_LAST_SWEEP_KEY);
      const at = raw === null ? NaN : Number(raw);
      if (Number.isFinite(at) && at < now) {
        since = Math.max(at, now - FAILING_JOB_MAX_ANCHOR_AGE_MS);
      }
    } catch (e) {
      this.logger.debug(
        `Could not read the last failing-job sweep: ${(e as Error)?.message ?? e}`,
      );
    }

    const failing = await this.findFailingJobs(100, now, since);

    try {
      await client.set(
        FAILING_JOB_LAST_SWEEP_KEY,
        String(now),
        "EX",
        Math.round((2 * FAILING_JOB_MAX_ANCHOR_AGE_MS) / 1000),
      );
    } catch (e) {
      this.logger.debug(
        `Could not record the failing-job sweep: ${(e as Error)?.message ?? e}`,
      );
    }
    return failing;
  }

  /** Bull stores the whole stack in failedReason; the first line is the fact. */
  private firstLine(reason: string | null | undefined): string {
    return (reason ?? "unknown").split("\n")[0].slice(0, 300);
  }
}
