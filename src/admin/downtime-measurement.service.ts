import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { outageRunBreaks } from "../common/utils/outage-rows.sql";
import { OUTAGE_QUEUE_TYPE } from "../common/utils/outage-rows.sql";

/**
 * Phase 0 of the downtime work: count events, publish nothing.
 *
 * Everything the plan says about thresholds is provisional because every number
 * anybody had was in the wrong unit. `downCount`
 * (`analytics.service.ts:computeParkDownCountForDate`) is
 * `COUNT(DISTINCT date_trunc('hour', timestamp))` over the whole park-local
 * calendar day with **no opening-hours bound**, so it counts DOWN-hours and not
 * outages: Universal Studios Singapore's Revenge of the Mummy scored 16 on
 * 12 August, on a park day running 10:00 to 20:00. Two halts in one hour read as
 * 1, a four-day outage as about 40.
 *
 * So before a single figure reaches a reader, this answers three questions with
 * real numbers, writes nothing, and changes nothing:
 *
 * 1. **How many outages are there**, as events, with a duration histogram.
 * 2. **How many parks could ever report one** — the capability census, read
 *    from `parks.wiki_entity_id` rather than guessed from the outcome.
 * 3. **Which parks are in the artefact regime**, where every run consists of a
 *    single reading and a duration cannot be read out of it at all.
 *
 * The event floor of 24 in `docs/analytics/ride-downtime.md` is re-derived from
 * what this returns, or replaced by it.
 *
 * ## Why this is not the reconstruction
 *
 * It deliberately stops short of exposure. Normalising by operating minutes
 * needs the flattened park windows, the 70-minute carry cap and the
 * `is_heartbeat` column, none of which exist yet; a rate computed without them
 * would be the very "confident number the data does not support" the plan is
 * written to avoid. Counts and raw durations need none of it, and they are what
 * the thresholds are made of.
 */
@Injectable()
export class DowntimeMeasurementService {
  private readonly logger = new Logger(DowntimeMeasurementService.name);

  constructor(
    @InjectRepository(QueueData)
    private readonly queueDataRepository: Repository<QueueData>,
  ) {}

  /**
   * Outages as events, for one park or for a list of park slugs.
   *
   * @param options.parkSlugs - Parks to measure. Empty means every park that
   *   could report an outage at all.
   * @param options.days - Lookback in days. Default 90.
   */
  async measure(options: {
    parkSlugs?: string[];
    days?: number;
  }): Promise<DowntimeMeasurement> {
    const days = Math.min(Math.max(options.days ?? 90, 1), 365);
    const slugs = options.parkSlugs?.filter(Boolean) ?? [];
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const [capability, outages] = await Promise.all([
      this.capabilityCensus(),
      this.outageEvents(slugs, since),
    ]);

    const byRide = new Map<string, OutageRow[]>();
    for (const row of outages) {
      const list = byRide.get(row.attractionId) ?? [];
      list.push(row);
      byRide.set(row.attractionId, list);
    }

    const rides: RideMeasurement[] = [...byRide.entries()]
      .map(([attractionId, rows]) => {
        const durations = rows
          .filter((r) => r.endedAt !== null)
          .map((r) => minutesBetween(r.startedAt, r.endedAt as Date));
        return {
          attractionId,
          parkId: rows[0].parkId,
          parkSlug: rows[0].parkSlug,
          parkName: rows[0].parkName,
          parkCity: rows[0].parkCity,
          attractionName: rows[0].attractionName,
          outages: rows.length,
          ongoing: rows.filter((r) => r.endedAt === null).length,
          // A run of exactly one row proves nothing about how long it lasted:
          // the next reading is the only thing that would have ended it, and it
          // never came. Counted separately rather than folded into the median.
          singleReadingRuns: rows.filter((r) => r.rowsInRun === 1).length,
          medianMinutes: median(durations),
          longestMinutes: durations.length > 0 ? Math.max(...durations) : null,
          histogram: histogram(durations),
        };
      })
      .sort((a, b) => b.outages - a.outages);

    const parks = this.perPark(rides, outages);

    return {
      measuredAt: new Date().toISOString(),
      windowDays: days,
      parkSlugs: slugs,
      capability,
      totals: {
        rides: rides.length,
        outages: rides.reduce((sum, r) => sum + r.outages, 0),
        ongoing: rides.reduce((sum, r) => sum + r.ongoing, 0),
        singleReadingRuns: rides.reduce(
          (sum, r) => sum + r.singleReadingRuns,
          0,
        ),
        ridesAtOrAboveEventFloor: rides.filter((r) => r.outages >= 24).length,
      },
      parks,
      rides,
    };
  }

  /**
   * How much of the DOWN signal the merge deletes before it is stored.
   *
   * `ConflictResolverService` rewrites DOWN and CLOSED to OPERATING whenever a
   * second source reports a wait of five minutes or more. `queue_data.raw_status`
   * records the pre-override value so this can be counted; before it existed,
   * the erased readings simply were not there and the size of the effect was an
   * argument rather than a number.
   *
   * Counted in ROWS **and** in the minutes they carried, because rows alone
   * understate it badly: an overridden state reads OPERATING afterwards, which is
   * stable, so it writes no further row — one erased row can stand for hours
   * while an outage that survived writes a heartbeat every sixty minutes.
   *
   * It answers nothing until thirty days after the column shipped, and says so.
   *
   * @param days - Lookback. The useful figure needs the column to have existed
   *   for the whole window.
   */
  async measureErasure(days = 30): Promise<ErasureMeasurement> {
    const window = Math.min(Math.max(days, 1), 365);
    const since = new Date(Date.now() - window * 24 * 60 * 60 * 1000);

    const rows: ErasureRow[] = await this.queueDataRepository.manager.query(
      `
      WITH marked AS (
        SELECT qd."attractionId" AS aid,
               a."parkId"        AS pid,
               p.slug            AS pslug,
               p.name            AS pname,
               a.name            AS aname,
               qd.timestamp      AS ts,
               qd.raw_status     AS raw,
               -- How long this reading stood for, capped the same way the
               -- reconstruction caps a segment.
               LEAST(
                 COALESCE(
                   LEAD(qd.timestamp) OVER (
                     PARTITION BY qd."attractionId" ORDER BY qd.timestamp
                   ),
                   NOW()
                 ),
                 qd.timestamp + INTERVAL '70 minutes'
               ) - qd.timestamp AS held
          FROM queue_data qd
          JOIN attractions a ON a.id = qd."attractionId"
          JOIN parks p       ON p.id = a."parkId"
         WHERE qd."queueType" = '${OUTAGE_QUEUE_TYPE}'
           AND qd.timestamp >= $1
      )
      SELECT pslug                                              AS "parkSlug",
             pname                                              AS "parkName",
             COUNT(*) FILTER (WHERE raw = 'DOWN')::int          AS "erasedDownRows",
             COUNT(*) FILTER (WHERE raw = 'CLOSED')::int        AS "erasedClosedRows",
             COUNT(DISTINCT aid) FILTER (WHERE raw = 'DOWN')::int
                                                                AS "ridesAffected",
             COALESCE(ROUND(EXTRACT(EPOCH FROM SUM(held) FILTER (WHERE raw = 'DOWN')) / 60.0), 0)::int
                                                                AS "erasedDownMinutes",
             COUNT(*) FILTER (WHERE raw IS NULL AND ts IS NOT NULL)::int
                                                                AS "rowsSeen"
        FROM marked
       GROUP BY pslug, pname
      HAVING COUNT(*) FILTER (WHERE raw IS NOT NULL) > 0
       ORDER BY COUNT(*) FILTER (WHERE raw = 'DOWN') DESC
      `,
      [since],
    );

    const totals = rows.reduce(
      (acc, row) => ({
        erasedDownRows: acc.erasedDownRows + Number(row.erasedDownRows),
        erasedClosedRows: acc.erasedClosedRows + Number(row.erasedClosedRows),
        erasedDownMinutes:
          acc.erasedDownMinutes + Number(row.erasedDownMinutes),
        ridesAffected: acc.ridesAffected + Number(row.ridesAffected),
      }),
      {
        erasedDownRows: 0,
        erasedClosedRows: 0,
        erasedDownMinutes: 0,
        ridesAffected: 0,
      },
    );

    return {
      measuredAt: new Date().toISOString(),
      windowDays: window,
      // The column is only written from the deploy that introduced it, so a
      // window reaching back further than that is measuring a period in which
      // nothing could have been recorded. Saying so beats reporting a small
      // number that looks like good news.
      caveat:
        "raw_status is written only from the deploy that introduced it. A " +
        "window reaching before that deploy under-reports, and reads as though " +
        "the override rarely fires.",
      totals,
      parks: rows,
    };
  }

  /**
   * How many parks can emit DOWN at all, and how many cannot.
   *
   * From configuration. A park with no `wiki_entity_id` has no source that
   * produces the status: Queue-Times maps `is_open` onto `OPERATING`/`CLOSED`
   * and wartezeiten collapses everything that is not running onto `CLOSED` or
   * `REFURBISHMENT`. Reading capability off "did we see any outages" instead
   * would call a quiet quarter a blind park and a blind park a flawless one.
   */
  private async capabilityCensus(): Promise<CapabilityCensus> {
    const rows: Array<{ capable: string; total: string; scheduled: string }> =
      await this.queueDataRepository.manager.query(`
        SELECT COUNT(*) FILTER (WHERE p.wiki_entity_id IS NOT NULL) AS capable,
               COUNT(*)                                            AS total,
               COUNT(*) FILTER (
                 WHERE EXISTS (
                   SELECT 1 FROM schedule_entries se
                    WHERE se."parkId" = p.id
                      AND se."attractionId" IS NULL
                      AND se."scheduleType" = 'OPERATING'
                 )
               ) AS scheduled
          FROM parks p
      `);
    const row = rows[0] ?? { capable: "0", total: "0", scheduled: "0" };
    return {
      downCapableParks: Number(row.capable),
      totalParks: Number(row.total),
      parksWithSchedule: Number(row.scheduled),
    };
  }

  /**
   * Every outage interval in the window, as events.
   *
   * The run-boundary rule is `outageRunBreaks()`, the same expression the live
   * query uses, so this measurement and the thing it sizes cannot disagree
   * about what an outage is.
   *
   * A gap-based end is deliberately NOT applied here. The 70-minute carry cap
   * belongs to the exposure model and needs `is_heartbeat`; without it a capped
   * end would be a guess, and this is the measurement the guesses are supposed
   * to be replaced by. What is reported instead is `rowsInRun`, which says how
   * much evidence each interval rests on.
   */
  private async outageEvents(
    parkSlugs: string[],
    since: Date,
  ): Promise<OutageRow[]> {
    const filtered = parkSlugs.length > 0;
    return this.queueDataRepository.manager.query(
      `
      WITH tracked AS (
        SELECT a.id AS aid, a.name AS aname, p.id AS pid, p.slug AS pslug,
               p.name AS pname, p.city AS pcity
          FROM attractions a
          JOIN parks p ON p.id = a."parkId"
         WHERE a.retired_at IS NULL
           AND p.wiki_entity_id IS NOT NULL
           AND ($1::boolean = false OR p.slug = ANY($2::text[]))
      ),
      marked AS (
        SELECT t.aid, t.aname, t.pid, t.pslug, t.pname, t.pcity,
               qd.timestamp AS ts,
               ${outageRunBreaks("qd")} AS breaks
          FROM tracked t
          JOIN queue_data qd ON qd."attractionId" = t.aid
         WHERE qd."queueType" = '${OUTAGE_QUEUE_TYPE}'
           AND qd.timestamp >= $3
      ),
      seq AS (
        SELECT *,
               -- One group id per maximal run of non-breaking rows: the count
               -- of breaking rows STRICTLY BEFORE this one.
               --
               -- The frame excludes the current row on purpose. Including it
               -- would push each breaking row into the NEXT group, so every run
               -- would lose the reading that ends it and every outage would come
               -- back open-ended. Excluding it makes a breaking row share the id
               -- of the run it closes, which is what lets the MAX(ts) FILTER
               -- (WHERE breaks) below be that run's end.
               COALESCE(
                 SUM(CASE WHEN breaks THEN 1 ELSE 0 END)
                   OVER (PARTITION BY aid ORDER BY ts
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                 0
               ) AS grp
          FROM marked
      ),
      runs AS (
        SELECT aid, aname, pid, pslug, pname, pcity, grp,
               MIN(ts) FILTER (WHERE NOT breaks) AS started_at,
               -- The breaking row IS the end: it is the first reading that says
               -- the ride is no longer down. NULL when the run is still open at
               -- the edge of the window or right now.
               MAX(ts) FILTER (WHERE breaks)     AS ended_at,
               COUNT(*) FILTER (WHERE NOT breaks)::int AS rows_in_run
          FROM seq
         GROUP BY aid, aname, pid, pslug, pname, pcity, grp
        HAVING COUNT(*) FILTER (WHERE NOT breaks) > 0
      )
      SELECT aid        AS "attractionId",
             aname      AS "attractionName",
             pid        AS "parkId",
             pslug      AS "parkSlug",
             pname      AS "parkName",
             pcity      AS "parkCity",
             started_at AS "startedAt",
             ended_at   AS "endedAt",
             rows_in_run AS "rowsInRun"
        FROM runs
       ORDER BY pslug, pcity, aname, started_at
      `,
      [filtered, parkSlugs, since],
    );
  }

  /**
   * The per-park report, and the regime test that actually holds.
   *
   * ## What this used to test, and why it was wrong
   *
   * The first version called a park an artefact when 70 % of its runs rested on
   * a single reading, reasoning that `ConflictResolverService` erases DOWN
   * between polls. Measured against production over 90 days that test does not
   * do what it says:
   *
   * - `corr(singleReadingShare, share of outages under 60 min) = 0.996`.
   * - 99.9 % of single-row runs are under 65 minutes; 4.5 % of multi-row runs
   *   are. That is the hourly heartbeat's edge, not a source's.
   * - 99.8 % of single-row runs have an OBSERVED end — the breaking row that
   *   says the ride is running again. Their duration is measured, not guessed.
   * - EPCOT has one queue-times row in thirty days, so the resolver cannot fire
   *   there at all, and it still scored 0.715.
   *
   * `queue_data` is a change log: an outage that ends before the hourly
   * heartbeat fires writes exactly one row. One reading is what a SHORT outage
   * looks like. The old test threw out 47 of 78 parks — every well-covered one —
   * and kept the parks whose feed leaves rides sitting on DOWN for hours.
   *
   * ## What it tests now
   *
   * The refusal it feeds says „Störungsmeldungen liegen nur stundengenau vor.
   * Eine Dauer lässt sich daraus nicht ablesen." So it measures exactly that:
   * the temporal resolution of the timestamps a duration is read from.
   *
   * A feed publishing hourly puts every reading on the same minute of the hour.
   * The test is therefore the share held by the single most common minute
   * value, which is timezone-independent — a park at a :30 offset publishing
   * hourly piles up on :30, not on :00, and a test hard-coded to zero would
   * miss it.
   *
   * Highest value in production is 0.236 (Lotte World Adventure); the threshold
   * is 0.5, so the regime is currently empty. An empty refusal is the honest
   * outcome when no feed behaves that way, and it is a very different statement
   * from the old one.
   */
  private perPark(
    rides: RideMeasurement[],
    outages: OutageRow[],
  ): ParkMeasurement[] {
    const byPark = new Map<string, RideMeasurement[]>();
    for (const ride of rides) {
      const list = byPark.get(ride.parkId) ?? [];
      list.push(ride);
      byPark.set(ride.parkId, list);
    }

    // Minute-of-hour histogram per park, over both edges of every run: those
    // are the instants a duration is subtracted from.
    const minutes = new Map<string, Map<number, number>>();
    for (const row of outages) {
      const hist = minutes.get(row.parkId) ?? new Map<number, number>();
      for (const edge of [row.startedAt, row.endedAt]) {
        if (!edge) continue;
        const m = new Date(edge).getUTCMinutes();
        hist.set(m, (hist.get(m) ?? 0) + 1);
      }
      minutes.set(row.parkId, hist);
    }

    return [...byPark.entries()]
      .map(([parkId, list]) => {
        const outageCount = list.reduce((sum, r) => sum + r.outages, 0);
        const single = list.reduce((sum, r) => sum + r.singleReadingRuns, 0);
        const share = outageCount > 0 ? single / outageCount : 0;

        const hist = minutes.get(parkId);
        const edges = hist ? [...hist.values()].reduce((a, b) => a + b, 0) : 0;
        const topMinute = hist ? Math.max(0, ...hist.values()) : 0;
        // Too few edges to say anything about resolution. Claiming a park is
        // fine-grained on four readings is the same error in the other
        // direction, so it reads as unknown and the regime stays `reports`.
        const onTheHourShare =
          edges >= MIN_EDGES_FOR_RESOLUTION ? topMinute / edges : 0;

        return {
          parkId,
          parkSlug: list[0].parkSlug,
          parkName: list[0].parkName,
          parkCity: list[0].parkCity,
          ridesWithOutages: list.length,
          outages: outageCount,
          singleReadingShare: Math.round(share * 1000) / 1000,
          onTheHourShare: Math.round(onTheHourShare * 1000) / 1000,
          regime:
            onTheHourShare >= ARTEFACT_ON_THE_HOUR_SHARE
              ? ("artefact" as const)
              : ("reports" as const),
        };
      })
      .sort((a, b) => b.outages - a.outages);
  }
}

/**
 * Share of one minute-of-hour value above which a feed is hourly, not observed.
 *
 * Mirrors `DOWNTIME_GATES.artefactOnTheHourShare`; kept here too because this
 * service is the read-only measurement and must not import the writer.
 */
const ARTEFACT_ON_THE_HOUR_SHARE = 0.5;

/** Run edges a park needs before its resolution is called either way. */
const MIN_EDGES_FOR_RESOLUTION = 20;

// ── shapes ───────────────────────────────────────────────────────────────────

interface OutageRow {
  attractionId: string;
  attractionName: string;
  parkId: string;
  parkSlug: string;
  parkName: string;
  parkCity: string | null;
  startedAt: Date;
  endedAt: Date | null;
  rowsInRun: number;
}

export interface CapabilityCensus {
  downCapableParks: number;
  totalParks: number;
  parksWithSchedule: number;
}

export interface RideMeasurement {
  attractionId: string;
  /**
   * The park's id, and the only safe grouping key.
   *
   * `parkSlug` is NOT unique: `disneyland-park` is Anaheim *and* Paris, and
   * grouping the per-park report on it merged the two into one row with 71
   * rides and 4979 outages.
   */
  parkId: string;
  parkSlug: string;
  parkName: string;
  parkCity: string | null;
  attractionName: string;
  outages: number;
  ongoing: number;
  singleReadingRuns: number;
  medianMinutes: number | null;
  longestMinutes: number | null;
  histogram: Record<string, number>;
}

export interface ParkMeasurement {
  parkId: string;
  /** Not unique — see {@link RideMeasurement.parkId}. Carried for readability. */
  parkSlug: string;
  parkName: string;
  /** What tells two same-slug parks apart in a report a human reads. */
  parkCity: string | null;
  ridesWithOutages: number;
  outages: number;
  /**
   * Share of runs resting on a single reading.
   *
   * Kept as a descriptive figure and NO LONGER a regime test. Measured over 90
   * days it correlates with "share of outages shorter than an hour" at r =
   * 0.996: `queue_data` is a change log, so an outage that ends before the
   * hourly heartbeat fires writes exactly one row. It is the signature of a
   * short outage, not of an eroded one. See {@link DowntimeMeasurement.regime}.
   */
  singleReadingShare: number;
  /**
   * Share of DOWN readings whose timestamp lands exactly on the hour.
   *
   * This is what the artefact regime was always meant to catch: a feed that
   * only publishes hourly cannot yield a duration. Measured over every
   * down-capable park, the highest observed value is 0.236 (Lotte World), so
   * the regime is currently empty — which is the honest answer, and a very
   * different one from "every big park is an artefact".
   */
  onTheHourShare: number;
  regime: "artefact" | "reports";
}

export interface ErasureRow {
  parkSlug: string;
  parkName: string;
  erasedDownRows: number | string;
  erasedClosedRows: number | string;
  ridesAffected: number | string;
  erasedDownMinutes: number | string;
  rowsSeen: number | string;
}

export interface ErasureMeasurement {
  measuredAt: string;
  windowDays: number;
  caveat: string;
  totals: {
    erasedDownRows: number;
    erasedClosedRows: number;
    erasedDownMinutes: number;
    ridesAffected: number;
  };
  parks: ErasureRow[];
}

export interface DowntimeMeasurement {
  measuredAt: string;
  windowDays: number;
  parkSlugs: string[];
  capability: CapabilityCensus;
  totals: {
    rides: number;
    outages: number;
    ongoing: number;
    singleReadingRuns: number;
    ridesAtOrAboveEventFloor: number;
  };
  parks: ParkMeasurement[];
  rides: RideMeasurement[];
}

// ── arithmetic ───────────────────────────────────────────────────────────────

function minutesBetween(from: Date, to: Date): number {
  return Math.round(
    (new Date(to).getTime() - new Date(from).getTime()) / 60000,
  );
}

/**
 * The empirical median, not a Kaplan-Meier one.
 *
 * Under censoring a KM median is not "half of the observed outages", and the
 * sentence beside a published figure has to be checkable against the counts
 * beside it. Runs with no observed end are excluded and counted separately.
 */
function median(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? Math.round((sorted[middle - 1] + sorted[middle]) / 2)
    : sorted[middle];
}

/**
 * Buckets chosen around the poll, not around round numbers.
 *
 * The first bucket is one poll interval: anything inside it is a run whose
 * start and end landed in adjacent cycles, which says the reading was erased
 * rather than that the ride recovered.
 */
const HISTOGRAM_BUCKETS: Array<[string, number]> = [
  ["0-5", 5],
  ["5-15", 15],
  ["15-30", 30],
  ["30-60", 60],
  ["60-180", 180],
  ["180-720", 720],
  ["720+", Number.POSITIVE_INFINITY],
];

function histogram(values: number[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [label] of HISTOGRAM_BUCKETS) out[label] = 0;
  for (const value of values) {
    for (const [label, upper] of HISTOGRAM_BUCKETS) {
      if (value <= upper) {
        out[label]++;
        break;
      }
    }
  }
  return out;
}
