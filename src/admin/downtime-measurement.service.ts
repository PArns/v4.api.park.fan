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
          parkSlug: rows[0].parkSlug,
          parkName: rows[0].parkName,
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

    const parks = this.perPark(rides);

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
               p.name AS pname
          FROM attractions a
          JOIN parks p ON p.id = a."parkId"
         WHERE a.retired_at IS NULL
           AND p.wiki_entity_id IS NOT NULL
           AND ($1::boolean = false OR p.slug = ANY($2::text[]))
      ),
      marked AS (
        SELECT t.aid, t.aname, t.pslug, t.pname,
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
        SELECT aid, aname, pslug, pname, grp,
               MIN(ts) FILTER (WHERE NOT breaks) AS started_at,
               -- The breaking row IS the end: it is the first reading that says
               -- the ride is no longer down. NULL when the run is still open at
               -- the edge of the window or right now.
               MAX(ts) FILTER (WHERE breaks)     AS ended_at,
               COUNT(*) FILTER (WHERE NOT breaks)::int AS rows_in_run
          FROM seq
         GROUP BY aid, aname, pslug, pname, grp
        HAVING COUNT(*) FILTER (WHERE NOT breaks) > 0
      )
      SELECT aid        AS "attractionId",
             aname      AS "attractionName",
             pslug      AS "parkSlug",
             pname      AS "parkName",
             started_at AS "startedAt",
             ended_at   AS "endedAt",
             rows_in_run AS "rowsInRun"
        FROM runs
       ORDER BY pslug, aname, started_at
      `,
      [filtered, parkSlugs, since],
    );
  }

  /**
   * The artefact signature, per park.
   *
   * A park where nearly every run is a single reading is not a park with brief
   * outages. It is a park whose DOWN readings are being erased between polls —
   * `ConflictResolverService` rewrites DOWN to OPERATING as soon as a second
   * source reports a wait of five minutes or more, and a queue takes fifteen to
   * thirty minutes to drain, so what survives is the first reading and nothing
   * after it. A duration read off that measures how fast the queue emptied.
   */
  private perPark(rides: RideMeasurement[]): ParkMeasurement[] {
    const byPark = new Map<string, RideMeasurement[]>();
    for (const ride of rides) {
      const list = byPark.get(ride.parkSlug) ?? [];
      list.push(ride);
      byPark.set(ride.parkSlug, list);
    }
    return [...byPark.entries()]
      .map(([parkSlug, list]) => {
        const outages = list.reduce((sum, r) => sum + r.outages, 0);
        const single = list.reduce((sum, r) => sum + r.singleReadingRuns, 0);
        const share = outages > 0 ? single / outages : 0;
        return {
          parkSlug,
          parkName: list[0].parkName,
          ridesWithOutages: list.length,
          outages,
          singleReadingShare: Math.round(share * 1000) / 1000,
          regime: share >= 0.7 ? ("artefact" as const) : ("reports" as const),
        };
      })
      .sort((a, b) => b.outages - a.outages);
  }
}

// ── shapes ───────────────────────────────────────────────────────────────────

interface OutageRow {
  attractionId: string;
  attractionName: string;
  parkSlug: string;
  parkName: string;
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
  parkSlug: string;
  parkName: string;
  attractionName: string;
  outages: number;
  ongoing: number;
  singleReadingRuns: number;
  medianMinutes: number | null;
  longestMinutes: number | null;
  histogram: Record<string, number>;
}

export interface ParkMeasurement {
  parkSlug: string;
  parkName: string;
  ridesWithOutages: number;
  outages: number;
  singleReadingShare: number;
  regime: "artefact" | "reports";
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
