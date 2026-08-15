import { Injectable, Logger } from "@nestjs/common";
import { DataSource } from "typeorm";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";

/**
 * Two detectors for the two ways this system went quietly wrong for weeks.
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
 */

export interface SilencedCluster {
  parkId: string;
  parkName: string;
  attractionCount: number;
  lastOperating: string;
  sampleNames: string[];
}

export interface FailingJob {
  queue: string;
  jobName: string;
  failures: number;
  lastReason: string;
  lastFailedAt: string | null;
}

/** Queues whose failures matter — every queue this app registers. */
const MONITORED_QUEUES = [
  "wait-times",
  "park-metadata",
  "children-metadata",
  "manual-metadata",
  "entity-mappings",
  "weather",
  "holidays",
  "ml-training",
  "prediction-accuracy",
  "predictions",
  "park-enrichment",
  "analytics",
  "ml-monitoring",
  "stats",
] as const;

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
             (array_agg(a.name ORDER BY a.name))[1:4] AS names
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
    }));
  }

  /**
   * Jobs that are scheduled, run, and throw.
   *
   * Complementary to the boot-time `hasRepeatableJob` check in
   * QueueSchedulerService, which catches the opposite failure: a repeatable job
   * that stopped being scheduled at all. Neither covers the other, and
   * detect-seasonal was the second kind for 73 days.
   *
   * Read straight off Bull's Redis keys rather than by injecting fourteen
   * queues: `<prefix>:<queue>:failed` is a ZSET of job ids (repeatable runs
   * appear as `repeat:<hash>:<millis>`), and each id resolves to a hash holding
   * `name`, `failedReason` and `finishedOn`. Verified against production, where
   * the detect-seasonal corpse still reads
   * `syntax error at or near "attr_activity"`.
   */
  async findFailingJobs(perQueueLimit = 100): Promise<FailingJob[]> {
    const client = this.analyticsQueue.client;
    const prefix = process.env.BULL_PREFIX || "parkfan";
    const results: FailingJob[] = [];

    for (const queueName of MONITORED_QUEUES) {
      try {
        const ids: string[] = await client.zrange(
          `${prefix}:${queueName}:failed`,
          -perQueueLimit,
          -1,
        );
        if (ids.length === 0) continue;

        const byJobName = new Map<string, FailingJob>();
        for (const id of ids) {
          const [name, failedReason, finishedOn] = await client.hmget(
            `${prefix}:${queueName}:${id}`,
            "name",
            "failedReason",
            "finishedOn",
          );
          const jobName = name ?? "unknown";
          const failedAt = finishedOn
            ? new Date(Number(finishedOn)).toISOString()
            : null;
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
          if (
            failedAt &&
            (!existing.lastFailedAt || failedAt > existing.lastFailedAt)
          ) {
            existing.lastFailedAt = failedAt;
            existing.lastReason = this.firstLine(failedReason);
          }
        }

        results.push(...byJobName.values());
      } catch (e) {
        this.logger.debug(
          `Could not read failures for queue ${queueName}: ${(e as Error)?.message ?? e}`,
        );
      }
    }

    return results.sort((a, b) => b.failures - a.failures);
  }

  /** Bull stores the whole stack in failedReason; the first line is the fact. */
  private firstLine(reason: string | null | undefined): string {
    return (reason ?? "unknown").split("\n")[0].slice(0, 300);
  }
}
