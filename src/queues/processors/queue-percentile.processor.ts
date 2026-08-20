import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, DataSource } from "typeorm";
import { QueueDataAggregate } from "../../analytics/entities/queue-data-aggregate.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Show } from "../../shows/entities/show.entity";

/**
 * Queue Percentile Processor
 *
 * Pre-computes hourly percentiles for queue data.
 *
 * Strategy:
 * - Runs daily at 2am
 * - Calculates percentiles for yesterday (complete 24 hours)
 * - Uses PostgreSQL percentile_cont() for efficiency
 * - Idempotent upsert: the row id is a deterministic hash of
 *   (attractionId, hour), so the existing (id, hour) PK enforces one row
 *   per bucket and ON CONFLICT (id, hour) actually fires on re-runs
 *
 * Benefits:
 * - Fast ML feature lookups (no on-the-fly calculation)
 * - Efficient analytics API responses
 * - Temporal percentile comparisons
 *
 * Schedule: Daily at 2am (after midnight + buffer)
 */
@Processor("analytics")
export class QueuePercentileProcessor {
  private readonly logger = new Logger(QueuePercentileProcessor.name);

  constructor(
    @InjectRepository(QueueDataAggregate)
    private aggregateRepository: Repository<QueueDataAggregate>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    @InjectRepository(Show)
    private showRepository: Repository<Show>,
    private dataSource: DataSource,
  ) {}

  @Process("calculate-percentiles")
  async handleCalculatePercentiles(_job: Job): Promise<void> {
    this.logger.log("📊 Calculating hourly percentiles for yesterday...");

    try {
      // Calculate for yesterday (complete 24-hour period)
      // We calculate this over UTC since we aggregate by hour across all timezones.
      // This job runs globally, not per-park.
      const today = new Date();
      today.setUTCHours(0, 0, 0, 0);

      const yesterday = new Date(today);
      yesterday.setUTCDate(yesterday.getUTCDate() - 1);

      this.logger.log(
        `   Period: ${yesterday.toISOString()} to ${today.toISOString()}`,
      );

      // Use PostgreSQL percentile_cont for efficient calculation
      // Aggregates by hour for each attraction
      const result = await this.aggregateRepository.query(
        `
        INSERT INTO queue_data_aggregates (
          id, hour, "attractionId", "parkId",
          p25, p50, p75, p90, p95, p99,
          iqr, "stdDev", mean, "sampleCount",
          "createdAt", "updatedAt"
        )
        SELECT
          -- DETERMINISTIC id derived from the natural key (attractionId, hour).
          -- The PK is (id, hour), so a stable id makes that PK enforce one row
          -- per (attractionId, hour) and lets ON CONFLICT (id, hour) actually
          -- fire. Previously this was gen_random_uuid(), so the conflict target
          -- never matched and every re-run/retry/backfill inserted duplicate
          -- rows — double-counting sampleCount and skewing the percentiles.
          md5(qd."attractionId" || '|' || date_trunc('hour', qd.timestamp)::text)::uuid as id,
          date_trunc('hour', qd.timestamp) as hour,
          qd."attractionId",
          a."parkId",
          percentile_cont(0.25) WITHIN GROUP (ORDER BY qd."waitTime") as p25,
          percentile_cont(0.50) WITHIN GROUP (ORDER BY qd."waitTime") as p50,
          percentile_cont(0.75) WITHIN GROUP (ORDER BY qd."waitTime") as p75,
          percentile_cont(0.90) WITHIN GROUP (ORDER BY qd."waitTime") as p90,
          percentile_cont(0.95) WITHIN GROUP (ORDER BY qd."waitTime") as p95,
          percentile_cont(0.99) WITHIN GROUP (ORDER BY qd."waitTime") as p99,
          percentile_cont(0.75) WITHIN GROUP (ORDER BY qd."waitTime") - 
            percentile_cont(0.25) WITHIN GROUP (ORDER BY qd."waitTime") as iqr,
          STDDEV(qd."waitTime") as "stdDev",
          AVG(qd."waitTime") as mean,
          COUNT(*) as "sampleCount",
          NOW() as "createdAt",
          NOW() as "updatedAt"
        FROM queue_data qd
        INNER JOIN attractions a ON a.id = qd."attractionId"
        WHERE qd.timestamp >= $1 
          AND qd.timestamp < $2
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
          AND qd."queueType" = 'STANDBY'
        GROUP BY date_trunc('hour', qd.timestamp), qd."attractionId", a."parkId"
        HAVING COUNT(*) >= 3
        ON CONFLICT (id, hour) DO UPDATE SET
          p25 = EXCLUDED.p25,
          p50 = EXCLUDED.p50,
          p75 = EXCLUDED.p75,
          p90 = EXCLUDED.p90,
          p95 = EXCLUDED.p95,
          p99 = EXCLUDED.p99,
          iqr = EXCLUDED.iqr,
          "stdDev" = EXCLUDED."stdDev",
          mean = EXCLUDED.mean,
          "sampleCount" = EXCLUDED."sampleCount",
          "updatedAt" = NOW()
      `,
        [yesterday, today],
      );

      const rowCount = result[0]?.count || 0;
      this.logger.log(
        `✅ Percentile calculation complete: ${rowCount} hourly aggregates`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to calculate percentiles: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * Detect seasonal attractions.
   *
   * An attraction is seasonal when it was CLOSED (not REFURBISHMENT/DOWN) for ≥7 days
   * during which the park was demonstrably open (other attractions OPERATING).
   * We also derive seasonMonths from historical OPERATING data to know when it runs.
   * Reset: if OPERATING in the last 14 days → not seasonal.
   */
  @Process("detect-seasonal")
  async handleDetectSeasonal(_job: Job): Promise<void> {
    this.logger.log("🌸 Starting seasonal attraction detection...");

    const MIN_PARK_OPEN_DAYS_CLOSED = 7;
    // A season cannot be read off less than a year of watching. Until we have
    // seen an entity through a full cycle, the months it was "ever OPERATING"
    // are just the months we happened to be recording — see MIN_OBSERVED_DAYS.
    const MIN_OBSERVED_DAYS = 330;
    const LOOKBACK_DAYS = 60;
    const RESET_DAYS = 14;

    // Refresh the per-(attraction, park-local day) operating rollup the detection CTEs read
    // below, so they no longer re-scan ~60 days of raw queue_data on every run (that was the
    // ~227s top slow-query). Incremental upsert — only the full backfill (first run) is heavy.
    await this.refreshOperatingDayRollup(LOOKBACK_DAYS);

    // Step 1: Find attractions that were recently OPERATING (reset candidates)
    const recentlyOperating: { attractionId: string }[] =
      await this.dataSource.query(
        `
      SELECT DISTINCT q."attractionId"
      FROM queue_data q
      WHERE q.status = 'OPERATING'
        AND q.timestamp >= NOW() - $1 * INTERVAL '1 day'
    `,
        [RESET_DAYS],
      );
    const recentlyOperatingIds = new Set(
      recentlyOperating.map((r) => r.attractionId),
    );

    // Step 2: For each currently-marked-seasonal attraction that was recently OPERATING → reset
    if (recentlyOperatingIds.size > 0) {
      const ids = Array.from(recentlyOperatingIds);
      const resetResult = await this.dataSource.query(
        `
        UPDATE attractions
        SET is_seasonal = false, season_months = NULL
        WHERE id = ANY($1) AND is_seasonal = true
          -- Free-flow rows are curated by hand, and they DO emit the odd
          -- OPERATING record (Avoras produced 154), so this reset would wipe a
          -- curated season the first time one appeared in the feed.
          AND NOT open_with_park
      `,
        [ids],
      );
      if (resetResult[1] > 0) {
        this.logger.log(
          `   ♻️  Reset ${resetResult[1]} attractions (now operating again)`,
        );
      }
    }

    // Step 2b: Clear free-flow attractions that an earlier run mislabelled.
    // Step 2 resets on "recently OPERATING", which a playground rarely is — its
    // queue_data says CLOSED nearly always and only the read-time override says
    // otherwise. Without this the false positives are permanent.
    //
    // Scoped to rows with NO months, because that is exactly the mislabel's
    // signature: this detector cannot derive months for an attraction it never
    // sees OPERATING, so every false positive it produced had NULL. Months on a
    // free-flow row are therefore human-written — a curated season like
    // Europa-Park's summer-only water playgrounds — and this job does not own
    // them. Same two-writers boundary as curated_may_get_wet.
    const freeFlowReset = await this.dataSource.query(
      `UPDATE attractions
          SET is_seasonal = false
        WHERE open_with_park AND is_seasonal = true
          AND season_months IS NULL`,
    );

    // Step 2c: same shape, same reason. Excluding retired attractions from the
    // candidate searches stops them being marked AGAIN; it does not clear the
    // flag they already carry, and no reset path can reach them either — a
    // demolished ride will never report OPERATING. "Seasonal" says it closes
    // for part of the year, which is not what happened to it.
    const retiredReset = await this.dataSource.query(
      `UPDATE attractions
          SET is_seasonal = false, season_months = NULL
        WHERE retired_at IS NOT NULL AND is_seasonal = true`,
    );
    if (retiredReset[1] > 0) {
      this.logger.log(
        `   🪦 Reset ${retiredReset[1]} retired attractions (gone, not seasonal)`,
      );
    }
    if (freeFlowReset[1] > 0) {
      this.logger.log(
        `   🎠 Reset ${freeFlowReset[1]} free-flow attractions (never seasonal)`,
      );
    }

    // Step 3: Find attractions fully CLOSED (zero operating records that day) while park open
    // Requirements:
    // - Fully closed on ≥ MIN_PARK_OPEN_DAYS_CLOSED park-open days
    // - Current status = 'CLOSED' (not REFURBISHMENT/DOWN)
    // - Has ≥ MIN_EVER_OPERATING all-time OPERATING records (rules out new/untracked rides)
    // - Not recently operating (already handled by reset in step 1)
    const MIN_EVER_OPERATING = 20;
    const candidates: { attractionId: string; parkId: string }[] =
      await this.dataSource.query(
        `
      WITH park_open_days AS (
        -- park-local days any attraction in the park was OPERATING (precomputed rollup)
        SELECT DISTINCT "parkId", op_day AS open_day
        FROM attraction_day_operating
        WHERE op_day >= CURRENT_DATE - $2::int
      ),
      attraction_operating_days AS (
        -- (attraction, park-local day) it was OPERATING (precomputed rollup)
        SELECT "attractionId", op_day
        FROM attraction_day_operating
        WHERE op_day >= CURRENT_DATE - $2::int
      ),
      ever_operating AS (
        SELECT "attractionId", COUNT(*) as op_count
        FROM queue_data
        WHERE status = 'OPERATING'
          AND timestamp >= NOW() - INTERVAL '365 days'
        GROUP BY "attractionId"
        HAVING COUNT(*) >= $3
      ),
      current_status AS (
        SELECT DISTINCT ON ("attractionId")
          "attractionId", status
        FROM queue_data
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        ORDER BY "attractionId", timestamp DESC
      ),
      days_fully_closed AS (
        SELECT
          a.id as "attractionId",
          a."parkId",
          COUNT(DISTINCT pod.open_day) as fully_closed_days
        FROM park_open_days pod
        -- Free-flow attractions (playgrounds, splash pads) have no queue, so
        -- their feed reports CLOSED every single day. That is exactly this
        -- detector's seasonal signature, and it is a false positive: they are
        -- open whenever the park is. Three of Phantasialand's four were marked
        -- seasonal with no months; open_with_park already states the truth.
        JOIN attractions a ON a."parkId" = pod."parkId"
                          AND NOT a.open_with_park
                          -- A retired ride is this detector's perfect
                          -- candidate: permanently CLOSED on every park-open
                          -- day. It is not seasonal, it is gone.
                          AND a.retired_at IS NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM attraction_operating_days aod
          WHERE aod."attractionId" = a.id AND aod.op_day = pod.open_day
        )
        GROUP BY a.id, a."parkId"
      )
      SELECT d."attractionId", d."parkId"
      FROM days_fully_closed d
      JOIN ever_operating eo ON eo."attractionId" = d."attractionId"
      JOIN current_status cs ON cs."attractionId" = d."attractionId"
      WHERE d.fully_closed_days >= $4
        AND cs.status = 'CLOSED'
        AND NOT (d."attractionId" = ANY($1))
    `,
        [
          Array.from(recentlyOperatingIds),
          LOOKBACK_DAYS,
          MIN_EVER_OPERATING,
          MIN_PARK_OPEN_DAYS_CLOSED,
        ],
      );

    this.logger.log(`   🔍 Found ${candidates.length} seasonal candidates`);

    // Step 3a: drop the candidates whose "season" began at the same instant as
    // everybody else's.
    //
    // On 2026-06-07 at 13:18 thirty-eight Europa-Park rides stopped reporting
    // OPERATING within the same minute, and eighteen at Rulantica twenty-five
    // minutes earlier. Universal Studios Singapore did it on 2026-04-25, Six
    // Flags Fiesta Texas on 2026-04-12. The park stayed open, the rides kept
    // being reported — as CLOSED, for months. To this detector that is the
    // exact signature of a season, so it called half of Europa-Park seasonal.
    //
    // It is a source event: a feed that changed what it says about a class of
    // rides. Forty seasons do not start in the same minute, and a real one is
    // ragged — each ride has its own last day. So a cluster of candidates in
    // one park sharing a last-OPERATING minute is not evidence of a season,
    // and this job's whole doctrine is that it describes the feed rather than
    // speaking for the operator. The honest answer for these rides is that we
    // do not know, which is what NOT flagging them says.
    // Clustered over today's candidates AND everything an earlier run already
    // marked: a ride that stopped reporting in April is no longer a candidate
    // (its current status has moved on) but still carries the flag, and no
    // reset path can reach it — it will never report OPERATING again on its
    // own. Both sets are the same question asked at different times.
    const alreadyFlagged: Array<{ attractionId: string; parkId: string }> =
      await this.dataSource.query(
        `SELECT id AS "attractionId", "parkId"
           FROM attractions
          WHERE is_seasonal = true
            AND retired_at IS NULL
            AND NOT open_with_park
            -- A human's verdict is not this job's to re-litigate.
            AND curated_is_seasonal IS NULL`,
      );
    const clusterInput = [
      ...candidates,
      ...alreadyFlagged.filter(
        (f) => !candidates.some((c) => c.attractionId === f.attractionId),
      ),
    ];
    const lastOperating = await this.lastOperatingByAttraction(
      clusterInput.map((c) => c.attractionId),
    );
    const feedEventIds = await this.findFeedEventCluster(
      clusterInput,
      lastOperating,
    );
    const survivingCandidates = candidates.filter(
      (c) => !feedEventIds.has(c.attractionId),
    );

    if (feedEventIds.size > 0) {
      // Written down, not just skipped: a run that silently drops candidates
      // is indistinguishable from a run that found none.
      this.logger.warn(
        `   📡 Ignored ${feedEventIds.size} candidates whose last OPERATING falls in one shared minute per park — that is a feed change, not a season`,
      );
      // And cleared, because skipping them only stops the flag being written
      // again; the ones an earlier run already wrote would stay forever. No
      // reset path can reach them either: they will never report OPERATING
      // again on their own.
      const cleared = await this.dataSource.query(
        `UPDATE attractions
            SET is_seasonal = false, season_months = NULL, season_out_since = NULL
          WHERE id = ANY($1::uuid[]) AND is_seasonal = true`,
        [Array.from(feedEventIds)],
      );
      if (cleared[1] > 0) {
        this.logger.log(
          `   🧹 Cleared ${cleared[1]} attractions an earlier run marked seasonal on the same evidence`,
        );
      }
    }

    // Step 3b: "Zero-history" candidates — never seen OPERATING at all, but CLOSED on
    // every park-open day in the lookback window (≥ MIN_PARK_OPEN_DAYS_CLOSED days).
    // Catches event-only rides (e.g. Halloween) at parks where we started tracking
    // after the last event ran. Reset in Step 2 corrects them once they operate again.
    const MIN_ZERO_HISTORY_OPEN_DAYS = MIN_PARK_OPEN_DAYS_CLOSED;
    const zeroHistoryCandidates: { attractionId: string; parkId: string }[] =
      await this.dataSource.query(
        `
      -- Per-attraction all-time activity in ONE pass over queue_data.
      -- Row present   => attraction has >=1 queue_data record (ever).
      -- has_operating => attraction was OPERATING at least once (ever).
      -- This replaces the previous correlated EXISTS subqueries against
      -- queue_data (one per attraction), which re-scanned the whole compressed
      -- queue_data hypertable once PER attraction (thousands of chunk-
      -- decompressing probes, ~240s). Equivalent semantics, single scan.
      WITH attr_activity AS (
        SELECT "attractionId", bool_or(status = 'OPERATING') AS has_operating
        FROM queue_data
        GROUP BY "attractionId"
      ),
      -- park-local days any attraction in the park was OPERATING (precomputed rollup)
      park_open_day_counts AS (
        SELECT "parkId", COUNT(DISTINCT op_day) as open_days
        FROM attraction_day_operating
        WHERE op_day >= CURRENT_DATE - $2::int
        GROUP BY "parkId"
      ),
      -- Only apply zero-history logic to parks where the majority of
      -- tracked attractions have at least one OPERATING record.
      -- Prevents flagging parks with systematic data-source gaps
      -- (e.g. parks where the API never reports OPERATING status).
      -- JOIN attr_activity == "attraction has >=1 queue_data record".
      park_tracking_quality AS (
        SELECT a."parkId"
        FROM attractions a
        JOIN attr_activity aa ON aa."attractionId" = a.id
        GROUP BY a."parkId"
        HAVING SUM(CASE WHEN aa.has_operating THEN 1 ELSE 0 END)
             > SUM(CASE WHEN NOT aa.has_operating THEN 1 ELSE 0 END)
      ),
      never_operating AS (
        SELECT a.id as "attractionId", a."parkId"
        FROM attractions a
        JOIN attr_activity aa ON aa."attractionId" = a.id
        -- Same reason as above: "never seen OPERATING" is the normal state of
        -- a playground, not evidence of a season.
        WHERE aa.has_operating = false AND NOT a.open_with_park
          AND a.retired_at IS NULL
      ),
      current_status AS (
        SELECT DISTINCT ON ("attractionId")
          "attractionId", status
        FROM queue_data
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        ORDER BY "attractionId", timestamp DESC
      )
      -- A never_operating attraction has, by definition, ZERO OPERATING days,
      -- so it never appears in the attraction_day_operating rollup (that rollup
      -- is populated only from status='OPERATING' rows — see refreshOperatingDayRollup).
      -- Therefore it is CLOSED on EVERY park-open day, i.e. closed_days always
      -- equals the park's open_days. The previous closed_on_open_days CTE
      -- (never_operating × park_open_days, with a per-pair NOT EXISTS against the
      -- rollup) computed that always-true count over ~74M buffer hits (~133s).
      -- It filtered nothing, so it is dropped: keep the park open-day-count gate
      -- (poc.open_days >= $3) and the "currently CLOSED" gate directly.
      SELECT no."attractionId", no."parkId"
      FROM never_operating no
      JOIN current_status cs ON cs."attractionId" = no."attractionId"
      JOIN park_open_day_counts poc ON poc."parkId" = no."parkId"
      JOIN park_tracking_quality ptq ON ptq."parkId" = no."parkId"
      WHERE poc.open_days >= $3
        AND cs.status = 'CLOSED'
        AND NOT (no."attractionId" = ANY($1))
    `,
        [
          Array.from(recentlyOperatingIds),
          LOOKBACK_DAYS,
          MIN_ZERO_HISTORY_OPEN_DAYS,
        ],
      );

    const existingCandidateIds = new Set(
      survivingCandidates.map((c) => c.attractionId),
    );
    const allCandidates = [
      ...survivingCandidates,
      ...zeroHistoryCandidates.filter(
        (c) => !existingCandidateIds.has(c.attractionId),
      ),
    ];

    this.logger.log(
      `   🎃 Found ${zeroHistoryCandidates.length} zero-history seasonal candidates`,
    );

    // Step 4: Derive seasonMonths for ALL candidates in ONE grouped query
    // (zero-history candidates will have seasonMonths = null — unknown season).
    //
    // This used to run one 730-day queue_data scan PLUS one UPDATE per
    // candidate. Against a hypertable of this size that per-candidate scan was
    // by far the most expensive N+1 in the codebase; the work is identical when
    // done as a single GROUP BY, and the write collapses into one UPDATE.
    if (allCandidates.length > 0) {
      const candidateIds = allCandidates.map((c) => c.attractionId);

      const monthRows: { attractionId: string; months: number[] }[] =
        await this.dataSource.query(
          `WITH observed AS (
             -- How long we have WATCHED each candidate — every row, whatever
             -- the status. Deliberately not the OPERATING span: a real winter
             -- attraction only ever operates for about forty days, and that
             -- says nothing about whether we have seen a full year of it.
             SELECT "attractionId",
                    max(timestamp) - min(timestamp) AS watched
               FROM queue_data
              WHERE "attractionId" = ANY($1::uuid[])
              GROUP BY "attractionId"
           )
           SELECT a.id AS "attractionId",
                  ARRAY_AGG(DISTINCT EXTRACT(MONTH FROM q.timestamp AT TIME ZONE p.timezone)::int) AS months
           FROM queue_data q
           JOIN attractions a ON a.id = q."attractionId"
           JOIN parks p ON p.id = a."parkId"
           JOIN observed o ON o."attractionId" = a.id
           WHERE a.id = ANY($1::uuid[])
             AND q.status = 'OPERATING'
             AND q.timestamp >= NOW() - INTERVAL '730 days'
             AND o.watched >= ($2::int * INTERVAL '1 day')
           GROUP BY a.id`,
          [candidateIds, MIN_OBSERVED_DAYS],
        );

      const monthsById = new Map(
        monthRows.map((r) => [
          r.attractionId,
          [...(r.months ?? [])].sort((a, b) => a - b),
        ]),
      );

      // One UPDATE for every candidate. The payload is a single jsonb object
      // (id → months) so there is no array-of-jsonb escaping to get wrong;
      // a JSON `null` is mapped back to SQL NULL to match the previous write.
      // `season_out_since` rides along: the last park-local day the ride was
      // seen OPERATING. It is the fact this job already had to establish in
      // order to flag anything — and, until `MIN_OBSERVED_DAYS` can be met, the
      // only thing a seasonal ride can say about itself. Without it every
      // flagged ride reads "seasonal, months unknown", `isCurrentlyInSeason`
      // answers null, and an ice rink sits on the ride list in August.
      const zeroHistoryLastOperating = await this.lastOperatingByAttraction(
        zeroHistoryCandidates.map((c) => c.attractionId),
      );
      const payload: Record<
        string,
        { months: number[] | null; outSince: string | null }
      > = {};
      for (const id of candidateIds) {
        payload[id] = {
          months: monthsById.get(id) ?? null,
          outSince:
            lastOperating.get(id)?.day ??
            zeroHistoryLastOperating.get(id)?.day ??
            null,
        };
      }

      await this.dataSource.query(
        `UPDATE attractions a
         SET is_seasonal = true,
             season_months = CASE
               WHEN v.value->'months' = 'null'::jsonb THEN NULL
               ELSE v.value->'months'
             END,
             season_out_since = CASE
               WHEN v.value->>'outSince' IS NULL THEN NULL
               ELSE (v.value->>'outSince')::date
             END
         FROM jsonb_each($1::jsonb) v
         WHERE a.id = v.key::uuid`,
        [JSON.stringify(payload)],
      );
    }

    this.logger.log(
      `✅ Attractions: marked ${allCandidates.length} as seasonal (${survivingCandidates.length} with history, ${zeroHistoryCandidates.length} zero-history).`,
    );

    // ── Shows ──────────────────────────────────────────────────────────────
    // Signal: ThemeParks.wiki stops updating `lastUpdated` when a show is no
    // longer running. We use the same thresholds as attractions.
    //
    // Reset: lastUpdated within RESET_DAYS → show is running again.
    // Detect: show's lastUpdated has been stale for ≥ MIN_PARK_OPEN_DAYS_CLOSED
    //         park-open days (park open = other attractions OPERATING).
    // seasonMonths: months where the show's lastUpdated was fresh
    //               (timestamp − lastUpdated < 24h → show was actively running).

    // Step S1: Reset shows that are running again (fresh lastUpdated)
    const recentlyUpdatedShows: { showId: string }[] =
      await this.dataSource.query(
        `
      SELECT DISTINCT "showId"
      FROM show_live_data
      WHERE "lastUpdated" >= NOW() - $1 * INTERVAL '1 day'
    `,
        [RESET_DAYS],
      );
    const recentShowIds = new Set(recentlyUpdatedShows.map((r) => r.showId));

    if (recentShowIds.size > 0) {
      const ids = Array.from(recentShowIds);
      const resetShows = await this.dataSource.query(
        `
        UPDATE shows
        SET is_seasonal = false, season_months = NULL
        WHERE id = ANY($1) AND is_seasonal = true
      `,
        [ids],
      );
      if (resetShows[1] > 0) {
        this.logger.log(`   ♻️  Reset ${resetShows[1]} shows (running again)`);
      }
    }

    // Step S2: Find show candidates — lastUpdated stale for ≥ MIN_PARK_OPEN_DAYS_CLOSED park-open days
    const showCandidates: { showId: string }[] = await this.dataSource.query(
      `
      WITH park_open_days AS (
        -- park-local days any attraction in the park was OPERATING (precomputed rollup)
        SELECT DISTINCT "parkId", op_day AS open_day
        FROM attraction_day_operating
        WHERE op_day >= CURRENT_DATE - $2::int
      ),
      show_last_updated AS (
        SELECT DISTINCT ON ("showId")
          "showId",
          "lastUpdated"
        FROM show_live_data
        WHERE "lastUpdated" IS NOT NULL
          AND "lastUpdated" >= NOW() - INTERVAL '18 months'
        ORDER BY "showId", "lastUpdated" DESC
      ),
      stale_days AS (
        SELECT
          slu."showId",
          COUNT(DISTINCT pod.open_day) as stale_open_days
        FROM show_last_updated slu
        JOIN shows s ON s.id = slu."showId"
        JOIN parks p ON p.id = s."parkId"
        JOIN park_open_days pod ON pod."parkId" = s."parkId"
          AND pod.open_day > DATE(slu."lastUpdated" AT TIME ZONE p.timezone)
        GROUP BY slu."showId"
      )
      SELECT "showId"
      FROM stale_days
      WHERE stale_open_days >= $3
        AND NOT ("showId" = ANY($1))
    `,
      [Array.from(recentShowIds), LOOKBACK_DAYS, MIN_PARK_OPEN_DAYS_CLOSED],
    );

    this.logger.log(
      `   🔍 Found ${showCandidates.length} seasonal show candidates`,
    );

    // Step S3: Derive seasonMonths from months where lastUpdated was fresh —
    // one grouped query + one UPDATE for all candidates (was: both per show).
    if (showCandidates.length > 0) {
      const showIds = showCandidates.map((c) => c.showId);

      const monthRows: { showId: string; months: number[] }[] =
        await this.dataSource.query(
          `WITH observed AS (
             -- Same rule as the attractions above: the months only mean
             -- something once we have watched a full cycle.
             SELECT "showId", max(timestamp) - min(timestamp) AS watched
               FROM show_live_data
              WHERE "showId" = ANY($1::uuid[])
              GROUP BY "showId"
           )
           SELECT sld."showId" AS "showId",
                  ARRAY_AGG(DISTINCT EXTRACT(MONTH FROM sld.timestamp AT TIME ZONE p.timezone)::int) AS months
           FROM show_live_data sld
           JOIN shows s ON s.id = sld."showId"
           JOIN parks p ON p.id = s."parkId"
           JOIN observed o ON o."showId" = sld."showId"
           WHERE sld."showId" = ANY($1::uuid[])
             AND sld."lastUpdated" IS NOT NULL
             AND (sld.timestamp - sld."lastUpdated") < INTERVAL '24 hours'
             AND o.watched >= ($2::int * INTERVAL '1 day')
           GROUP BY sld."showId"`,
          [showIds, MIN_OBSERVED_DAYS],
        );

      const monthsById = new Map(
        monthRows.map((r) => [
          r.showId,
          [...(r.months ?? [])].sort((a, b) => a - b),
        ]),
      );

      const payload: Record<string, number[] | null> = {};
      for (const id of showIds) {
        payload[id] = monthsById.get(id) ?? null;
      }

      await this.dataSource.query(
        `UPDATE shows s
         SET is_seasonal = true,
             season_months = CASE WHEN v.value = 'null'::jsonb THEN NULL ELSE v.value END
         FROM jsonb_each($1::jsonb) v
         WHERE s.id = v.key::uuid`,
        [JSON.stringify(payload)],
      );
    }

    this.logger.log(`✅ Shows: marked ${showCandidates.length} as seasonal.`);
  }

  /**
   * The last moment each attraction was seen OPERATING, in park-local time.
   *
   * Two things read it: the feed-event guard below, and `season_out_since`,
   * which is the only thing a seasonal ride can say about itself while the
   * months are still out of reach.
   */
  private async lastOperatingByAttraction(
    attractionIds: string[],
  ): Promise<Map<string, { at: Date; day: string }>> {
    if (attractionIds.length === 0) return new Map();

    const rows: Array<{ attractionId: string; at: Date; day: string }> =
      await this.dataSource.query(
        `SELECT q."attractionId",
                max(q.timestamp) AS at,
                to_char(
                  max(q.timestamp) AT TIME ZONE p.timezone, 'YYYY-MM-DD'
                ) AS day
           FROM queue_data q
           JOIN attractions a ON a.id = q."attractionId"
           JOIN parks p ON p.id = a."parkId"
          WHERE q."attractionId" = ANY($1::uuid[])
            AND q.status = 'OPERATING'
          GROUP BY q."attractionId", p.timezone`,
        [attractionIds],
      );

    return new Map(rows.map((row) => [row.attractionId, row]));
  }

  /**
   * The candidates whose "season" started in the same minute as everybody
   * else's, which is a feed changing rather than a park closing something.
   *
   * A real seasonal closure is ragged: each ride has its own last day, because
   * each ride stops when it stops. A source event is not — it is one upstream
   * write, and it lands on every affected ride in the same minute. That is the
   * whole discriminator, and it is why this compares minutes rather than days:
   * a park shutting for winter does close its rides on one *day*, and this
   * must not mistake that for a feed change.
   *
   * Two thresholds, both needed. An absolute floor, because three rides
   * sharing a minute is a coincidence a park can produce. And a share of the
   * park, because five rides is a lot at a park with twelve and nothing at a
   * park with two hundred.
   */
  private async findFeedEventCluster(
    candidates: Array<{ attractionId: string; parkId: string }>,
    lastOperating: Map<string, { at: Date; day: string }>,
  ): Promise<Set<string>> {
    const MIN_CLUSTER_SIZE = 5;
    // 15 %, measured rather than chosen: the four clusters in production sit at
    // 42 % (Europa-Park), 44 % (Rulantica), 49 % (Universal Studios Singapore)
    // and 19 % (Six Flags Fiesta Texas), and Knott's Berry Farm at 17 %.
    //
    // The cost of the threshold is worth stating: a park that really does close
    // a whole area on one evening produces the same shape, and this will
    // decline to flag it. That is the direction to fail in — the flag then
    // stays off and a curator can set `curated_is_seasonal`, which is a fact
    // somebody checked, instead of the detector asserting a season it cannot
    // tell from a feed change.
    const MIN_PARK_SHARE = 0.15;
    if (candidates.length === 0) return new Set<string>();

    const trackedPerPark = await this.trackedAttractionsPerPark(
      Array.from(new Set(candidates.map((c) => c.parkId))),
    );

    const clusters = findSharedMinuteClusters(
      candidates,
      lastOperating,
      trackedPerPark,
      { minSize: MIN_CLUSTER_SIZE, minParkShare: MIN_PARK_SHARE },
    );

    const ignored = new Set<string>();
    for (const cluster of clusters) {
      cluster.attractionIds.forEach((id) => ignored.add(id));
      this.logger.warn(
        `   📡 ${cluster.attractionIds.length} of ${cluster.tracked} tracked rides in park ${cluster.parkId} last reported OPERATING at ${cluster.minute}Z — reading that as a season would be reading the feed`,
      );
    }

    return ignored;
  }

  /** Attractions a park has that this detector considers at all. */
  private async trackedAttractionsPerPark(
    parkIds: string[],
  ): Promise<Map<string, number>> {
    if (parkIds.length === 0) return new Map();
    const rows: Array<{ parkId: string; tracked: string }> =
      await this.dataSource.query(
        `SELECT "parkId", COUNT(*)::text AS tracked
           FROM attractions
          WHERE "parkId" = ANY($1::uuid[])
            AND retired_at IS NULL
            AND NOT open_with_park
          GROUP BY "parkId"`,
        [parkIds],
      );
    return new Map(rows.map((row) => [row.parkId, Number(row.tracked)]));
  }

  /**
   * Maintain the `attraction_day_operating` rollup (any-OPERATING / any-queueType, per
   * park-local day). detect-seasonal's CTEs read this small table instead of re-scanning
   * ~60 days of raw `queue_data` every run (the old multi-CTE scan peaked at ~227s).
   *
   * Incremental: scans only from 2 local days before the last stored day (boundary-safe;
   * ON CONFLICT dedupes). The one-time full backfill (lookback + 5d buffer) runs only when
   * the table is empty. Old rows beyond lookback + 30d are pruned to bound growth.
   */
  private async refreshOperatingDayRollup(lookbackDays: number): Promise<void> {
    const maxRow: { max: string | null }[] = await this.dataSource.query(
      `SELECT MAX(op_day)::text AS max FROM attraction_day_operating`,
    );
    const maxDay = maxRow[0]?.max ?? null;
    const since = maxDay
      ? { clause: `($1::date - INTERVAL '2 days')`, param: maxDay }
      : { clause: `(NOW() - $1 * INTERVAL '1 day')`, param: lookbackDays + 5 };

    await this.dataSource.query(
      `
      INSERT INTO attraction_day_operating ("attractionId", "parkId", op_day, "updatedAt")
      SELECT DISTINCT
        q."attractionId",
        a."parkId",
        DATE(q.timestamp AT TIME ZONE p.timezone) AS op_day,
        NOW()
      FROM queue_data q
      JOIN attractions a ON a.id = q."attractionId"
      JOIN parks p ON p.id = a."parkId"
      WHERE q.status = 'OPERATING'
        AND q.timestamp >= ${since.clause}
      ON CONFLICT ("attractionId", op_day) DO NOTHING
      `,
      [since.param],
    );

    await this.dataSource.query(
      `DELETE FROM attraction_day_operating
       WHERE op_day < CURRENT_DATE - ($1 + 30) * INTERVAL '1 day'`,
      [lookbackDays],
    );

    this.logger.log(
      `   🗓️  attraction_day_operating refreshed (${maxDay ? `incremental from ${maxDay}` : `backfill ${lookbackDays + 5}d`})`,
    );
  }

  /**
   * Backfill percentiles for historical data
   * Can be triggered manually via job scheduler
   */
  @Process("backfill-percentiles")
  async handleBackfillPercentiles(job: Job<{ days: number }>): Promise<void> {
    const days = job.data?.days || 90;
    this.logger.log(`📊 Backfilling percentiles for last ${days} days...`);

    try {
      const endDate = new Date();
      endDate.setUTCHours(0, 0, 0, 0);

      const startDate = new Date(endDate);
      startDate.setUTCDate(startDate.getUTCDate() - days);

      this.logger.log(
        `   Period: ${startDate.toISOString()} to ${endDate.toISOString()}`,
      );

      // Process in batches of 7 days to avoid memory issues
      let currentDate = new Date(startDate);
      let totalRows = 0;

      while (currentDate < endDate) {
        const batchEnd = new Date(currentDate);
        batchEnd.setUTCDate(batchEnd.getUTCDate() + 7);
        const actualEnd = batchEnd > endDate ? endDate : batchEnd;

        this.logger.log(
          `   Processing batch: ${currentDate.toISOString()} to ${actualEnd.toISOString()}`,
        );

        const result = await this.aggregateRepository.query(
          `
          INSERT INTO queue_data_aggregates (
            id, hour, "attractionId", "parkId",
            p25, p50, p75, p90, p95, p99,
            iqr, "stdDev", mean, "sampleCount",
            "createdAt", "updatedAt"
          )
          SELECT
            -- Deterministic id from (attractionId, hour) — see calculate-
            -- percentiles. Makes the (id, hour) PK dedupe and ON CONFLICT fire.
            md5(qd."attractionId" || '|' || date_trunc('hour', qd.timestamp)::text)::uuid as id,
            date_trunc('hour', qd.timestamp) as hour,
            qd."attractionId",
            a."parkId",
            percentile_cont(0.25) WITHIN GROUP (ORDER BY qd."waitTime") as p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY qd."waitTime") as p50,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY qd."waitTime") as p75,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY qd."waitTime") as p90,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY qd."waitTime") as p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY qd."waitTime") as p99,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY qd."waitTime") - 
              percentile_cont(0.25) WITHIN GROUP (ORDER BY qd."waitTime") as iqr,
            STDDEV(qd."waitTime") as "stdDev",
            AVG(qd."waitTime") as mean,
            COUNT(*) as "sampleCount",
            NOW() as "createdAt",
            NOW() as "updatedAt"
          FROM queue_data qd
          INNER JOIN attractions a ON a.id = qd."attractionId"
          WHERE qd.timestamp >= $1 
            AND qd.timestamp < $2
            AND qd.status = 'OPERATING'
            AND qd."waitTime" IS NOT NULL
            AND qd."queueType" = 'STANDBY'
          GROUP BY date_trunc('hour', qd.timestamp), qd."attractionId", a."parkId"
          HAVING COUNT(*) >= 3
          ON CONFLICT (id, hour) DO NOTHING
        `,
          [currentDate, actualEnd],
        );

        const batchRows = result[0]?.count || 0;
        totalRows += batchRows;

        currentDate = actualEnd;
      }

      this.logger.log(
        `✅ Backfill complete: ${totalRows} total hourly aggregates for ${days} days`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to backfill percentiles: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  /**
   * One-time (re-runnable) cleanup of historical DUPLICATE rows in
   * queue_data_aggregates.
   *
   * Before the deterministic-id fix, the upsert used gen_random_uuid() so
   * re-runs/retries/backfills inserted duplicate (attractionId, hour) rows.
   * The id fix stops NEW duplicates, but the old ones still skew the
   * percentile-of-percentiles + SUM(sampleCount) reads (and the ML percentile
   * features) until removed. This collapses each (attractionId, hour) bucket
   * back to a single row, keeping the most recently updated one.
   *
   * Idempotent: a clean table deletes 0 rows. Triggered manually (admin /
   * script) rather than at boot — duplicates can live in compressed chunks,
   * and a DELETE there forces decompression, which we don't want on the
   * startup path.
   */
  @Process("dedupe-percentile-aggregates")
  async handleDedupePercentileAggregates(_job: Job): Promise<void> {
    this.logger.log(
      "🧹 Deduplicating queue_data_aggregates by (attractionId, hour)...",
    );

    try {
      const dupCheck = await this.aggregateRepository.query(
        `SELECT count(*)::int AS groups FROM (
           SELECT 1 FROM queue_data_aggregates
           GROUP BY "attractionId", hour
           HAVING count(*) > 1
         ) d`,
      );
      const duplicateBuckets = dupCheck[0]?.groups ?? 0;

      if (duplicateBuckets === 0) {
        this.logger.log(
          "✅ No duplicate (attractionId, hour) rows — nothing to clean.",
        );
        return;
      }

      this.logger.warn(
        `Found ${duplicateBuckets} duplicated (attractionId, hour) bucket(s) — collapsing to one row each...`,
      );

      // Keep the most-recently-updated row per bucket. The ctid join is scoped
      // by (attractionId, hour) too, so it stays correct even though ctid is
      // only unique within a chunk on a hypertable (duplicates of one bucket
      // share a time chunk, so this never crosses chunks).
      await this.aggregateRepository.query(
        `WITH ranked AS (
           SELECT
             ctid AS ct,
             "attractionId" AS aid,
             hour AS h,
             row_number() OVER (
               PARTITION BY "attractionId", hour
               ORDER BY "updatedAt" DESC NULLS LAST, ctid
             ) AS rn
           FROM queue_data_aggregates
         )
         DELETE FROM queue_data_aggregates q
         USING ranked r
         WHERE q.ctid = r.ct
           AND q."attractionId" = r.aid
           AND q.hour = r.h
           AND r.rn > 1`,
      );

      this.logger.log(
        `✅ Dedupe complete: ${duplicateBuckets} bucket(s) collapsed to a single row each.`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to dedupe percentile aggregates: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }
}

/**
 * Groups of attractions in one park whose last OPERATING record falls in the
 * same minute — one upstream write, not a season starting for all of them at
 * once.
 *
 * Pure, and exported, because the thresholds are the whole judgement and a
 * judgement worth arguing about is worth testing. Two of them: an absolute
 * floor, because a handful of rides can share a minute by coincidence, and a
 * share of the park, because five rides means something different at a park
 * with twelve than at one with two hundred.
 */
export interface SharedMinuteCluster {
  parkId: string;
  minute: string;
  tracked: number;
  attractionIds: string[];
}

export function findSharedMinuteClusters(
  candidates: Array<{ attractionId: string; parkId: string }>,
  lastOperating: Map<string, { at: Date | string }>,
  trackedPerPark: Map<string, number>,
  thresholds: { minSize: number; minParkShare: number },
): SharedMinuteCluster[] {
  const byParkAndMinute = new Map<string, Map<string, string[]>>();

  for (const candidate of candidates) {
    const last = lastOperating.get(candidate.attractionId);
    // Never OPERATING means there is no minute to share. Those belong to the
    // zero-history rule, which asks a different question.
    if (!last) continue;
    const at = new Date(last.at);
    // A row whose timestamp did not survive the trip is not evidence of
    // anything, and `toISOString()` throws on an invalid date — which would
    // take the whole nightly job down over one malformed value.
    if (Number.isNaN(at.getTime())) continue;
    const minute = at.toISOString().slice(0, 16);
    const byMinute = byParkAndMinute.get(candidate.parkId) ?? new Map();
    byMinute.set(minute, [
      ...(byMinute.get(minute) ?? []),
      candidate.attractionId,
    ]);
    byParkAndMinute.set(candidate.parkId, byMinute);
  }

  const clusters: SharedMinuteCluster[] = [];
  for (const [parkId, byMinute] of byParkAndMinute) {
    const tracked = trackedPerPark.get(parkId) ?? 0;
    for (const [minute, attractionIds] of byMinute) {
      if (attractionIds.length < thresholds.minSize) continue;
      // An unknown park size cannot rule the cluster out; the floor already
      // did the work.
      if (
        tracked > 0 &&
        attractionIds.length / tracked < thresholds.minParkShare
      ) {
        continue;
      }
      clusters.push({ parkId, minute, tracked, attractionIds });
    }
  }

  return clusters;
}
