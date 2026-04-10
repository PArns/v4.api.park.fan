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
 * - Upserts via ON CONFLICT (idempotent)
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
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      yesterday.setHours(0, 0, 0, 0);

      const today = new Date(yesterday);
      today.setDate(today.getDate() + 1);

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
          gen_random_uuid() as id,
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
    const LOOKBACK_DAYS = 60;
    const RESET_DAYS = 14;

    // Step 1: Find attractions that were recently OPERATING (reset candidates)
    const recentlyOperating: { attractionId: string }[] = await this.dataSource
      .query(`
      SELECT DISTINCT q."attractionId"
      FROM queue_data q
      WHERE q.status = 'OPERATING'
        AND q.timestamp >= NOW() - INTERVAL '${RESET_DAYS} days'
    `);
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
      `,
        [ids],
      );
      if (resetResult[1] > 0) {
        this.logger.log(
          `   ♻️  Reset ${resetResult[1]} attractions (now operating again)`,
        );
      }
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
        SELECT DISTINCT
          a."parkId",
          DATE(q.timestamp AT TIME ZONE p.timezone) as open_day
        FROM queue_data q
        JOIN attractions a ON a.id = q."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE q.status = 'OPERATING'
          AND q.timestamp >= NOW() - INTERVAL '${LOOKBACK_DAYS} days'
      ),
      attraction_operating_days AS (
        SELECT DISTINCT
          q."attractionId",
          DATE(q.timestamp AT TIME ZONE p.timezone) as op_day
        FROM queue_data q
        JOIN attractions a ON a.id = q."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE q.status = 'OPERATING'
          AND q.timestamp >= NOW() - INTERVAL '${LOOKBACK_DAYS} days'
      ),
      ever_operating AS (
        SELECT "attractionId", COUNT(*) as op_count
        FROM queue_data
        WHERE status = 'OPERATING'
          AND timestamp >= NOW() - INTERVAL '365 days'
        GROUP BY "attractionId"
        HAVING COUNT(*) >= ${MIN_EVER_OPERATING}
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
        JOIN attractions a ON a."parkId" = pod."parkId"
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
      WHERE d.fully_closed_days >= ${MIN_PARK_OPEN_DAYS_CLOSED}
        AND cs.status = 'CLOSED'
        AND NOT (d."attractionId" = ANY($1))
    `,
        [Array.from(recentlyOperatingIds)],
      );

    this.logger.log(`   🔍 Found ${candidates.length} seasonal candidates`);

    // Step 4: For each candidate, derive seasonMonths from all-time OPERATING history
    for (const { attractionId } of candidates) {
      const monthRows: { month: number }[] = await this.dataSource.query(
        `SELECT DISTINCT EXTRACT(MONTH FROM q.timestamp AT TIME ZONE p.timezone)::int as month
        FROM queue_data q
        JOIN attractions a ON a.id = q."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE a.id = $1::uuid
          AND q.status = 'OPERATING'
          AND q.timestamp >= NOW() - INTERVAL '730 days'
        ORDER BY month`,
        [attractionId],
      );

      const seasonMonths =
        monthRows.length > 0 ? monthRows.map((r) => r.month) : null;

      await this.attractionRepository.update(attractionId, {
        isSeasonal: true,
        seasonMonths,
      });
    }

    this.logger.log(`✅ Attractions: marked ${candidates.length} as seasonal.`);

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
    const recentlyUpdatedShows: { showId: string }[] = await this.dataSource
      .query(`
      SELECT DISTINCT "showId"
      FROM show_live_data
      WHERE "lastUpdated" >= NOW() - INTERVAL '${RESET_DAYS} days'
    `);
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
        SELECT DISTINCT
          a."parkId",
          DATE(q.timestamp AT TIME ZONE p.timezone) as open_day
        FROM queue_data q
        JOIN attractions a ON a.id = q."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE q.status = 'OPERATING'
          AND q.timestamp >= NOW() - INTERVAL '${LOOKBACK_DAYS} days'
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
      WHERE stale_open_days >= ${MIN_PARK_OPEN_DAYS_CLOSED}
        AND NOT ("showId" = ANY($1))
    `,
      [Array.from(recentShowIds)],
    );

    this.logger.log(
      `   🔍 Found ${showCandidates.length} seasonal show candidates`,
    );

    // Step S3: Derive seasonMonths from months where lastUpdated was fresh
    for (const { showId } of showCandidates) {
      const monthRows: { month: number }[] = await this.dataSource.query(
        `SELECT DISTINCT
          EXTRACT(MONTH FROM sld.timestamp AT TIME ZONE p.timezone)::int as month
        FROM show_live_data sld
        JOIN shows s ON s.id = sld."showId"
        JOIN parks p ON p.id = s."parkId"
        WHERE sld."showId" = $1
          AND sld."lastUpdated" IS NOT NULL
          AND (sld.timestamp - sld."lastUpdated") < INTERVAL '24 hours'
        ORDER BY month`,
        [showId],
      );

      const seasonMonths =
        monthRows.length > 0 ? monthRows.map((r) => r.month) : null;

      await this.showRepository.update(showId, {
        isSeasonal: true,
        seasonMonths,
      });
    }

    this.logger.log(`✅ Shows: marked ${showCandidates.length} as seasonal.`);
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
      endDate.setHours(0, 0, 0, 0);

      const startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - days);

      this.logger.log(
        `   Period: ${startDate.toISOString()} to ${endDate.toISOString()}`,
      );

      // Process in batches of 7 days to avoid memory issues
      let currentDate = new Date(startDate);
      let totalRows = 0;

      while (currentDate < endDate) {
        const batchEnd = new Date(currentDate);
        batchEnd.setDate(batchEnd.getDate() + 7);
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
            gen_random_uuid() as id,
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
}
