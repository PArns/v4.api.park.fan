import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueDataAggregate } from "../../analytics/entities/queue-data-aggregate.entity";

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
