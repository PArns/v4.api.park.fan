import { Processor, Process } from "@nestjs/bull";
import { Logger, Inject } from "@nestjs/common";
import { Job } from "bull";
import { Redis } from "ioredis";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, LessThan } from "typeorm";
import {
  PredictionAccuracyService,
  servedAttractionStatsRow,
} from "../../ml/services/prediction-accuracy.service";
import { AttractionAccuracyStats } from "../../ml/entities/attraction-accuracy-stats.entity";
import { PredictionAccuracy } from "../../ml/entities/prediction-accuracy.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";

/**
 * Prediction Accuracy Processor
 *
 * Processes jobs in the 'prediction-accuracy' queue:
 * 1. compare-accuracy: Compare predictions with actual wait times (hourly)
 * 2. aggregate-stats: Pre-aggregate MAE per attraction (daily)
 * 3. cleanup-old: Delete old MISSED/PENDING records (daily)
 */
@Processor("prediction-accuracy")
export class PredictionAccuracyProcessor {
  private readonly logger = new Logger(PredictionAccuracyProcessor.name);

  constructor(
    private predictionAccuracyService: PredictionAccuracyService,
    @InjectRepository(AttractionAccuracyStats)
    private statsRepository: Repository<AttractionAccuracyStats>,
    @InjectRepository(PredictionAccuracy)
    private accuracyRepository: Repository<PredictionAccuracy>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("compare-accuracy")
  async handleCalculateAccuracy(_job: Job): Promise<void> {
    this.logger.log("🔄 Starting prediction accuracy comparison...");

    try {
      const { newComparisons } =
        await this.predictionAccuracyService.compareWithActuals();

      const TTL_30_DAYS = 30 * 24 * 60 * 60;
      await Promise.all([
        this.redis.set(
          "ml:accuracy:last_run",
          new Date().toISOString(),
          "EX",
          TTL_30_DAYS,
        ),
        this.redis.set(
          "ml:accuracy:last_run_count",
          newComparisons.toString(),
          "EX",
          TTL_30_DAYS,
        ),
      ]);

      this.logger.log(
        `✅ Prediction accuracy comparison completed (${newComparisons} new comparisons)`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Prediction accuracy comparison failed: ${errorMessage}`,
      );
      throw error;
    }
  }

  /**
   * Aggregate MAE per attraction into attraction_accuracy_stats table
   * This pre-computes badges so park endpoint doesn't need N+1 queries
   */
  @Process("aggregate-stats")
  async handleAggregateStats(_job: Job): Promise<void> {
    this.logger.log("📊 Starting accuracy stats aggregation...");

    try {
      const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

      // Single SQL query to aggregate stats per attraction.
      // Only COMPLETED rows are counted — PENDING/MISSED have no actual_wait_time.
      // We additionally filter out unplanned closures, low wait times (< 5 min)
      // and implausible data-source sentinels (> MAX_PLAUSIBLE_WAIT_TIME) to
      // align with the ML training set and remove noise.
      //
      // SERVED basis (PAR-854): the error is `served_absolute_error` — what the
      // reader was shown, PCN + blend where it served — and only rows that
      // recorded a served value count. The GROUP BY still runs over every
      // completed row, so a ride with only pre-PAR-854 (CatBoost) rows comes
      // back with zero compared predictions and is rewritten as
      // `insufficient_data` instead of keeping its CatBoost badge.
      const results = await this.accuracyRepository.query(
        `
        SELECT
          attraction_id,
          COUNT(*) FILTER (WHERE served_source IS NOT NULL) as total_predictions,
          COUNT(*) FILTER (WHERE served_source IS NOT NULL AND actual_wait_time >= 5 AND actual_wait_time <= $2 AND "wasUnplannedClosure" = false) as compared_predictions,
          AVG(served_absolute_error) FILTER (WHERE served_source IS NOT NULL AND actual_wait_time >= 5 AND actual_wait_time <= $2 AND "wasUnplannedClosure" = false) as mae,
          -- Raw CatBoost error over every compared row, for the ride day curve,
          -- which draws CatBoost's own forecast (see AttractionAccuracyStats.rawMae).
          COUNT(*) FILTER (WHERE actual_wait_time >= 5 AND actual_wait_time <= $2 AND "wasUnplannedClosure" = false) as raw_compared_predictions,
          AVG(absolute_error) FILTER (WHERE actual_wait_time >= 5 AND actual_wait_time <= $2 AND "wasUnplannedClosure" = false) as raw_mae
        FROM prediction_accuracy
        WHERE comparison_status = 'COMPLETED'
          AND target_time >= $1
        GROUP BY attraction_id
        `,
        [ninetyDaysAgo, MAX_PLAUSIBLE_WAIT_TIME],
      );

      // Build every row first, then write them in chunks. The per-attraction
      // upsert this replaces issued one statement per attraction — thousands of
      // round-trips for a job whose input is a single GROUP BY.
      const statsRows = (
        results as Parameters<typeof servedAttractionStatsRow>[0][]
      ).map(servedAttractionStatsRow);

      // Keep each statement well under Postgres' 65535-parameter limit
      // (7 columns per row → 1000 rows leaves ample headroom).
      const CHUNK = 1000;
      for (let i = 0; i < statsRows.length; i += CHUNK) {
        await this.statsRepository.upsert(statsRows.slice(i, i + CHUNK), [
          "attractionId",
        ]);
      }
      const upsertCount = statsRows.length;

      // A ride with no completed row in the window got no row above, so its
      // stats row would keep whatever an earlier run wrote — before PAR-854, a
      // CatBoost badge. Nothing measured it; nothing should be shown for it.
      const kept = statsRows.map((r) => r.attractionId);
      // An empty result means the comparison job produced nothing in the whole
      // window (it was down), not that every ride stopped being measured — so
      // the sweep is skipped instead of emptying the table.
      if (kept.length > 0) {
        await this.statsRepository.query(
          `DELETE FROM attraction_accuracy_stats
            WHERE NOT (attraction_id::text = ANY($1::text[]))`,
          [kept],
        );
      } else {
        this.logger.warn(
          "aggregate-stats: no completed comparison in the window — stale-row sweep skipped",
        );
      }

      await this.redis.set(
        "ml:accuracy:last_aggregation",
        new Date().toISOString(),
        "EX",
        30 * 24 * 60 * 60,
      );
      this.logger.log(`✅ Aggregated stats for ${upsertCount} attractions`);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Stats aggregation failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Delete old MISSED and PENDING records to save storage
   * Keeps COMPLETED records for 90 days, MISSED/PENDING for 7 days
   */
  @Process("cleanup-old")
  async handleCleanupOld(_job: Job): Promise<void> {
    this.logger.log("🧹 Starting prediction accuracy cleanup...");

    try {
      // Delete MISSED/PENDING older than 7 days
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);

      const missedResult = await this.accuracyRepository.delete({
        comparisonStatus: "MISSED" as const,
        targetTime: LessThan(sevenDaysAgo),
      });

      const pendingResult = await this.accuracyRepository.delete({
        comparisonStatus: "PENDING" as const,
        targetTime: LessThan(sevenDaysAgo),
      });

      const completedResult = await this.accuracyRepository.delete({
        comparisonStatus: "COMPLETED" as const,
        targetTime: LessThan(ninetyDaysAgo),
      });

      const totalDeleted =
        (missedResult.affected || 0) +
        (pendingResult.affected || 0) +
        (completedResult.affected || 0);

      const TTL_30_DAYS = 30 * 24 * 60 * 60;
      await this.redis.set(
        "ml:accuracy:last_cleanup",
        new Date().toISOString(),
        "EX",
        TTL_30_DAYS,
      );
      await this.redis.set(
        "ml:accuracy:last_cleanup_count",
        totalDeleted.toString(),
        "EX",
        TTL_30_DAYS,
      );

      this.logger.log(
        `✅ Cleanup completed: ${missedResult.affected} MISSED, ${pendingResult.affected} PENDING, ${completedResult.affected} old COMPLETED`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Cleanup failed: ${errorMessage}`);
      throw error;
    }
  }
}
