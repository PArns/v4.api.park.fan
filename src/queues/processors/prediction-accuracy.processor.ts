import { Processor, Process } from "@nestjs/bull";
import { Logger, Inject } from "@nestjs/common";
import { Job } from "bull";
import { Redis } from "ioredis";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, LessThan } from "typeorm";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { AttractionAccuracyStats } from "../../ml/entities/attraction-accuracy-stats.entity";
import { PredictionAccuracy } from "../../ml/entities/prediction-accuracy.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

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

      await Promise.all([
        this.redis.set("ml:accuracy:last_run", new Date().toISOString()),
        this.redis.set("ml:accuracy:last_run_count", newComparisons.toString()),
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
      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

      // Single SQL query to aggregate stats per attraction
      const results = await this.accuracyRepository.query(
        `
        SELECT 
          attraction_id,
          COUNT(*) as total_predictions,
          COUNT(CASE WHEN actual_wait_time IS NOT NULL THEN 1 END) as compared_predictions,
          AVG(absolute_error) FILTER (WHERE actual_wait_time IS NOT NULL) as mae
        FROM prediction_accuracy
        WHERE target_time >= $1
        GROUP BY attraction_id
        `,
        [thirtyDaysAgo],
      );

      let upsertCount = 0;
      for (const row of results) {
        const mae = parseFloat(row.mae) || 0;
        const comparedPredictions = parseInt(row.compared_predictions) || 0;
        const totalPredictions = parseInt(row.total_predictions) || 0;

        // Calculate badge
        let badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
        let message: string | null = null;

        if (comparedPredictions < 10) {
          badge = "insufficient_data";
          message = `Need at least 10 compared predictions (currently ${comparedPredictions})`;
        } else if (mae < 5) {
          badge = "excellent";
          message = "Predictions are highly accurate (±5 min average error)";
        } else if (mae < 10) {
          badge = "good";
          message =
            "Predictions are reliable for planning (±10 min average error)";
        } else if (mae < 15) {
          badge = "fair";
          message =
            "Predictions provide general guidance (±15 min average error)";
        } else {
          badge = "poor";
          message = `Predictions need improvement (${Math.round(mae)} min average error)`;
        }

        // Upsert into stats table
        await this.statsRepository.upsert(
          {
            attractionId: row.attraction_id,
            mae: Math.round(mae * 10) / 10,
            comparedPredictions,
            totalPredictions,
            badge,
            message,
          },
          ["attractionId"],
        );
        upsertCount++;
      }

      await this.redis.set(
        "ml:accuracy:last_aggregation",
        new Date().toISOString(),
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

      await this.redis.set(
        "ml:accuracy:last_cleanup",
        new Date().toISOString(),
      );
      await this.redis.set(
        "ml:accuracy:last_cleanup_count",
        totalDeleted.toString(),
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
