import { Injectable, Logger, HttpException, HttpStatus } from "@nestjs/common";
import { MLModelService } from "./ml-model.service";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { MLDashboardDto } from "../dto/ml-dashboard.dto";

/**
 * MLDashboardService
 *
 * Orchestrates all ML monitoring dashboard data
 * Combines data from:
 * - MLModelService (model info, file size, comparison)
 * - PredictionAccuracyService (system accuracy, trends, performers)
 *
 * Performs parallel queries for optimal performance
 */
@Injectable()
export class MLDashboardService {
  private readonly logger = new Logger(MLDashboardService.name);

  constructor(
    private mlModelService: MLModelService,
    private accuracyService: PredictionAccuracyService,
  ) {}

  /**
   * Get complete ML dashboard data
   *
   * Fetches all dashboard sections in parallel:
   * 1. Current model with file size
   * 2. System accuracy stats (last 7 days)
   * 3. Top/bottom performers
   * 4. Model comparison (current vs previous)
   * 5. Daily trends (last 30 days)
   * 6. Hourly/day-of-week patterns
   *
   * Expected performance: ~300-500ms (6 parallel queries)
   *
   * @throws {HttpException} 404 if no active model found
   * @returns {Promise<MLDashboardDto>} Complete dashboard data
   */
  async getDashboard(): Promise<MLDashboardDto> {
    this.logger.log("🔄 Fetching ML dashboard data...");

    // Parallel fetch for optimal performance
    const [
      currentModel,
      systemAccuracyStats,
      topBottomPerformers,
      modelComparison,
      dailyTrends,
      hourlyPatterns,
      dayOfWeekPatterns,
    ] = await Promise.all([
      this.mlModelService.getActiveModelWithDetails(),
      this.accuracyService.getSystemAccuracyStats(7), // Last 7 days
      this.accuracyService.getTopBottomPerformers(7, 5), // Top/bottom 5
      this.mlModelService.getModelComparison(),
      this.accuracyService.getDailyAccuracyTrends(30), // Last 30 days
      this.accuracyService.getHourlyAccuracyPatterns(30),
      this.accuracyService.getDayOfWeekAccuracyPatterns(30),
    ]);

    // Validate: must have an active model
    if (!currentModel) {
      this.logger.error("No active model found");
      throw new HttpException(
        "No active model found. Train a model first.",
        HttpStatus.NOT_FOUND,
      );
    }

    // Calculate accuracy badge
    const badge = this.accuracyService.calculateAccuracyBadge(
      systemAccuracyStats.overall.mae,
      systemAccuracyStats.overall.matchedPredictions,
    );

    // System health calculations
    const modelAge = this.mlModelService.getModelAge(
      new Date(currentModel.trainedAt),
    );

    const systemHealth = {
      lastTrainingJob: {
        completedAt: currentModel.trainedAt,
        durationSeconds: 420, // TODO: Track actual job duration from Bull queue
        status: "success" as const,
      },
      lastAccuracyCheck: {
        completedAt: new Date().toISOString(), // TODO: Track from prediction-accuracy processor
        newComparisonsAdded: 0, // TODO: Track incremental comparisons
      },
      nextScheduledTraining: this.getNextScheduledTraining(),
      modelAge,
    };

    // Assemble complete dashboard
    const dashboard: MLDashboardDto = {
      currentModel,
      systemAccuracy: {
        overall: {
          ...systemAccuracyStats.overall,
          badge: badge.badge,
        },
        byPredictionType: systemAccuracyStats.byPredictionType,
        topPerformers: topBottomPerformers.topPerformers,
        bottomPerformers: topBottomPerformers.bottomPerformers,
      },
      trends: {
        modelComparison,
        dailyAccuracy: dailyTrends,
        byHourOfDay: hourlyPatterns,
        byDayOfWeek: dayOfWeekPatterns,
      },
      systemHealth,
    };

    this.logger.log(
      `✅ Dashboard data ready - Model: ${currentModel.version}, ` +
        `MAE: ${systemAccuracyStats.overall.mae} min, Badge: ${badge.badge}`,
    );

    return dashboard;
  }

  /**
   * Calculate next scheduled training time
   *
   * Training is scheduled daily at 6am UTC
   * If 6am has already passed today, returns tomorrow's 6am
   *
   * @private
   * @returns {string} Next training timestamp (ISO 8601)
   */
  private getNextScheduledTraining(): string {
    const now = new Date();
    const next = new Date(now);
    next.setUTCHours(6, 0, 0, 0);

    // If 6am has passed today, schedule for tomorrow
    if (next <= now) {
      next.setDate(next.getDate() + 1);
    }

    return next.toISOString();
  }
}
