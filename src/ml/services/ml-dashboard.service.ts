import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { Redis } from "ioredis";
import { MLModelService } from "./ml-model.service";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { PredictionDeviationService } from "./prediction-deviation.service";
import { MLDriftMonitoringService } from "./ml-drift-monitoring.service";
import { MLDashboardDto } from "../dto/ml-dashboard.dto";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

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
    private deviationService: PredictionDeviationService,
    private driftService: MLDriftMonitoringService,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) { }

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
      this.logger.warn("No active model found, returning initialization state");
      // Return initialization state instead of 404
      return {
        currentModel: {
          version: "none",
          trainedAt: new Date().toISOString(),
          trainingDurationSeconds: null,
          fileSizeBytes: 0,
          fileSizeMB: 0,
          modelSize: "0 MB",
          modelType: "none",
          isActive: false,
          trainingMetrics: {
            mae: 0,
            rmse: 0,
            mape: 0,
            r2Score: 0,
          },
          trainingData: {
            startDate: new Date().toISOString(),
            endDate: new Date().toISOString(),
            totalSamples: 0,
            trainSamples: 0,
            validationSamples: 0,
            dataDurationDays: 0,
          },
          configuration: {
            featuresUsed: [],
            featureCount: 0,
            hyperparameters: {},
          },
        },
        systemAccuracy: {
          overall: {
            mae: 0,
            rmse: 0,
            mape: 0,
            r2Score: 0,
            totalPredictions: 0,
            matchedPredictions: 0,
            coveragePercent: 0,
            uniqueAttractions: 0,
            uniqueParks: 0,
            badge: "insufficient_data",
          },
          byPredictionType: {
            HOURLY: { mae: 0, totalPredictions: 0, coveragePercent: 0 },
            DAILY: { mae: 0, totalPredictions: 0, coveragePercent: 0 },
          },
          topPerformers: [],
          bottomPerformers: [],
        },
        trends: {
          modelComparison: {
            current: {
              version: "none",
              mae: 0,
              r2: 0,
              trainedAt: new Date().toISOString(),
            },
            previous: {
              version: "none",
              mae: 0,
              r2: 0,
              trainedAt: new Date().toISOString(),
            },
            improvement: {
              maeDelta: 0,
              maePercentChange: 0,
              isImproving: false,
            },
          },
          dailyAccuracy: [],
          byHourOfDay: [],
          byDayOfWeek: [],
        },
        systemHealth: {
          lastTrainingJob: {
            completedAt: new Date().toISOString(),
            durationSeconds: 0,
            status: "unknown",
          },
          lastAccuracyCheck: {
            completedAt: new Date().toISOString(),
            newComparisonsAdded: 0,
          },
          nextScheduledTraining: this.getNextScheduledTraining(),
          modelAge: {
            days: 0,
            hours: 0,
            minutes: 0,
          },
        },
      };
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

    // Fetch real system health metrics
    const [lastTrainingDuration, lastAccuracyTimestamp, lastComparisonCount] =
      await Promise.all([
        this.getLastTrainingDuration(),
        this.redis.get("ml:accuracy:last_run"),
        this.redis.get("ml:accuracy:last_run_count"),
      ]);

    const systemHealth = {
      lastTrainingJob: {
        completedAt: currentModel.trainedAt,
        durationSeconds: lastTrainingDuration || 0,
        status: "success" as const,
      },
      lastAccuracyCheck: {
        completedAt: lastAccuracyTimestamp || new Date().toISOString(),
        newComparisonsAdded: lastComparisonCount
          ? parseInt(lastComparisonCount)
          : 0,
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
      modelDrift: await this.getDriftMetrics(),
    };

    this.logger.log(
      `✅ Dashboard data ready - Model: ${currentModel.version}, ` +
      `MAE: ${systemAccuracyStats.overall.mae} min, Badge: ${badge.badge}`,
    );

    return dashboard;
  }

  /**
   * Get duration of last completed training job from Bull queue
   *
   * @private
   * @returns {Promise<number | null>} Duration in seconds, or null if no completed jobs
   */
  private async getLastTrainingDuration(): Promise<number | null> {
    try {
      const completedJobs = await this.mlTrainingQueue.getCompleted();
      if (completedJobs.length === 0) {
        return null;
      }

      // Get most recent completed job
      const lastJob = completedJobs[completedJobs.length - 1];

      if (lastJob.finishedOn && lastJob.processedOn) {
        const durationMs = lastJob.finishedOn - lastJob.processedOn;
        return Math.floor(durationMs / 1000); // Convert to seconds
      }

      return null;
    } catch (error) {
      this.logger.warn(`Failed to get last training duration: ${error}`);
      return null;
    }
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
