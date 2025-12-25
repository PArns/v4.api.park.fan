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
 * MLDashboardService - V2 Restructured
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

  async getDashboard(): Promise<MLDashboardDto> {
    this.logger.log("🔄 Fetching ML dashboard (V2 restructured)...");

    const [
      currentModelData,
      modelComparison,
      systemAccuracyStats,
      topBottomPerformers,
      hourlyPatterns,
      weekdayPatterns,
      drift,
    ] = await Promise.all([
      this.mlModelService.getActiveModel(),
      this.mlModelService.getModelComparison(),
      this.accuracyService.getSystemAccuracyStats(7),
      this.accuracyService.getTopBottomPerformers(5),
      this.accuracyService.getHourlyAccuracyPatterns(30),
      this.accuracyService.getDayOfWeekAccuracyPatterns(30),
      this.getDriftMetrics(),
    ]);

    if (!currentModelData) {
      throw new Error("No active model found");
    }

    return {
      model: {
        current: {
          version: currentModelData.version,
          trainedAt: currentModelData.trainedAt.toISOString(),
          trainingDurationSeconds: currentModelData.trainingDurationSeconds,
          modelType: currentModelData.modelType,
          fileSizeMB: currentModelData.fileSizeMB,
        },
        previous: modelComparison.previous,
        configuration: {
          featuresUsed: currentModelData.featuresUsed || [],
          featureCount: (currentModelData.featuresUsed || []).length,
          hyperparameters: currentModelData.hyperparameters || {},
        },
        trainingData: {
          startDate: currentModelData.trainDataStartDate
            .toISOString()
            .split("T")[0],
          endDate: currentModelData.trainDataEndDate
            .toISOString()
            .split("T")[0],
          totalSamples:
            currentModelData.trainSamples +
            (currentModelData.validationSamples || 0),
          trainSamples: currentModelData.trainSamples,
          validationSamples: currentModelData.validationSamples || 0,
          dataDurationDays: Math.floor(
            (currentModelData.trainDataEndDate.getTime() -
              currentModelData.trainDataStartDate.getTime()) /
            (1000 * 60 * 60 * 24),
          ),
        },
      },
      performance: {
        training: {
          mae: parseFloat(currentModelData.mae.toFixed(2)),
          rmse: parseFloat(currentModelData.rmse.toFixed(2)),
          mape: parseFloat(currentModelData.mape.toFixed(2)),
          r2Score: parseFloat(currentModelData.r2Score.toFixed(2)),
        },
        live: {
          ...systemAccuracyStats.overall,
          badge: this.getBadge(systemAccuracyStats.overall.mae),
        },
        drift,
        improvement: modelComparison.improvement,
      },
      insights: {
        topPerformers: topBottomPerformers.topPerformers,
        bottomPerformers: topBottomPerformers.bottomPerformers,
        byPredictionType: systemAccuracyStats.byPredictionType,
        patterns: {
          hourly: hourlyPatterns,
          weekday: weekdayPatterns,
        },
      },
      system: {
        nextTraining: this.getNextScheduledTraining(),
        modelAge: this.calculateModelAge(currentModelData.trainedAt),
        lastAccuracyCheck: await this.getLastAccuracyCheck(),
      },
    };
  }

  private async getDriftMetrics() {
    try {
      return await this.driftService.getDriftMetrics(30);
    } catch (error) {
      this.logger.warn(`Could not fetch drift metrics: ${error}`);
      return null;
    }
  }

  private calculateModelAge(trainedAt: Date) {
    const now = new Date();
    const diff = now.getTime() - trainedAt.getTime();
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
    return { days, hours, minutes };
  }

  private getNextScheduledTraining(): string {
    const now = new Date();
    const next = new Date(now);
    next.setUTCHours(6, 0, 0, 0);
    if (next <= now) {
      next.setDate(next.getDate() + 1);
    }
    return next.toISOString();
  }

  private async getLastAccuracyCheck() {
    const cacheKey = "ml:last-accuracy-check";
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      const data = JSON.parse(cached);
      return {
        completedAt: data.completedAt,
        newComparisonsAdded: data.newComparisonsAdded || 0,
      };
    }
    return {
      completedAt: new Date().toISOString(),
      newComparisonsAdded: 0,
    };
  }

  private getBadge(mae: number): string {
    if (mae < 8) return "excellent";
    if (mae < 12) return "good";
    if (mae < 18) return "fair";
    return "poor";
  }
}
