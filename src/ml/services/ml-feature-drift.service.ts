import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, Not } from "typeorm";
import { MLFeatureStats } from "../entities/ml-feature-stats.entity";
import { MLFeatureDrift } from "../entities/ml-feature-drift.entity";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { MLModel } from "../entities/ml-model.entity";

/**
 * ML Feature Drift Service
 *
 * Detects data drift by comparing production feature distributions
 * against training feature distributions.
 */
@Injectable()
export class MLFeatureDriftService {
  private readonly logger = new Logger(MLFeatureDriftService.name);

  // Drift thresholds
  private readonly DRIFT_WARNING_THRESHOLD = 15; // 15% deviation
  private readonly DRIFT_CRITICAL_THRESHOLD = 30; // 30% deviation

  constructor(
    @InjectRepository(MLFeatureStats)
    private featureStatsRepository: Repository<MLFeatureStats>,
    @InjectRepository(MLFeatureDrift)
    private featureDriftRepository: Repository<MLFeatureDrift>,
    @InjectRepository(PredictionAccuracy)
    private accuracyRepository: Repository<PredictionAccuracy>,
    @InjectRepository(MLModel)
    private modelRepository: Repository<MLModel>,
  ) {}

  /**
   * Store feature statistics from model training
   */
  async storeFeatureStats(
    modelVersion: string,
    featureStats: Array<{
      featureName: string;
      mean: number;
      std: number;
      min: number;
      max: number;
      percentile10: number;
      percentile50: number;
      percentile90: number;
      sampleCount: number;
      featureType: "numeric" | "categorical";
      topValues?: Record<string, number>;
    }>,
  ): Promise<void> {
    this.logger.log(
      `Storing feature stats for model ${modelVersion} (${featureStats.length} features)`,
    );

    // Delete existing stats for this model version
    await this.featureStatsRepository.delete({ modelVersion });

    // Insert new stats
    const entities = featureStats.map((stats) => {
      const entity = new MLFeatureStats();
      entity.modelVersion = modelVersion;
      entity.featureName = stats.featureName;
      entity.mean = stats.mean;
      entity.std = stats.std;
      entity.min = stats.min;
      entity.max = stats.max;
      entity.percentile10 = stats.percentile10;
      entity.percentile50 = stats.percentile50;
      entity.percentile90 = stats.percentile90;
      entity.sampleCount = stats.sampleCount;
      entity.featureType = stats.featureType;
      entity.topValues = stats.topValues || null;
      return entity;
    });

    await this.featureStatsRepository.save(entities);
    this.logger.log(`✅ Stored ${entities.length} feature stats`);
  }

  /**
   * Detect feature drift for active model
   */
  async detectFeatureDrift(days: number = 7): Promise<{
    driftedFeatures: Array<{
      featureName: string;
      driftScore: number;
      trainingMean: number;
      productionMean: number;
      status: "healthy" | "warning" | "critical";
      ksStatistic?: number;
      wassersteinDistance?: number;
    }>;
    summary: {
      totalFeatures: number;
      healthyCount: number;
      warningCount: number;
      criticalCount: number;
    };
  }> {
    // Get active model
    const activeModel = await this.modelRepository.findOne({
      where: { isActive: true },
      order: { trainedAt: "DESC" },
    });

    if (!activeModel) {
      throw new Error("No active model found");
    }

    const modelVersion = activeModel.version;

    // Get training feature stats
    const trainingStats = await this.featureStatsRepository.find({
      where: { modelVersion },
    });

    if (trainingStats.length === 0) {
      this.logger.warn(
        `No feature stats found for model ${modelVersion}. Run storeFeatureStats() first.`,
      );
      return {
        driftedFeatures: [],
        summary: {
          totalFeatures: 0,
          healthyCount: 0,
          warningCount: 0,
          criticalCount: 0,
        },
      };
    }

    // Get production features from recent predictions
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const recentPredictions = await this.accuracyRepository.find({
      where: {
        targetTime: Between(startDate, new Date()),
        features: Not(null), // Only predictions with features
      },
      take: 10000, // Sample size
    });

    if (recentPredictions.length === 0) {
      this.logger.warn("No recent predictions with features found");
      return {
        driftedFeatures: [],
        summary: {
          totalFeatures: trainingStats.length,
          healthyCount: trainingStats.length,
          warningCount: 0,
          criticalCount: 0,
        },
      };
    }

    // Extract production feature values
    const productionFeatures: Record<string, number[]> = {};
    for (const pred of recentPredictions) {
      if (pred.features) {
        for (const [key, value] of Object.entries(pred.features)) {
          if (typeof value === "number") {
            if (!productionFeatures[key]) {
              productionFeatures[key] = [];
            }
            productionFeatures[key].push(value);
          }
        }
      }
    }

    // Compare distributions
    const driftedFeatures: Array<{
      featureName: string;
      driftScore: number;
      trainingMean: number;
      productionMean: number;
      status: "healthy" | "warning" | "critical";
      ksStatistic?: number;
      wassersteinDistance?: number;
    }> = [];

    for (const trainingStat of trainingStats) {
      const productionValues = productionFeatures[trainingStat.featureName];
      if (!productionValues || productionValues.length < 10) {
        // Skip if insufficient production data
        continue;
      }

      // Calculate production statistics
      const productionMean =
        productionValues.reduce((a, b) => a + b, 0) / productionValues.length;
      const productionStd = Math.sqrt(
        productionValues.reduce(
          (sum, val) => sum + Math.pow(val - productionMean, 2),
          0,
        ) / productionValues.length,
      );

      // Calculate drift score (percentage deviation from training mean)
      const meanDeviation =
        Math.abs((productionMean - trainingStat.mean) / trainingStat.mean) *
        100;

      // Calculate KS statistic (simplified)
      const ksStatistic = this.calculateKSStatistic(
        trainingStat,
        productionValues,
      );

      // Calculate Wasserstein distance (simplified)
      const wassersteinDistance = this.calculateWassersteinDistance(
        trainingStat,
        productionValues,
      );

      // Combined drift score
      const driftScore = Math.max(meanDeviation, ksStatistic * 100);

      // Determine status
      let status: "healthy" | "warning" | "critical";
      if (driftScore >= this.DRIFT_CRITICAL_THRESHOLD) {
        status = "critical";
      } else if (driftScore >= this.DRIFT_WARNING_THRESHOLD) {
        status = "warning";
      } else {
        status = "healthy";
      }

      driftedFeatures.push({
        featureName: trainingStat.featureName,
        driftScore: parseFloat(driftScore.toFixed(2)),
        trainingMean: trainingStat.mean,
        productionMean: parseFloat(productionMean.toFixed(2)),
        status,
        ksStatistic: parseFloat(ksStatistic.toFixed(4)),
        wassersteinDistance: parseFloat(wassersteinDistance.toFixed(4)),
      });

      // Store drift record
      const driftRecord = new MLFeatureDrift();
      driftRecord.modelVersion = modelVersion;
      driftRecord.featureName = trainingStat.featureName;
      driftRecord.driftScore = driftScore;
      driftRecord.trainingMean = trainingStat.mean;
      driftRecord.productionMean = productionMean;
      driftRecord.trainingStd = trainingStat.std;
      driftRecord.productionStd = productionStd;
      driftRecord.ksStatistic = ksStatistic;
      driftRecord.wassersteinDistance = wassersteinDistance;
      driftRecord.status = status;
      driftRecord.productionSampleCount = productionValues.length;
      driftRecord.detectedAt = new Date();

      await this.featureDriftRepository.save(driftRecord);
    }

    const summary = {
      totalFeatures: trainingStats.length,
      healthyCount: driftedFeatures.filter((f) => f.status === "healthy")
        .length,
      warningCount: driftedFeatures.filter((f) => f.status === "warning")
        .length,
      criticalCount: driftedFeatures.filter((f) => f.status === "critical")
        .length,
    };

    this.logger.log(
      `Feature drift detection: ${summary.healthyCount} healthy, ${summary.warningCount} warning, ${summary.criticalCount} critical`,
    );

    return { driftedFeatures, summary };
  }

  /**
   * Get feature drift history
   */
  async getFeatureDriftHistory(
    featureName?: string,
    days: number = 30,
  ): Promise<MLFeatureDrift[]> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const query = this.featureDriftRepository
      .createQueryBuilder("drift")
      .where("drift.detectedAt >= :startDate", { startDate })
      .orderBy("drift.detectedAt", "DESC");

    if (featureName) {
      query.andWhere("drift.featureName = :featureName", { featureName });
    }

    return query.getMany();
  }

  /**
   * Calculate Kolmogorov-Smirnov statistic (simplified)
   */
  private calculateKSStatistic(
    trainingStat: MLFeatureStats,
    productionValues: number[],
  ): number {
    // Simplified KS test: compare CDFs at key points
    const sortedProd = [...productionValues].sort((a, b) => a - b);
    const n = sortedProd.length;

    // Sample points from training distribution
    const samplePoints = [
      trainingStat.percentile10,
      trainingStat.percentile50,
      trainingStat.percentile90,
    ];

    let maxDiff = 0;
    for (const point of samplePoints) {
      // Training CDF at this point (approximate)
      const trainingCDF = this.estimateCDF(trainingStat, point);

      // Production CDF at this point
      const productionCDF = sortedProd.filter((v) => v <= point).length / n;

      const diff = Math.abs(trainingCDF - productionCDF);
      maxDiff = Math.max(maxDiff, diff);
    }

    return maxDiff;
  }

  /**
   * Calculate Wasserstein distance (simplified)
   */
  private calculateWassersteinDistance(
    trainingStat: MLFeatureStats,
    productionValues: number[],
  ): number {
    // Simplified: compare means and stds
    const productionMean =
      productionValues.reduce((a, b) => a + b, 0) / productionValues.length;

    const meanDiff = Math.abs(trainingStat.mean - productionMean);
    const stdDiff = Math.abs(
      trainingStat.std -
        Math.sqrt(
          productionValues.reduce(
            (sum, val) => sum + Math.pow(val - productionMean, 2),
            0,
          ) / productionValues.length,
        ),
    );

    // Normalized distance
    return (meanDiff + stdDiff) / (trainingStat.std + 1);
  }

  /**
   * Estimate CDF value at a point (simplified)
   */
  private estimateCDF(stats: MLFeatureStats, point: number): number {
    // Simple linear interpolation based on percentiles
    if (point <= stats.percentile10) {
      return (0.1 * (point - stats.min)) / (stats.percentile10 - stats.min);
    } else if (point <= stats.percentile50) {
      return (
        0.1 +
        (0.4 * (point - stats.percentile10)) /
          (stats.percentile50 - stats.percentile10)
      );
    } else if (point <= stats.percentile90) {
      return (
        0.5 +
        (0.4 * (point - stats.percentile50)) /
          (stats.percentile90 - stats.percentile50)
      );
    } else {
      return (
        0.9 +
        (0.1 * (point - stats.percentile90)) / (stats.max - stats.percentile90)
      );
    }
  }
}
