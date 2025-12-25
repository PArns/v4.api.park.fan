import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { MLAccuracyComparison } from "../entities/ml-accuracy-comparison.entity";
import { MLModel } from "../entities/ml-model.entity";
import { MLDriftDto, DailyAccuracyDto } from "../dto/ml-drift.dto";

/**
 * ML Drift Monitoring Service
 *
 * Tracks model performance degradation over time by comparing
 * predicted vs actual wait times
 */
@Injectable()
export class MLDriftMonitoringService {
  private readonly logger = new Logger(MLDriftMonitoringService.name);
  private readonly DRIFT_WARNING_THRESHOLD = 20; // 20% worse than training
  private readonly DRIFT_CRITICAL_THRESHOLD = 30; // 30% worse

  constructor(
    @InjectRepository(MLAccuracyComparison)
    private accuracyRepo: Repository<MLAccuracyComparison>,
    @InjectRepository(MLModel)
    private mlModelRepo: Repository<MLModel>,
  ) {}

  /**
   * Get drift metrics for specified number of days
   */
  async getDriftMetrics(days: number = 30): Promise<MLDriftDto> {
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    // Get active model's training MAE
    const activeModel = await this.mlModelRepo.findOne({
      where: { isActive: true },
      order: { trainedAt: "DESC" },
    });

    if (!activeModel) {
      throw new Error("No active model found");
    }

    const trainingMae = activeModel.mae;

    // Get daily accuracy metrics
    const dailyMetrics = await this.getDailyAccuracy(startDate);

    // Calculate current live MAE (last 7 days average)
    const recentMetrics = dailyMetrics.slice(-7);
    const liveMae =
      recentMetrics.length > 0
        ? recentMetrics.reduce((sum, m) => sum + m.mae, 0) /
          recentMetrics.length
        : trainingMae;

    // Calculate drift percentage
    const currentDrift = ((liveMae - trainingMae) / trainingMae) * 100;

    // Determine status
    let status: string;
    if (currentDrift > this.DRIFT_CRITICAL_THRESHOLD) {
      status = "critical";
    } else if (currentDrift > this.DRIFT_WARNING_THRESHOLD) {
      status = "warning";
    } else {
      status = "healthy";
    }

    return {
      currentDrift: parseFloat(currentDrift.toFixed(2)),
      threshold: this.DRIFT_WARNING_THRESHOLD,
      status,
      trainingMae: parseFloat(trainingMae.toFixed(2)),
      liveMae: parseFloat(liveMae.toFixed(2)),
      dailyMetrics,
    };
  }

  /**
   * Get daily accuracy breakdown
   */
  private async getDailyAccuracy(startDate: Date): Promise<DailyAccuracyDto[]> {
    const comparisons = await this.accuracyRepo
      .createQueryBuilder("comp")
      .select("DATE(comp.date)", "date")
      .addSelect("AVG(comp.absoluteError)", "mae")
      .addSelect("COUNT(*)", "count")
      .where("comp.date >= :startDate", { startDate })
      .groupBy("DATE(comp.date)")
      .orderBy("DATE(comp.date)", "ASC")
      .getRawMany();

    return comparisons.map((c) => ({
      date: c.date,
      mae: parseFloat(parseFloat(c.mae).toFixed(2)),
      predictionsCount: parseInt(c.count),
    }));
  }

  /**
   * Record accuracy comparison (called after actual wait time is observed)
   */
  async recordComparison(
    parkId: string,
    attractionId: string,
    predictedWaitTime: number,
    actualWaitTime: number,
    predictedAt: Date,
    actualAt: Date,
    predictionType: string = "hourly",
  ): Promise<void> {
    const absoluteError = Math.abs(predictedWaitTime - actualWaitTime);

    const comparison = this.accuracyRepo.create({
      date: new Date(actualAt.toDateString()), // Date only
      parkId,
      attractionId,
      predictedAt,
      actualAt,
      predictedWaitTime,
      actualWaitTime,
      absoluteError,
      predictionType,
    });

    await this.accuracyRepo.save(comparison);
  }

  /**
   * Check if retraining is recommended based on drift
   */
  async shouldRetrain(): Promise<{ should: boolean; reason: string }> {
    try {
      const drift = await this.getDriftMetrics(7); // Check last 7 days

      if (drift.status === "critical") {
        return {
          should: true,
          reason: `Critical drift detected: ${drift.currentDrift}% (threshold: ${drift.threshold}%)`,
        };
      }

      if (drift.status === "warning" && drift.currentDrift > 25) {
        return {
          should: true,
          reason: `High drift detected: ${drift.currentDrift}%`,
        };
      }

      return {
        should: false,
        reason: `Drift is healthy: ${drift.currentDrift}%`,
      };
    } catch (error) {
      this.logger.error(`Error checking drift: ${error}`);
      return { should: false, reason: "Error checking drift" };
    }
  }
}
