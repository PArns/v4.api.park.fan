import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { MLModel } from "../entities/ml-model.entity";
import { MLDriftDto, DailyAccuracyDto } from "../dto/ml-drift.dto";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";

/**
 * ML Drift Monitoring Service
 *
 * Tracks model performance degradation over time by comparing
 * predicted vs actual wait times. Uses the prediction_accuracy table
 * (populated by compareWithActuals job) as the single source of truth —
 * no separate accuracy_comparisons table needed.
 */
@Injectable()
export class MLDriftMonitoringService {
  private readonly logger = new Logger(MLDriftMonitoringService.name);
  private readonly DRIFT_WARNING_THRESHOLD = 20; // 20% worse than training
  private readonly DRIFT_CRITICAL_THRESHOLD = 30; // 30% worse

  constructor(
    @InjectRepository(PredictionAccuracy)
    private accuracyRepo: Repository<PredictionAccuracy>,
    @InjectRepository(MLModel)
    private mlModelRepo: Repository<MLModel>,
  ) {}

  async getDriftMetrics(days: number = 30): Promise<MLDriftDto> {
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    const activeModel = await this.mlModelRepo.findOne({
      where: { isActive: true },
      order: { trainedAt: "DESC" },
    });

    if (!activeModel) {
      throw new Error("No active model found");
    }

    const trainingMae = activeModel.mae;
    const dailyMetrics = await this.getDailyAccuracy(startDate);

    const recentMetrics = dailyMetrics.slice(-7);
    const liveMae =
      recentMetrics.length > 0
        ? recentMetrics.reduce((sum, m) => sum + m.mae, 0) /
          recentMetrics.length
        : trainingMae;

    const currentDrift = ((liveMae - trainingMae) / trainingMae) * 100;

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

  private async getDailyAccuracy(startDate: Date): Promise<DailyAccuracyDto[]> {
    const rows = await this.accuracyRepo
      .createQueryBuilder("pa")
      .select("DATE(pa.targetTime)", "date")
      .addSelect("AVG(pa.absoluteError)", "mae")
      .addSelect("COUNT(*)", "count")
      .where("pa.targetTime >= :startDate", { startDate })
      .andWhere("pa.comparisonStatus = 'COMPLETED'")
      .andWhere("pa.absoluteError IS NOT NULL")
      // Match training population: exclude unplanned closures (actualWaitTime=0,
      // absoluteError=predictedWaitTime), sub-5-min records and implausible
      // data-source sentinels (> MAX_PLAUSIBLE_WAIT_TIME) — none are in the
      // training data. Without these, liveMae includes closure/sentinel noise
      // and inflates drift vs trainingMae.
      .andWhere("pa.wasUnplannedClosure = :notClosed", { notClosed: false })
      .andWhere("pa.actualWaitTime >= 5")
      .andWhere("pa.actualWaitTime <= :maxWait", {
        maxWait: MAX_PLAUSIBLE_WAIT_TIME,
      })
      .groupBy("DATE(pa.targetTime)")
      .orderBy("DATE(pa.targetTime)", "ASC")
      .getRawMany();

    return rows.map((r) => ({
      date: r.date,
      mae: parseFloat(parseFloat(r.mae).toFixed(2)),
      predictionsCount: parseInt(r.count),
    }));
  }

  async shouldRetrain(): Promise<{ should: boolean; reason: string }> {
    try {
      const drift = await this.getDriftMetrics(7);

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
