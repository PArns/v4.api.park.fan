import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";
import { MLModel } from "../entities/ml-model.entity";
import {
  MLDriftDto,
  DailyAccuracyDto,
  HorizonDriftDto,
} from "../dto/ml-drift.dto";
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
    // Only 'hourly' predictions are ever scored — daily/far-daily are tracked:false and
    // never reach comparisonStatus='COMPLETED'. So this drift measures CatBoost's INTRADAY
    // accuracy, which PCN now serves as the fallback (see byHorizon).
    const dailyMetrics = await this.getDailyAccuracy(startDate, "hourly");

    const liveMae = this.recentLiveMae(dailyMetrics, trainingMae);
    const currentDrift = ((liveMae - trainingMae) / trainingMae) * 100;
    const status = this.driftStatus(currentDrift);

    const byHorizon: HorizonDriftDto[] = [
      {
        horizon: "hourly",
        tracked: true,
        currentDrift: parseFloat(currentDrift.toFixed(2)),
        liveMae: parseFloat(liveMae.toFixed(2)),
        status,
        note: "CatBoost intraday accuracy. Intraday is now served by the PCN champion-swap, so this tracks the FALLBACK, not the served product — use the PCN board for served intraday quality.",
      },
      {
        horizon: "daily",
        tracked: false,
        currentDrift: null,
        liveMae: null,
        status: "untracked",
        note: "Far-daily (31–365d) predictions are never scored against actuals; CatBoost is the sole level provider here, but its accuracy is currently unmeasured.",
      },
    ];

    return {
      currentDrift: parseFloat(currentDrift.toFixed(2)),
      threshold: this.DRIFT_WARNING_THRESHOLD,
      status,
      trainingMae: parseFloat(trainingMae.toFixed(2)),
      liveMae: parseFloat(liveMae.toFixed(2)),
      dailyMetrics,
      byHorizon,
    };
  }

  /** Mean MAE over the last 7 scored days (falls back to trainingMae on a cold start). */
  private recentLiveMae(
    dailyMetrics: DailyAccuracyDto[],
    trainingMae: number,
  ): number {
    const recent = dailyMetrics.slice(-7);
    return recent.length > 0
      ? recent.reduce((sum, m) => sum + m.mae, 0) / recent.length
      : trainingMae;
  }

  private driftStatus(currentDrift: number): string {
    if (currentDrift > this.DRIFT_CRITICAL_THRESHOLD) return "critical";
    if (currentDrift > this.DRIFT_WARNING_THRESHOLD) return "warning";
    return "healthy";
  }

  private async getDailyAccuracy(
    startDate: Date,
    predictionType?: string,
  ): Promise<DailyAccuracyDto[]> {
    const qb = this.accuracyRepo
      .createQueryBuilder("pa")
      // Match the training population's thin-data gate: only ratable parks
      // (typical-day-peak baseline present) so liveMae compares like-for-like
      // with trainingMae, which also excludes thin parks (ml-service/db.py).
      .innerJoin("attractions", "a", "a.id = pa.attractionId")
      .innerJoin(
        "park_p50_baselines",
        "pb",
        'pb."parkId" = a."parkId" AND pb."typicalDayPeak" IS NOT NULL',
      )
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
      .orderBy("DATE(pa.targetTime)", "ASC");

    if (predictionType) {
      qb.andWhere("pa.predictionType = :pt", { pt: predictionType });
    }

    const rows = await qb.getRawMany();
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
