import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThan, Not, IsNull } from "typeorm";
import { MLPredictionAnomaly } from "../entities/ml-prediction-anomaly.entity";
import { PredictionAccuracy } from "../entities/prediction-accuracy.entity";

/**
 * ML Anomaly Detection Service
 *
 * Detects anomalous predictions that deviate significantly from expected patterns.
 * Identifies model issues, data quality problems, and edge cases.
 */
@Injectable()
export class MLAnomalyDetectionService {
  private readonly logger = new Logger(MLAnomalyDetectionService.name);

  // Anomaly thresholds
  private readonly EXTREME_VALUE_THRESHOLD = 3; // 3 standard deviations
  private readonly LARGE_ERROR_THRESHOLD = 50; // 50 minutes
  private readonly CONFIDENCE_MISMATCH_THRESHOLD = 0.3; // 30% difference

  constructor(
    @InjectRepository(MLPredictionAnomaly)
    private anomalyRepository: Repository<MLPredictionAnomaly>,
    @InjectRepository(PredictionAccuracy)
    private accuracyRepository: Repository<PredictionAccuracy>,
  ) {}

  /**
   * Detect anomalies in recent predictions
   */
  async detectAnomalies(days: number = 7): Promise<{
    detected: number;
    anomalies: MLPredictionAnomaly[];
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Get recent predictions with actuals
    const predictions = await this.accuracyRepository.find({
      where: {
        targetTime: Between(startDate, new Date()),
        actualWaitTime: Not(IsNull()),
        comparisonStatus: "COMPLETED",
      },
      take: 10000, // Sample size
    });

    if (predictions.length === 0) {
      this.logger.warn(
        "No predictions with actuals found for anomaly detection",
      );
      return { detected: 0, anomalies: [] };
    }

    // Calculate baseline statistics
    const errors = predictions
      .map((p) => p.absoluteError || 0)
      .filter((e) => e > 0);
    const meanError = errors.reduce((a, b) => a + b, 0) / errors.length;
    const stdError = Math.sqrt(
      errors.reduce((sum, e) => sum + Math.pow(e - meanError, 2), 0) /
        errors.length,
    );

    const waitTimes = predictions
      .map((p) => p.actualWaitTime || 0)
      .filter((wt) => wt > 0);
    const meanWaitTime =
      waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length;
    const stdWaitTime = Math.sqrt(
      waitTimes.reduce((sum, wt) => sum + Math.pow(wt - meanWaitTime, 2), 0) /
        waitTimes.length,
    );

    const anomalies: MLPredictionAnomaly[] = [];

    // Pre-load every recently-detected anomaly that could collide with
    // the predictions we're about to process. The old per-anomaly
    // findOne (called up to 3× per prediction) becomes a single SELECT
    // + an in-memory bucket lookup; 30 000 round-trips → 1 on a busy
    // day. The window starts 1 h before the earliest predictedTime
    // because createAnomaly's de-dup span is "predictedTime ± 1 h".
    const earliestPredictedTime = predictions.reduce<Date>(
      (acc, p) =>
        !acc || p.targetTime.getTime() < acc.getTime() ? p.targetTime : acc,
      null as unknown as Date,
    );
    const dedupWindowStart = new Date(
      (earliestPredictedTime?.getTime() ?? startDate.getTime()) -
        60 * 60 * 1000,
    );
    const existingAnomalies = await this.anomalyRepository.find({
      where: { detectedAt: Between(dedupWindowStart, new Date()) },
      select: ["attractionId", "anomalyType", "detectedAt"],
    });
    const recentByKey = new Map<string, Date[]>();
    for (const ex of existingAnomalies) {
      const key = `${ex.attractionId}:${ex.anomalyType}`;
      let arr = recentByKey.get(key);
      if (!arr) {
        arr = [];
        recentByKey.set(key, arr);
      }
      arr.push(ex.detectedAt);
    }

    // Helper closing over the in-memory map: replaces the per-call
    // findOne that createAnomaly used to fire.
    const hasRecentAnomaly = (
      attractionId: string,
      anomalyType: MLPredictionAnomaly["anomalyType"],
      predictedTime: Date,
    ): boolean => {
      const bucket = recentByKey.get(`${attractionId}:${anomalyType}`);
      if (!bucket) return false;
      const windowStart = predictedTime.getTime() - 60 * 60 * 1000;
      const now = Date.now();
      return bucket.some((d) => {
        const t = d.getTime();
        return t >= windowStart && t <= now;
      });
    };

    // Track anomalies we just created in this run so we don't double-
    // emit within the same loop iteration (e.g. two predictions for the
    // same attraction within an hour, both flagged extreme_value).
    const justCreated = new Set<string>();
    const noteCreated = (
      attractionId: string,
      anomalyType: MLPredictionAnomaly["anomalyType"],
    ) => {
      justCreated.add(`${attractionId}:${anomalyType}`);
    };
    const wasJustCreated = (
      attractionId: string,
      anomalyType: MLPredictionAnomaly["anomalyType"],
    ): boolean => justCreated.has(`${attractionId}:${anomalyType}`);

    for (const pred of predictions) {
      const actualWaitTime = pred.actualWaitTime || 0;
      const absoluteError = pred.absoluteError || 0;

      // Check for extreme values
      if (
        actualWaitTime >
          meanWaitTime + this.EXTREME_VALUE_THRESHOLD * stdWaitTime &&
        !hasRecentAnomaly(
          pred.attractionId,
          "extreme_value",
          pred.targetTime,
        ) &&
        !wasJustCreated(pred.attractionId, "extreme_value")
      ) {
        const anomaly = await this.createAnomaly({
          attractionId: pred.attractionId,
          anomalyType: "extreme_value",
          severity: "high",
          predictedTime: pred.targetTime,
          predictedWaitTime: pred.predictedWaitTime,
          actualWaitTime,
          absoluteError,
          confidence: null,
          anomalyScore: this.calculateAnomalyScore(
            actualWaitTime,
            meanWaitTime,
            stdWaitTime,
          ),
          reason: `Extreme wait time: ${actualWaitTime} min (mean: ${meanWaitTime.toFixed(1)}, std: ${stdWaitTime.toFixed(1)})`,
          featureValues: pred.features || null,
          modelVersion: pred.modelVersion,
        });
        if (anomaly) {
          anomalies.push(anomaly);
          noteCreated(pred.attractionId, "extreme_value");
        }
      }

      // Check for large errors
      if (
        absoluteError > this.LARGE_ERROR_THRESHOLD &&
        !hasRecentAnomaly(pred.attractionId, "large_error", pred.targetTime) &&
        !wasJustCreated(pred.attractionId, "large_error")
      ) {
        const anomaly = await this.createAnomaly({
          attractionId: pred.attractionId,
          anomalyType: "large_error",
          severity: absoluteError > 100 ? "high" : "medium",
          predictedTime: pred.targetTime,
          predictedWaitTime: pred.predictedWaitTime,
          actualWaitTime,
          absoluteError,
          confidence: null,
          anomalyScore: this.calculateAnomalyScore(
            absoluteError,
            meanError,
            stdError,
          ),
          reason: `Large prediction error: ${absoluteError} min (mean error: ${meanError.toFixed(1)})`,
          featureValues: pred.features || null,
          modelVersion: pred.modelVersion,
        });
        if (anomaly) {
          anomalies.push(anomaly);
          noteCreated(pred.attractionId, "large_error");
        }
      }

      // NOTE: "unexpected_closure" anomalies were removed here. They flooded the
      // anomaly board (89% of all anomalies: 534/602 live) and were almost all
      // genuine ride closures during opening hours (CLOSED/DOWN/REFURBISHMENT) —
      // an operational reality, NOT a model-quality problem: the model correctly
      // predicted a wait for a ride that *should* have been running; the ride
      // simply went down. Drowning the real model anomalies (large_error /
      // extreme_value, ~68 on genuine rides) under closure noise made the board
      // useless. Closures belong in operational monitoring, not model-quality
      // anomaly detection. The enum value is kept for historical rows.
      // (The status-only-park OPERATING+null mis-classification is fixed
      // separately in prediction-accuracy.service compareWithActuals.)
    }

    this.logger.log(`Detected ${anomalies.length} anomalies`);
    return { detected: anomalies.length, anomalies };
  }

  /**
   * Get anomalies
   */
  async getAnomalies(
    days: number = 7,
    severity?: "low" | "medium" | "high",
  ): Promise<MLPredictionAnomaly[]> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const query = this.anomalyRepository
      .createQueryBuilder("anomaly")
      .where("anomaly.detectedAt >= :startDate", { startDate })
      .orderBy("anomaly.detectedAt", "DESC")
      .addOrderBy("anomaly.anomalyScore", "DESC");

    if (severity) {
      query.andWhere("anomaly.severity = :severity", { severity });
    }

    return query.getMany();
  }

  /**
   * Get anomaly statistics
   */
  async getAnomalyStats(days: number = 7): Promise<{
    totalAnomalies: number;
    byType: Record<string, number>;
    bySeverity: Record<string, number>;
    avgAnomalyScore: number;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const anomalies = await this.anomalyRepository.find({
      where: {
        detectedAt: Between(startDate, new Date()),
      },
    });

    const byType: Record<string, number> = {};
    const bySeverity: Record<string, number> = {};

    for (const anomaly of anomalies) {
      byType[anomaly.anomalyType] = (byType[anomaly.anomalyType] || 0) + 1;
      bySeverity[anomaly.severity] = (bySeverity[anomaly.severity] || 0) + 1;
    }

    const avgAnomalyScore =
      anomalies.length > 0
        ? anomalies.reduce((sum, a) => sum + a.anomalyScore, 0) /
          anomalies.length
        : 0;

    return {
      totalAnomalies: anomalies.length,
      byType,
      bySeverity,
      avgAnomalyScore: Math.round(avgAnomalyScore * 10) / 10,
    };
  }

  /**
   * Create anomaly record
   */
  private async createAnomaly(data: {
    attractionId: string;
    anomalyType: MLPredictionAnomaly["anomalyType"];
    severity: MLPredictionAnomaly["severity"];
    predictedTime: Date;
    predictedWaitTime: number;
    actualWaitTime: number | null;
    absoluteError: number | null;
    confidence: number | null;
    anomalyScore: number;
    reason: string | null;
    featureValues: Record<string, unknown> | null;
    modelVersion: string;
  }): Promise<MLPredictionAnomaly | null> {
    // The "no duplicate within 1 h" guard is enforced by the caller
    // (detectAnomalies) via a single batched pre-load — see
    // `hasRecentAnomaly` / `wasJustCreated` above. createAnomaly itself
    // is now a pure insert helper. Calling it on a duplicate would
    // produce a real row, so callers must check first.
    const anomaly = new MLPredictionAnomaly();
    anomaly.attractionId = data.attractionId;
    anomaly.anomalyType = data.anomalyType;
    anomaly.severity = data.severity;
    anomaly.predictedTime = data.predictedTime;
    anomaly.predictedWaitTime = data.predictedWaitTime;
    anomaly.actualWaitTime = data.actualWaitTime;
    anomaly.absoluteError = data.absoluteError;
    anomaly.confidence = data.confidence;
    anomaly.anomalyScore = data.anomalyScore;
    anomaly.reason = data.reason;
    anomaly.featureValues = data.featureValues;
    anomaly.modelVersion = data.modelVersion;
    anomaly.detectedAt = new Date();

    return await this.anomalyRepository.save(anomaly);
  }

  /**
   * Calculate anomaly score (0-100)
   */
  private calculateAnomalyScore(
    value: number,
    mean: number,
    std: number,
  ): number {
    if (std === 0) return 50; // Default if no variance

    const zScore = Math.abs((value - mean) / std);
    // Convert z-score to 0-100 scale (capped at 100)
    return Math.min(100, (zScore / this.EXTREME_VALUE_THRESHOLD) * 100);
  }

  /**
   * Cleanup old anomalies (older than 90 days)
   */
  async cleanupOldAnomalies(): Promise<number> {
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
    const result = await this.anomalyRepository.delete({
      detectedAt: LessThan(ninetyDaysAgo),
    });
    return result.affected || 0;
  }
}
