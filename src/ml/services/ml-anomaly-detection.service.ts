import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThan, Not } from "typeorm";
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
        actualWaitTime: Not(null) as any, // TypeORM type issue workaround
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

    for (const pred of predictions) {
      const actualWaitTime = pred.actualWaitTime || 0;
      const absoluteError = pred.absoluteError || 0;

      // Check for extreme values
      if (
        actualWaitTime >
        meanWaitTime + this.EXTREME_VALUE_THRESHOLD * stdWaitTime
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
        if (anomaly) anomalies.push(anomaly);
      }

      // Check for large errors
      if (absoluteError > this.LARGE_ERROR_THRESHOLD) {
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
        if (anomaly) anomalies.push(anomaly);
      }

      // Check for unplanned closures
      if (pred.wasUnplannedClosure) {
        const anomaly = await this.createAnomaly({
          attractionId: pred.attractionId,
          anomalyType: "unexpected_closure",
          severity: "medium",
          predictedTime: pred.targetTime,
          predictedWaitTime: pred.predictedWaitTime,
          actualWaitTime: 0,
          absoluteError,
          confidence: null,
          anomalyScore: 80, // High score for closures
          reason: "Attraction was closed but model predicted operating",
          featureValues: pred.features || null,
          modelVersion: pred.modelVersion,
        });
        if (anomaly) anomalies.push(anomaly);
      }
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
    // Check if similar anomaly already exists (within 1 hour)
    const oneHourAgo = new Date(data.predictedTime.getTime() - 60 * 60 * 1000);
    const existing = await this.anomalyRepository.findOne({
      where: {
        attractionId: data.attractionId,
        anomalyType: data.anomalyType,
        detectedAt: Between(oneHourAgo, new Date()),
      },
    });

    if (existing) {
      // Don't create duplicate
      return null;
    }

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
