import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, LessThan, In } from "typeorm";
import { MLAlert } from "../entities/ml-alert.entity";
import { PredictionAccuracyService } from "./prediction-accuracy.service";
import { MLFeatureDriftService } from "./ml-feature-drift.service";

/**
 * ML Alert Service
 *
 * Manages alerts for model performance issues, data drift, and anomalies.
 * Provides alert creation, acknowledgment, and resolution tracking.
 */
@Injectable()
export class MLAlertService {
  private readonly logger = new Logger(MLAlertService.name);

  constructor(
    @InjectRepository(MLAlert)
    private alertRepository: Repository<MLAlert>,
    private accuracyService: PredictionAccuracyService,
    private featureDriftService: MLFeatureDriftService,
  ) {}

  /**
   * Check for issues and create alerts
   */
  async checkAndCreateAlerts(): Promise<{
    created: number;
    alerts: MLAlert[];
  }> {
    const alerts: MLAlert[] = [];

    // Use a single stats fetch for all MAE/coverage checks to avoid duplicate DB queries.
    const systemStats = await this.accuracyService.getSystemAccuracyStats(7);
    const mae = systemStats.overall.mae;
    const MAE_THRESHOLD = 8;

    // 1. Check accuracy degradation — MAE only, independent of coverage
    if (mae > MAE_THRESHOLD) {
      const alert = await this.createAlert({
        alertType: "accuracy_degradation",
        severity: this.determineSeverity(mae),
        title: "Model Accuracy Degradation",
        message: `MAE: ${mae.toFixed(1)} min (threshold: ${MAE_THRESHOLD} min)`,
        metrics: systemStats.overall,
        context: null,
      });
      if (alert) alerts.push(alert);
    } else {
      await this.resolveAlertIfActive("accuracy_degradation");
    }

    // 2. Check feature drift
    const driftResult = await this.featureDriftService.detectFeatureDrift(7);
    const criticalDrift = driftResult.driftedFeatures.filter(
      (f) => f.status === "critical",
    );
    if (criticalDrift.length > 0) {
      const alert = await this.createAlert({
        alertType: "feature_drift",
        severity: "high",
        title: "Critical Feature Drift Detected",
        message: `${criticalDrift.length} features showing critical drift (>30% deviation)`,
        metrics: {
          criticalCount: criticalDrift.length,
          warningCount: driftResult.summary.warningCount,
          totalFeatures: driftResult.summary.totalFeatures,
        },
        context: {
          criticalFeatures: criticalDrift.map((f) => f.featureName),
        },
      });
      if (alert) alerts.push(alert);
    } else {
      await this.resolveAlertIfActive("feature_drift");
    }

    // 3. Check coverage
    if (systemStats.overall.coveragePercent < 80) {
      const alert = await this.createAlert({
        alertType: "low_coverage",
        severity: "medium",
        title: "Low Prediction Coverage",
        message: `Coverage: ${systemStats.overall.coveragePercent.toFixed(1)}% (threshold: 80%)`,
        metrics: {
          coveragePercent: systemStats.overall.coveragePercent,
          totalPredictions: systemStats.overall.totalPredictions,
          matchedPredictions: systemStats.overall.matchedPredictions,
        },
      });
      if (alert) alerts.push(alert);
    } else {
      await this.resolveAlertIfActive("low_coverage");
    }

    this.logger.log(`Created ${alerts.length} alerts`);
    return { created: alerts.length, alerts };
  }

  /**
   * Create alert (if not already active)
   */
  private async createAlert(data: {
    alertType: MLAlert["alertType"];
    severity: MLAlert["severity"];
    title: string;
    message: string;
    metrics?: Record<string, unknown> | null;
    context?: Record<string, unknown> | null;
  }): Promise<MLAlert | null> {
    // Check if similar alert already exists (active)
    const existing = await this.alertRepository.findOne({
      where: {
        alertType: data.alertType,
        status: "active",
      },
      order: { createdAt: "DESC" },
    });

    if (existing) {
      // Update existing alert if severity increased
      if (
        this.getSeverityLevel(data.severity) >
        this.getSeverityLevel(existing.severity)
      ) {
        existing.severity = data.severity;
        existing.message = data.message;
        existing.metrics = data.metrics || null;
        existing.context = data.context || null;
        await this.alertRepository.save(existing);
        return existing;
      }
      // Don't create duplicate
      return null;
    }

    const alert = new MLAlert();
    alert.alertType = data.alertType;
    alert.severity = data.severity;
    alert.title = data.title;
    alert.message = data.message;
    alert.metrics = data.metrics || null;
    alert.context = data.context || null;
    alert.status = "active";

    return await this.alertRepository.save(alert);
  }

  /**
   * Get active alerts
   */
  async getActiveAlerts(): Promise<MLAlert[]> {
    return this.alertRepository.find({
      where: { status: "active" },
      order: { severity: "DESC", createdAt: "DESC" },
    });
  }

  /**
   * Acknowledge alert
   */
  async acknowledgeAlert(
    alertId: string,
    acknowledgedBy: string,
  ): Promise<MLAlert> {
    const alert = await this.alertRepository.findOne({
      where: { id: alertId },
    });
    if (!alert) {
      throw new Error(`Alert ${alertId} not found`);
    }

    alert.status = "acknowledged";
    alert.acknowledgedBy = acknowledgedBy;
    alert.acknowledgedAt = new Date();

    return await this.alertRepository.save(alert);
  }

  /**
   * Resolve alert
   */
  async resolveAlert(
    alertId: string,
    resolutionNote?: string,
  ): Promise<MLAlert> {
    const alert = await this.alertRepository.findOne({
      where: { id: alertId },
    });
    if (!alert) {
      throw new Error(`Alert ${alertId} not found`);
    }

    alert.status = "resolved";
    alert.resolutionNote = resolutionNote || null;
    alert.resolvedAt = new Date();

    return await this.alertRepository.save(alert);
  }

  /**
   * Cleanup old resolved alerts (older than 30 days)
   */
  async cleanupOldAlerts(): Promise<number> {
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const result = await this.alertRepository.delete({
      status: "resolved",
      resolvedAt: LessThan(thirtyDaysAgo),
    });
    return result.affected || 0;
  }

  /**
   * Auto-resolve all active/acknowledged alerts of a given type when condition clears.
   */
  private async resolveAlertIfActive(
    alertType: MLAlert["alertType"],
  ): Promise<void> {
    const staleAlerts = await this.alertRepository.find({
      where: {
        alertType,
        status: In(["active", "acknowledged"]),
      },
    });
    for (const alert of staleAlerts) {
      alert.status = "resolved";
      alert.resolvedAt = new Date();
      alert.resolutionNote = "Auto-resolved: condition cleared";
      await this.alertRepository.save(alert);
      this.logger.log(`Auto-resolved ${alertType} alert (id: ${alert.id})`);
    }
  }

  /**
   * Determine severity based on MAE relative to training baseline (~4.6 min).
   */
  private determineSeverity(
    mae: number,
  ): "low" | "medium" | "high" | "critical" {
    if (mae > 15) return "critical";
    if (mae > 10) return "high";
    if (mae > 7) return "medium";
    return "low";
  }

  /**
   * Get severity level (for comparison)
   */
  private getSeverityLevel(
    severity: "low" | "medium" | "high" | "critical",
  ): number {
    const levels = { low: 1, medium: 2, high: 3, critical: 4 };
    return levels[severity];
  }
}
