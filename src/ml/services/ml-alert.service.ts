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

    // Both inputs are independent 7-day aggregates with no shared
    // intermediate state — kick them off in parallel so the hourly
    // alert cron finishes in roughly max(accuracy, drift) wall-time
    // instead of the sum of the two.
    const [systemStats, driftResult] = await Promise.all([
      this.accuracyService.getSystemAccuracyStats(7),
      this.featureDriftService.detectFeatureDrift(7),
    ]);
    const mae = systemStats.overall.mae;
    // Calibrated for the Quantile(alpha=0.8) loss era (config.py
    // CATBOOST_LOSS_FUNCTION). That loss predicts the UPPER conditional quantile
    // by design, so it deliberately over-reads quiet slots (~92% of all slots) →
    // overall live MAE sits at ~10-12 min as a structural baseline, not as
    // degradation. The old threshold of 8 (and the 7/10/15 severity ladder) came
    // from the RMSE-loss era when MAE was ~4-5, so it fired permanently. 13 keeps
    // the alert useful (catches a real climb above the q0.8 baseline) without the
    // constant false positive. The honest fix for the quiet over-read is
    // MultiQuantile serving (median for the wait display, q0.8 for the crowd
    // signal) — see docs/ml/tft-vs-catboost-clean-comparison.md §6.5.
    //
    // PAR-912: most of that "structural baseline" was not the quantile at all.
    // `overall.mae` averaged every 0 on a day the park was open, and 65–70 % of
    // the compared rows were 0 — rides not running and feeds that publish no
    // wait. On scored slots only the same 7 days read 6.9 instead of 10.1
    // (measured 2026-10-17), and about 7.5–8 once real 0-minute waits of new
    // rows count again. 10 keeps the old ~1.3× margin over that baseline; 13
    // would now never fire.
    const MAE_THRESHOLD = 10;

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

    // 2. Check feature drift (already resolved above via Promise.all)
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
    const result = await this.alertRepository.update(
      { alertType, status: In(["active", "acknowledged"]) },
      {
        status: "resolved",
        resolvedAt: new Date(),
        resolutionNote: "Auto-resolved: condition cleared",
      },
    );
    if (result.affected) {
      this.logger.log(`Auto-resolved ${result.affected} ${alertType} alert(s)`);
    }
  }

  /**
   * Determine severity based on MAE relative to training baseline (~4.6 min).
   */
  private determineSeverity(
    mae: number,
  ): "low" | "medium" | "high" | "critical" {
    // Scored-slot calibration (PAR-912, see MAE_THRESHOLD above): baseline
    // live MAE ~7-8. The old 13/17/22 ladder scaled by the same 10/13.
    if (mae > 17) return "critical";
    if (mae > 13) return "high";
    if (mae > 10) return "medium";
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
