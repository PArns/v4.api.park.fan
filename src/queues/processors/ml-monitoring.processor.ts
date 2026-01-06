import { Processor, Process } from "@nestjs/bull";
import { Job } from "bull";
import { Logger, Injectable } from "@nestjs/common";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
import { MLAlertService } from "../../ml/services/ml-alert.service";
import { MLAnomalyDetectionService } from "../../ml/services/ml-anomaly-detection.service";
import { MLRequestLoggingService } from "../../ml/services/ml-request-logging.service";

/**
 * ML Monitoring Processor
 *
 * Background jobs for ML monitoring and observability:
 * - Feature drift detection
 * - Alert checking
 * - Anomaly detection
 * - Cleanup tasks
 */
@Processor("ml-monitoring")
@Injectable()
export class MLMonitoringProcessor {
  private readonly logger = new Logger(MLMonitoringProcessor.name);

  constructor(
    private featureDriftService: MLFeatureDriftService,
    private alertService: MLAlertService,
    private anomalyDetectionService: MLAnomalyDetectionService,
    private requestLoggingService: MLRequestLoggingService,
  ) {}

  /**
   * Detect feature drift (daily)
   */
  @Process("detect-feature-drift")
  async handleFeatureDriftDetection(_job: Job): Promise<void> {
    this.logger.log("🔄 Starting feature drift detection...");
    try {
      const result = await this.featureDriftService.detectFeatureDrift(7);
      this.logger.log(
        `✅ Feature drift detection complete: ${result.summary.healthyCount} healthy, ${result.summary.warningCount} warning, ${result.summary.criticalCount} critical`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Feature drift detection failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Check and create alerts (hourly)
   */
  @Process("check-alerts")
  async handleAlertCheck(_job: Job): Promise<void> {
    this.logger.log("🔄 Checking for ML alerts...");
    try {
      const result = await this.alertService.checkAndCreateAlerts();
      if (result.created > 0) {
        this.logger.warn(
          `⚠️  Created ${result.created} new alerts: ${result.alerts.map((a) => a.title).join(", ")}`,
        );
      } else {
        this.logger.log("✅ No new alerts needed");
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Alert check failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Detect anomalies (daily)
   */
  @Process("detect-anomalies")
  async handleAnomalyDetection(_job: Job): Promise<void> {
    this.logger.log("🔄 Starting anomaly detection...");
    try {
      const result = await this.anomalyDetectionService.detectAnomalies(7);
      this.logger.log(
        `✅ Anomaly detection complete: ${result.detected} anomalies detected`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Anomaly detection failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Cleanup old records (daily)
   */
  @Process("cleanup")
  async handleCleanup(_job: Job): Promise<void> {
    this.logger.log("🧹 Starting cleanup of old monitoring records...");
    try {
      const [alertsDeleted, anomaliesDeleted, logsDeleted] = await Promise.all([
        this.alertService.cleanupOldAlerts(),
        this.anomalyDetectionService.cleanupOldAnomalies(),
        this.requestLoggingService.cleanupOldLogs(),
      ]);

      this.logger.log(
        `✅ Cleanup complete: ${alertsDeleted} alerts, ${anomaliesDeleted} anomalies, ${logsDeleted} request logs deleted`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Cleanup failed: ${errorMessage}`);
      throw error;
    }
  }
}
