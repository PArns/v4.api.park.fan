import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { InjectQueue } from "@nestjs/bull";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";

/**
 * ML Model Health Check Processor
 *
 * Monitors model performance and triggers retraining when needed
 *
 * Schedule: Daily at 2am
 *
 * Checks:
 * - Prediction accuracy (MAE, MAPE, Coverage)
 * - Triggers retraining if accuracy degrades
 */
@Processor("ml-health")
export class MLHealthCheckProcessor {
  private readonly logger = new Logger(MLHealthCheckProcessor.name);

  constructor(
    private predictionAccuracyService: PredictionAccuracyService,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
  ) {}

  @Process("check-model-health")
  async handleHealthCheck(_job: Job): Promise<void> {
    this.logger.log("🏥 Checking ML model health...");

    try {
      // Check if retraining is needed (last 7 days)
      const healthCheck =
        await this.predictionAccuracyService.checkRetrainingNeeded(7);

      if (healthCheck.needed) {
        this.logger.warn(
          `🔄 Triggering model retraining - Reason: ${healthCheck.reason}`,
        );
        this.logger.warn(
          `   Metrics: MAE=${healthCheck.metrics.mae.toFixed(1)} min, ` +
            `Coverage=${healthCheck.metrics.coveragePercent.toFixed(1)}%, ` +
            `MAPE=${healthCheck.metrics.mape.toFixed(1)}%`,
        );

        // Trigger training job
        await this.mlTrainingQueue.add("train-model", {
          reason: healthCheck.reason,
          triggeredBy: "automated-health-check",
          currentMetrics: healthCheck.metrics,
        });

        this.logger.log("✅ Retraining job queued");
      } else {
        this.logger.log(
          `✅ Model health good: MAE=${healthCheck.metrics.mae.toFixed(1)} min, ` +
            `Coverage=${healthCheck.metrics.coveragePercent.toFixed(1)}%`,
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Model health check failed: ${errorMessage}`);
      throw error;
    }
  }
}
