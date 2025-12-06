import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";

/**
 * Prediction Accuracy Processor
 *
 * Processes jobs in the 'prediction-accuracy' queue.
 * Compares stored predictions with actual wait times for accuracy tracking.
 *
 * Strategy:
 * 1. Find predictions that have passed (targetTime < now)
 * 2. Match with actual wait times from queue_data (±15 min window)
 * 3. Calculate error metrics (MAE, RMSE, MAPE)
 * 4. Update PredictionAccuracy table
 *
 * Scheduled: Every hour
 * Batch size: 1000 predictions per run
 */
@Processor("prediction-accuracy")
export class PredictionAccuracyProcessor {
  private readonly logger = new Logger(PredictionAccuracyProcessor.name);

  constructor(private predictionAccuracyService: PredictionAccuracyService) {}

  @Process("compare-accuracy")
  async handleCalculateAccuracy(_job: Job): Promise<void> {
    this.logger.log("🔄 Starting prediction accuracy comparison...");

    try {
      await this.predictionAccuracyService.compareWithActuals();

      this.logger.log("✅ Prediction accuracy comparison completed");
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Prediction accuracy comparison failed: ${errorMessage}`,
      );
      throw error; // Re-throw so Bull marks the job as failed
    }
  }
}
