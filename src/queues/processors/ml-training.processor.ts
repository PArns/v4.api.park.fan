import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { exec } from "child_process";
import { promisify } from "util";
import * as fs from "fs/promises";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import axios from "axios";

const _execAsync = promisify(exec);

/**
 * ML Training Queue Processor
 *
 * Handles daily model training jobs
 * - Triggers Python training script in ml-service container
 * - Stores model metadata in database
 * - Runs daily at 6am
 */
@Processor("ml-training")
export class MLTrainingProcessor {
  private readonly logger = new Logger(MLTrainingProcessor.name);

  constructor(
    @InjectRepository(MLModel)
    private mlModelRepository: Repository<MLModel>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
  ) {}

  @Process("train-model")
  async handleTrainModels(_job: Job): Promise<void> {
    this.logger.log("🤖 Starting ML model training...");
    const startTime = Date.now();

    try {
      // Generate model version (date + time based for multiple trainings per day)
      const now = new Date();
      const version = `v${now.toISOString().split("T")[0].replace(/-/g, "")}_${now.toISOString().split("T")[1].substring(0, 5).replace(":", "")}`;

      this.logger.log(`Training version: ${version}`);

      // Trigger training via HTTP API (replaces docker exec)
      const mlServiceUrl =
        process.env.ML_SERVICE_URL || "http://ml-service:8000";
      this.logger.log(`Triggering training via ${mlServiceUrl}/train`);

      const response = await axios.post(`${mlServiceUrl}/train`, {
        version,
      });

      this.logger.log("Training started:", response.data);

      // Poll for training completion
      let isTraining = true;
      let attempts = 0;
      const maxAttempts = 60; // 30 minutes max (30s intervals)

      while (isTraining && attempts < maxAttempts) {
        await new Promise((resolve) => setTimeout(resolve, 30000)); // Wait 30s
        attempts++;

        const statusResponse = await axios.get(`${mlServiceUrl}/train/status`);
        const status = statusResponse.data;

        this.logger.log(
          `Training status check ${attempts}/${maxAttempts}: ${status.status}`,
        );

        if (status.status === "completed") {
          isTraining = false;
          this.logger.log("✅ Training completed successfully");
        } else if (status.status === "failed") {
          throw new Error(`Training failed: ${status.error}`);
        }
      }

      if (attempts >= maxAttempts) {
        throw new Error("Training timeout - exceeded 30 minutes");
      }

      // Fetch training metrics from ML service
      const modelInfoResponse = await axios.get(`${mlServiceUrl}/model/info`);
      const modelInfo = modelInfoResponse.data;

      const metrics = {
        mae: modelInfo.metrics?.mae || 0,
        rmse: modelInfo.metrics?.rmse || 0,
        mape: modelInfo.metrics?.mape || 0,
        r2: modelInfo.metrics?.r2 || 0,
        trainSamples: modelInfo.metrics?.train_samples || 0,
        valSamples: modelInfo.metrics?.val_samples || 0,
      };

      // Deactivate old models
      await this.mlModelRepository.update(
        { isActive: true },
        { isActive: false },
      );

      // Fetch actual training data time range from database
      const dataRange = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("MIN(qd.timestamp)", "minTime")
        .addSelect("MAX(qd.timestamp)", "maxTime")
        .where("qd.waitTime IS NOT NULL")
        .andWhere("qd.status = :status", { status: "OPERATING" })
        .getRawOne();

      const trainDataStartDate = dataRange?.minTime
        ? new Date(dataRange.minTime)
        : new Date(Date.now() - 2 * 365 * 24 * 60 * 60 * 1000); // Fallback: 2 years ago
      const trainDataEndDate = dataRange?.maxTime
        ? new Date(dataRange.maxTime)
        : new Date(); // Fallback: now

      // Store model metadata in database
      const model = this.mlModelRepository.create({
        version,
        modelType: "catboost",
        filePath: `/app/models/catboost_${version}.cbm`,
        mae: metrics.mae,
        rmse: metrics.rmse,
        mape: metrics.mape,
        r2Score: metrics.r2,
        trainedAt: new Date(),
        trainDataStartDate,
        trainDataEndDate,
        trainSamples: metrics.trainSamples || 0,
        validationSamples: metrics.valSamples || 0,
        featuresUsed: this.getFeaturesList(),
        hyperparameters: {
          iterations: 1000,
          learning_rate: 0.03,
          depth: 6,
          l2_leaf_reg: 3.0,
        },
        isActive: true,
        notes: `Trained on ${new Date().toISOString().split("T")[0]}`,
      });

      await this.mlModelRepository.save(model);

      const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
      this.logger.log(`✅ Training completed in ${duration} minutes`);
      this.logger.log(`   Version: ${version}`);
      this.logger.log(`   MAE: ${metrics.mae?.toFixed(2)} min`);
      this.logger.log(`   RMSE: ${metrics.rmse?.toFixed(2)} min`);
      this.logger.log(`   R²: ${metrics.r2?.toFixed(4)}`);

      // Cleanup old models (keep only active + last 2 backups)
      await this.cleanupOldModels();
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error(`❌ Training failed: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Parse metrics from training output
   */
  private parseMetrics(output: string): {
    mae?: number;
    rmse?: number;
    mape?: number;
    r2?: number;
    trainSamples?: number;
    valSamples?: number;
  } {
    const metrics: Record<string, number> = {};

    // Extract MAE
    const maeMatch = output.match(/MAE:\s+([\d.]+)/);
    if (maeMatch) metrics.mae = parseFloat(maeMatch[1]);

    // Extract RMSE
    const rmseMatch = output.match(/RMSE:\s+([\d.]+)/);
    if (rmseMatch) metrics.rmse = parseFloat(rmseMatch[1]);

    // Extract MAPE
    const mapeMatch = output.match(/MAPE:\s+([\d.]+)/);
    if (mapeMatch) metrics.mape = parseFloat(mapeMatch[1]);

    // Extract R²
    const r2Match = output.match(/R²:\s+([\d.]+)/);
    if (r2Match) metrics.r2 = parseFloat(r2Match[1]);

    // Extract sample counts
    const trainMatch = output.match(/Training samples:\s+([\d,]+)/);
    if (trainMatch)
      metrics.trainSamples = parseInt(trainMatch[1].replace(/,/g, ""));

    const valMatch = output.match(/Validation samples:\s+([\d,]+)/);
    if (valMatch) metrics.valSamples = parseInt(valMatch[1].replace(/,/g, ""));

    return metrics;
  }

  /**
   * Get list of features used
   */
  private getFeaturesList(): string[] {
    return [
      "parkId",
      "attractionId",
      "hour",
      "day_of_week",
      "month",
      "season",
      "is_weekend",
      "temperature_avg",
      "precipitation",
      "weatherCode",
      "is_raining",
      "is_holiday_primary",
      "is_holiday_neighbor_1",
      "is_holiday_neighbor_2",
      "is_holiday_neighbor_3",
      "holiday_count_total",
      "is_park_open",
      "has_special_event",
      "has_extra_hours",
      "avg_wait_last_24h",
      "avg_wait_same_hour_last_week",
      "rolling_avg_7d",
    ];
  }

  /**
   * Cleanup old ML models
   *
   * Keeps:
   * - Active model
   * - Last 2 inactive models (as backup)
   *
   * Deletes:
   * - Older inactive models (both files and DB entries)
   * - Orphaned model files without DB entries
   */
  private async cleanupOldModels(): Promise<void> {
    try {
      this.logger.log("🧹 Cleaning up old models...");

      // Get all models sorted by training date (newest first)
      const allModels = await this.mlModelRepository.find({
        order: { trainedAt: "DESC" },
      });

      if (allModels.length <= 3) {
        this.logger.log(
          `   Skipping cleanup: Only ${allModels.length} model(s) exist`,
        );
        return;
      }

      // Keep: active model + last 2 inactive = top 3
      const modelsToKeep = allModels.slice(0, 3);
      const modelsToDelete = allModels.slice(3);

      this.logger.log(
        `   Keeping ${modelsToKeep.length} models, deleting ${modelsToDelete.length}`,
      );

      let deletedFiles = 0;
      let deletedDbEntries = 0;

      for (const model of modelsToDelete) {
        // Delete model file (.cbm)
        try {
          const modelPath = `/app/models/catboost_${model.version}.cbm`;
          await fs.unlink(modelPath);
          this.logger.debug(`   ✓ Deleted model file: ${model.version}`);
          deletedFiles++;
        } catch (fileError) {
          // File might not exist, that's ok
          this.logger.debug(
            `   ⚠ Could not delete model file ${model.version}: ${fileError instanceof Error ? fileError.message : String(fileError)}`,
          );
        }

        // Delete metadata file (.pkl)
        try {
          const metadataPath = `/app/models/metadata_${model.version}.pkl`;
          await fs.unlink(metadataPath);
          this.logger.debug(`   ✓ Deleted metadata file: ${model.version}`);
        } catch (_metaError) {
          // Metadata might not exist, that's ok
        }

        // Delete DB entry
        await this.mlModelRepository.remove(model);
        deletedDbEntries++;
        this.logger.debug(`   ✓ Deleted DB entry: ${model.version}`);
      }

      this.logger.log(
        `✅ Cleanup complete: Deleted ${deletedFiles} model files and ${deletedDbEntries} DB entries`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Model cleanup failed: ${errorMessage}`);
      // Don't throw - cleanup failure shouldn't fail the training job
    }
  }
}
