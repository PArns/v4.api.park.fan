import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { exec } from "child_process";
import { promisify } from "util";
import * as fs from "fs/promises";
import * as path from "path";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import axios from "axios";
import { logJobFailure } from "../../common/utils/file-logger.util";

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
      // Configurable timeout via ML_TRAINING_TIMEOUT_MINUTES (default: 45 minutes)
      const timeoutMinutes = parseInt(
        process.env.ML_TRAINING_TIMEOUT_MINUTES || "45",
        10,
      );
      const pollIntervalSeconds = 30; // Check every 30 seconds
      const maxAttempts = (timeoutMinutes * 60) / pollIntervalSeconds; // Convert to attempts

      this.logger.log(
        `Training timeout: ${timeoutMinutes} minutes (${maxAttempts} attempts at ${pollIntervalSeconds}s intervals)`,
      );

      let isTraining = true;
      let attempts = 0;

      while (isTraining && attempts < maxAttempts) {
        await new Promise((resolve) =>
          setTimeout(resolve, pollIntervalSeconds * 1000),
        );
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
        } else if (status.status === "idle" && attempts >= 2) {
          // "idle" can mean training already finished before our first poll.
          // Check if the model/info endpoint reports a version matching ours.
          try {
            const infoRes = await axios.get(`${mlServiceUrl}/model/info`);
            const activeVersion = infoRes.data?.version;
            if (activeVersion === version) {
              isTraining = false;
              this.logger.log(
                `✅ Training already completed (detected via model/info: ${activeVersion})`,
              );
            } else if (
              activeVersion &&
              activeVersion > version &&
              attempts >= 5
            ) {
              // A newer version is active — our training superseded by another run
              isTraining = false;
              this.logger.log(
                `ℹ️  Newer model active (${activeVersion}), stopping poll`,
              );
            }
          } catch {
            // model/info not reachable, keep polling
          }
        }
      }

      if (attempts >= maxAttempts) {
        throw new Error(
          `Training timeout - exceeded ${timeoutMinutes} minutes`,
        );
      }

      // Calculate training duration
      const trainingDurationSeconds = Math.floor(
        (Date.now() - startTime) / 1000,
      );

      // Fetch training metrics from ML service
      const modelInfoResponse = await axios.get(`${mlServiceUrl}/model/info`);
      const modelInfo = modelInfoResponse.data;

      const metrics = {
        mae: modelInfo.metrics?.mae || 0,
        rmse: modelInfo.metrics?.rmse || 0,
        mape: modelInfo.metrics?.mape || 0,
        r2: modelInfo.metrics?.r2 || 0,
        trainSamples: modelInfo.train_samples || 0, // From metadata root
        valSamples: modelInfo.val_samples || 0, // From metadata root
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
        trainingDurationSeconds, // Add duration
        trainDataStartDate,
        trainDataEndDate,
        trainSamples: metrics.trainSamples || 0,
        validationSamples: metrics.valSamples || 0,
        featuresUsed: modelInfo.features || [], // Use features from ML service API
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
      this.logger.log(
        `   Samples: ${metrics.trainSamples} train, ${metrics.valSamples} validation`,
      );
      this.logger.log(`   Features: ${modelInfo.features?.length || 0}`);

      // Cleanup old models (keep only active + last 2 backups)
      await this.cleanupOldModels();
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error(`❌ Training failed: ${errorMessage}`);

      // Log to dedicated file for critical job failures
      logJobFailure("train-model", "ml-training", error, {
        mlServiceUrl: process.env.ML_SERVICE_URL || "http://ml-service:8000",
      });

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
  @Process("cleanup-models")
  async handleCleanupModels(_job: Job): Promise<void> {
    await this.cleanupOldModels();
  }

  private async cleanupOldModels(): Promise<void> {
    try {
      this.logger.log("🧹 Cleaning up old models...");

      // Get all models sorted by training date (newest first)
      const allModels = await this.mlModelRepository.find({
        order: { trainedAt: "DESC" },
      });

      // Keep models from the last 3 days; always keep the active model
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - 3);

      const modelsToKeep = allModels.filter(
        (m) => m.isActive || new Date(m.trainedAt) >= cutoff,
      );
      const modelsToDelete = allModels.filter(
        (m) => !m.isActive && new Date(m.trainedAt) < cutoff,
      );

      if (modelsToDelete.length === 0) {
        this.logger.log(
          `   Skipping cleanup: All ${allModels.length} model(s) are within 3 days`,
        );
        return;
      }

      this.logger.log(
        `   Keeping ${modelsToKeep.length} models (last 3 days), deleting ${modelsToDelete.length}`,
      );

      let deletedFiles = 0;
      let deletedDbEntries = 0;

      for (const model of modelsToDelete) {
        // SECURITY: Validate version to prevent path traversal
        const sanitizedVersion = this.sanitizeVersion(model.version);
        if (!sanitizedVersion) {
          this.logger.warn(
            `   ⚠ Invalid model version format, skipping: ${model.version}`,
          );
          continue;
        }

        // Delete model file (.cbm)
        try {
          // SECURITY: Use path.join to prevent path traversal, validate against MODEL_DIR
          const modelPath = path.join(
            process.env.MODEL_DIR || "/app/models",
            `catboost_${sanitizedVersion}.cbm`,
          );
          // SECURITY: Ensure path is within MODEL_DIR to prevent directory traversal
          if (
            !this.isPathSafe(modelPath, process.env.MODEL_DIR || "/app/models")
          ) {
            this.logger.warn(
              `   ⚠ Unsafe model path detected, skipping: ${modelPath}`,
            );
            continue;
          }
          await fs.unlink(modelPath);
          this.logger.debug(`   ✓ Deleted model file: ${sanitizedVersion}`);
          deletedFiles++;
        } catch (fileError) {
          // File might not exist, that's ok
          this.logger.debug(
            `   ⚠ Could not delete model file ${sanitizedVersion}: ${fileError instanceof Error ? fileError.message : String(fileError)}`,
          );
        }

        // Delete metadata file (.pkl)
        try {
          // SECURITY: Use path.join and validate path
          const metadataPath = path.join(
            process.env.MODEL_DIR || "/app/models",
            `metadata_${sanitizedVersion}.pkl`,
          );
          // SECURITY: Ensure path is within MODEL_DIR
          if (
            !this.isPathSafe(
              metadataPath,
              process.env.MODEL_DIR || "/app/models",
            )
          ) {
            this.logger.warn(
              `   ⚠ Unsafe metadata path detected, skipping: ${metadataPath}`,
            );
            continue;
          }
          await fs.unlink(metadataPath);
          this.logger.debug(`   ✓ Deleted metadata file: ${sanitizedVersion}`);
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

  /**
   * SECURITY: Sanitize version string to prevent path traversal
   * Only allows alphanumeric, dash, underscore, and dot characters
   */
  private sanitizeVersion(version: string): string | null {
    // Allow only safe characters: alphanumeric, dash, underscore, dot
    if (!/^[a-zA-Z0-9._-]+$/.test(version)) {
      return null;
    }
    // Prevent path traversal patterns
    if (
      version.includes("..") ||
      version.includes("/") ||
      version.includes("\\")
    ) {
      return null;
    }
    return version;
  }

  /**
   * SECURITY: Check if file path is safe (within allowed directory)
   * Prevents directory traversal attacks
   */
  private isPathSafe(filePath: string, allowedDir: string): boolean {
    try {
      const resolvedPath = path.resolve(filePath);
      const resolvedDir = path.resolve(allowedDir);
      // Check if resolved path starts with allowed directory
      return resolvedPath.startsWith(resolvedDir);
    } catch {
      return false;
    }
  }
}
