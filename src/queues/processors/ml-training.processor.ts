import { Processor, Process } from "@nestjs/bull";
import { getMlServiceUrl } from "../../config/ml-services.config";
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
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
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
    private featureDriftService: MLFeatureDriftService,
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
      const mlServiceUrl = getMlServiceUrl();
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

      // Wait for the new model to be loaded by all workers before reading metrics.
      // The "completed" status is set before the sentinel file is written; workers
      // reload lazily on the next request, so /model/info can still return the old
      // version for a few seconds after training finishes.
      let modelInfo: Record<string, unknown> = {};
      for (let i = 0; i < 12; i++) {
        const res = await axios.get(`${mlServiceUrl}/model/info`);
        if (res.data?.version === version) {
          modelInfo = res.data;
          break;
        }
        this.logger.log(
          `Waiting for workers to load ${version} (current: ${res.data?.version}), retry ${i + 1}/12...`,
        );
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }

      if (!modelInfo.version) {
        this.logger.warn(
          `Workers did not load ${version} within 60s — using last available metrics`,
        );
        const fallback = await axios.get(`${mlServiceUrl}/model/info`);
        modelInfo = fallback.data;
      }

      const metricsData = (modelInfo.metrics ?? {}) as Record<string, number>;
      const metrics = {
        mae: metricsData.mae || 0,
        rmse: metricsData.rmse || 0,
        mape: metricsData.mape || 0,
        r2: metricsData.r2 || 0,
        trainSamples: (modelInfo.train_samples as number) || 0,
        valSamples: (modelInfo.val_samples as number) || 0,
      };

      // Store feature distribution stats for drift detection
      const rawFeatureStats = modelInfo.featureStats as
        | Array<Record<string, unknown>>
        | undefined;
      if (rawFeatureStats && rawFeatureStats.length > 0) {
        try {
          await this.featureDriftService.storeFeatureStats(
            version,
            rawFeatureStats.map((s) => ({
              featureName: s.featureName as string,
              mean: (s.mean as number) ?? 0,
              std: (s.std as number) ?? 0,
              min: (s.min as number) ?? 0,
              max: (s.max as number) ?? 0,
              percentile10: (s.percentile10 as number) ?? 0,
              percentile50: (s.percentile50 as number) ?? 0,
              percentile90: (s.percentile90 as number) ?? 0,
              sampleCount: (s.sampleCount as number) ?? 0,
              featureType:
                (s.featureType as "numeric" | "categorical") ?? "numeric",
              topValues: (s.topValues as Record<string, number>) ?? undefined,
            })),
          );
          this.logger.log(
            `   Feature stats stored: ${rawFeatureStats.length} features`,
          );
        } catch (driftError) {
          this.logger.warn(
            `   Failed to store feature stats: ${driftError instanceof Error ? driftError.message : String(driftError)}`,
          );
        }
      } else {
        this.logger.warn(
          `   No feature stats in model info — drift detection will be skipped until next training`,
        );
      }

      // Champion/challenger gate: only a CATASTROPHICALLY-worse model (e.g. an
      // untuned GPU port that regressed MAE 5.6 → 26 with R² −1.15) must be blocked
      // from auto-replacing a good champion. A freshly trained model has seen newer
      // data and should generally win, so we only reject large regressions — normal
      // day-to-day MAE variance (a few %) is expected and the newer model is kept.
      // The ml-service writes its own sentinel on training success, so on rejection
      // we re-load the DB-active champion below to revert that. Validation MAE is
      // compared apples-to-apples (both from model metadata).
      const champion = await this.mlModelRepository.findOne({
        where: { isActive: true },
      });
      // 1.25 = reject only if >25% worse than champion. Catches disasters (26 vs 5.6
      // = 4.6× → rejected) while letting the fresher model win on normal variance
      // (e.g. 6.06 vs 5.62 = 1.08× → accepted).
      const REGRESSION_TOLERANCE = 1.25;
      const championMae = champion?.mae ?? 0;
      const rejectChallenger =
        champion != null &&
        championMae > 0 &&
        metrics.mae > 0 &&
        metrics.mae > championMae * REGRESSION_TOLERANCE;

      if (rejectChallenger) {
        this.logger.warn(
          `⛔ Challenger ${version} (MAE ${metrics.mae.toFixed(2)}) is worse than ` +
            `champion ${champion!.version} (MAE ${championMae.toFixed(2)}) × ${REGRESSION_TOLERANCE} — ` +
            `keeping champion active, registering challenger as inactive.`,
        );
      } else {
        // Accepted: deactivate the previous champion(s)
        await this.mlModelRepository.update(
          { isActive: true },
          { isActive: false },
        );
      }

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
        featuresUsed: (modelInfo.features as string[]) || [],
        hyperparameters:
          (modelInfo.hyperparameters as Record<string, unknown>) ?? {},
        isActive: !rejectChallenger,
        notes: rejectChallenger
          ? `Challenger rejected (MAE ${metrics.mae.toFixed(2)} > champion ${championMae.toFixed(2)}) ${new Date().toISOString().split("T")[0]}`
          : `Trained on ${new Date().toISOString().split("T")[0]}`,
      });

      await this.mlModelRepository.save(model);

      // If rejected, the ml-service already activated the challenger via its own
      // sentinel during training — revert it to the still-active DB champion.
      if (rejectChallenger) {
        try {
          await axios.post(`${mlServiceUrl}/model/reload`);
          this.logger.warn(
            `   Reverted ml-service to champion ${champion!.version}`,
          );
        } catch (e) {
          this.logger.error(
            `   Failed to revert ml-service to champion: ${e instanceof Error ? e.message : String(e)}`,
          );
        }
      }

      const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
      this.logger.log(`✅ Training completed in ${duration} minutes`);
      this.logger.log(`   Version: ${version}`);
      this.logger.log(`   MAE: ${metrics.mae?.toFixed(2)} min`);
      this.logger.log(`   RMSE: ${metrics.rmse?.toFixed(2)} min`);
      this.logger.log(`   R²: ${metrics.r2?.toFixed(4)}`);
      this.logger.log(
        `   Samples: ${metrics.trainSamples} train, ${metrics.valSamples} validation`,
      );
      this.logger.log(
        `   Features: ${(modelInfo.features as string[] | undefined)?.length || 0}`,
      );

      // Cleanup old models (keep only active + last 2 backups)
      await this.cleanupOldModels();
    } catch (error) {
      // 409 = ML service already training (e.g. previous run still in progress).
      // Treat as a soft skip — no failure log, no BullMQ retry storm.
      const status = (error as any)?.response?.status;
      if (status === 409) {
        this.logger.warn(
          "⏭️ Training skipped — ML service already has a training run in progress",
        );
        return;
      }

      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error(`❌ Training failed: ${errorMessage}`);

      // Log to dedicated file for critical job failures
      logJobFailure("train-model", "ml-training", error, {
        mlServiceUrl: getMlServiceUrl(),
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
   * - The last 30 models by training date (for sparkline history)
   * - Always keeps the active model regardless of position
   *
   * Deletes:
   * - Models beyond the 30-model window (both files and DB entries)
   * - Orphaned model files without DB entries
   */
  @Process("cleanup-models")
  async handleCleanupModels(_job: Job): Promise<void> {
    await this.cleanupOldModels();
  }

  private readonly MODELS_TO_KEEP = 30;

  private async cleanupOldModels(): Promise<void> {
    try {
      this.logger.log("🧹 Cleaning up old models...");

      // Get all models sorted by training date (newest first)
      const allModels = await this.mlModelRepository.find({
        order: { trainedAt: "DESC" },
      });

      // Keep the last 30 models; always keep the active model even if outside that window
      const keepSet = new Set(
        allModels.slice(0, this.MODELS_TO_KEEP).map((m) => m.id),
      );
      // Ensure the active model is always kept
      allModels.filter((m) => m.isActive).forEach((m) => keepSet.add(m.id));

      const modelsToKeep = allModels.filter((m) => keepSet.has(m.id));
      const modelsToDelete = allModels.filter((m) => !keepSet.has(m.id));

      if (modelsToDelete.length === 0) {
        this.logger.log(
          `   Skipping cleanup: All ${allModels.length} model(s) within retention limit (${this.MODELS_TO_KEEP})`,
        );
        return;
      }

      this.logger.log(
        `   Keeping ${modelsToKeep.length} models (last ${this.MODELS_TO_KEEP}), deleting ${modelsToDelete.length}`,
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
