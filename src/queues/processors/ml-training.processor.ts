import { Processor, Process } from "@nestjs/bull";
import { getMlServiceUrl } from "../../config/ml-services.config";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { exec } from "child_process";
import { promisify } from "util";
import { InjectRepository } from "@nestjs/typeorm";
import { Not, Repository } from "typeorm";
import { MLModel } from "../../ml/entities/ml-model.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { MLFeatureDriftService } from "../../ml/services/ml-feature-drift.service";
import axios from "axios";
import { logJobFailure } from "../../common/utils/file-logger.util";

const _execAsync = promisify(exec);

/** One saved version on the ml-service's models volume (`GET /models/files`). */
export interface SavedModelFiles {
  version: string;
  files?: string[];
  bytes?: number;
  /** Newest file mtime, epoch SECONDS. */
  mtime: number;
}

/**
 * Versions whose files can go: no `ml_models` row, not protected (sentinel,
 * loaded, in training — as the ml-service reports them), and untouched for at
 * least `minAgeMs`, so a run that is still training or not yet registered is
 * never caught.
 */
export function selectOrphanVersions(
  onDisk: SavedModelFiles[],
  registeredVersions: Iterable<string>,
  protectedVersions: Iterable<string>,
  nowMs: number,
  minAgeMs: number,
): SavedModelFiles[] {
  const keep = new Set<string>([...registeredVersions, ...protectedVersions]);
  return onDisk.filter(
    (v) =>
      typeof v.version === "string" &&
      !keep.has(v.version) &&
      typeof v.mtime === "number" &&
      nowMs - v.mtime * 1000 >= minAgeMs,
  );
}

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

      // Configurable timeout via ML_TRAINING_TIMEOUT_MINUTES. Default 90: a
      // run took 2520 s on 2026-10-07 against the old 45-minute (2700 s) limit.
      const timeoutMinutes = parseInt(
        process.env.ML_TRAINING_TIMEOUT_MINUTES || "90",
        10,
      );

      // The ml-service skips the optional refit when it would not finish
      // inside this budget (PAR-815), instead of running into our timeout.
      const response = await axios.post(`${mlServiceUrl}/train`, {
        version,
        timeBudgetSeconds: timeoutMinutes * 60,
      });

      this.logger.log("Training started:", response.data);

      // Poll for training completion
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
          // "idle" can mean training already finished before our first poll
          // and the status file was reset. The version's own saved files are
          // the proof it completed (/model/info would only name the model the
          // workers serve, which training no longer changes).
          try {
            await axios.get(
              `${mlServiceUrl}/model/info/${encodeURIComponent(version)}`,
            );
            isTraining = false;
            this.logger.log(
              `✅ Training already completed (saved files for ${version} found)`,
            );
          } catch {
            // not saved (yet) or not reachable — keep polling
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

      // Metrics come from THIS version's own saved metadata, never from
      // /model/info: that describes whatever model the answering worker has
      // loaded, and a worker only reloads on its next /predict. When workers had
      // not switched within the old 60 s wait, the previous model's metrics were
      // registered under the new version and the gate compared the champion with
      // itself (v20261006 stored v20261003's MAE 5.06; its own pkl said 4.43).
      const modelInfo = await this.fetchTrainedModelInfo(mlServiceUrl, version);
      if (!modelInfo) {
        // Nothing trustworthy to register. Serving is untouched: the training
        // subprocess never activates a version, only activateModel() below does.
        throw new Error(
          `No saved metrics for ${version} — not registering it as a model`,
        );
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
        Array<Record<string, unknown>> | undefined;
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
      // Training never changes what is served; only activateModel() below does,
      // after this gate. Validation MAE is compared apples-to-apples (both from
      // the versions' own saved metadata).
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

      // One transaction: retire the previous champion(s) and insert the new
      // row together, so no reader ever sees zero or two active rows.
      await this.mlModelRepository.manager.transaction(async (em) => {
        if (!rejectChallenger) {
          await em.update(
            MLModel,
            { isActive: true, version: Not(version) },
            { isActive: false },
          );
        }
        await em.save(MLModel, model);
      });

      if (!rejectChallenger) {
        // Only now do the workers switch (PAR-815). Before, the training
        // subprocess wrote the sentinel itself and a rejected or failed run had
        // to be reverted afterwards — racing threadpool reloads that could flip
        // the workers back to the unregistered version.
        await this.activateModel(mlServiceUrl, version, champion);
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
      // Per-phase durations and row counts the ml-service saved with the model
      // (PAR-815), so a slow run shows whether fetch, features or fit grew.
      if (modelInfo.refitSkipped) {
        this.logger.warn(
          `   Final refit was SKIPPED (serving the early-stopped model): ${JSON.stringify(modelInfo.refitSkipped)}`,
        );
      }
      if (modelInfo.trainingTimings) {
        this.logger.log(
          `   Phase timings: ${JSON.stringify(modelInfo.trainingTimings)}`,
        );
      }

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
   * The saved metadata of exactly `version` (`GET /model/info/:version`), or
   * null when the ml-service has no model file and metadata for it or reports
   * no MAE. Retries briefly, because the request can land on a worker while the
   * shared volume is busy; a 404 is final.
   */
  private async fetchTrainedModelInfo(
    mlServiceUrl: string,
    version: string,
  ): Promise<Record<string, unknown> | null> {
    const attempts = 3;
    for (let i = 1; i <= attempts; i++) {
      try {
        const res = await axios.get(
          `${mlServiceUrl}/model/info/${encodeURIComponent(version)}`,
        );
        const info = res.data as Record<string, unknown> | undefined;
        const mae = (info?.metrics as Record<string, unknown> | undefined)?.mae;
        if (info?.version !== version || typeof mae !== "number" || mae <= 0) {
          this.logger.error(
            `ml-service returned no usable metrics for ${version} (version ${String(info?.version)}, MAE ${String(mae)})`,
          );
          return null;
        }
        return info;
      } catch (e) {
        const status = (e as { response?: { status?: number } })?.response
          ?.status;
        this.logger.warn(
          `Reading saved metadata for ${version} failed (attempt ${i}/${attempts}): ${e instanceof Error ? e.message : String(e)}`,
        );
        if (status === 404 || i === attempts) return null;
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
    return null;
  }

  /**
   * Make the ml-service serve the DB-active model, which is now `version`.
   * `POST /model/reload` loads the DB-active version in one worker and writes
   * the sentinel the other workers pick up on their next /predict; the boot path
   * (`_load_active_model`) reads the same DB row. If the reload does not confirm
   * `version`, the DB is rolled back to the previous champion, so the DB and what
   * is served never disagree, and the job fails.
   */
  private async activateModel(
    mlServiceUrl: string,
    version: string,
    previousChampion: MLModel | null,
  ): Promise<void> {
    const attempts = 3;
    let lastError = "";
    for (let i = 1; i <= attempts; i++) {
      try {
        const res = await axios.post(`${mlServiceUrl}/model/reload`);
        if (res.data?.version === version) {
          this.logger.log(`   Activated ${version} on the ml-service`);
          return;
        }
        lastError = `reload reported version ${String(res.data?.version)}`;
      } catch (e) {
        lastError = e instanceof Error ? e.message : String(e);
      }
      this.logger.warn(
        `   Activating ${version} failed (attempt ${i}/${attempts}): ${lastError}`,
      );
      if (i < attempts) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }

    await this.mlModelRepository.manager.transaction(async (em) => {
      await em.update(
        MLModel,
        { version },
        {
          isActive: false,
          notes: `Activation failed (${lastError}) ${new Date().toISOString().split("T")[0]}`,
        },
      );
      if (previousChampion) {
        await em.update(
          MLModel,
          { version: previousChampion.version },
          { isActive: true },
        );
      }
    });
    // Best effort: point the sentinel back at whatever the DB now says is
    // active, in case one of the failed attempts got as far as writing it.
    try {
      await axios.post(`${mlServiceUrl}/model/reload`);
    } catch {
      // Workers keep what they serve; boot reads the DB-active row anyway.
    }
    throw new Error(
      `Could not activate ${version} on the ml-service (${lastError}) — ` +
        `kept ${previousChampion?.version ?? "no model"} active`,
    );
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
   * - Models beyond the 30-model window (files and DB entries)
   * - Orphaned model files: versions on disk with no `ml_models` row, older
   *   than ORPHAN_MIN_AGE_MS (runs that failed, timed out or were never
   *   registered, ~60 MB each)
   *
   * Files are listed and deleted through the ml-service (`GET/DELETE
   * /models/files`): the API container does not mount the models volume, so
   * the `fs.unlink` this used to call never deleted anything (PAR-815). The
   * ml-service refuses to delete the sentinel, loaded or in-training version.
   */
  @Process("cleanup-models")
  async handleCleanupModels(_job: Job): Promise<void> {
    await this.cleanupOldModels();
  }

  private readonly MODELS_TO_KEEP = 30;
  private readonly ORPHAN_MIN_AGE_MS = 2 * 24 * 60 * 60 * 1000;
  // Sanity caps on one orphan sweep. The first run after PAR-815 deletes 196
  // of 226 versions (86.7%); anything beyond these looks like a broken
  // registry rather than leftovers, so nothing is deleted.
  private readonly ORPHAN_MAX_PER_RUN = 250;
  private readonly ORPHAN_MAX_SHARE = 0.9;

  private async cleanupOldModels(): Promise<void> {
    try {
      this.logger.log("🧹 Cleaning up old models...");
      const mlServiceUrl = getMlServiceUrl();

      // Get all models sorted by training date (newest first)
      const allModels = await this.mlModelRepository.find({
        order: { trainedAt: "DESC" },
      });

      // Keep the last 30 models; always keep the active model even if outside that window
      const keepSet = new Set(
        allModels.slice(0, this.MODELS_TO_KEEP).map((m) => m.id),
      );
      allModels.filter((m) => m.isActive).forEach((m) => keepSet.add(m.id));

      const modelsToDelete = allModels.filter((m) => !keepSet.has(m.id));
      const removedIds = new Set<string>();
      let deletedFiles = 0;

      if (modelsToDelete.length > 0) {
        this.logger.log(
          `   Keeping ${allModels.length - modelsToDelete.length} models (last ${this.MODELS_TO_KEEP}), deleting ${modelsToDelete.length}`,
        );
      }

      for (const model of modelsToDelete) {
        // SECURITY: Validate version to prevent path traversal
        const sanitizedVersion = this.sanitizeVersion(model.version);
        if (!sanitizedVersion) {
          this.logger.warn(
            `   ⚠ Invalid model version format, skipping: ${model.version}`,
          );
          continue;
        }
        const outcome = await this.deleteModelFiles(
          mlServiceUrl,
          sanitizedVersion,
        );
        // Keep the row when the files could not be dealt with, so the next run
        // retries instead of leaving them behind as orphans.
        if (outcome === "failed" || outcome === "protected") continue;
        if (outcome === "deleted") deletedFiles++;
        await this.mlModelRepository.remove(model);
        removedIds.add(model.id);
      }

      // Orphans: files on disk whose version has no ml_models row. Versions
      // retired above count as handled, so they are not asked for twice.
      if (allModels.length === 0) {
        // An empty registry is far more likely a wrong database or a failed
        // read than a world without models — every file would look orphaned.
        this.logger.error(
          "❌ Orphan sweep skipped: ml_models returned 0 rows — deleting nothing",
        );
        return;
      }
      const registered = allModels.map((m) => m.version);
      const listing = await axios.get(`${mlServiceUrl}/models/files`);
      const onDisk = (listing.data?.versions ?? []) as SavedModelFiles[];
      const protectedVersions = Object.values(
        (listing.data?.protected ?? {}) as Record<string, string | null>,
      ).filter((v): v is string => typeof v === "string");
      const orphans = selectOrphanVersions(
        onDisk,
        registered,
        protectedVersions,
        Date.now(),
        this.ORPHAN_MIN_AGE_MS,
      );
      if (
        orphans.length > this.ORPHAN_MAX_PER_RUN ||
        (onDisk.length > 0 &&
          orphans.length > this.ORPHAN_MAX_SHARE * onDisk.length)
      ) {
        this.logger.error(
          `❌ Orphan sweep skipped: ${orphans.length} of ${onDisk.length} versions on disk look orphaned ` +
            `(limits: ${this.ORPHAN_MAX_PER_RUN} per run, ${this.ORPHAN_MAX_SHARE * 100}% of disk) — deleting nothing`,
        );
        return;
      }
      let deletedOrphans = 0;
      let orphanBytes = 0;
      for (const orphan of orphans) {
        const version = this.sanitizeVersion(orphan.version);
        if (!version) continue;
        if (
          (await this.deleteModelFiles(mlServiceUrl, version)) === "deleted"
        ) {
          deletedOrphans++;
          orphanBytes += orphan.bytes ?? 0;
        }
      }

      this.logger.log(
        `✅ Cleanup complete: ${deletedFiles} retired model(s) and ${removedIds.size} DB entries removed, ` +
          `${deletedOrphans} orphaned version(s) deleted (${(orphanBytes / 1024 / 1024).toFixed(0)} MB)`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Model cleanup failed: ${errorMessage}`);
      // Don't throw - cleanup failure shouldn't fail the training job
    }
  }

  /** Delete one version's files through the ml-service. */
  private async deleteModelFiles(
    mlServiceUrl: string,
    version: string,
  ): Promise<"deleted" | "missing" | "protected" | "failed"> {
    try {
      await axios.delete(
        `${mlServiceUrl}/models/files/${encodeURIComponent(version)}`,
      );
      this.logger.debug(`   ✓ Deleted model files: ${version}`);
      return "deleted";
    } catch (e) {
      const status = (e as { response?: { status?: number } })?.response
        ?.status;
      if (status === 404) return "missing";
      if (status === 409) {
        this.logger.warn(`   ⚠ ${version} is protected by the ml-service`);
        return "protected";
      }
      this.logger.warn(
        `   ⚠ Could not delete model files ${version}: ${e instanceof Error ? e.message : String(e)}`,
      );
      return "failed";
    }
  }

  /**
   * SECURITY: Sanitize version string to prevent path traversal
   * Only allows alphanumeric, dash, underscore, and dot characters
   */
  private sanitizeVersion(version: string): string | null {
    // Same rule as the ml-service's _SAFE_VERSION: alphanumeric first (so no
    // leading "." or "-"), then alphanumeric, dash, underscore, dot.
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(version)) {
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
}
