import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { MLModel } from "../entities/ml-model.entity";
import * as fs from "fs/promises";
import {
  CurrentModelDto,
  ModelComparisonDto,
  ModelVersionInfoDto,
} from "../dto/ml-dashboard.dto";

/**
 * MLModelService
 *
 * Centralized service for MLModel-related operations
 * - Get active model with file size information
 * - Model version history
 * - Model comparison (current vs previous)
 * - Model age calculation
 */
@Injectable()
export class MLModelService {
  private readonly logger = new Logger(MLModelService.name);
  private readonly MODEL_DIR = process.env.MODEL_DIR || "/app/models";

  constructor(
    @InjectRepository(MLModel)
    private mlModelRepository: Repository<MLModel>,
  ) {}

  /**
   * Get active model with complete details including file size
   *
   * Retrieves the currently active model and enriches it with:
   * - File size information (bytes and MB)
   * - Calculated training data duration
   * - Feature count
   *
   * @returns {Promise<CurrentModelDto | null>} Complete model information or null if no active model
   */
  async getActiveModelWithDetails(): Promise<CurrentModelDto | null> {
    const model = await this.mlModelRepository.findOne({
      where: { isActive: true },
      order: { trainedAt: "DESC" },
    });

    if (!model) {
      this.logger.warn("No active model found");
      return null;
    }

    // Get file size (graceful degradation if file not found)
    let fileSizeBytes: number | null = null;
    let fileSizeMB: number | null = null;

    try {
      const stats = await fs.stat(model.filePath);
      fileSizeBytes = stats.size;
      fileSizeMB = parseFloat((fileSizeBytes / 1024 / 1024).toFixed(2));
      this.logger.debug(
        `Model file size: ${fileSizeMB} MB (${model.filePath})`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Could not read model file size: ${model.filePath} - ${errorMessage}`,
      );
      // File not found or not accessible - continue with null values
    }

    // Calculate training data duration
    const startDate = model.trainDataStartDate.toISOString().split("T")[0];
    const endDate = model.trainDataEndDate.toISOString().split("T")[0];
    const dataDurationDays = Math.floor(
      (model.trainDataEndDate.getTime() - model.trainDataStartDate.getTime()) /
        (1000 * 60 * 60 * 24),
    );

    // Build complete DTO
    const currentModelDto: CurrentModelDto = {
      version: model.version,
      trainedAt: model.trainedAt.toISOString(),
      trainingDurationSeconds: model.trainingDurationSeconds || null,
      fileSizeBytes,
      fileSizeMB,
      modelSize: fileSizeMB ? `${fileSizeMB} MB` : null,
      modelType: model.modelType,
      isActive: model.isActive,
      trainingMetrics: {
        mae: model.mae ? parseFloat(model.mae.toFixed(2)) : 0,
        rmse: model.rmse ? parseFloat(model.rmse.toFixed(2)) : 0,
        mape: model.mape ? parseFloat(model.mape.toFixed(2)) : 0,
        r2Score: model.r2Score ? parseFloat(model.r2Score.toFixed(2)) : 0,
      },
      trainingData: {
        startDate,
        endDate,
        totalSamples: model.trainSamples + (model.validationSamples || 0),
        trainSamples: model.trainSamples,
        validationSamples: model.validationSamples || 0,
        dataDurationDays,
      },
      configuration: {
        featuresUsed: model.featuresUsed || [],
        featureCount: (model.featuresUsed || []).length,
        hyperparameters: model.hyperparameters || {},
      },
    };

    return currentModelDto;
  }

  /**
   * Get model version history
   *
   * Returns last N models ordered by training date (most recent first)
   *
   * @param {number} limit - Number of models to return (default: 10)
   * @returns {Promise<MLModel[]>} Array of models
   */
  async getModelHistory(limit: number = 10): Promise<MLModel[]> {
    const models = await this.mlModelRepository.find({
      order: { trainedAt: "DESC" },
      take: limit,
    });

    this.logger.debug(`Retrieved ${models.length} model(s) from history`);
    return models;
  }

  /**
   * Compare current model with previous version
   *
   * Calculates improvement metrics:
   * - MAE delta (negative = improvement)
   * - MAE percentage change
   * - Boolean flag if model is improving
   *
   * @returns {Promise<ModelComparisonDto>} Model comparison with improvement metrics
   */
  async getModelComparison(): Promise<ModelComparisonDto> {
    const models = await this.mlModelRepository.find({
      order: { trainedAt: "DESC" },
      take: 2, // Get current and previous
    });

    if (models.length === 0) {
      this.logger.warn("No models found for comparison");
      return {
        current: null,
        previous: null,
        improvement: null,
      };
    }

    const current = models[0];
    const previous = models.length > 1 ? models[1] : null;

    const currentInfo: ModelVersionInfoDto = {
      version: current.version,
      mae: current.mae ? parseFloat(current.mae.toFixed(2)) : 0,
      r2: current.r2Score ? parseFloat(current.r2Score.toFixed(2)) : 0,
      trainedAt: current.trainedAt.toISOString(),
    };

    let previousInfo: ModelVersionInfoDto | null = null;
    let improvement = null;

    if (previous) {
      previousInfo = {
        version: previous.version,
        mae: previous.mae ? parseFloat(previous.mae.toFixed(2)) : 0,
        r2: previous.r2Score ? parseFloat(previous.r2Score.toFixed(2)) : 0,
        trainedAt: previous.trainedAt.toISOString(),
      };

      // Calculate improvement metrics
      const maeDelta = current.mae - previous.mae;
      const maePercentChange = (maeDelta / previous.mae) * 100;

      improvement = {
        maeDelta: parseFloat(maeDelta.toFixed(2)),
        maePercentChange: parseFloat(maePercentChange.toFixed(2)),
        isImproving: current.mae < previous.mae,
      };

      this.logger.debug(
        `Model comparison: ${current.version} vs ${previous.version} - ` +
          `MAE delta: ${improvement.maeDelta} (${improvement.isImproving ? "IMPROVING" : "DEGRADING"})`,
      );
    } else {
      this.logger.debug("Only one model exists - no comparison available");
    }

    return {
      current: currentInfo,
      previous: previousInfo,
      improvement,
    };
  }

  /**
   * Calculate model age (days and hours since training)
   *
   * Breaks down the time since training into:
   * - Full days
   * - Remaining hours
   *
   * @param {Date} trainedAt - Model training timestamp
   * @returns {{ days: number; hours: number; minutes: number }} Model age breakdown
   */
  getModelAge(trainedAt: Date): {
    days: number;
    hours: number;
    minutes: number;
  } {
    const now = new Date();
    const ageMs = now.getTime() - trainedAt.getTime();

    const days = Math.floor(ageMs / (1000 * 60 * 60 * 24));
    const hours = Math.floor(
      (ageMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60),
    );
    const minutes = Math.floor((ageMs % (1000 * 60 * 60)) / (1000 * 60));

    return { days, hours, minutes };
  }

  /**
   * Get active model entity (raw)
   *
   * Utility method to get the active model without DTO transformation
   *
   * @returns {Promise<MLModel | null>} Active model entity or null
   */
  async getActiveModel(): Promise<MLModel | null> {
    return this.mlModelRepository.findOne({
      where: { isActive: true },
      order: { trainedAt: "DESC" },
    });
  }
}
