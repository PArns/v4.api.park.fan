import { Injectable, Logger, HttpException, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In, MoreThan } from "typeorm";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  PredictionRequestDto,
  ModelInfoDto,
  WeatherForecastItemDto,
  BulkPredictionResponseDto,
  PredictionDto,
} from "./dto";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { OpenMeteoClient } from "../external-apis/weather/open-meteo.client"; // Kept if needed for other methods, but we're removing usage
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { WeatherService } from "../parks/weather.service";

@Injectable()
export class MLService {
  private readonly logger = new Logger(MLService.name);
  private readonly mlClient: AxiosInstance;
  private readonly ML_SERVICE_URL: string;

  // Cache TTLs based on prediction generation frequency
  private readonly TTL_HOURLY_PREDICTIONS = 60 * 60; // 1 hour - aligned with hourly generation
  private readonly TTL_DAILY_PREDICTIONS = 6 * 60 * 60; // 6 hours - more stable, less volatile

  constructor(
    @InjectRepository(WaitTimePrediction)
    private predictionRepository: Repository<WaitTimePrediction>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    private configService: ConfigService,
    private predictionAccuracyService: PredictionAccuracyService,
    private weatherService: WeatherService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    // ML service URL from environment or default
    this.ML_SERVICE_URL =
      process.env.ML_SERVICE_URL || "http://ml-service:8000";

    this.mlClient = axios.create({
      baseURL: this.ML_SERVICE_URL,
      timeout: 30000, // 30 seconds
      headers: {
        "Content-Type": "application/json",
      },
    });

    if (process.env.ML_SERVICE_URL) {
      this.logger.log(`ML Service URL: ${this.ML_SERVICE_URL}`);
    } else {
      this.logger.warn(
        `ML Service URL not configured (using default: ${this.ML_SERVICE_URL}). Set ML_SERVICE_URL env variable to enable ML predictions.`,
      );
    }
  }

  /**
   * Check if ML service is healthy
   */
  async isHealthy(): Promise<boolean> {
    try {
      const response = await this.mlClient.get("/health");
      return response.status === 200;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.warn("ML service health check failed:", errorMessage);
      return false;
    }
  }

  /**
   * Get ML model information
   */
  async getModelInfo(): Promise<ModelInfoDto> {
    try {
      const response = await this.mlClient.get<ModelInfoDto>("/model/info");
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error("Failed to get model info:", errorMessage);
      throw new HttpException("ML service unavailable", 503);
    }
  }

  /**
   * Get predictions from ML service
   */
  async getPredictions(
    request: PredictionRequestDto,
  ): Promise<BulkPredictionResponseDto> {
    try {
      const response = await this.mlClient.post<BulkPredictionResponseDto>(
        "/predict",
        request,
      );
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error("Prediction request failed:", errorMessage);
      throw new HttpException("Failed to get predictions from ML service", 503);
    }
  }

  /**
   * Get predictions for a park from ML service
   * Optimized to use hourly weather forecast and efficient bulk prediction
   *
   * Cached in Redis:
   * - Hourly: 1 hour TTL (aligned with generation frequency)
   * - Daily: 6 hours TTL (more stable data)
   *
   * @param parkId - Park ID
   * @param predictionType - Type of prediction (hourly or daily)
   * @param maxDays - Optional limit on number of days (only applies to daily predictions)
   */
  async getParkPredictions(
    parkId: string,
    predictionType: "hourly" | "daily" = "hourly",
    maxDays?: number,
  ): Promise<BulkPredictionResponseDto> {
    // Try cache first (include date to invalidate daily)
    const today = new Date().toISOString().split("T")[0];
    const cacheKey = `ml:park:${parkId}:${predictionType}:${today}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const cachedData = JSON.parse(cached);

      // Apply maxDays filter to cached data as well
      if (maxDays && predictionType === "daily" && cachedData.predictions) {
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() + maxDays);

        cachedData.predictions = cachedData.predictions.filter((p: any) => {
          const predTime = new Date(p.predictedTime);
          return predTime <= cutoffDate;
        });
        cachedData.count = cachedData.predictions.length;
      }

      return cachedData;
    }

    try {
      // 1. Fetch park to get coordinates
      const park = await this.parkRepository.findOne({ where: { id: parkId } });
      if (!park) {
        throw new Error(`Park not found: ${parkId}`);
      }

      // 2. Fetch all attractions for this park
      const attractions = await this.attractionRepository.find({
        where: { parkId },
        select: ["id"],
      });

      if (attractions.length === 0) {
        this.logger.warn(`No attractions found for park ${parkId}`);
        return { predictions: [], count: 0, modelVersion: "none" };
      }

      const attractionIds = attractions.map((a) => a.id);

      // 3. Fetch hourly weather forecast (cached by WeatherService)
      let weatherForecast: WeatherForecastItemDto[] = [];
      try {
        weatherForecast = await this.weatherService.getHourlyForecast(parkId);
      } catch (error) {
        this.logger.warn(`Failed to fetch weather for prediction: ${error}`);
      }

      // 4. Fetch current wait times for attractions (for input optimization)
      const currentWaitTimes: Record<string, number> = {};
      if (predictionType === "hourly") {
        try {
          // Get latest queue data for all attractions in park
          const latestData = await this.queueDataRepository
            .createQueryBuilder("q")
            .distinctOn(["q.attractionId"])
            .where("q.attractionId = ANY(:ids)", { ids: attractionIds })
            .andWhere("q.timestamp >= :recent", {
              recent: new Date(Date.now() - 60 * 60 * 1000), // Last hour
            })
            .orderBy("q.attractionId")
            .addOrderBy("q.timestamp", "DESC")
            .getMany();

          for (const item of latestData) {
            if (item.waitTime !== null) {
              currentWaitTimes[item.attractionId] = item.waitTime;
            }
          }
        } catch (error) {
          this.logger.warn(
            `Failed to fetch current wait times for prediction: ${error}`,
          );
        }
      }

      // 5. Call ML Service via POST (Bulk Prediction)
      const payload: PredictionRequestDto = {
        attractionIds,
        parkIds: attractionIds.map(() => parkId), // Same length as attractionIds
        predictionType,
        weatherForecast, // Empty array if failed or no coords
        currentWaitTimes,
      };

      const response = await this.mlClient.post<BulkPredictionResponseDto>(
        "/predict",
        payload,
      );

      // Cache the response with appropriate TTL
      const ttl =
        predictionType === "hourly"
          ? this.TTL_HOURLY_PREDICTIONS
          : this.TTL_DAILY_PREDICTIONS;

      await this.redis.set(cacheKey, JSON.stringify(response.data), "EX", ttl);

      // Apply maxDays filter if specified (for daily predictions only)
      if (maxDays && predictionType === "daily" && response.data.predictions) {
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() + maxDays);

        response.data.predictions = response.data.predictions.filter((p) => {
          const predTime = new Date(p.predictedTime);
          return predTime <= cutoffDate;
        });
        response.data.count = response.data.predictions.length;
      }

      return response.data;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);

      // Use WARN instead of ERROR since this is expected when ML service is not running
      this.logger.warn(
        `ML service unavailable for park ${parkId}: ${errorMessage}`,
      );

      // Return empty response on error to handle gracefully in controller
      return {
        predictions: [],
        count: 0,
        modelVersion: "unavailable",
      };
    }
  }

  /**
   * Get yearly predictions for a park (full 365 days)
   * Used for the yearly predictions route
   *
   * Cached separately from regular daily predictions
   * TTL: 24 hours (same as daily predictions)
   */
  async getParkPredictionsYearly(
    parkId: string,
  ): Promise<BulkPredictionResponseDto> {
    // Separate cache key for yearly predictions
    const today = new Date().toISOString().split("T")[0];
    const cacheKey = `ml:park:${parkId}:yearly:${today}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      return JSON.parse(cached);
    }

    try {
      // Fetch daily predictions (full dataset, no limit)
      const response = await this.getParkPredictions(parkId, "daily");

      // Cache for 24 hours
      await this.redis.set(
        cacheKey,
        JSON.stringify(response),
        "EX",
        this.TTL_DAILY_PREDICTIONS,
      );

      return response;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);

      this.logger.warn(
        `ML service unavailable for park ${parkId} yearly predictions: ${errorMessage}`,
      );

      return {
        predictions: [],
        count: 0,
        modelVersion: "unavailable",
      };
    }
  }

  /**
   * Get predictions for a single attraction
   */
  async getAttractionPredictions(
    attractionId: string,
    predictionType: "hourly" | "daily" = "hourly",
  ): Promise<PredictionDto[]> {
    // 1. Fetch park to get coordinates (for weather)
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
      relations: ["park"], // Fetch park relationship
      select: ["id", "parkId"],
    });

    if (!attraction) {
      throw new HttpException("Attraction not found", 404);
    }

    // 2. Fetch hourly weather forecast (if we have coordinates)
    // 2. Fetch hourly weather forecast (cached by WeatherService)
    let weatherForecast: WeatherForecastItemDto[] = [];
    if (attraction.parkId) {
      try {
        weatherForecast = await this.weatherService.getHourlyForecast(
          attraction.parkId,
        );
      } catch (error) {
        this.logger.warn(`Failed to fetch weather for prediction: ${error}`);
      }
    }

    // 3. Fetch current wait times (for input optimization)
    const currentWaitTimes: Record<string, number> = {};
    if (predictionType === "hourly") {
      try {
        const latestData = await this.queueDataRepository.findOne({
          where: { attractionId },
          order: { timestamp: "DESC" },
        });

        if (latestData && latestData.waitTime !== null) {
          currentWaitTimes[attractionId] = latestData.waitTime;
        }
      } catch (error) {
        this.logger.warn(`Failed to fetch current wait time: ${error}`);
      }
    }

    // 4. Request predictions
    const request: PredictionRequestDto = {
      attractionIds: [attractionId],
      parkIds: [attraction.parkId], // property actually accessed from loaded relation or column
      predictionType,
      weatherForecast,
      currentWaitTimes,
    };

    const response = await this.getPredictions(request);
    return response.predictions;
  }

  /**
   * Store predictions in database
   * Used by queue jobs for pre-computing daily predictions
   *
   * Also records predictions in PredictionAccuracy table for feedback loop
   *
   * OPTIMIZATION: Filters out predictions for times when park is closed
   * to prevent storing predictions that will never have matching queue_data
   */
  async storePredictions(predictions: PredictionDto[]): Promise<void> {
    if (predictions.length === 0) {
      this.logger.warn("No predictions to store");
      return;
    }

    // Get unique attraction IDs to look up their parkIds
    const attractionIds = [...new Set(predictions.map((p) => p.attractionId))];

    // Fetch attractions with their parkIds
    const attractions = await this.attractionRepository.find({
      where: { id: In(attractionIds) },
      select: ["id", "parkId"],
    });

    // Create map: attractionId -> parkId
    const attractionToPark = new Map<string, string>();
    for (const attraction of attractions) {
      attractionToPark.set(attraction.id, attraction.parkId);
    }

    // Group predictions by parkId
    const predictionsByPark = new Map<string, PredictionDto[]>();
    for (const pred of predictions) {
      const parkId = attractionToPark.get(pred.attractionId);
      if (!parkId) {
        this.logger.warn(
          `Could not find park for attraction ${pred.attractionId}, skipping`,
        );
        continue;
      }

      if (!predictionsByPark.has(parkId)) {
        predictionsByPark.set(parkId, []);
      }
      predictionsByPark.get(parkId)!.push(pred);
    }

    // Get park statuses in batch
    const parkIds = Array.from(predictionsByPark.keys());
    const parks = await this.parkRepository.find({
      where: { id: In(parkIds) },
      select: ["id"],
    });

    // Get current live status for each park
    const validPredictions: PredictionDto[] = [];
    let filteredCount = 0;

    for (const [parkId, parkPredictions] of predictionsByPark) {
      const park = parks.find((p) => p.id === parkId);
      if (!park) {
        filteredCount += parkPredictions.length;
        continue;
      }

      // Check if park is currently operating by looking at recent queue data
      // If park has recent queue data, it's operating
      const recentQueueData = await this.queueDataRepository.findOne({
        where: {
          attractionId: In(
            parkPredictions.map((p) => p.attractionId).slice(0, 10),
          ), // Check first 10 attractions
          timestamp: MoreThan(new Date(Date.now() - 30 * 60 * 1000)), // Last 30 min
        },
        order: { timestamp: "DESC" },
      });

      if (recentQueueData) {
        validPredictions.push(...parkPredictions);
      } else {
        filteredCount += parkPredictions.length;
      }
    }

    if (filteredCount > 0) {
      this.logger.debug(
        `🕒 Filtered ${filteredCount}/${predictions.length} predictions (parks closed/not operating)`,
      );
    }

    if (validPredictions.length === 0) {
      this.logger.verbose("No valid predictions to store (all parks closed)");
      return;
    }

    // Convert to entities
    const entities = validPredictions.map((pred) => {
      const entity = new WaitTimePrediction();
      entity.attractionId = pred.attractionId;
      entity.predictedTime = new Date(pred.predictedTime);
      entity.predictedWaitTime = pred.predictedWaitTime;
      entity.predictionType = pred.predictionType;
      entity.confidence = pred.confidence;
      entity.crowdLevel = pred.crowdLevel;
      entity.baseline = pred.baseline;
      entity.modelVersion = pred.modelVersion;
      entity.status = pred.status || null;
      return entity;
    });

    const savedPredictions = await this.predictionRepository.save(entities);
    this.logger.debug(
      `Stored ${savedPredictions.length} predictions in database`,
    );

    // Record predictions for accuracy tracking (feedback loop)
    // ONLY record predictions for OPERATING status (park was open)
    // This prevents recording predictions for scheduled closed periods
    // Unplanned closures will still be detected in compareWithActuals()
    const validPredictionsForFeedback = savedPredictions.filter(
      (pred) => pred.status === "OPERATING" || pred.status === null,
    );

    let recordedCount = 0;

    if (validPredictionsForFeedback.length < savedPredictions.length) {
      this.logger.debug(
        `Filtering: Recording ${validPredictionsForFeedback.length}/${savedPredictions.length} predictions (excluding scheduled closures)`,
      );
    }

    for (let i = 0; i < validPredictionsForFeedback.length; i++) {
      try {
        await this.predictionAccuracyService.recordPrediction(
          validPredictionsForFeedback[i],
        );
        recordedCount++;
      } catch (error) {
        // Log error but don't fail the whole operation
        const errorMessage =
          error instanceof Error ? error.message : "Unknown error";
        this.logger.warn(
          `Failed to record prediction for accuracy tracking: ${errorMessage}`,
        );
      }
    }

    this.logger.verbose(
      `✅ Recorded ${recordedCount}/${validPredictionsForFeedback.length} OPERATING predictions for accuracy tracking`,
    );
  }

  /**
   * Get stored predictions from database (for faster access)
   */
  async getStoredPredictions(
    attractionId: string,
    predictionType: "hourly" | "daily",
    startTime?: Date,
    endTime?: Date,
  ): Promise<WaitTimePrediction[]> {
    const queryBuilder = this.predictionRepository
      .createQueryBuilder("p")
      .where("p.attractionId = :attractionId", { attractionId })
      .andWhere("p.predictionType = :predictionType", { predictionType })
      .orderBy("p.predictedTime", "ASC");

    if (startTime) {
      queryBuilder.andWhere("p.predictedTime >= :startTime", { startTime });
    }

    if (endTime) {
      queryBuilder.andWhere("p.predictedTime <= :endTime", { endTime });
    }

    // Only get recent predictions (created in last hour)
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    queryBuilder.andWhere("p.createdAt >= :oneHourAgo", { oneHourAgo });

    return queryBuilder.getMany();
  }

  /**
   * Get predictions (try DB first, fall back to ML service)
   */
  async getAttractionPredictionsWithFallback(
    attractionId: string,
    predictionType: "hourly" | "daily" = "hourly",
  ): Promise<PredictionDto[]> {
    // Try to get from database first (for daily predictions)
    if (predictionType === "daily") {
      const stored = await this.getStoredPredictions(
        attractionId,
        predictionType,
      );

      if (stored.length > 0) {
        this.logger.debug(
          `Using ${stored.length} stored predictions for ${attractionId}`,
        );
        return stored.map((p) => ({
          attractionId: p.attractionId,
          predictedTime: p.predictedTime.toISOString(),
          predictedWaitTime: p.predictedWaitTime,
          predictionType: p.predictionType,
          confidence: p.confidence,
          crowdLevel: p.crowdLevel,
          baseline: p.baseline,
          modelVersion: p.modelVersion,
          status: p.status || undefined,
        }));
      }
    }

    // Fall back to ML service
    return this.getAttractionPredictions(attractionId, predictionType);
  }

  /**
   * Delete old predictions to manage database size
   *
   * @param predictionType - Type of prediction to delete
   * @param cutoffDate - Delete predictions older than this date
   * @returns Number of deleted records
   */
  async deleteOldPredictions(
    predictionType: "hourly" | "daily",
    cutoffDate: Date,
  ): Promise<number> {
    const result = await this.predictionRepository
      .createQueryBuilder()
      .delete()
      .where("predictionType = :predictionType", { predictionType })
      .andWhere("predictedTime < :cutoffDate", { cutoffDate })
      .execute();

    return result.affected || 0;
  }

  /**
   * Deduplicate predictions for a park
   * Deletes existing predictions for the same time range before new generation
   *
   * @param parkId - Park ID to deduplicate for
   * @param predictionType - Type of prediction
   */
  async deduplicatePredictions(
    parkId: string,
    predictionType: "hourly" | "daily",
  ): Promise<number> {
    const now = new Date();
    const startTime = new Date(now);
    const endTime = new Date(now);

    if (predictionType === "hourly") {
      // Delete predictions for next 24 hours
      endTime.setHours(endTime.getHours() + 24);
    } else {
      // Delete predictions for next 30 days
      endTime.setDate(endTime.getDate() + 30);
    }

    // Get all attraction IDs for this park
    const attractions = await this.attractionRepository.find({
      where: { parkId },
      select: ["id"],
    });

    const attractionIds = attractions.map((a) => a.id);

    if (attractionIds.length === 0) {
      return 0;
    }

    const result = await this.predictionRepository
      .createQueryBuilder()
      .delete()
      .where("attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("predictionType = :predictionType", { predictionType })
      .andWhere("predictedTime >= :startTime", { startTime })
      .andWhere("predictedTime <= :endTime", { endTime })
      .execute();

    return result.affected || 0;
  }
}
