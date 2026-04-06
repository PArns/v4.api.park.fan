import { Injectable, Logger, HttpException, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In, MoreThan } from "typeorm";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  getCurrentDateInTimezone,
  formatInParkTimezone,
} from "../common/utils/date.util";
import { getTimezoneForCountry } from "../common/utils/timezone.util";
import { logMLServiceError } from "../common/utils/file-logger.util";
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
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { ScheduleType } from "../parks/entities/schedule-entry.entity";
import { QueueType } from "../external-apis/themeparks/themeparks.types";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { forwardRef } from "@nestjs/common";
import {
  FeatureContext,
  QueueDataInfo,
} from "../common/types/feature-context.type";

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
    @InjectRepository(ScheduleEntry)
    private scheduleEntryRepository: Repository<ScheduleEntry>,
    private configService: ConfigService,
    private predictionAccuracyService: PredictionAccuracyService,
    private weatherService: WeatherService,
    private analyticsService: AnalyticsService,
    @Inject(forwardRef(() => HolidaysService))
    private holidaysService: HolidaysService,
    @Inject(forwardRef(() => ParksService))
    private parksService: ParksService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    // ML service URL from environment or default
    this.ML_SERVICE_URL =
      process.env.ML_SERVICE_URL || "http://ml-service:8000";

    this.mlClient = axios.create({
      baseURL: this.ML_SERVICE_URL,
      timeout: 120000, // 120 seconds (ML predictions + DB queries can be slow)
      headers: {
        "Content-Type": "application/json",
        Connection: "keep-alive",
      },
      // Improve connection stability
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      // Retry configuration for transient network errors
      validateStatus: (status) => status < 500, // Don't throw on 4xx
    });

    // Add retry interceptor for transient connection errors (ECONNRESET, etc.)
    this.mlClient.interceptors.response.use(
      (response) => response,
      async (error) => {
        const config = error.config;

        // Check if this is a retryable error
        const isRetryable =
          error.code === "ECONNRESET" ||
          error.code === "ETIMEDOUT" ||
          error.code === "ECONNABORTED" ||
          error.message?.includes("socket hang up");

        // Initialize retry count
        if (!config._retryCount) {
          config._retryCount = 0;
        }

        // Retry up to 2 times for connection errors (total 3 attempts)
        if (isRetryable && config._retryCount < 2) {
          config._retryCount += 1;

          // Exponential backoff: 1s, 2s
          const delay = 1000 * config._retryCount;

          this.logger.warn(
            `ML service connection error (${error.code}), retry ${config._retryCount}/2 in ${delay}ms`,
          );

          await new Promise((resolve) => setTimeout(resolve, delay));

          return this.mlClient.request(config);
        }

        // If not retryable or max retries exceeded, throw the error
        return Promise.reject(error);
      },
    );

    if (process.env.ML_SERVICE_URL) {
      this.logger.log(`ML Service URL: ${this.ML_SERVICE_URL} `);
    } else {
      this.logger.warn(
        `ML Service URL not configured(using default: ${this.ML_SERVICE_URL}).Set ML_SERVICE_URL env variable to enable ML predictions.`,
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

      // Log to dedicated file
      logMLServiceError("getModelInfo", error, {
        mlServiceUrl: this.ML_SERVICE_URL,
      });

      throw new HttpException("ML service unavailable", 503);
    }
  }

  /**
   * Get predictions from ML service
   */
  async getPredictions(
    request: PredictionRequestDto,
  ): Promise<BulkPredictionResponseDto> {
    const startTime = Date.now();
    let modelVersion = "unknown";

    try {
      const response = await this.mlClient.post<BulkPredictionResponseDto>(
        "/predict",
        request,
      );
      const duration = Date.now() - startTime;
      modelVersion = response.data.modelVersion || "unknown";
      this.logger.debug(
        `Prediction request completed in ${duration}ms (${response.data.count} predictions, model ${modelVersion})`,
      );

      return response.data;
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.error("Prediction request failed:", errorMessage);

      // Log to dedicated file
      logMLServiceError("getPredictions", error, {
        mlServiceUrl: this.ML_SERVICE_URL,
        parkCount: request.parkIds?.length || 0,
        attractionCount: request.attractionIds?.length || 0,
        predictionType: request.predictionType,
        durationMs: duration,
      });

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
    liveStatus?: "OPERATING" | "CLOSED",
  ): Promise<BulkPredictionResponseDto> {
    // Get park timezone for cache key
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone", "countryCode", "regionCode"],
    });

    if (!park) {
      throw new Error(`Park not found: ${parkId}`);
    }

    // Try cache first (include date in park timezone to invalidate daily)
    const today = getCurrentDateInTimezone(park.timezone);
    const cacheKey = `ml:park:${parkId}:${predictionType}:${today}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const cachedData = JSON.parse(cached);

      // Apply maxDays filter to cached data as well
      if (maxDays && predictionType === "daily" && cachedData.predictions) {
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() + maxDays);

        cachedData.predictions = cachedData.predictions.filter(
          (p: PredictionDto) => {
            const predTime = new Date(p.predictedTime);
            return predTime <= cutoffDate;
          },
        );
        cachedData.count = cachedData.predictions.length;
      }

      return cachedData;
    }

    try {
      // 1. Fetch park with timezone (already fetched above for cache key)
      // Park timezone is already loaded

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

      // 2a. Filter: Only predict for attractions with data in last 90 days
      // This prevents generating predictions for attractions that are closed for season or invalid.
      // Cached 6h: active attraction set changes rarely (only on seasonal open/close).
      const activeCacheKey = `ml:active-attractions:${parkId}:90d`;
      let activeIdSet: Set<string>;
      const cachedActiveIds = await this.redis.get(activeCacheKey);
      if (cachedActiveIds) {
        activeIdSet = new Set(JSON.parse(cachedActiveIds) as string[]);
      } else {
        const activeAttractions = await this.queueDataRepository
          .createQueryBuilder("q")
          .select("DISTINCT q.attractionId", "id")
          .where("q.attractionId IN (:...ids)", { ids: attractionIds })
          .andWhere("q.timestamp > :cutoff", {
            cutoff: new Date(Date.now() - 90 * 24 * 60 * 60 * 1000),
          })
          .andWhere("q.queueType = :queueType", {
            queueType: QueueType.STANDBY,
          })
          .getRawMany();
        activeIdSet = new Set(activeAttractions.map((a) => a.id as string));
        await this.redis
          .set(
            activeCacheKey,
            JSON.stringify(Array.from(activeIdSet)),
            "EX",
            60 * 60, // 1h: responsive to seasonal open/close, still eliminates per-prediction DB scan
          )
          .catch(() => {});
      }
      const activeAttractionIds = attractionIds.filter((id) =>
        activeIdSet.has(id),
      );

      if (activeAttractionIds.length === 0) {
        this.logger.debug(
          `No active attractions(with data in last 90d) for park ${parkId}`,
        );
        return { predictions: [], count: 0, modelVersion: "none" };
      }

      const skippedCount = attractionIds.length - activeAttractionIds.length;
      if (skippedCount > 0) {
        // this.logger.debug(
        //   `Skipping ${skippedCount} inactive attractions for park ${parkId}`,
        // );
      }

      // 3. Fetch hourly weather forecast (cached by WeatherService)
      let weatherForecast: WeatherForecastItemDto[] = [];
      try {
        const rawForecast = await this.weatherService.getHourlyForecast(parkId);
        weatherForecast = await this.enrichForecastWithHolidays(
          rawForecast,
          park,
        );
      } catch (error) {
        this.logger.warn(`Failed to fetch weather for prediction: ${error} `);
      }

      const currentWaitTimes: Record<string, number> = {};
      const recentWaitTimes: Record<string, number> = {};

      if (predictionType === "hourly") {
        try {
          // 4.1 Get latest queue data (Current)
          const latestData = await this.queueDataRepository
            .createQueryBuilder("q")
            .distinctOn(["q.attractionId"])
            .where("q.attractionId = ANY(:ids)", { ids: activeAttractionIds })
            .andWhere("q.timestamp >= :recent", {
              recent: new Date(Date.now() - 3 * 60 * 60 * 1000), // Last 3 hours (tolerant to stale data)
            })
            .andWhere("q.queueType = :queueType", {
              queueType: QueueType.STANDBY,
            })
            .orderBy("q.attractionId")
            .addOrderBy("q.timestamp", "DESC")
            .getMany();

          for (const item of latestData) {
            if (item.waitTime !== null) {
              currentWaitTimes[item.attractionId] = item.waitTime;
            }
          }

          // 4.2 Get recent queue data (~30 mins ago) for Velocity calculation
          // Target: 30 minutes ago. Window: +/- 15 minutes.
          const thirtyAgo = Date.now() - 30 * 60 * 1000;
          const windowMin = new Date(thirtyAgo - 15 * 60 * 1000);
          const windowMax = new Date(thirtyAgo + 15 * 60 * 1000);

          const recentDataRaw = await this.queueDataRepository
            .createQueryBuilder("q")
            .select(["q.attractionId", "q.waitTime", "q.timestamp"])
            .where("q.attractionId = ANY(:ids)", { ids: activeAttractionIds })
            .andWhere("q.timestamp BETWEEN :min AND :max", {
              min: windowMin,
              max: windowMax,
            })
            .andWhere("q.queueType = :queueType", {
              queueType: QueueType.STANDBY,
            })
            .andWhere("q.waitTime IS NOT NULL")
            .getMany();

          // Process in memory to find the record closest to 30 mins ago for each attraction
          const bestMatchMap = new Map<
            string,
            { diff: number; wait: number }
          >();

          for (const item of recentDataRaw) {
            const diff = Math.abs(item.timestamp.getTime() - thirtyAgo);
            const currentBest = bestMatchMap.get(item.attractionId);

            if (!currentBest || diff < currentBest.diff) {
              bestMatchMap.set(item.attractionId, {
                diff,
                wait: item.waitTime,
              });
            }
          }

          for (const [id, match] of bestMatchMap.entries()) {
            recentWaitTimes[id] = match.wait;
          }
        } catch (error) {
          this.logger.warn(
            `Failed to fetch current/recent wait times for prediction: ${error} `,
          );
        }
      }

      // 4a. Build Phase 2 feature context
      const featureContext = await this.buildFeatureContext(
        parkId,
        activeAttractionIds,
        liveStatus,
      );

      // 4b. Fetch P50 baseline for crowd level calculation
      // This ensures TypeScript and Python ML service produce identical crowd levels
      let p50Baseline: number | undefined;
      try {
        p50Baseline =
          await this.analyticsService.getP50BaselineFromCache(parkId);
        if (p50Baseline === 0) {
          p50Baseline = undefined; // Let Python fallback to rolling_avg_7d
        }
      } catch (error) {
        this.logger.warn(
          `Failed to fetch P50 baseline for park ${parkId}: ${error}`,
        );
        p50Baseline = undefined; // Graceful degradation
      }

      // 5. Call ML Service via POST (Bulk Prediction)
      const payload: PredictionRequestDto = {
        attractionIds: activeAttractionIds,
        parkIds: activeAttractionIds.map(() => parkId), // Same length as activeAttractionIds
        predictionType,
        weatherForecast, // Empty array if failed or no coords
        currentWaitTimes,
        recentWaitTimes, // ~30 mins ago
        featureContext, // Phase 2: Real-time context features
        p50Baseline, // NEW: P50 baseline for crowd level alignment
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
        `ML service unavailable for park ${parkId}: ${errorMessage} `,
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
   * Build feature context for Phase 2 ML features
   */
  private async buildFeatureContext(
    parkId: string,
    attractionIds: string[],
    precomputedLiveStatus?: "OPERATING" | "CLOSED",
  ): Promise<FeatureContext> {
    try {
      // 1. Get park occupancy percentage
      let parkOccupancy: Record<string, number> = {};
      try {
        const occupancyPct =
          await this.analyticsService.getCurrentOccupancy(parkId);
        parkOccupancy[parkId] = occupancyPct;
      } catch (error) {
        this.logger.warn(`Failed to get park occupancy: ${error}`);
      }

      // 2. Get park opening time today (today = park timezone)
      let parkOpeningTimes: Record<string, string> = {};
      try {
        const park = await this.parkRepository.findOne({
          where: { id: parkId },
          select: ["id", "timezone"],
        });
        const todayStr = park?.timezone
          ? getCurrentDateInTimezone(park.timezone)
          : getCurrentDateInTimezone("UTC");

        const schedule = await this.scheduleEntryRepository.findOne({
          where: {
            parkId,
            date: todayStr as any,
            scheduleType: ScheduleType.OPERATING,
          },
        });

        if (schedule?.openingTime) {
          parkOpeningTimes[parkId] = schedule.openingTime.toISOString();
        }
      } catch (error) {
        this.logger.warn(`Failed to get park opening time: ${error}`);
      }

      // 3. Get downtime cache from Redis
      let downtimeCache: Record<string, number> = {};
      try {
        // Get park timezone for downtime cache keys
        const parkForDowntime = await this.parkRepository.findOne({
          where: { id: parkId },
          select: ["id", "timezone", "countryCode"],
        });

        const today = formatInParkTimezone(
          new Date(),
          parkForDowntime?.timezone ||
            (parkForDowntime?.countryCode
              ? getTimezoneForCountry(parkForDowntime.countryCode)
              : null) ||
            "UTC",
        );
        const keys = attractionIds.map((id) => `downtime:daily:${id}:${today}`);

        const downtimeValues = await this.redis.mget(...keys);

        for (let i = 0; i < attractionIds.length; i++) {
          if (downtimeValues[i]) {
            const minutes = parseInt(downtimeValues[i]!);
            if (minutes > 0) {
              downtimeCache[attractionIds[i]] = minutes;
            }
          }
        }
      } catch (error) {
        this.logger.warn(`Failed to get downtime cache: ${error}`);
      }

      // 4. Get queue data for virtual queue detection
      let queueData: Record<string, QueueDataInfo> = {};
      try {
        const latestQueueData = await this.queueDataRepository
          .createQueryBuilder("q")
          .distinctOn(["q.attractionId"])
          .where("q.attractionId = ANY(:ids)", { ids: attractionIds })
          .andWhere("q.timestamp >= :recent", {
            recent: new Date(Date.now() - 120 * 60 * 1000), // 2 hours window
          })
          .orderBy("q.attractionId")
          .addOrderBy("q.timestamp", "DESC")
          .getMany();

        for (const item of latestQueueData) {
          queueData[item.attractionId] = {
            queueType: item.queueType,
            status: item.status,
          };
        }
      } catch (error) {
        this.logger.warn(`Failed to get queue data: ${error}`);
      }

      // 5. Get Bridge Day Status
      let isBridgeDay: Record<string, boolean> = {};
      try {
        const park = await this.parkRepository.findOne({
          where: { id: parkId },
          select: ["id", "countryCode", "regionCode"],
        });

        if (park?.countryCode) {
          const now = new Date();
          const isBridge = await this.holidaysService.isBridgeDay(
            now,
            park.countryCode,
            park.regionCode,
            park.timezone,
          );
          isBridgeDay[parkId] = isBridge;
        }
      } catch (error) {
        this.logger.warn(`Failed to check bridge day status: ${error}`);
      }

      // 6. Check if park has schedule data
      // This helps ML understand data quality: Parks with schedules have more reliable patterns
      let parkHasSchedule: Record<string, boolean> = {};
      try {
        const scheduleExists = await this.scheduleEntryRepository.findOne({
          where: {
            parkId,
            scheduleType: ScheduleType.OPERATING,
          },
          select: ["id"],
        });

        parkHasSchedule[parkId] = !!scheduleExists;
      } catch (error) {
        this.logger.warn(`Failed to check schedule existence: ${error}`);
        parkHasSchedule[parkId] = false; // Safe default
      }

      // 7. Check if park has school holiday
      // This is a NEW feature for ML to improve predictions during school breaks
      // It considers both the park's local region AND influencing regions (e.g. neighboring states)
      let isSchoolHoliday: Record<string, boolean> = {};
      try {
        const park = await this.parkRepository.findOne({
          where: { id: parkId },
          select: [
            "id",
            "countryCode",
            "regionCode",
            "timezone",
            "influencingRegions",
          ],
        });

        if (park?.countryCode) {
          const now = new Date();
          const isSchool =
            await this.holidaysService.isSchoolHolidayInInfluenceZone(
              now,
              park.countryCode,
              park.regionCode,
              park.timezone,
              park.influencingRegions || [],
            );
          isSchoolHoliday[parkId] = isSchool;
        }
      } catch (error) {
        this.logger.warn(`Failed to check school holiday status: ${error}`);
      }

      // 8. Get current live park status (used by ML to fix is_park_open for UNKNOWN parks).
      // If pre-computed by the caller (prediction generator batch), use that directly.
      // Otherwise fetch it now (e.g. for on-demand single-park prediction calls).
      let parkLiveStatus: Record<string, string> = {};
      try {
        if (precomputedLiveStatus !== undefined) {
          parkLiveStatus[parkId] = precomputedLiveStatus;
        } else {
          const liveStatusMap = await this.parksService.getBatchParkStatus([
            parkId,
          ]);
          parkLiveStatus[parkId] = liveStatusMap.get(parkId) ?? "UNKNOWN";
        }
      } catch (error) {
        this.logger.warn(`Failed to get live park status: ${error}`);
      }

      return {
        parkOccupancy,
        parkOpeningTimes,
        downtimeCache,
        queueData,
        isBridgeDay,
        parkHasSchedule,
        isSchoolHoliday,
        parkLiveStatus,
      };
    } catch (error) {
      this.logger.warn(`Failed to build feature context: ${error}`);
      return {}; // Return empty context on error
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
    // Get park timezone for cache key
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) {
      throw new Error(`Park not found: ${parkId}`);
    }

    // Separate cache key for yearly predictions (timezone-aware)
    const today = getCurrentDateInTimezone(park.timezone);
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
        `ML service unavailable for park ${parkId} yearly predictions: ${errorMessage} `,
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

    // 2–3.5: Fetch weather forecast, current wait time, and recent wait time in parallel
    const thirtyAgo = new Date(Date.now() - 30 * 60 * 1000);
    const windowMin = new Date(thirtyAgo.getTime() - 15 * 60 * 1000);
    const windowMax = new Date(thirtyAgo.getTime() + 15 * 60 * 1000);

    const [weatherForecastRaw, latestData, recentData] = await Promise.all([
      attraction.parkId
        ? this.weatherService
            .getHourlyForecast(attraction.parkId)
            .catch((err) => {
              this.logger.warn(
                `Failed to fetch weather for prediction: ${err}`,
              );
              return [] as WeatherForecastItemDto[];
            })
        : Promise.resolve([] as WeatherForecastItemDto[]),
      predictionType === "hourly"
        ? this.queueDataRepository
            .findOne({
              where: {
                attractionId,
                queueType: QueueType.STANDBY,
                timestamp: MoreThan(
                  new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
                ),
              },
              order: { timestamp: "DESC" },
            })
            .catch((err) => {
              this.logger.warn(`Failed to fetch current wait time: ${err}`);
              return null;
            })
        : Promise.resolve(null),
      predictionType === "hourly"
        ? this.queueDataRepository
            .createQueryBuilder("q")
            .where("q.attractionId = :attractionId", { attractionId })
            .andWhere("q.timestamp >= :windowMin", { windowMin })
            .andWhere("q.timestamp <= :windowMax", { windowMax })
            .andWhere("q.queueType = :queueType", {
              queueType: QueueType.STANDBY,
            })
            .orderBy("ABS(EXTRACT(EPOCH FROM (q.timestamp - :target)))", "ASC")
            .setParameter("target", thirtyAgo)
            .getOne()
            .catch((err) => {
              this.logger.warn(`Failed to fetch recent wait time: ${err}`);
              return null;
            })
        : Promise.resolve(null),
    ]);

    const weatherForecast: WeatherForecastItemDto[] = weatherForecastRaw;
    const currentWaitTimes: Record<string, number> = {};
    if (latestData && latestData.waitTime !== null) {
      currentWaitTimes[attractionId] = latestData.waitTime;
    }
    const recentWaitTimes: Record<string, number> = {};
    if (recentData && recentData.waitTime !== null) {
      recentWaitTimes[attractionId] = recentData.waitTime;
    }

    // 4. Request predictions
    const request: PredictionRequestDto = {
      attractionIds: [attractionId],
      parkIds: [attraction.parkId], // property actually accessed from loaded relation or column
      predictionType,
      weatherForecast,
      currentWaitTimes,
      recentWaitTimes,
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

    // Get park statuses in batch using CONSOLIDATED function
    // This ensures we don't store predictions for parks that are closed
    const parkIds = Array.from(predictionsByPark.keys());

    // Use consolidated status calculation (hybrid: schedule + ride fallback)
    const parkStatusMap = await this.parksService.getBatchParkStatus(parkIds);

    // Filter predictions: Only keep predictions for OPERATING parks
    const validPredictions: PredictionDto[] = [];
    let filteredCount = 0;

    for (const [parkId, parkPredictions] of predictionsByPark) {
      const status = parkStatusMap.get(parkId);

      // Only store predictions for parks that are currently OPERATING
      if (status === "OPERATING") {
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
    // OPTIMIZATION: Sample-based storage to reduce DB load (90% reduction)
    // Statistical sampling is sufficient for MAE calculation
    // ACCURACY_SAMPLE_RATE: 0.1 = 10% (1000s of samples daily = valid stats)
    const ACCURACY_SAMPLE_RATE = parseFloat(
      process.env.ACCURACY_SAMPLE_RATE || "0.1", // Default: 10% sampling
    );

    // ONLY record predictions for OPERATING status (park was open)
    // This prevents recording predictions for scheduled closed periods
    // Unplanned closures will still be detected in compareWithActuals()
    const validPredictionsForFeedback = savedPredictions.filter(
      (pred) => pred.status === "OPERATING" || pred.status === null,
    );

    let recordedCount = 0;
    let sampledCount = 0;

    if (validPredictionsForFeedback.length < savedPredictions.length) {
      this.logger.debug(
        `Filtering: Recording ${validPredictionsForFeedback.length}/${savedPredictions.length} predictions (excluding scheduled closures)`,
      );
    }

    for (let i = 0; i < validPredictionsForFeedback.length; i++) {
      // Apply sampling: Only record X% of predictions
      if (Math.random() >= ACCURACY_SAMPLE_RATE) {
        sampledCount++;
        continue; // Skip this prediction (not in sample)
      }

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
      `✅ Recorded ${recordedCount}/${validPredictionsForFeedback.length} predictions for accuracy tracking ` +
        `(${sampledCount} filtered by ${(ACCURACY_SAMPLE_RATE * 100).toFixed(0)}% sampling)`,
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

    // Require predictions to have been created recently.
    // Hourly predictions are regenerated every hour — allow 2h window to survive a delayed cron run.
    // Daily predictions are regenerated once per day — allow 26h window.
    const createdAtCutoff =
      predictionType === "hourly"
        ? new Date(Date.now() - 2 * 60 * 60 * 1000)
        : new Date(Date.now() - 26 * 60 * 60 * 1000);
    queryBuilder.andWhere("p.createdAt >= :createdAtCutoff", {
      createdAtCutoff,
    });

    return queryBuilder.getMany();
  }

  /**
   * Get predictions (try DB first, fall back to ML service)
   */
  async getAttractionPredictionsWithFallback(
    attractionId: string,
    predictionType: "hourly" | "daily" = "hourly",
  ): Promise<PredictionDto[]> {
    // Try stored predictions first for both hourly and daily.
    // Pass startTime=now for hourly so we never serve already-elapsed time slots.
    const stored = await this.getStoredPredictions(
      attractionId,
      predictionType,
      predictionType === "hourly" ? new Date() : undefined,
    );

    if (stored.length > 0) {
      this.logger.debug(
        `Using ${stored.length} stored ${predictionType} predictions for ${attractionId}`,
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

    // Fall back to ML service (new ride or predictions expired)
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
      // Delete all future hourly predictions (not just next 24h) to prevent
      // stale entries from prior runs accumulating alongside new ones
      endTime.setHours(endTime.getHours() + 48);
    } else {
      // Delete predictions for next 60 days to cover extended daily forecasts
      endTime.setDate(endTime.getDate() + 60);
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

  /**
   * Enrich weather forecast items with holiday information for ML
   * @private
   */
  private async enrichForecastWithHolidays(
    forecast: WeatherForecastItemDto[],
    park: Park,
  ): Promise<WeatherForecastItemDto[]> {
    if (forecast.length === 0 || !park.countryCode) {
      return forecast;
    }

    // Process forecast items sequentially — 3 holiday checks per day are
    // kept concurrent (they're independent), but days are processed one at a
    // time to avoid 16 × 3 = 48 concurrent promises per park during batch runs.
    const enriched: WeatherForecastItemDto[] = [];
    for (const item of forecast) {
      const date = new Date(item.time);
      const [isHoliday, isSchoolHoliday, isBridgeDay] = await Promise.all([
        this.holidaysService.isHoliday(
          date,
          park.countryCode,
          park.regionCode || undefined,
          park.timezone,
        ),
        this.holidaysService.isEffectiveSchoolHoliday(
          date,
          park.countryCode,
          park.regionCode || undefined,
          park.timezone,
        ),
        this.holidaysService.isBridgeDay(
          date,
          park.countryCode,
          park.regionCode || undefined,
          park.timezone,
        ),
      ]);
      enriched.push({ ...item, isHoliday, isSchoolHoliday, isBridgeDay });
    }
    return enriched;
  }
}
