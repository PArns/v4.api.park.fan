import { Injectable, Logger, HttpException, Inject } from "@nestjs/common";
import { CacheKeys } from "../common/cache/cache-keys";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { getMlServiceUrl } from "../config/ml-services.config";
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
import { currentSlotStartMs } from "../common/utils/best-visit-times.util";
import { determineCrowdLevel } from "../common/utils/crowd-level.util";
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
  private readonly TTL_HOURLY_PREDICTIONS = 30 * 60; // 30 minutes - matches createdAtCutoff window
  // 13h: daily (future-day) forecasts change at most ~2×/day (CatBoost retrains nightly,
  // weather syncs every 12h). A background warmup force-refreshes this every 12h so users
  // never pay the ~15s cold CatBoost daily-inference cost; the 13h TTL (>12h interval) is a
  // safety net with 1h overlap so the key never expires between two background refreshes,
  // while still self-healing a degraded/empty ML response within 13h instead of 25h.
  private readonly TTL_DAILY_PREDICTIONS = 13 * 60 * 60; // 13 hours

  // Collapses concurrent cold rebuilds of the serving daily forecast (the
  // ~15s CatBoost path) so a TTL lapse / warmup eviction triggers one compute,
  // not one per concurrent request. Shared by the calendar and yearly views.
  private readonly servingFlight = new SingleFlight();

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
    this.ML_SERVICE_URL = getMlServiceUrl();

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
    const cacheKey = CacheKeys.mlParkPredictions(parkId, predictionType, today);
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
        this.logger.debug(`No attractions found for park ${parkId}`);
        return { predictions: [], count: 0, modelVersion: "none" };
      }

      const attractionIds = attractions.map((a) => a.id);

      // 2a. Filter: Only predict for attractions with OPERATING STANDBY data in last 90 days.
      // Requiring status=OPERATING excludes amenities (restrooms, lockers) and seasonally-closed
      // attractions that only have CLOSED/null status records — they would generate predictions
      // that always become wasUnplannedClosure=true, killing coverage.
      const activeCacheKey = `ml:active-attractions:${parkId}:90d:op`;
      let activeIdSet: Set<string>;
      const cachedActiveIds = safeJsonParse<string[]>(
        await this.redis.get(activeCacheKey),
      );
      if (cachedActiveIds) {
        activeIdSet = new Set(cachedActiveIds);
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
          .andWhere("q.status = :status", { status: "OPERATING" })
          .getRawMany();
        activeIdSet = new Set(activeAttractions.map((a) => a.id as string));
        await this.redis
          .set(
            activeCacheKey,
            JSON.stringify(Array.from(activeIdSet)),
            "EX",
            60 * 60, // 1h: responsive to seasonal open/close, still eliminates per-prediction DB scan
          )
          .catch((e) =>
            this.logger.debug(
              `Redis active-attractions cache set failed: ${e?.message ?? e}`,
            ),
          );
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

      // 4b. Fetch the typical-day-peak baseline (primary crowd-level
      // reference, so the ML service's crowd levels match the calendar:
      // 100% = a typical day = moderate) and the P50 baseline (kept for the
      // park-occupancy feature and as the crowd-level fallback). The old
      // p90Baseline was never read by the Python service, so it's dropped.
      let p50Baseline: number | undefined;
      let typicalDayPeakBaseline: number | undefined;
      try {
        const [p50, typicalPeak] = await Promise.all([
          this.analyticsService.getP50BaselineFromCache(parkId),
          this.analyticsService.getTypicalDayPeakFromCache(parkId),
        ]);
        p50Baseline = p50 || undefined;
        typicalDayPeakBaseline = typicalPeak || undefined;
      } catch (error) {
        this.logger.warn(
          `Failed to fetch baselines for park ${parkId}: ${error}`,
        );
      }

      // 5. Call ML Service via POST (Bulk Prediction)
      const payload: PredictionRequestDto = {
        attractionIds: activeAttractionIds,
        parkIds: activeAttractionIds.map(() => parkId),
        predictionType,
        weatherForecast,
        currentWaitTimes,
        recentWaitTimes,
        featureContext,
        p50Baseline,
        typicalDayPeakBaseline,
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

        // Compare the date column against the park-local YYYY-MM-DD string
        // explicitly (the entity types `date` as Date, but the column is a
        // Postgres `date` — string comparison avoids TZ-dependent shifts).
        const schedule = await this.scheduleEntryRepository
          .createQueryBuilder("schedule")
          .where("schedule.parkId = :parkId", { parkId })
          .andWhere("schedule.date = :date", { date: todayStr })
          .andWhere("schedule.scheduleType = :type", {
            type: ScheduleType.OPERATING,
          })
          .getOne();

        if (schedule?.openingTime) {
          parkOpeningTimes[parkId] = schedule.openingTime.toISOString();
        } else {
          // No official schedule: try to derive opening time from today's activity
          const derived = await this.parksService.getDerivedHistoricalHours(
            parkId,
            todayStr,
            todayStr,
            park?.timezone || "UTC",
          );
          const todayDerived = derived.get(todayStr);
          if (todayDerived) {
            parkOpeningTimes[parkId] = todayDerived.openingTime;
          }
          // If no activity yet today, parkOpeningTimes[parkId] remains undefined
          // which correctly signals the model that the park hasn't "really" opened yet.
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
    const cacheKey = CacheKeys.mlParkPredictions(parkId, "yearly", today);
    const cached = safeJsonParse<BulkPredictionResponseDto>(
      await this.redis.get(cacheKey),
    );

    if (cached) {
      return cached;
    }

    try {
      // Serving path: TFT near-term (≤60d headliners) merged over CatBoost long tail,
      // same source as the calendar so the two views' crowd levels agree.
      const response = await this.getServingDailyPredictions(parkId);

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
   * Near-term daily forecast from the TFT (nf-service), HEADLINERS ONLY — the
   * scope the TFT backtest validated (~2x better than CatBoost on busy days).
   * Reads the freshest forward forecast per (attraction, day) from tft_forecasts,
   * for the next `days` park-local days, with a 3-day staleness guard so a stalled
   * nf-service falls back to CatBoost instead of serving old forecasts. Returned as
   * PredictionDto so it drops straight into the calendar/yearly crowd-level path
   * (which recompute crowdLevel from predictedWaitTime; the placeholder fields here
   * are not read). Cached per park-day (TFT only changes on the nightly run).
   */
  async getTftDailyPredictions(
    parkId: string,
    days = 60,
  ): Promise<PredictionDto[]> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });
    if (!park) return [];
    const today = getCurrentDateInTimezone(park.timezone);

    const cacheKey = `ml:tft-daily:${parkId}:${days}:${today}`;
    const cached = await this.redis.get(cacheKey);
    const cachedPredictions = safeJsonParse<PredictionDto[]>(cached);
    if (cachedPredictions) return cachedPredictions;

    // Freshest forward forecast per (attraction, target_date); headliners of THIS
    // park; within the horizon; not stale. ::text casts avoid uuid=text mismatches.
    const rows: Array<{
      attractionId: string;
      targetDate: string;
      peak: string;
    }> = await this.attractionRepository.query(
      `
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id::text   AS "attractionId",
          f.target_date::text      AS "targetDate",
          f.predicted_peak::float  AS peak
        FROM tft_forecasts f
        JOIN headliner_attractions h
          ON h."attractionId" = f.attraction_id AND h."parkId"::text = $1
        WHERE f.target_date >= $2::date
          AND f.target_date <  ($2::date + $3::int)
          AND f.forecast_date >= ($2::date - 3)
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
        `,
      [parkId, today, days],
    );

    const preds: PredictionDto[] = rows.map((r) => ({
      attractionId: r.attractionId,
      predictedTime: `${r.targetDate}T12:00:00`,
      predictedWaitTime: Math.max(0, Math.round(Number(r.peak))),
      predictionType: "daily",
      confidence: 0.7,
      crowdLevel: "moderate", // placeholder — consumers recompute from predictedWaitTime
      baseline: 0,
      modelVersion: "tft",
    }));

    await this.redis
      .set(cacheKey, JSON.stringify(preds), "EX", this.TTL_DAILY_PREDICTIONS)
      .catch(() => undefined);
    return preds;
  }

  /**
   * Daily predictions for SERVING the calendar / yearly view: TFT for the near
   * term (days 1-`tftDays`, headliners — where TFT clearly beats CatBoost), CatBoost
   * for the long tail (TFT can't reach a yearly horizon from short history) and for
   * any (attraction, day) TFT doesn't cover. Keeps the calendar and yearly views on
   * the SAME source. NOT used by the prediction-generator writer (which must persist
   * pure CatBoost into wait_time_predictions so the TFT-vs-CatBoost scoreboard stays
   * fair) — that path keeps calling getParkPredictions("daily") directly.
   */
  async getServingDailyPredictions(
    parkId: string,
    tftDays = 60,
  ): Promise<BulkPredictionResponseDto> {
    // Single-flight: concurrent calendar/yearly requests that all miss the
    // underlying cache share one CatBoost rebuild instead of stampeding it.
    return this.servingFlight.run(`${parkId}:${tftDays}`, async () => {
      const base = await this.getParkPredictions(parkId, "daily");
      let tft: PredictionDto[] = [];
      try {
        tft = await this.getTftDailyPredictions(parkId, tftDays);
      } catch (e: unknown) {
        this.logger.warn(
          `TFT daily merge skipped for ${parkId}: ${e instanceof Error ? e.message : e}`,
        );
      }
      if (tft.length === 0) return base;

      const key = (p: PredictionDto) =>
        `${p.attractionId}|${p.predictedTime.slice(0, 10)}`;
      const tftKeys = new Set(tft.map(key));
      const farCatboost = base.predictions.filter((p) => !tftKeys.has(key(p)));
      const merged = [...tft, ...farCatboost];
      return {
        predictions: merged,
        count: merged.length,
        modelVersion: `${base.modelVersion ?? "catboost"}+tft${tftDays}`,
      };
    });
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
      // Services don't throw HTTP exceptions; an unknown attraction simply
      // has no predictions (the integration caller falls back to [] anyway).
      this.logger.warn(
        `getAttractionPredictions: attraction ${attractionId} not found`,
      );
      return [];
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

    // Filter predictions: only keep predictions for parks that we successfully mapped
    // and that are not explicitly CLOSED or in a seasonal gap on the predicted date.
    const validPredictions: PredictionDto[] = [];
    const parkInfoCache = new Map<
      string,
      {
        hasHistory: boolean;
        minDate: string | null;
        maxDate: string | null;
        isSeasonal: boolean;
        timezone: string;
      }
    >();

    for (const [parkId, parkPredictions] of predictionsByPark) {
      // Get park info for seasonal gap detection
      let info = parkInfoCache.get(parkId);
      if (!info) {
        const park = await this.parkRepository.findOne({
          where: { id: parkId },
          select: ["timezone"],
        });
        const timezone = park?.timezone || "UTC";
        const [range, isSeasonal] = await Promise.all([
          this.parksService.getOperatingDateRange(parkId, timezone),
          this.parksService.isParkSeasonal(parkId),
        ]);
        info = {
          hasHistory: !!(range.minDate && range.maxDate),
          minDate: range.minDate,
          maxDate: range.maxDate,
          isSeasonal,
          timezone,
        };
        parkInfoCache.set(parkId, info);
      }

      // Get unique dates for this park's predictions to batch schedule lookup
      const dates = [
        ...new Set(parkPredictions.map((p) => p.predictedTime.split("T")[0])),
      ];
      const schedules = await this.scheduleEntryRepository.find({
        where: {
          parkId,
          date: In(dates.map((d) => new Date(d + "T12:00:00Z"))),
        },
      });

      const scheduleMap = new Map<string, ScheduleType>();
      schedules.forEach((s) => {
        const dStr = formatInParkTimezone(s.date, info!.timezone);
        scheduleMap.set(dStr, s.scheduleType);
      });

      for (const pred of parkPredictions) {
        const dateStr = pred.predictedTime.split("T")[0];
        const scheduleType = scheduleMap.get(dateStr);

        // 1. Skip if explicitly CLOSED
        if (scheduleType === ScheduleType.CLOSED) continue;

        // 2. Skip if in seasonal gap (UNKNOWN/missing between min and max operating dates)
        if (
          info.hasHistory &&
          (!scheduleType || scheduleType === ScheduleType.UNKNOWN) &&
          info.minDate &&
          info.maxDate &&
          dateStr > info.minDate &&
          dateStr < info.maxDate
        ) {
          continue;
        }

        // 3. Skip if before first known operating date (only for seasonal parks)
        if (
          info.hasHistory &&
          info.isSeasonal &&
          info.minDate &&
          dateStr < info.minDate
        ) {
          continue;
        }

        // 4. Skip if after last known operating date (only for seasonal parks)
        // — BUT only when maxDate is genuinely in the past. maxDate is the last
        // OPERATING *schedule* day, which reflects the schedule-sync horizon, not
        // necessarily the season's end: a park that is open right now but whose
        // source only publishes a schedule a few days out has maxDate ≈ today and
        // would otherwise have its ENTIRE future calendar dropped (e.g. Energylandia
        // — open daily, but 0 future OPERATING entries). Only when maxDate is days
        // in the past has the park demonstrably stopped operating (real off-season,
        // e.g. Hansa-Park in winter), so the skip is correct there. Future schedule
        // gaps inside an active season are still caught by filter #2.
        const todayInTz = getCurrentDateInTimezone(info.timezone);
        if (
          info.hasHistory &&
          info.isSeasonal &&
          info.maxDate &&
          info.maxDate < todayInTz &&
          dateStr > info.maxDate
        ) {
          continue;
        }

        validPredictions.push(pred);
      }
    }

    if (validPredictions.length === 0) {
      this.logger.verbose(
        "No valid predictions to store (all filtered by schedule/gaps)",
      );
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

    // Chunked: a single multi-row INSERT for a big park (e.g. 60 attractions ×
    // 365 daily rows × 11 columns) exceeds the Postgres wire-protocol limit of
    // 65535 bind parameters — the driver then fails with "bind message has N
    // parameter formats but 0 parameters" and the whole park gets no predictions.
    const savedPredictions = await this.predictionRepository.save(entities, {
      chunk: 1000,
    });
    this.logger.debug(
      `Stored ${savedPredictions.length} predictions in database`,
    );

    // Record predictions for accuracy tracking (feedback loop).
    // No sampling — upsert on (attractionId, targetTime) ensures each future slot
    // is stored exactly once regardless of how many 15-min cycles cover it.
    // Full coverage is required so every ride has data for ML sample weighting.
    //
    // ONLY record hourly predictions — daily predictions span up to 365 days ahead
    // and can't be compared against actuals until those dates arrive, inflating
    // PENDING counts and skewing coverage metrics.
    const validPredictionsForFeedback = savedPredictions.filter(
      (pred) =>
        pred.predictionType === "hourly" &&
        (pred.status === "OPERATING" || pred.status === null),
    );

    if (validPredictionsForFeedback.length < savedPredictions.length) {
      this.logger.debug(
        `Filtering: Recording ${validPredictionsForFeedback.length}/${savedPredictions.length} predictions (excluding scheduled closures and daily predictions)`,
      );
    }

    let recordedCount = 0;
    try {
      // Batched upsert (few multi-row statements, synchronous_commit=off) instead
      // of one round-trip per prediction — avoids the multi-second lock waits the
      // per-row loop produced under the concurrent comparison job.
      recordedCount = await this.predictionAccuracyService.recordPredictions(
        validPredictionsForFeedback,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.warn(
        `Failed to record predictions for accuracy tracking: ${errorMessage}`,
      );
    }

    this.logger.verbose(
      `✅ Recorded ${recordedCount}/${validPredictionsForFeedback.length} predictions for accuracy tracking`,
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
    // Hourly predictions are regenerated every 15 min — allow 30 min (2 intervals) to survive a delayed run.
    // Daily predictions are regenerated once per day — allow 26h window.
    const createdAtCutoff =
      predictionType === "hourly"
        ? new Date(Date.now() - 30 * 60 * 1000)
        : new Date(Date.now() - 26 * 60 * 60 * 1000);
    queryBuilder.andWhere("p.createdAt >= :createdAtCutoff", {
      createdAtCutoff,
    });

    return queryBuilder.getMany();
  }

  async getBatchStoredPredictions(
    attractionIds: string[],
    predictionType: "hourly" | "daily" = "hourly",
    startTime?: Date,
  ): Promise<Map<string, PredictionDto[]>> {
    if (attractionIds.length === 0) {
      return new Map();
    }

    const createdAtCutoff =
      predictionType === "hourly"
        ? new Date(Date.now() - 30 * 60 * 1000)
        : new Date(Date.now() - 26 * 60 * 60 * 1000);

    const queryBuilder = this.predictionRepository
      .createQueryBuilder("p")
      .where("p.attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("p.predictionType = :predictionType", { predictionType })
      .andWhere("p.createdAt >= :createdAtCutoff", { createdAtCutoff })
      .orderBy("p.predictedTime", "ASC");

    if (startTime) {
      queryBuilder.andWhere("p.predictedTime >= :startTime", { startTime });
    }

    const rows = await queryBuilder.getMany();

    const result = new Map<string, PredictionDto[]>();
    for (const p of rows) {
      const list = result.get(p.attractionId) ?? [];
      list.push({
        attractionId: p.attractionId,
        predictedTime: p.predictedTime.toISOString(),
        predictedWaitTime: p.predictedWaitTime,
        predictionType: p.predictionType,
        confidence: p.confidence,
        crowdLevel: p.crowdLevel,
        baseline: p.baseline,
        modelVersion: p.modelVersion,
        status: p.status || undefined,
      });
      result.set(p.attractionId, list);
    }

    if (this.servePcnIntraday && predictionType === "hourly") {
      await this.applyPcnIntradayOverride(
        [...result.values()].flat(),
        startTime,
      );
    }
    return result;
  }

  /** Champion-swap flag: serve PCN's intraday 15-min forecast in place of CatBoost's
   * `hourly` predictions (PCN beats CatBoost on the matched board). Default OFF —
   * enable per env once the win is confirmed over a few days. Always falls back to
   * CatBoost where PCN has no forecast (or the table/flag is absent). */
  private get servePcnIntraday(): boolean {
    return process.env.SERVE_PCN_INTRADAY === "true";
  }

  /** Freshest forward PCN q0.5 (displayed wait) per (attraction, slot) for upcoming slots,
   * keyed by the slot's UTC ISO string (matching CatBoost's predictedTime). pcn_forecasts
   * stores park-LOCAL naive slots, so `target_slot AT TIME ZONE tz` recovers the instant.
   * Returns an empty map on any error (missing table / flag misuse) → pure CatBoost. */
  private async getPcnIntradayWaits(
    attractionIds: string[],
    startTime?: Date,
  ): Promise<Map<string, Map<string, number>>> {
    const out = new Map<string, Map<string, number>>();
    if (attractionIds.length === 0) return out;
    try {
      const rows: Array<{ aid: string; predicted_time: Date; wait: string }> =
        await this.predictionRepository.manager.query(
          `SELECT DISTINCT ON (f.attraction_id, f.target_slot)
              f.attraction_id::text AS aid,
              (f.target_slot AT TIME ZONE p.timezone) AS predicted_time,
              f.predicted_wait AS wait
           FROM pcn_forecasts f
           JOIN attractions a ON a.id = f.attraction_id
           JOIN parks p ON p.id = a."parkId"
           WHERE f.attraction_id = ANY($1::uuid[])
             AND f.quantile = 0.5
             AND (f.target_slot AT TIME ZONE p.timezone) >= COALESCE($2, now())
           ORDER BY f.attraction_id, f.target_slot, f.origin_slot DESC`,
          [attractionIds, startTime ?? null],
        );
      for (const r of rows) {
        const m = out.get(r.aid) ?? new Map<string, number>();
        m.set(new Date(r.predicted_time).toISOString(), Number(r.wait));
        out.set(r.aid, m);
      }
    } catch (e: unknown) {
      this.logger.warn(
        `PCN intraday override skipped: ${e instanceof Error ? e.message : e}`,
      );
    }
    return out;
  }

  /** Override matched hourly predictions in place with PCN's q0.5 wait, recomputing
   * crowdLevel from the new wait against the carried baseline. Unmatched slots/attractions
   * keep CatBoost (the fallback). Mutates `preds`. */
  private async applyPcnIntradayOverride(
    preds: PredictionDto[],
    startTime?: Date,
  ): Promise<void> {
    const hourly = preds.filter((p) => p.predictionType === "hourly");
    if (hourly.length === 0) return;
    const ids = [...new Set(hourly.map((p) => p.attractionId))];
    const pcn = await this.getPcnIntradayWaits(ids, startTime);
    let overridden = 0;
    for (const p of hourly) {
      const wait = pcn.get(p.attractionId)?.get(p.predictedTime);
      if (wait === undefined) continue;
      p.predictedWaitTime = Math.round(wait);
      if (p.baseline && p.baseline > 0) {
        // determineCrowdLevel never returns "unknown" (we guard baseline>0), so it is a
        // valid PredictionDto crowdLevel — narrow the wider CrowdLevel type with a cast.
        p.crowdLevel = determineCrowdLevel(
          (p.predictedWaitTime / p.baseline) * 100,
        ) as PredictionDto["crowdLevel"];
      }
      p.modelVersion = `${p.modelVersion ?? "catboost"}+pcn`;
      overridden++;
    }
    if (overridden > 0) {
      this.logger.debug(`PCN intraday override: ${overridden}/${hourly.length} slots`);
    }
  }

  /**
   * Get predictions (try DB first, fall back to ML service)
   */
  async getAttractionPredictionsWithFallback(
    attractionId: string,
    predictionType: "hourly" | "daily" = "hourly",
  ): Promise<PredictionDto[]> {
    // Try stored predictions first for both hourly and daily.
    // Include the currently-active 15-min slot (timestamp may be up to 15 min in the past)
    // so "go now" can surface as a best-visit-time recommendation.
    const startTime =
      predictionType === "hourly" ? new Date(currentSlotStartMs()) : undefined;
    const stored = await this.getStoredPredictions(
      attractionId,
      predictionType,
      startTime,
    );

    if (stored.length > 0) {
      this.logger.debug(
        `Using ${stored.length} stored ${predictionType} predictions for ${attractionId}`,
      );
      const dtos: PredictionDto[] = stored.map((p) => ({
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
      if (this.servePcnIntraday && predictionType === "hourly") {
        await this.applyPcnIntradayOverride(dtos, startTime);
      }
      return dtos;
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
    // wait_time_predictions is a hypertable partitioned on createdAt with
    // compression after 14 days; rows this old live in compressed chunks, and a
    // plain DELETE aborts once it decompresses more than
    // timescaledb.max_tuples_decompressed_per_dml_transaction (100k) tuples.
    // Lift the limit locally for this one bounded nightly cleanup.
    const rows: Array<{ affected: string }> =
      await this.predictionRepository.manager.transaction(async (em) => {
        await em.query(
          `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0`,
        );
        return em.query(
          `WITH del AS (
             DELETE FROM wait_time_predictions
             WHERE "predictionType" = $1 AND "predictedTime" < $2
             RETURNING 1
           ) SELECT count(*)::text AS affected FROM del`,
          [predictionType, cutoffDate],
        );
      });

    return Number(rows?.[0]?.affected ?? 0);
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

    // Scope the delete to chunks the planner can prove are uncompressed: the
    // hypertable is partitioned on createdAt and compressed after 14 days, but
    // this predicate only filtered on predictedTime, so the DELETE scanned every
    // chunk and died with "tuple decompression limit exceeded" on parks whose
    // stale rows had been compressed — those parks then NEVER got fresh daily
    // predictions again (the stale rows stayed, failing every night). Rows older
    // than the window are superseded duplicates; readers pick the freshest
    // createdAt and the nightly cleanup-old job removes them.
    const createdAfter = new Date(now);
    createdAfter.setDate(createdAfter.getDate() - 13);

    const result = await this.predictionRepository
      .createQueryBuilder()
      .delete()
      .where("attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("predictionType = :predictionType", { predictionType })
      .andWhere("predictedTime >= :startTime", { startTime })
      .andWhere("predictedTime <= :endTime", { endTime })
      .andWhere('"createdAt" >= :createdAfter', { createdAfter })
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
