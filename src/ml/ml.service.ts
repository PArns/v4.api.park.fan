import { Injectable, Logger, HttpException, Inject } from "@nestjs/common";
import { CacheKeys } from "../common/cache/cache-keys";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { getMlServiceUrl } from "../config/ml-services.config";
import { isQueueTimesExcluded } from "../external-apis/queue-times/queue-times.exclusions";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In, IsNull, MoreThan } from "typeorm";
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
import {
  PCN_MAX_FORECAST_AGE_H,
  SERVED_ACCURACY_MIN_LEAD_MS,
  roundServedWait,
  servePcnIntraday as servePcnIntradayFlag,
} from "./pcn-serving.constants";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { ScheduleType } from "../parks/entities/schedule-entry.entity";
import { dayStatusRank } from "../common/utils/schedule.util";
import {
  LiveStatus,
  QueueType,
} from "../external-apis/themeparks/themeparks.types";
import {
  PredictionAccuracyService,
  PredictionQuantiles,
  ServedIntradayValue,
} from "./services/prediction-accuracy.service";
import { ForecastAccuracyService } from "./services/forecast-accuracy.service";
import { ForecastAccuracyProfile } from "./entities/forecast-accuracy-profile.entity";
import { persistenceBlendServe } from "./utils/persistence-blend.util";
import {
  applyChronosOverride,
  readChronosServeCache,
  serveChronosIntraday,
} from "./chronos-serving";
import {
  HOURLY_FRESHNESS_MS,
  HOURLY_LOOKBACK_MS,
  StoredSlotRow,
  hourlyPredictionsFreshKey,
  newestPerSlot,
  planHourlyPredictionWrite,
} from "./utils/prediction-diff-write.util";
import { predictionLocalDate } from "./utils/prediction-local-date.util";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { forwardRef } from "@nestjs/common";
import {
  FeatureContext,
  QueueDataInfo,
} from "../common/types/feature-context.type";

/**
 * How far back {@link MLService.deduplicatePredictions} looks for the rows its
 * run supersedes, per prediction type. Down from the flat 13 days it was until
 * 2026-10-12.
 *
 * SINCE THE DIFF-WRITE, ONLY `daily` REACHES THIS. The hourly generator used to
 * call `deduplicatePredictions(parkId, "hourly")` before inserting its horizon
 * again; it now diff-writes instead (`storePredictions(…, {
 * replaceHourlyForPark })`), which supersedes rows per slot and never deletes a
 * slot it is keeping. The `hourly` entry stays because the method still takes
 * the type, and the margin below `compress_after` has to hold for both.
 *
 * The DELETE needs a `createdAt` predicate at all because `createdAt` is the
 * hypertable's partition key — see the comment at the call site for the
 * decompression failure that put it there. The question is only how wide it has
 * to be, and 13 days was never derived from anything.
 *
 * WHY 72h IS ENOUGH FOR HOURLY. A run writes slots `[T, T+48h]` from its own
 * `createdAt = T` (HOURLY_PREDICTIONS = 48; `createdAt` is stamped at insert,
 * after the model call, so it is never earlier than the slot base). For a row to
 * still hold a slot this run wants to overwrite — a `predictedTime >= now` — it
 * needs `T + 48h >= now`, so nothing created before `now - 48h` can hold one:
 * there is nothing to delete out there. Being missed by the window and still
 * holding a future slot requires the horizon to exceed the window, which at 48h
 * against 72h is unsatisfiable however late or queued a run is. The extra 24h is
 * slack for clock skew between this process and Postgres.
 *
 * WHY DAILY GETS MORE. The daily run is skipped for a park that is not
 * operating and had no ride activity in 24h, so an off-season or weekend-only
 * park can go 4-5 days between runs — longer than 72h, which would leave the
 * previous cohort's rows for targets inside `[now, now+60d]` undeleted. 8 days
 * covers that gap, costs one wider DELETE per night rather than one per 15
 * minutes, and is still 6 days clear of the compression policy. It does not
 * cover a park skipped for weeks; nothing short of the horizon would, the rows
 * are not servable (see below), and the 90-day chunk drop is the backstop.
 *
 * WHY BOTH MUST STAY WELL BELOW `compress_after`. The policy on this hypertable
 * is 14 days (`HYPERTABLES`, job 1006 in production). A DELETE that reaches into
 * a compressed chunk is the failure the call site describes, so these windows
 * have to keep a margin of days below it — the spec asserts that against the
 * configured value rather than against a literal.
 *
 * WHAT THE NARROWING BUYS. 13 days spans three of the 7-day chunks, 72h spans
 * one or two, so each of the ~96 hourly runs a day touches less than half the
 * chunk indexes it used to (DB audit 2026-10-12, finding 4a / recommendation 3a).
 *
 * WHAT IT GIVES UP. A row older than its window that still holds a future target
 * is no longer deleted. Nothing serves it:
 * {@link storedPredictionsCreatedAtCutoff} bounds a reader at `now - 50h` for
 * hourly (the generation horizon plus slack, since PAR-874 moved freshness off
 * the row onto the per-ride marker) and `now - 26h` for daily — both inside
 * these windows by a wide margin, so the narrowing cannot leave a duplicate
 * that any reader sees. The nightly `cleanup-old` job and the 90-day chunk drop
 * still remove them.
 */
export const DEDUPE_CREATED_AFTER_HOURS: Record<"hourly" | "daily", number> = {
  hourly: 72,
  daily: 8 * 24,
};

/**
 * The model's q0.8/q0.95 off one prediction, as whole minutes (PAR-912), or
 * `null` when it carries neither (a model without a quantile head, an
 * ml-service from before the fields existed). A non-finite or negative value is
 * dropped rather than stored: a coverage figure built on it would be noise.
 */
export function predictionQuantilesOf(
  p: Pick<PredictionDto, "predictedQ80" | "predictedQ95">,
): PredictionQuantiles | null {
  const minutes = (v: number | null | undefined): number | null =>
    typeof v === "number" && Number.isFinite(v) && v >= 0
      ? Math.round(v)
      : null;
  const q80 = minutes(p.predictedQ80);
  const q95 = minutes(p.predictedQ95);
  return q80 === null && q95 === null ? null : { q80, q95 };
}

/**
 * The response with `predictedQ80`/`predictedQ95` removed from COPIES of its
 * predictions (PAR-912). The two fields exist for the accuracy rows only; the
 * served read feeds park pages, `/plan/day` and the calendar, several of which
 * spread a prediction into their payload, so they would otherwise reach the
 * public API. The raw read (`getRawParkPredictions`, the generator's input)
 * keeps them, and the Redis entry is never touched.
 */
export function withoutInternalQuantiles(
  response: BulkPredictionResponseDto,
): BulkPredictionResponseDto {
  if (!response.predictions) return response;
  return {
    ...response,
    predictions: response.predictions.map((p) => {
      const copy = { ...p };
      delete copy.predictedQ80;
      delete copy.predictedQ95;
      return copy;
    }),
  };
}

/**
 * The oldest `createdAt` a reader of stored predictions looks at.
 *
 * Hourly: the generation horizon plus slack (PAR-874). It used to be 30 min,
 * which doubled as the freshness test; since the diff-write keeps an unchanged
 * row with the `createdAt` of the run that last changed it, freshness is the
 * per-ride marker and this bound only keeps the scan on the newest chunks.
 * Daily: regenerated once a night — 26 h survives one late run.
 */
export function storedPredictionsCreatedAtCutoff(
  predictionType: "hourly" | "daily",
  now: number = Date.now(),
): Date {
  return new Date(
    now - (predictionType === "hourly" ? HOURLY_LOOKBACK_MS : 26 * 3_600_000),
  );
}

@Injectable()
export class MLService {
  private readonly logger = new Logger(MLService.name);
  private readonly mlClient: AxiosInstance;
  private readonly ML_SERVICE_URL: string;

  // Cache TTLs based on prediction generation frequency
  private readonly TTL_HOURLY_PREDICTIONS = 30 * 60; // 30 minutes - matches HOURLY_FRESHNESS_MS
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

  // Same guard for the park-level ml-service read (`ml:park:{id}:{type}:{date}`):
  // the hourly key lapses every 30 min and every park page then misses at once.
  private readonly parkPredictionFlight = new SingleFlight();

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
    private forecastAccuracyService: ForecastAccuracyService,
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
    options: { chronos?: boolean } = {},
  ): Promise<BulkPredictionResponseDto> {
    const raw = await this.getRawParkPredictions(
      parkId,
      predictionType,
      maxDays,
      liveStatus,
    );
    if (!this.serveIntradayOverride || predictionType !== "hourly") {
      return withoutInternalQuantiles(raw);
    }

    // Champion-swap consistency: the park-level hourly curve (park page, plan/day,
    // calendar today, favorites) must show the SAME served waits as the
    // per-attraction read paths, which apply the PCN override + persistence blend.
    // Overridden on COPIES, so neither the Redis entry nor the raw object a caller
    // may still hold ever carries a PCN number.
    // `chronos: false` is the forward archive's incumbent read (PAR-911): the
    // curve as it would be served without Chronos, so the live A/B keeps a
    // reference once Chronos is part of what is served. No endpoint passes it.
    const predictions = withoutInternalQuantiles(raw).predictions ?? [];
    await this.applyServedIntradayOverride(
      predictions,
      new Date(currentSlotStartMs()),
      options.chronos ?? true,
    );
    return { ...raw, predictions };
  }

  /**
   * The ml-service answer for a park exactly as CatBoost produced it — Redis-cached,
   * never touched by the PCN champion-swap override.
   *
   * This is the ONLY park-level read `storePredictions` may be fed from:
   * `wait_time_predictions` and `prediction_accuracy` are the CatBoost side of the
   * PCN-vs-CatBoost board, so a served (PCN/blended) number written there would make
   * the board compare PCN against itself. Serving surfaces use
   * {@link getParkPredictions}, which applies the override on top of this.
   */
  async getRawParkPredictions(
    parkId: string,
    predictionType: "hourly" | "daily" = "hourly",
    maxDays?: number,
    liveStatus?: "OPERATING" | "CLOSED",
  ): Promise<BulkPredictionResponseDto> {
    // Get park timezone for cache key
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: {
        id: true,
        timezone: true,
        countryCode: true,
        regionCode: true,
      },
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

    // maxDays and liveStatus change what the build returns, so they are part of the flight key.
    return this.parkPredictionFlight.run(
      `${cacheKey}:${maxDays ?? ""}:${liveStatus ?? ""}`,
      () =>
        this.buildRawParkPredictions(
          park,
          predictionType,
          cacheKey,
          maxDays,
          liveStatus,
        ),
    );
  }

  /** The cache-miss half of {@link getRawParkPredictions}: one ml-service call, written to `cacheKey`. */
  private async buildRawParkPredictions(
    park: Park,
    predictionType: "hourly" | "daily",
    cacheKey: string,
    maxDays?: number,
    liveStatus?: "OPERATING" | "CLOSED",
  ): Promise<BulkPredictionResponseDto> {
    const parkId = park.id;
    try {
      // 1. Fetch park with timezone (already fetched above for cache key)
      // Park timezone is already loaded

      // 2. Fetch all attractions for this park
      const attractions = await this.attractionRepository.find({
        where: { parkId },
        select: {
          id: true,
        },
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
          select: {
            id: true,
            timezone: true,
          },
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
          // The park's own row, not a ride's: the table holds both and this
          // query has no ORDER BY (PAR-246).
          .andWhere("schedule.attractionId IS NULL")
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
          select: {
            id: true,
            timezone: true,
            countryCode: true,
          },
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

      // 5. Bridge days: the ML service computes them per predicted date from
      // the holidays it already reads. One flag for today, sent here, was
      // applied to every row up to a year ahead (PAR-1030).

      // 6. Check if park has schedule data
      // This helps ML understand data quality: Parks with schedules have more reliable patterns
      let parkHasSchedule: Record<string, boolean> = {};
      try {
        const scheduleExists = await this.scheduleEntryRepository.findOne({
          where: {
            parkId,
            scheduleType: ScheduleType.OPERATING,
            // Same filter as `ParksService.hasOperatingSchedule`, which answers
            // the same question: a ride's row is not the park having a schedule.
            attractionId: IsNull(),
          },
          select: {
            id: true,
          },
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
          select: {
            id: true,
            countryCode: true,
            regionCode: true,
            timezone: true,
            influencingRegions: true,
          },
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
      select: {
        id: true,
        timezone: true,
      },
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
   * Near-term daily forecast from the TFT (nf-service), for every ride it covers.
   *
   * It was headliners only, "the scope the TFT backtest validated", and that
   * scope was too small. Measured against the realised day-P90 on 2026-09-03,
   * at 1-2 days' lead: TFT 15.47 MAE against CatBoost's 23.84 on headliners
   * (n=5,986) and **8.67 against 12.06 on everything else** (n=4,696) — 28%
   * better on the rides the restriction was excluding. The advantage holds
   * across the horizon: non-headliner MAE runs 8.65 / 9.55 / 10.48 at leads of
   * <=7, 8-30 and 31-60 days, the same gentle slope the headliners show.
   * `tft_forecasts` covers 3,581 rides against 1,237 headliners, so this hands
   * roughly 2,350 more rides the better model. See
   * docs/ml/long-range-forecasting.md.
   *
   * This does NOT move the calendar's crowd levels: `buildPredictedCrowdLevels`
   * filters the serving set to headliners itself before averaging, which is why
   * the restriction could live here rather than there.
   * Reads the freshest forward forecast per (attraction, day) from tft_forecasts,
   * for the next `days` park-local days, with a 3-day staleness guard so a stalled
   * nf-service falls back to CatBoost instead of serving old forecasts. Returned as
   * PredictionDto so it drops straight into the calendar/yearly crowd-level path
   * (which recompute crowdLevel from predictedWaitTime; the placeholder fields here
   * are not read). Cached per park-day: both inputs move on a nightly run — the
   * TFT forecast itself, and the accuracy grid the band comes from
   * (`rebuild-accuracy-profile`) — so a cached park-day can carry the previous
   * night's band for at most the 13-hour TTL, which is the same staleness the
   * forecast beside it already has.
   *
   * THE BAND IS MEASURED HERE, BECAUSE THE MODEL DOES NOT CARRY ONE.
   * `tft_forecasts` holds a `predicted_peak` and nothing else, so until now every
   * day inside this horizon reached the calendar with `uncertaintyMinutes`
   * absent — 155 of 155 headliner entries at Europa-Park on 2026-09-13 — while
   * CatBoost brought one from day 60 on. The product said less about its
   * uncertainty over the near future than over the far one.
   *
   * What fills it is {@link ForecastAccuracyProfile.uncertaintyP95}: the 95th
   * percentile of this cell's realised residuals, looked up by predicted band and
   * lead distance, out-of-sample calibrated to 92-98 % coverage. The alternative
   * of scaling the prediction is measured wrong in the entity docblock, and the
   * alternative of serving the MAE would put `/plan/day`'s `expectedError` under
   * a second name.
   *
   * IT IS NOT THE SAME STATISTIC CATBOOST SERVES, AND THAT IS VISIBLE AT DAY 60.
   * CatBoost's `uncertaintyMinutes` is its top trained quantile minus its served
   * median — a model-internal spread. Measured against realised days over 45
   * days (2026-09-13, production), that spread covers **52.7 % (quiet), 54.8 %
   * (mid) and 56.7 % (busy)** of outcomes, not the 95 % its name claims, so it
   * runs roughly a third the width of the measured band. The calendar therefore
   * steps down at the seam. Putting CatBoost's side on this same basis is the
   * fix, and it is not possible yet: its error past 60 days has never been
   * measurable, and `prediction_lead_snapshots` cannot report the `d60` bucket
   * before 2026-11-02. Tracked as PAR-167; do not "smooth" the seam by narrowing
   * this side, which would mean serving a band nobody measured.
   */
  async getTftDailyPredictions(
    parkId: string,
    days = 60,
  ): Promise<PredictionDto[]> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: {
        id: true,
        timezone: true,
      },
    });
    if (!park) return [];
    const today = getCurrentDateInTimezone(park.timezone);

    const cacheKey = `ml:tft-daily:${parkId}:${days}:${today}`;
    const cached = await this.redis.get(cacheKey);
    const cachedPredictions = safeJsonParse<PredictionDto[]>(cached);
    if (cachedPredictions) return cachedPredictions;

    // Freshest forward forecast per (attraction, target_date); headliners of THIS
    // park; within the horizon; not stale. The ::text casts are on the OUTPUT
    // only. The park filter compares uuid to uuid with the id the lookup above
    // returned, never `"parkId"::text = $1`, which hides the column from its
    // index (PAR-871).
    const rows: Array<{
      attractionId: string;
      targetDate: string;
      forecastDate: string;
      peak: string;
    }> = await this.attractionRepository.query(
      `
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id::text   AS "attractionId",
          f.target_date::text      AS "targetDate",
          -- The band is keyed on how far ahead the forecast was MADE, not on
          -- how far ahead the day is from today; those differ by up to the
          -- staleness guard below. See the lookup in the mapper.
          f.forecast_date::text    AS "forecastDate",
          f.predicted_peak::float  AS peak
        FROM tft_forecasts f
        JOIN attractions a
          ON a.id = f.attraction_id AND a."parkId" = $1::uuid AND a.retired_at IS NULL
        WHERE f.target_date >= $2::date
          AND f.target_date <  ($2::date + $3::int)
          AND f.forecast_date >= ($2::date - 3)
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
        `,
      [park.id, today, days],
    );

    // The measured error grid, for the band and the confidence below. One read
    // for the whole park-day: 18 rows, already the shape `lookup` wants. A
    // failure here costs the band, not the forecast — the rest of this method
    // does not depend on it.
    const accuracy = await this.forecastAccuracyService
      .getProfile()
      .catch((e: unknown) => {
        this.logger.warn(
          `TFT band unavailable for ${parkId}: ${e instanceof Error ? e.message : e}`,
        );
        return new Map<string, ForecastAccuracyProfile>();
      });

    const preds: PredictionDto[] = rows.map((r) => {
      const predictedWaitTime = Math.max(0, Math.round(Number(r.peak)));
      // From the forecast's OWN origin, not from today. `rebuild()` buckets by
      // `target_date - forecast_date`, and the staleness guard above admits a
      // forecast up to three days old — so a stalled nf-service would otherwise
      // be handed the band measured for a distance three days SHORTER than the
      // one it actually forecast at. That narrows the band exactly when it
      // should widen, which is the one direction `lookup` is built to avoid.
      const leadDays = MLService.daysBetween(r.forecastDate, r.targetDate);
      const cell = ForecastAccuracyService.lookup(
        accuracy,
        predictedWaitTime,
        leadDays,
      );
      return {
        attractionId: r.attractionId,
        predictedTime: `${r.targetDate}T12:00:00`,
        predictedWaitTime,
        predictionType: "daily" as const,
        confidence: MLService.dailyConfidence(
          leadDays,
          predictedWaitTime,
          cell?.uncertaintyP95,
        ),
        // Absent, not zero, where the grid has no cell for this distance OR
        // holds one that predates the column (the night between the deploy and
        // the next rebuild): a zero-wide band draws as a confident hairline.
        // `!= null` covers both, and keeps a genuine measured 0.
        uncertaintyMinutes:
          cell?.uncertaintyP95 != null ? Math.round(cell.uncertaintyP95) : null,
        // TFT rates nothing; "unknown" so no reader can take it for a level (PAR-1028).
        crowdLevel: "unknown" as const,
        baseline: 0,
        modelVersion: "tft",
      };
    });

    await this.redis
      .set(cacheKey, JSON.stringify(preds), "EX", this.TTL_DAILY_PREDICTIONS)
      .catch(() => undefined);
    return preds;
  }

  /**
   * Whole days between two park-local `YYYY-MM-DD` dates.
   *
   * Both anchored at UTC midnight on purpose: they are already park-local
   * calendar dates, so the only job left is to count days, and parsing them in a
   * zone that observes DST would make one day of the year 23 or 25 hours long
   * and round the wrong way.
   */
  /** The park-local calendar day a prediction is about — see {@link predictionLocalDate}. */
  static localDateOf(predictedTime: string, timezone: string): string {
    return predictionLocalDate(predictedTime, timezone);
  }

  private static daysBetween(from: string, to: string): number {
    return Math.round(
      (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
        86_400_000,
    );
  }

  /**
   * A confidence figure for a TFT day, on the same scale and by the same formula
   * CatBoost uses (`predict.py:2037-2062`): 60 % from how far ahead the day is,
   * 40 % from how wide the band is relative to the prediction, each floored at 30.
   *
   * It replaces the constant `0.7` that sat here, which was wrong twice over.
   * It was invented — no part of it came from the model or from a measurement —
   * and it was on the WRONG SCALE: CatBoost emits 30-100 from the formula above,
   * so at the seam between the two models a consumer reading `confidence` saw it
   * jump by a factor of a hundred. Anything comparing the two silently preferred
   * whichever side happened to be CatBoost's.
   *
   * The 40 % term is fed from {@link ForecastAccuracyProfile.uncertaintyP95} —
   * measured, where CatBoost feeds it a trained quantile spread. That is the
   * "same basis" the band comes from, so the two numbers on a row cannot
   * disagree. Where the grid has no cell, this falls back to the time term
   * alone, which is exactly what `predict.py` does when a model reports no
   * spread.
   */
  private static dailyConfidence(
    leadDays: number,
    predictedWaitTime: number,
    band: number | null | undefined,
  ): number {
    const timeConfidence = Math.max(30, 85 - Math.max(0, leadDays) * 0.15);
    // `== null` and not `=== undefined`: a row written before `uncertainty_p95`
    // existed carries NULL, and `null / wait` is 0, which would hand that row a
    // model term of 100 — the highest possible confidence, on the one row that
    // has measured nothing.
    if (band == null || predictedWaitTime <= 0) {
      return Math.round(timeConfidence * 10) / 10;
    }
    const relative = Math.min(band / Math.max(predictedWaitTime, 1), 1);
    const modelConfidence = Math.max(30, 100 * (1 - relative));
    return Math.round((0.6 * timeConfidence + 0.4 * modelConfidence) * 10) / 10;
  }

  /**
   * Daily predictions for SERVING the calendar / yearly view: TFT for the near
   * term (days 1-`tftDays`, headliners — where TFT clearly beats CatBoost), CatBoost
   * for the long tail (TFT can't reach a yearly horizon from short history) and for
   * any (attraction, day) TFT doesn't cover. Keeps the calendar and yearly views on
   * the SAME source. NOT used by the prediction-generator writer (which must persist
   * pure CatBoost into wait_time_predictions so the TFT-vs-CatBoost scoreboard stays
   * fair) — that path calls getRawParkPredictions("daily"), never the served read.
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

      // Keyed on the PARK-LOCAL day for both sides (PAR-818): TFT names the
      // local day, CatBoost an instant whose UTC date can be the day after.
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: {
          id: true,
          timezone: true,
        },
      });
      const tz = park?.timezone || "UTC";
      const key = (p: PredictionDto) =>
        `${p.attractionId}|${MLService.localDateOf(p.predictedTime, tz)}`;
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
      relations: {
        park: true,
      }, // Fetch park relationship
      select: {
        id: true,
        parkId: true,
      },
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

    // The same baselines the park path sends (PAR-943): without them the ML
    // service has nothing real to rate against and answers `unknown`.
    const baselinesPromise = Promise.all([
      this.analyticsService.getP50BaselineFromCache(attraction.parkId),
      this.analyticsService.getTypicalDayPeakFromCache(attraction.parkId),
    ]).catch((err) => {
      this.logger.warn(
        `Failed to fetch baselines for park ${attraction.parkId}: ${err}`,
      );
      return [undefined, undefined];
    });

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

    const [p50, typicalPeak] = await baselinesPromise;

    // 4. Request predictions
    const request: PredictionRequestDto = {
      attractionIds: [attractionId],
      parkIds: [attraction.parkId], // property actually accessed from loaded relation or column
      predictionType,
      weatherForecast,
      currentWaitTimes,
      recentWaitTimes,
      p50Baseline: p50 || undefined,
      typicalDayPeakBaseline: typicalPeak || undefined,
    };

    const response = await this.getPredictions(request);
    // A served fallback path (getAttractionPredictionsWithFallback), so the
    // accuracy-only quantiles are dropped here as on the park read.
    return withoutInternalQuantiles(response).predictions;
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
  async storePredictions(
    predictions: PredictionDto[],
    options: {
      /**
       * Diff-write the hourly rows of this park instead of appending (PAR-874):
       * the run's answer REPLACES the park's stored future hourly slots, but a
       * slot whose value did not change is left alone. Takes the place of
       * `deduplicatePredictions(parkId, "hourly")` + append.
       */
      replaceHourlyForPark?: string;
    } = {},
  ): Promise<void> {
    if (predictions.length === 0) {
      this.logger.warn("No predictions to store");
      return;
    }

    // Get unique attraction IDs to look up their parkIds
    const attractionIds = [...new Set(predictions.map((p) => p.attractionId))];

    // Fetch attractions with their parkIds AND the park timezone in one go —
    // the timezone used to be re-queried once per park further down.
    const attractions = await this.attractionRepository
      .createQueryBuilder("a")
      .innerJoin("a.park", "p")
      .select("a.id", "id")
      .addSelect("a.parkId", "parkId")
      .addSelect("a.externalId", "externalId")
      .addSelect("p.timezone", "timezone")
      .where("a.id IN (:...ids)", { ids: attractionIds })
      .getRawMany<{
        id: string;
        parkId: string;
        externalId: string | null;
        timezone: string | null;
      }>();

    // Create map: attractionId -> parkId (+ parkId -> timezone)
    const attractionToPark = new Map<string, string>();
    const parkTimezones = new Map<string, string>();
    // Rows the ingestion filter rejects are not attractions at all (Energylandia
    // publishes its turnstile counters through the ride feed). They stop
    // receiving wait times, but they remain in `attractions` until they can be
    // deleted — and the generator keeps predicting for every row it finds. Drop
    // them here so no further predictions, and no accuracy rows derived from
    // them, are written for something that is not a ride.
    const notAnAttraction = new Set<string>();
    for (const attraction of attractions) {
      if (
        attraction.externalId &&
        isQueueTimesExcluded(attraction.externalId)
      ) {
        notAnAttraction.add(attraction.id);
        continue;
      }
      attractionToPark.set(attraction.id, attraction.parkId);
      parkTimezones.set(attraction.parkId, attraction.timezone || "UTC");
    }
    if (notAnAttraction.size > 0) {
      this.logger.debug(
        `Skipping predictions for ${notAnAttraction.size} excluded non-attraction row(s)`,
      );
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

    // Park info for every park at once. getOperatingDateRange / isParkSeasonal
    // are Redis-cached read-throughs, so the concurrency here is cheap; what
    // this removes is the sequential per-park round-trip chain.
    await Promise.all(
      [...predictionsByPark.keys()].map(async (parkId) => {
        const timezone = parkTimezones.get(parkId) || "UTC";
        const [range, isSeasonal] = await Promise.all([
          this.parksService.getOperatingDateRange(parkId, timezone),
          this.parksService.isParkSeasonal(parkId),
        ]);
        parkInfoCache.set(parkId, {
          hasHistory: !!(range.minDate && range.maxDate),
          minDate: range.minDate,
          maxDate: range.maxDate,
          isSeasonal,
          timezone,
        });
      }),
    );

    // ONE schedule query for every park/date pair (was: one per park). The
    // dates are each park's LOCAL day — the schedule's own key (PAR-818).
    const allScheduleDates = [
      ...new Set(
        [...predictionsByPark].flatMap(([parkId, preds]) =>
          preds.map((p) =>
            MLService.localDateOf(
              p.predictedTime,
              parkInfoCache.get(parkId)?.timezone || "UTC",
            ),
          ),
        ),
      ),
    ].map((d) => new Date(d + "T12:00:00Z"));

    const allSchedules = allScheduleDates.length
      ? await this.scheduleEntryRepository.find({
          where: {
            parkId: In([...predictionsByPark.keys()]),
            date: In(allScheduleDates),
            attractionId: IsNull(),
          },
        })
      : [];

    // parkId -> (park-local date string -> scheduleType). The row that states
    // the day's status wins (OPERATING > CLOSED > UNKNOWN > event rows); the
    // query has no order, so last-write-wins would let an event row hide CLOSED.
    const schedulesByPark = new Map<string, Map<string, ScheduleType>>();
    for (const s of allSchedules) {
      const timezone = parkInfoCache.get(s.parkId)?.timezone || "UTC";
      let forPark = schedulesByPark.get(s.parkId);
      if (!forPark) {
        forPark = new Map<string, ScheduleType>();
        schedulesByPark.set(s.parkId, forPark);
      }
      const dateKey = formatInParkTimezone(s.date, timezone);
      const current = forPark.get(dateKey);
      if (
        current === undefined ||
        dayStatusRank(s.scheduleType) < dayStatusRank(current)
      ) {
        forPark.set(dateKey, s.scheduleType);
      }
    }

    for (const [parkId, parkPredictions] of predictionsByPark) {
      const info = parkInfoCache.get(parkId)!;
      const scheduleMap =
        schedulesByPark.get(parkId) ?? new Map<string, ScheduleType>();

      for (const pred of parkPredictions) {
        // The park-local day, not the UTC one: a 20:00 slot in Los Angeles is
        // 03:00 UTC tomorrow and was gated by TOMORROW's schedule (PAR-818).
        const dateStr = MLService.localDateOf(
          pred.predictedTime,
          info.timezone,
        );
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

    // In replace mode an empty run still has work to do: every stored future
    // slot it no longer predicts has to go, as the old delete-first did.
    if (validPredictions.length === 0 && !options.replaceHourlyForPark) {
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
      entity.uncertaintyMinutes = pred.uncertaintyMinutes ?? null;
      entity.crowdLevel =
        pred.crowdLevel === "unknown" ? null : pred.crowdLevel;
      entity.baseline = pred.baseline;
      entity.modelVersion = pred.modelVersion;
      entity.status = pred.status || null;
      return entity;
    });

    if (options.replaceHourlyForPark) {
      await this.diffWriteHourlyPredictions(
        options.replaceHourlyForPark,
        entities,
      );
      if (entities.length === 0) return;
    } else {
      const stored = await this.insertPredictionRows(entities);
      this.logger.debug(`Stored ${stored} predictions in database`);
    }

    // Record predictions for accuracy tracking (feedback loop).
    // No sampling — upsert on (attractionId, targetTime) ensures each future slot
    // is stored exactly once regardless of how many 15-min cycles cover it.
    // Full coverage is required so every ride has data for ML sample weighting.
    //
    // ONLY record hourly predictions — daily predictions span up to 365 days ahead
    // and can't be compared against actuals until those dates arrive, inflating
    // PENDING counts and skewing coverage metrics.
    // UNKNOWN passes on purpose. A scheduled closure never reaches this line —
    // predict.py drops CLOSED rows before inference and schedule_filter drops
    // whatever falls outside an OPERATING window — so the only thing excluding
    // UNKNOWN would remove is hours on days nobody published a schedule for, in
    // parks that were demonstrably running (schedule_filter only keeps such an
    // hour when a ride reports a wait >= 5 min). Measured over 7 days before
    // `status` was declared: 1375 of 2336057 hourly rows, 0.059 %, all of it in
    // three parks with a patchy schedule source. Dropping them would blind the
    // accuracy metric exactly where forecasting is hardest, and it would let our
    // own missing bookkeeping stand in for an operator's statement — see
    // docs/rules/absent-facts.md. null covers rows written before PAR-117.
    // Index-aligned with `entities`, which map `validPredictions` 1:1: the
    // model's q0.8/q0.95 have no column on the hypertable and travel beside it
    // to the accuracy row only (PAR-912).
    const feedbackIdx = entities
      .map((pred, i) => ({ pred, i }))
      .filter(
        ({ pred }) =>
          pred.predictionType === "hourly" &&
          (pred.status === "OPERATING" ||
            pred.status === "UNKNOWN" ||
            pred.status === null),
      );
    const validPredictionsForFeedback = feedbackIdx.map(({ pred }) => pred);
    const feedbackQuantiles = feedbackIdx.map(({ i }) =>
      predictionQuantilesOf(validPredictions[i]),
    );

    if (validPredictionsForFeedback.length < entities.length) {
      this.logger.debug(
        `Filtering: Recording ${validPredictionsForFeedback.length}/${entities.length} predictions (excluding daily predictions)`,
      );
    }

    let recordedCount = 0;
    try {
      // Batched upsert (few multi-row statements, synchronous_commit=off) instead
      // of one round-trip per prediction — avoids the multi-second lock waits the
      // per-row loop produced under the concurrent comparison job.
      // The served value beside the raw one (PAR-854): the per-ride badge reads
      // it, the admin boards and the ride day curve keep reading the raw
      // CatBoost column.
      const served = await this.servedIntradayValues(
        validPredictionsForFeedback,
      );
      recordedCount = await this.predictionAccuracyService.recordPredictions(
        validPredictionsForFeedback,
        served,
        feedbackQuantiles,
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
   * Append rows. Chunked: a single multi-row INSERT for a big park (e.g. 60
   * attractions × 365 daily rows × 12 columns) exceeds the Postgres
   * wire-protocol limit of 65535 bind parameters — the driver then fails with
   * "bind message has N parameter formats but 0 parameters" and the whole park
   * gets no predictions.
   *
   * insert() rather than save(): with the natural primary key every key column
   * is populated up front, so save() could no longer tell a new row from an
   * existing one and would issue an existence check per chunk. These rows are
   * always new (deduplicatePredictions or the diff-write clears the range
   * first). orIgnore() keeps a racing double-generation from failing a whole
   * chunk — the previous surrogate key would have silently written a duplicate
   * row instead.
   */
  private async insertPredictionRows(
    entities: WaitTimePrediction[],
  ): Promise<number> {
    const CHUNK = 1000;
    let stored = 0;
    for (let i = 0; i < entities.length; i += CHUNK) {
      const result = await this.predictionRepository
        .createQueryBuilder()
        .insert()
        .into(WaitTimePrediction)
        // Cast: the builder's partial-entity type rejects the jsonb `features`
        // column; these are fully built entities. Same workaround as the
        // chunked upsert in prediction-accuracy.service.ts.

        .values(entities.slice(i, i + CHUNK) as any)
        .orIgnore()
        .execute();
      stored += result.identifiers?.length ?? 0;
    }
    return stored;
  }

  /**
   * Replace a park's future hourly slots with this run's answer, writing only
   * what changed (PAR-874). See `prediction-diff-write.util.ts` for why the
   * comparison happens here rather than in an `ON CONFLICT`.
   *
   * Order matters for readers, who never see a hole: the new rows go in first
   * (for a moment a changed slot holds two rows, and every reader takes the
   * newest), then the superseded rows are deleted, then the rides are marked
   * fresh. No transaction is needed for that, and none is held across the
   * insert of a few thousand rows.
   */
  private async diffWriteHourlyPredictions(
    parkId: string,
    entities: WaitTimePrediction[],
  ): Promise<void> {
    const now = new Date();
    const runCreatedAt = now;
    const hourly = entities.filter((e) => e.predictionType === "hourly");
    const other = entities.filter((e) => e.predictionType !== "hourly");
    for (const e of hourly) e.createdAt = runCreatedAt;

    // The park's rides, resolved separately rather than joined into the read
    // below. `attractions` is the table the nightly `drop_chunks` has to take
    // ACCESS EXCLUSIVE on (this hypertable references it), and while that lock
    // queues, every query touching `attractions` queues behind it — see the
    // comment on the chunk-retention job for the 25 consecutive 5-minute stalls
    // that taught us. A read running ~96 times a day per park does not belong in
    // that queue, and the id list also drives the
    // `(attractionId, predictionType, createdAt)` index prefix directly.
    // All of the park's rides, not just the ones this run predicts: a slot can
    // only be recognised as dropped if its ride is in the comparison.
    const attractions = await this.attractionRepository.find({
      where: { parkId },
      select: {
        id: true,
      },
    });
    const attractionIds = attractions.map((a) => a.id);
    if (attractionIds.length === 0) {
      await this.insertPredictionRows(entities);
      return;
    }

    const stored: StoredSlotRow[] = await this.predictionRepository.manager
      .query(
        `SELECT p."attractionId"::text AS "attractionId",
                p."predictedTime", p."createdAt",
                p."predictedWaitTime", p.confidence, p."uncertaintyMinutes",
                p."crowdLevel"::text AS "crowdLevel", p.status, p.baseline,
                p."modelVersion"
           FROM wait_time_predictions p
          WHERE p."attractionId" = ANY($1::uuid[])
            AND p."predictionType" = 'hourly'
            AND p."createdAt" >= $2
            AND p."predictedTime" >= $3`,
        [
          attractionIds,
          new Date(now.getTime() - HOURLY_LOOKBACK_MS).toISOString(),
          now.toISOString(),
        ],
      )
      .then((rows: Array<Record<string, unknown>>) =>
        rows.map((r) => ({
          attractionId: String(r.attractionId),
          predictedTime: new Date(r.predictedTime as string | Date),
          createdAt: new Date(r.createdAt as string | Date),
          predictedWaitTime: Number(r.predictedWaitTime),
          confidence: r.confidence == null ? null : Number(r.confidence),
          uncertaintyMinutes:
            r.uncertaintyMinutes == null ? null : Number(r.uncertaintyMinutes),
          crowdLevel: (r.crowdLevel as string | null) ?? null,
          status: (r.status as string | null) ?? null,
          baseline: r.baseline == null ? null : Number(r.baseline),
          modelVersion: String(r.modelVersion),
        })),
      );

    const plan = planHourlyPredictionWrite(stored, hourly, now, runCreatedAt);

    const inserted = await this.insertPredictionRows([
      ...plan.insert,
      ...other,
    ]);

    const DELETE_CHUNK = 2000;
    let deleted = 0;
    for (let i = 0; i < plan.delete.length; i += DELETE_CHUNK) {
      const slice = plan.delete.slice(i, i + DELETE_CHUNK);
      const rows: Array<{ affected: string }> =
        await this.predictionRepository.manager.query(
          `WITH del AS (
             DELETE FROM wait_time_predictions w
              USING unnest($1::uuid[], $2::timestamptz[], $3::timestamptz[])
                    AS d(aid, pt, before)
              WHERE w."predictionType" = 'hourly'
                AND w."createdAt" >= $4
                AND w."attractionId" = d.aid
                AND w."predictedTime" = d.pt
                AND w."createdAt" < d.before
              RETURNING 1
           ) SELECT count(*)::text AS affected FROM del`,
          [
            slice.map((d) => d.attractionId),
            slice.map((d) => d.predictedTime.toISOString()),
            slice.map((d) => d.before.toISOString()),
            new Date(now.getTime() - HOURLY_LOOKBACK_MS).toISOString(),
          ],
        );
      deleted += Number(rows?.[0]?.affected ?? 0);
    }

    await this.markHourlyPredictionsFresh([
      ...new Set(hourly.map((e) => e.attractionId)),
    ]);

    this.logger.debug(
      `Hourly diff-write park ${parkId}: ${plan.unchanged} unchanged, ` +
        `${plan.changed} changed, ${plan.added} new, ${plan.dropped} dropped — ` +
        `inserted ${inserted}, deleted ${deleted}`,
    );
  }

  /**
   * Freshness used to be read off each row's `createdAt` (written in the last
   * 30 min). Since the diff-write leaves an unchanged row with the `createdAt`
   * of the run that last CHANGED it, a run now vouches for its rides here
   * instead. Best effort: on a Redis failure the readers fall back to the old
   * per-row test, which can only under-report freshness, never invent it.
   */
  private async markHourlyPredictionsFresh(
    attractionIds: string[],
  ): Promise<void> {
    if (attractionIds.length === 0) return;
    try {
      const pipeline = this.redis.pipeline();
      const ttlS = Math.round(HOURLY_FRESHNESS_MS / 1000);
      const stamp = new Date().toISOString();
      for (const id of attractionIds) {
        pipeline.set(hourlyPredictionsFreshKey(id), stamp, "EX", ttlS);
      }
      await pipeline.exec();
    } catch (e: unknown) {
      this.logger.warn(
        `Could not mark hourly predictions fresh: ${e instanceof Error ? e.message : e}`,
      );
    }
  }

  /**
   * Which of these rides have an hourly forecast a reader may serve: the
   * generator vouched for them within {@link HOURLY_FRESHNESS_MS} (Redis
   * marker), or — the pre-PAR-874 test, kept as a second chance — one of their
   * rows was written that recently.
   *
   * How much that second chance is worth: it carries the deploy window (every
   * row is freshly written by the old path), and after that only a ride that
   * had at least one CHANGED slot in the last 30 minutes. A ride whose curve
   * sits unchanged writes no rows at all, so with the marker gone it is not
   * servable — hourly serving genuinely depends on Redis now. On an `mget`
   * error this stays fail-closed (the reader falls back to row age, which can
   * only under-report freshness, never invent it); making it fail open on a
   * connection error is the follow-up to weigh, not a thing to change in the
   * same deploy.
   */
  private async freshHourlyAttractions(
    rows: ReadonlyArray<Pick<WaitTimePrediction, "attractionId" | "createdAt">>,
  ): Promise<Set<string>> {
    const ids = [...new Set(rows.map((r) => r.attractionId))];
    const fresh = new Set<string>();
    if (ids.length === 0) return fresh;
    const cutoff = Date.now() - HOURLY_FRESHNESS_MS;
    for (const r of rows) {
      if (r.createdAt.getTime() >= cutoff) fresh.add(r.attractionId);
    }
    const unknown = ids.filter((id) => !fresh.has(id));
    if (unknown.length === 0) return fresh;
    try {
      const marks = await this.redis.mget(
        ...unknown.map((id) => hourlyPredictionsFreshKey(id)),
      );
      unknown.forEach((id, i) => {
        if (marks[i] != null) fresh.add(id);
      });
    } catch (e: unknown) {
      this.logger.warn(
        `Hourly freshness markers unreadable, using row age only: ${e instanceof Error ? e.message : e}`,
      );
    }
    return fresh;
  }

  /**
   * The rows a reader is served: newest per slot, of the rides with a fresh
   * forecast. Hourly only — daily rows keep the per-row `createdAt` window.
   */
  private async servableHourlyRows(
    rows: WaitTimePrediction[],
  ): Promise<WaitTimePrediction[]> {
    const fresh = await this.freshHourlyAttractions(rows);
    return newestPerSlot(rows.filter((r) => fresh.has(r.attractionId)));
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

    // Hourly without a start would otherwise return 50 h of past slots now that
    // the createdAt window is the generation horizon: start at the current slot.
    const from =
      startTime ??
      (predictionType === "hourly"
        ? new Date(currentSlotStartMs())
        : undefined);
    if (from) {
      queryBuilder.andWhere("p.predictedTime >= :startTime", {
        startTime: from,
      });
    }

    if (endTime) {
      queryBuilder.andWhere("p.predictedTime <= :endTime", { endTime });
    }

    queryBuilder.andWhere("p.createdAt >= :createdAtCutoff", {
      createdAtCutoff: storedPredictionsCreatedAtCutoff(predictionType),
    });

    const rows = await queryBuilder.getMany();
    return predictionType === "hourly" ? this.servableHourlyRows(rows) : rows;
  }

  async getBatchStoredPredictions(
    attractionIds: string[],
    predictionType: "hourly" | "daily" = "hourly",
    startTime?: Date,
  ): Promise<Map<string, PredictionDto[]>> {
    if (attractionIds.length === 0) {
      return new Map();
    }

    const createdAtCutoff = storedPredictionsCreatedAtCutoff(predictionType);

    const queryBuilder = this.predictionRepository
      .createQueryBuilder("p")
      .where("p.attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("p.predictionType = :predictionType", { predictionType })
      .andWhere("p.createdAt >= :createdAtCutoff", { createdAtCutoff })
      .orderBy("p.predictedTime", "ASC");

    // Same default as getStoredPredictions: hourly starts at the current slot.
    const from =
      startTime ??
      (predictionType === "hourly"
        ? new Date(currentSlotStartMs())
        : undefined);
    if (from) {
      queryBuilder.andWhere("p.predictedTime >= :startTime", {
        startTime: from,
      });
    }

    const fetched = await queryBuilder.getMany();
    const rows =
      predictionType === "hourly"
        ? await this.servableHourlyRows(fetched)
        : fetched;

    const result = new Map<string, PredictionDto[]>();
    for (const p of rows) {
      const list = result.get(p.attractionId) ?? [];
      list.push({
        attractionId: p.attractionId,
        predictedTime: p.predictedTime.toISOString(),
        predictedWaitTime: p.predictedWaitTime,
        predictionType: p.predictionType,
        confidence: p.confidence,
        uncertaintyMinutes: p.uncertaintyMinutes,
        crowdLevel: p.crowdLevel ?? "unknown",
        baseline: p.baseline,
        modelVersion: p.modelVersion,
        status: p.status || undefined,
      });
      result.set(p.attractionId, list);
    }

    if (this.serveIntradayOverride && predictionType === "hourly") {
      await this.applyServedIntradayOverride(
        [...result.values()].flat(),
        startTime,
      );
    }
    return result;
  }

  /** Champion-swap flag: serve PCN's intraday 15-min forecast in place of CatBoost's
   * `hourly` predictions (PCN beats CatBoost on the matched board). Default OFF —
   * enable per env once the win is confirmed over a few days. Always falls back to
   * CatBoost where PCN has no forecast (or the table/flag is absent).
   *
   * Since PAR-911 the read paths ask whether EITHER served-model override is on:
   * the PCN champion swap or the Chronos-2 30–120 min window
   * (`SERVE_CHRONOS_INTRADAY`, also default OFF). With both off every read path
   * returns CatBoost untouched, exactly as before. */
  private get serveIntradayOverride(): boolean {
    return servePcnIntradayFlag() || serveChronosIntraday();
  }

  /** Freshest forward PCN forecast per (attraction, slot) for upcoming slots, keyed by
   * the slot's epoch milliseconds. Never by an ISO string: the DB read paths hand
   * CatBoost's predictedTime over as `…T12:15:00.000Z` (`Date.toISOString()`), the
   * park-level path as python's `…T12:15:00+00:00` (`isoformat()`) — the same instant
   * spelled two ways, and a string key matched only the first (PAR-817). Carries BOTH served
   * quantiles per slot: q0.5 → `display` (the shown wait), q0.8 → `crowd` (the crowd
   * signal) — mirroring the CatBoost per-purpose split
   * (docs/ml/quantile-serving-and-calibration.md). pcn_forecasts stores park-LOCAL naive
   * slots, so `target_slot AT TIME ZONE tz` recovers the instant. Returns an empty map
   * on any error (missing table / flag misuse) → pure CatBoost.
   *
   * STALENESS GUARD: only consider forecasts written in the last `PCN_MAX_FORECAST_AGE_H`
   * hours. PCN's whole edge is being a nowcaster (recent observations); a frozen producer
   * (the 15-min forecast cron stalling — happened 2026-06-30, stuck lock) would otherwise keep
   * serving 24h-old waits live instead of degrading to CatBoost. Under healthy operation the
   * newest row is <15 min old, so this never fires; it only trips on a real outage. Bonus:
   * stale-input parks (forecast skipped) auto-fall-back to CatBoost too. */
  private async getPcnIntradayWaits(
    attractionIds: string[],
    startTime?: Date,
  ): Promise<Map<string, Map<number, { display: number; crowd?: number }>>> {
    const out = new Map<
      string,
      Map<number, { display: number; crowd?: number }>
    >();
    if (attractionIds.length === 0) return out;
    try {
      const rows: Array<{
        aid: string;
        predicted_time: Date;
        quantile: number;
        wait: string;
      }> = await this.predictionRepository.manager.query(
        `SELECT DISTINCT ON (f.attraction_id, f.target_slot, f.quantile)
            f.attraction_id::text AS aid,
            (f.target_slot AT TIME ZONE p.timezone) AS predicted_time,
            f.quantile AS quantile,
            f.predicted_wait AS wait
         FROM pcn_forecasts f
         JOIN attractions a ON a.id = f.attraction_id
         JOIN parks p ON p.id = a."parkId"
         WHERE f.attraction_id = ANY($1::uuid[])
           AND f.quantile IN (0.5, 0.8)
           AND f.created_at >= now() - ($3 || ' hours')::interval
           AND (f.target_slot AT TIME ZONE p.timezone) >= COALESCE($2, now())
         ORDER BY f.attraction_id, f.target_slot, f.quantile, f.origin_slot DESC`,
        [attractionIds, startTime ?? null, PCN_MAX_FORECAST_AGE_H],
      );
      for (const r of rows) {
        const m =
          out.get(r.aid) ??
          new Map<number, { display: number; crowd?: number }>();
        const key = new Date(r.predicted_time).getTime();
        const entry = m.get(key) ?? { display: NaN };
        if (Math.abs(Number(r.quantile) - 0.8) < 1e-6) {
          entry.crowd = Number(r.wait);
        } else {
          entry.display = Number(r.wait);
        }
        m.set(key, entry);
        out.set(r.aid, m);
      }
      // A slot is servable only with a display (q0.5) value; drop crowd-only strays.
      for (const m of out.values()) {
        for (const [key, entry] of m) {
          if (!Number.isFinite(entry.display)) m.delete(key);
        }
      }
    } catch (e: unknown) {
      this.logger.warn(
        `PCN intraday override skipped: ${e instanceof Error ? e.message : e}`,
      );
    }
    return out;
  }

  /**
   * What a reader is served RIGHT NOW for each of these freshly generated hourly
   * rows, and which model it came from (PAR-854).
   *
   * Computed exactly as {@link getParkPredictions} computes it for the park
   * page, `/plan/day` and the ride page — copies of the CatBoost rows, the PCN
   * override and the §7.7 persistence blend applied from the current slot — so
   * the number scored later is the number shown at the time, with the blend at
   * the lead it had then. It is never re-derived after the fact: the blend's
   * anchor is "the wait now", and recomputing it once the slot has happened
   * would hand the forecast the answer.
   *
   * `chronos` where the Chronos-2 serving override (PAR-911) produced the
   * slot. Chronos serves only 45–120 min ahead, so under the two-hour scored
   * lead below it is recorded only where the two windows meet — see
   * docs/ml/forward-archive.md §10.4 before reading the badge as a Chronos
   * figure.
   *
   * `catboost` wherever CatBoost is what was served: the flag off, no PCN
   * forecast for that ride and slot, a producer older than the staleness guard,
   * or the PCN read failing (the override swallows that and falls back too).
   *
   * `null` for a slot less than {@link SERVED_ACCURACY_MIN_LEAD_MS} away: what
   * is served that close is mostly the reading of the moment, and scoring it
   * would grade the queue against itself. `null` tells the recorder to leave
   * the row's served columns as they are, so the value kept is the last one
   * shown at least that far ahead.
   */
  async servedIntradayValues(
    rows: Array<
      Pick<
        WaitTimePrediction,
        | "attractionId"
        | "predictedTime"
        | "predictedWaitTime"
        | "predictionType"
        | "baseline"
        | "crowdLevel"
        | "modelVersion"
        | "confidence"
      >
    >,
    now: number = Date.now(),
  ): Promise<Array<ServedIntradayValue | null>> {
    if (rows.length === 0) return [];
    const copies: PredictionDto[] = rows.map((r) => ({
      attractionId: r.attractionId,
      predictedTime: r.predictedTime.toISOString(),
      predictedWaitTime: r.predictedWaitTime,
      predictionType: r.predictionType,
      confidence: r.confidence,
      crowdLevel: r.crowdLevel ?? "unknown",
      baseline: r.baseline,
      modelVersion: r.modelVersion,
    }));
    const overridden = this.serveIntradayOverride
      ? await this.applyServedIntradayOverride(
          copies,
          new Date(currentSlotStartMs()),
          true,
          now,
        )
      : new Map<PredictionDto, "pcn" | "chronos">();
    return copies.map((c, i) =>
      rows[i].predictedTime.getTime() - now < SERVED_ACCURACY_MIN_LEAD_MS
        ? null
        : {
            wait: c.predictedWaitTime,
            source: overridden.get(c) ?? "catboost",
          },
    );
  }

  /**
   * Every served-model override, in order, on these rows (in place), and which
   * model each changed row ended on.
   *
   * 1. PCN + §7.7 persistence blend (`SERVE_PCN_INTRADAY`) on every slot it has.
   * 2. Chronos-2 (`SERVE_CHRONOS_INTRADAY`, PAR-911) on the 45–120 min slots
   *    its cached curve covers — so ≤ 30 min stays the blend (persistence wins
   *    at 15 min) and > 120 min stays PCN, and a stale or missing curve leaves
   *    whatever step 1 served.
   *
   * Chronos's lead base is the current slot start, never the caller's
   * `startTime`: a read that starts later must not shift Chronos's window.
   * `nowMs` is the moment the blend's lead is measured from (default: now).
   */
  private async applyServedIntradayOverride(
    preds: PredictionDto[],
    startTime?: Date,
    withChronos = true,
    nowMs: number = Date.now(),
  ): Promise<Map<PredictionDto, "pcn" | "chronos">> {
    const out = new Map<PredictionDto, "pcn" | "chronos">();
    if (servePcnIntradayFlag()) {
      for (const p of await this.applyPcnIntradayOverride(
        preds,
        startTime,
        nowMs,
      )) {
        out.set(p, "pcn");
      }
    }
    if (withChronos && serveChronosIntraday()) {
      const hourly = preds.filter((p) => p.predictionType === "hourly");
      if (hourly.length > 0) {
        const curves = await readChronosServeCache(this.redis, [
          ...new Set(hourly.map((p) => p.attractionId)),
        ]);
        const done = applyChronosOverride(
          hourly,
          curves,
          currentSlotStartMs(),
          nowMs,
        );
        for (const p of done) out.set(p, "chronos");
        if (done.size > 0) {
          this.logger.debug(
            `Chronos intraday override: ${done.size}/${hourly.length} slots`,
          );
        }
      }
    }
    return out;
  }

  /** Override matched hourly predictions in place with PCN's q0.5 wait, recomputing
   * crowdLevel from PCN's q0.8 against the carried baseline (the same per-purpose
   * quantile split CatBoost serves: median display, upper-quantile crowd signal).
   * Unmatched slots/attractions keep CatBoost (the fallback). Mutates `preds`, and
   * returns the rows it overrode — the caller that records which model was served
   * needs that, and reading it back off the `+pcn` suffix would tie the accuracy
   * label to a display string. */
  private async applyPcnIntradayOverride(
    preds: PredictionDto[],
    startTime?: Date,
    nowMs: number = Date.now(),
  ): Promise<Set<PredictionDto>> {
    const done = new Set<PredictionDto>();
    const hourly = preds.filter((p) => p.predictionType === "hourly");
    if (hourly.length === 0) return done;
    const ids = [...new Set(hourly.map((p) => p.attractionId))];
    const [pcn, anchors] = await Promise.all([
      this.getPcnIntradayWaits(ids, startTime),
      this.fetchCurrentWaits(ids), // §7.7 blend anchor: latest realised wait per attraction
    ]);
    // Lead from `nowMs` (the actual time), not from `startTime` (the current slot start
    // or a caller's read start): on the 1–3 h ramp α follows the clock inside a slot.
    let overridden = 0;
    for (const p of hourly) {
      // Compare instants, not strings — see getPcnIntradayWaits.
      const slotMs = Date.parse(p.predictedTime);
      const q = pcn.get(p.attractionId)?.get(slotMs);
      if (q === undefined) continue;
      // §7.7 persistence blend: at short lead the wait "L hours from now" is better predicted
      // by "the wait now" than by the model — the current wait in the first hour (any anchor
      // ≥ 5 min, PAR-1026), a ramp to pure PCN by 3h for rides with a real wait (≥ 40 min).
      const leadHours = (slotMs - nowMs) / 3_600_000;
      const anchor = anchors.get(p.attractionId);
      const displayWait = persistenceBlendServe(q.display, anchor, leadHours);
      // Same serve-side quantization CatBoost applies before storing (5er steps,
      // floor 5 when positive) — a plain Math.round leaked 1-minute waits into the UI.
      p.predictedWaitTime = roundServedWait(displayWait);
      // Never leave CatBoost's band next to a PCN wait. pcn_forecasts holds no q0.95
      // (PCN_SERVE_QUANTILES = 0.5,0.8), so there is no PCN band to serve.
      p.uncertaintyMinutes = null;
      if (p.baseline && p.baseline > 0) {
        // Crowd signal from q0.8, blended with the same α as the display so the tier and the
        // shown wait agree (fallback to the blended display for rows written before q0.8
        // serving); clamp to >= the BLENDED display so the served display/crowd pair stays
        // monotone (roundServedWait is monotone, so the rounded crowd stays >= the display).
        const crowdBlend =
          q.crowd === undefined
            ? displayWait
            : persistenceBlendServe(q.crowd, anchor, leadHours);
        const crowdWait = roundServedWait(Math.max(crowdBlend, displayWait));
        // determineCrowdLevel never returns "unknown" (we guard baseline>0), so it is a
        // valid PredictionDto crowdLevel — narrow the wider CrowdLevel type with a cast.
        p.crowdLevel = determineCrowdLevel(
          (crowdWait / p.baseline) * 100,
        ) as PredictionDto["crowdLevel"];
      }
      p.modelVersion = `${p.modelVersion ?? "catboost"}+pcn`;
      done.add(p);
      overridden++;
    }
    if (overridden > 0) {
      this.logger.debug(
        `PCN intraday override: ${overridden}/${hourly.length} slots`,
      );
    }
    return done;
  }

  /**
   * Latest realised STANDBY wait per attraction (within the last 3h) — the anchor for the
   * §7.7 persistence blend. The reading in force must be OPERATING with a wait (the rule of
   * `ForecastArchiveService.liveWaits`): a ride that just went DOWN still carries its last
   * wait, and blending toward it would serve a queue that is not there (PAR-1029).
   * Non-fatal: a missing anchor just leaves that ride's forecast raw.
   */
  private async fetchCurrentWaits(ids: string[]): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    if (ids.length === 0) return out;
    try {
      const rows = await this.queueDataRepository
        .createQueryBuilder("q")
        .distinctOn(["q.attractionId"])
        .where("q.attractionId = ANY(:ids)", { ids })
        .andWhere("q.timestamp >= :recent", {
          recent: new Date(Date.now() - 3 * 60 * 60 * 1000),
        })
        .andWhere("q.queueType = :queueType", { queueType: QueueType.STANDBY })
        .orderBy("q.attractionId")
        .addOrderBy("q.timestamp", "DESC")
        .getMany();
      for (const r of rows) {
        if (
          r.status === LiveStatus.OPERATING &&
          r.waitTime !== null &&
          r.waitTime !== undefined
        ) {
          out.set(r.attractionId, r.waitTime);
        }
      }
    } catch (err) {
      this.logger.debug(`PCN blend anchor fetch failed: ${err}`);
    }
    return out;
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
        uncertaintyMinutes: p.uncertaintyMinutes,
        crowdLevel: p.crowdLevel ?? "unknown",
        baseline: p.baseline,
        modelVersion: p.modelVersion,
        status: p.status || undefined,
      }));
      if (this.serveIntradayOverride && predictionType === "hourly") {
        await this.applyServedIntradayOverride(dtos, startTime);
      }
      return dtos;
    }

    // Fall back to ML service (new ride or predictions expired) — the served-model
    // override applies here too, so the fallback path shows the same numbers.
    const live = await this.getAttractionPredictions(
      attractionId,
      predictionType,
    );
    if (this.serveIntradayOverride && predictionType === "hourly") {
      await this.applyServedIntradayOverride(live, startTime);
    }
    return live;
  }

  /**
   * Deletes predictions whose `predictedTime` is before `cutoffDate`, from the
   * UNCOMPRESSED chunks only.
   *
   * `predictedTime` is not the partition key (`createdAt` is), so an unbounded
   * DELETE on it plans every chunk, and TimescaleDB has to decompress the
   * matching batches of every compressed chunk before it can delete from them.
   * Decompression writes the rows back through the chunk's primary key, and a
   * compressed chunk never enforced that key: the two July chunks held 1051 and
   * 1267 duplicate keys, and from 2026-10-10 on the nightly cleanup died on
   * `duplicate key value violates unique constraint "…_wait_time_predictions_pkey"`
   * as soon as the cutoff reached them (PAR-900).
   *
   * Leaving compressed chunks out loses nothing: they are past the 14-day
   * compression horizon, and the 90-day chunk drop that runs first in the same
   * job ({@link dropExpiredPredictionChunks}) removes them whole. On production
   * a daily row's `predictedTime` is never earlier than its `createdAt` (the
   * shortest lead is ~11 h), so a row past the 90-day cutoff lives in a chunk
   * older than 90 days — in practice this deletes nothing and is a guard for
   * the case where compression is behind or switched off.
   *
   * The bound is the end of the newest compressed chunk, so no compressed chunk
   * can sit above it even when an old one was decompressed by hand. It goes into
   * the SQL as a constant rather than a parameter on purpose: a constant lets
   * the planner leave those chunks out of the plan entirely, where a parameter
   * under a generic plan would only exclude them at executor startup with the
   * chunks still listed as delete targets. The value comes from our own catalog
   * read and is rendered by `toISOString()`, so nothing outside reaches the SQL.
   *
   * The `createdAt` bound is the safeguard: it keeps the DELETE off every chunk
   * that was compressed at lookup time. The decompression limit
   * (`max_tuples_decompressed_per_dml_transaction`, default 100 000) is left at
   * its default and only caps the damage of the remaining race — a chunk
   * compressed between the lookup and the DELETE would be unpacked up to that
   * limit, not refused. In practice nothing matches there: compression takes
   * chunks older than 14 days, the DELETE only rows predicted > 90 days ago.
   */
  async deleteOldPredictions(
    predictionType: "hourly" | "daily",
    cutoffDate: Date,
  ): Promise<{ deleted: number; createdFrom: Date | null }> {
    const createdFrom = await this.getNewestCompressedPredictionChunkEnd();
    const createdBound = createdFrom
      ? ` AND "createdAt" >= '${createdFrom.toISOString()}'::timestamptz`
      : "";

    const rows: Array<{ affected: string }> =
      await this.predictionRepository.manager.query(
        `WITH del AS (
           DELETE FROM wait_time_predictions
           WHERE "predictionType" = $1 AND "predictedTime" < $2${createdBound}
           RETURNING 1
         ) SELECT count(*)::text AS affected FROM del`,
        [predictionType, cutoffDate],
      );

    return { deleted: Number(rows?.[0]?.affected ?? 0), createdFrom };
  }

  /**
   * End of the newest compressed `wait_time_predictions` chunk, or null when no
   * chunk is compressed. Everything at or after it is uncompressed. Reads the
   * catalog only.
   */
  private async getNewestCompressedPredictionChunkEnd(): Promise<Date | null> {
    const rows: Array<{ boundary: Date | string | null }> =
      await this.predictionRepository.manager.query(
        `SELECT max(range_end) AS boundary
           FROM timescaledb_information.chunks
          WHERE hypertable_schema = 'public'
            AND hypertable_name = 'wait_time_predictions'
            AND is_compressed`,
      );
    const boundary = rows?.[0]?.boundary ?? null;
    if (boundary === null) return null;
    const date = boundary instanceof Date ? boundary : new Date(boundary);
    // A value that does not parse must not silently drop the bound.
    if (Number.isNaN(date.getTime())) {
      throw new Error(
        `Unreadable compressed-chunk boundary for wait_time_predictions: ${String(boundary)}`,
      );
    }
    return date;
  }

  /**
   * Drops whole `wait_time_predictions` chunks older than `retainDays`, without
   * ever letting the drop queue up the rest of the database behind it.
   *
   * This replaces the TimescaleDB retention policy (job 1007), and the reason is
   * a lock on a table this hypertable does not even contain: `drop_chunks`
   * begins by taking an ACCESS EXCLUSIVE lock on every table the hypertable has
   * a foreign key to (TimescaleDB `lock_referenced_tables`, a guard against
   * deadlocks), and `wait_time_predictions` references `attractions`. It takes
   * that lock on every run, whether a chunk is due or not. While the request
   * waits for some long-lived reader of `attractions` to finish, every new
   * query that touches `attractions` — nearly every API read — queues behind
   * it. The policy waited for its full five-minute `max_runtime`, 25 times in a
   * row on 2026-10-07/08, and each time ~30–70 API queries finished in the same
   * second the attempt gave up (docs/troubleshooting/db-health-runbook.md §0b).
   *
   * Three things make this version safe:
   *
   * 1. `show_chunks` first. It reads the catalog and locks nothing, so on the
   *    six days a week when no chunk has aged out, `attractions` is not touched.
   * 2. `lock_timeout` on the drop itself, local to its transaction. If the lock
   *    is not free within `lockTimeoutMs`, Postgres gives up the request instead
   *    of holding the queue, so readers wait that long at most — whoever holds
   *    the lock and however long they hold it.
   * 3. A few short retries, `retryDelayMs` apart. The job runs at 03:30 UTC,
   *    the busiest batch window, where one 2 s attempt is easily unlucky; a
   *    later attempt usually finds a gap. Each attempt still queues readers
   *    for at most `lockTimeoutMs`.
   *
   * Never throws for a lock timeout — that is the designed outcome when the
   * table is busy, not a failure. When every attempt timed out, `overdue`
   * counts the chunks that have been due for more than `overdueAfterDays`
   * already: the caller's signal that the misses are no longer one-offs.
   */
  async dropExpiredPredictionChunks(
    retainDays: number,
    options: {
      lockTimeoutMs?: number;
      attempts?: number;
      retryDelayMs?: number;
      overdueAfterDays?: number;
    } = {},
  ): Promise<{
    due: number;
    dropped: number;
    lockTimedOut: boolean;
    attempts: number;
    overdue: number;
  }> {
    const lockTimeoutMs = options.lockTimeoutMs ?? 2000;
    const maxAttempts = Math.max(1, options.attempts ?? 4);
    const retryDelayMs = options.retryDelayMs ?? 30_000;
    const overdueAfterDays = options.overdueAfterDays ?? 14;
    const olderThan = `${retainDays} days`;

    const due = await this.countPredictionChunksOlderThan(olderThan);
    if (due === 0) {
      return {
        due: 0,
        dropped: 0,
        lockTimedOut: false,
        attempts: 0,
        overdue: 0,
      };
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1 && retryDelayMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
      try {
        const dropped: Array<{ chunk: string }> =
          await this.predictionRepository.manager.transaction(async (em) => {
            // set_config(..., true) is SET LOCAL: it ends with this transaction
            // and never leaks onto the pooled connection.
            await em.query(`SELECT set_config('lock_timeout', $1, true)`, [
              `${Math.max(1, Math.round(lockTimeoutMs))}ms`,
            ]);
            return em.query(
              `SELECT c::text AS chunk
                 FROM drop_chunks('wait_time_predictions', older_than => $1::interval) AS c`,
              [olderThan],
            );
          });
        return {
          due,
          dropped: dropped.length,
          lockTimedOut: false,
          attempts: attempt,
          overdue: 0,
        };
      } catch (error) {
        // 55P03 lock_not_available: the lock_timeout above fired — retry.
        if ((error as { code?: string })?.code !== "55P03") throw error;
      }
    }

    const overdue = await this.countPredictionChunksOlderThan(
      `${retainDays + overdueAfterDays} days`,
    );
    return {
      due,
      dropped: 0,
      lockTimedOut: true,
      attempts: maxAttempts,
      overdue,
    };
  }

  /** `wait_time_predictions` chunks lying entirely before now - `olderThan`. */
  private async countPredictionChunksOlderThan(
    olderThan: string,
  ): Promise<number> {
    const rows: Array<{ chunk: string }> =
      await this.predictionRepository.manager.query(
        `SELECT c::text AS chunk
           FROM show_chunks('wait_time_predictions', older_than => $1::interval) AS c`,
        [olderThan],
      );
    return rows.length;
  }

  /**
   * Oldest `createdAt` still present for a prediction type, or null when there
   * are none. Served by the ["predictionType", "createdAt"] index.
   */
  async getOldestPredictionCreatedAt(
    predictionType: "hourly" | "daily",
  ): Promise<Date | null> {
    const rows: Array<{ oldest: Date | null }> =
      await this.predictionRepository.manager.query(
        `SELECT MIN("createdAt") AS oldest
           FROM wait_time_predictions
          WHERE "predictionType" = $1`,
        [predictionType],
      );
    return rows?.[0]?.oldest ?? null;
  }

  /**
   * Purges hourly predictions created before `cutoffDate`, one day-window at a
   * time.
   *
   * Two things make this different from {@link deleteOldPredictions}:
   *
   * 1. It prunes on `createdAt`, the hypertable's PARTITION KEY, so Timescale
   *    can exclude whole chunks. The old `predictedTime` predicate is unrelated
   *    to the partitioning, so every chunk had to be opened and decompressed.
   *    The substitution is safe for hourly only: lead time is bounded by the
   *    generation horizon (`HOURLY_PREDICTIONS`, 48h), so a row created before
   *    `cutoff` can never have been predicting for a time later than
   *    `cutoff + 48h` — the caller adds those two days of slack. Daily
   *    predictions run up to ~1 year ahead, where
   *    `createdAt` says nothing about the target, so they keep the old path.
   *
   * 2. It works in bounded day-windows instead of one statement. Each window
   *    touches one or two chunks, so a run cannot blow up into an unbounded
   *    decompression — which matters most on the first pass, where a long-dead
   *    cron has left a large backlog behind.
   *
   * Resumable: progress is the data itself, so an interrupted run simply
   * continues from the oldest remaining window next time.
   */
  async purgeHourlyPredictionsBefore(
    cutoffDate: Date,
    options: { maxWindows?: number; timeBudgetMs?: number } = {},
  ): Promise<{ deleted: number; windows: number; done: boolean }> {
    const maxWindows = options.maxWindows ?? 20;
    const timeBudgetMs = options.timeBudgetMs ?? 10 * 60 * 1000;
    const startedAt = Date.now();

    const oldest = await this.getOldestPredictionCreatedAt("hourly");
    if (!oldest || oldest >= cutoffDate) {
      return { deleted: 0, windows: 0, done: true };
    }

    // Align to a UTC day boundary so windows line up run over run.
    let windowStart = new Date(
      Date.UTC(
        oldest.getUTCFullYear(),
        oldest.getUTCMonth(),
        oldest.getUTCDate(),
      ),
    );

    let deleted = 0;
    let windows = 0;

    while (windowStart < cutoffDate && windows < maxWindows) {
      if (Date.now() - startedAt > timeBudgetMs) break;

      const nextStart = new Date(windowStart.getTime() + 24 * 60 * 60 * 1000);
      const windowEnd = nextStart < cutoffDate ? nextStart : cutoffDate;

      const rows: Array<{ affected: string }> =
        await this.predictionRepository.manager.transaction(async (em) => {
          await em.query(
            `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0`,
          );
          return em.query(
            `WITH del AS (
               DELETE FROM wait_time_predictions
               WHERE "predictionType" = 'hourly'
                 AND "createdAt" >= $1
                 AND "createdAt" < $2
               RETURNING 1
             ) SELECT count(*)::text AS affected FROM del`,
            [windowStart, windowEnd],
          );
        });

      deleted += Number(rows?.[0]?.affected ?? 0);
      windows++;
      windowStart = nextStart;
    }

    return { deleted, windows, done: windowStart >= cutoffDate };
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
      // Every future hourly prediction, so stale entries from prior runs cannot
      // accumulate alongside the new ones. 48h is no longer the generous
      // over-reach it was at a 24h horizon — it now EQUALS the generation
      // horizon (`HOURLY_PREDICTIONS`), and only just covers it: the last slot
      // of a run sits at most 48h past the generating service's clock, and this
      // runs after that call, so its `now` is the later one. Raise
      // HOURLY_PREDICTIONS past 48 and this window has to grow with it.
      endTime.setHours(endTime.getHours() + 48);
    } else {
      // Delete predictions for next 60 days to cover extended daily forecasts
      endTime.setDate(endTime.getDate() + 60);
    }

    // Get all attraction IDs for this park
    const attractions = await this.attractionRepository.find({
      where: { parkId },
      select: {
        id: true,
      },
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
    const createdAfter = new Date(
      now.getTime() -
        DEDUPE_CREATED_AFTER_HOURS[predictionType] * 60 * 60 * 1000,
    );

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
