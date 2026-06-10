import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, DataSource } from "typeorm";
import { Attraction } from "../entities/attraction.entity";
import { Park } from "../../parks/entities/park.entity";
import { AttractionResponseDto } from "../dto/attraction-response.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { ParksService } from "../../parks/parks.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { buildAttractionUrl } from "../../common/utils/url.util";
import { HistoryDayDto } from "../dto/history-day.dto";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../../parks/entities/schedule-entry.entity";
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getTomorrowDateInTimezone,
  getStartOfDayInTimezone,
} from "../../common/utils/date.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import {
  computeBestVisitTimes,
  currentSlotStartMs,
  ttlSecondsToNextBoundary,
} from "../../common/utils/best-visit-times.util";
import { subDays } from "date-fns";
import { fromZonedTime, formatInTimeZone } from "date-fns-tz";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";
import { buildRopeDropInfo } from "../../common/utils/rope-drop-info.util";
import { ParkEnrichmentService } from "../../parks/services/park-enrichment.service";
import { PopularityService } from "../../popularity/popularity.service";

/**
 * Attraction Integration Service
 *
 * Dedicated service for building integrated attraction responses with live data.
 * Follows NestJS best practices - separates complex business logic from controller.
 *
 * Responsibilities:
 * - Fetches and integrates data from multiple sources (queue, forecasts, ML, analytics)
 * - Caches responses for performance
 * - Builds complete AttractionResponseDto
 */
@Injectable()
export class AttractionIntegrationService {
  private readonly logger = new Logger(AttractionIntegrationService.name);

  constructor(
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
    private readonly mlService: MLService,
    private readonly predictionAccuracyService: PredictionAccuracyService,
    private readonly parksService: ParksService,
    private readonly parkEnrichmentService: ParkEnrichmentService,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(QueueData)
    private readonly queueDataRepository: Repository<QueueData>,
    @InjectRepository(ScheduleEntry)
    private readonly scheduleEntryRepository: Repository<ScheduleEntry>,
    private readonly popularityService: PopularityService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Build integrated attraction response with live data
   *
   * Fetches and integrates:
   * - Current queue data (all queue types)
   * - Status + trend analysis
   * - ThemeParks.wiki forecasts (24 hours)
   * - ML predictions (daily, up to 1 year)
   * - Statistics (analytics)
   * - Prediction accuracy metrics
   * - Historical data (utilization, hourly P90, down counts)
   *
   * Cached for 5 minutes for better performance on frequent requests
   *
   * @param attraction - Attraction entity
   * @param days - Number of days of history to include (default: 30)
   * @returns Complete attraction DTO with all integrated live data
   */
  async buildIntegratedResponse(
    attraction: Attraction,
    days: number = 30,
  ): Promise<AttractionResponseDto> {
    // Try cache first
    const cacheKey = `attraction:integrated:${attraction.id}`;

    // Track popularity hit (background)
    this.popularityService
      .recordAttractionHit(attraction.id)
      .catch((err) =>
        this.logger.debug(`Failed to record attraction hit: ${err}`),
      );

    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const cachedDto = JSON.parse(cached);
      // Check if cached response has URL (new feature - invalidate cache if missing)
      if (!cachedDto.url) {
        // Cache is missing URL, rebuild
        this.logger.debug(
          `Cache for attraction ${attraction.id} missing URL, rebuilding.`,
        );
      } else {
        return cachedDto;
      }
    }

    // Start with base DTO
    const dto = AttractionResponseDto.fromEntity(attraction);

    // --- BATCH 1: Fire all independent async operations in parallel ---
    const [
      queueData,
      forecasts,
      park,
      mlPredictionsRaw,
      parkStatusMap,
      predictionAccuracyResult,
    ] = await Promise.all([
      this.queueDataService.findCurrentStatusByAttraction(attraction.id),
      this.queueDataService.findForecastsByAttraction(attraction.id, 24),
      attraction.parkId
        ? this.parkRepository.findOne({
            where: { id: attraction.parkId },
            select: [
              "id",
              "slug",
              "continentSlug",
              "countrySlug",
              "citySlug",
              "continent",
              "country",
              "city",
              "timezone",
              "countryCode",
              "regionCode",
              "influencingRegions",
            ],
          })
        : Promise.resolve(null),
      this.mlService
        .getAttractionPredictionsWithFallback(attraction.id, "hourly")
        .catch((err) => {
          this.logger.warn(
            "ML predictions unavailable:",
            err instanceof Error ? err.message : "Unknown error",
          );
          return [];
        }),
      attraction.parkId
        ? this.parksService
            .getBatchParkStatus([attraction.parkId])
            .catch(() => new Map<string, "OPERATING" | "CLOSED">())
        : Promise.resolve(new Map<string, "OPERATING" | "CLOSED">()),
      this.predictionAccuracyService
        .getAttractionAccuracyWithBadge(attraction.id, 30)
        .catch((err) => {
          this.logger.warn("Failed to fetch prediction accuracy:", err);
          return null;
        }),
    ]);

    // --- Process queue data (trend detection per queue, already parallelized) ---
    if (queueData.length > 0) {
      dto.queues = await Promise.all(
        queueData.map(async (qd) => {
          const queueDto: QueueDataItemDto = {
            queueType: qd.queueType,
            status: qd.status,
            waitTime: qd.waitTime ?? null,
            state: qd.state ?? null,
            returnStart: qd.returnStart ? qd.returnStart.toISOString() : null,
            returnEnd: qd.returnEnd ? qd.returnEnd.toISOString() : null,
            price: qd.price ?? null,
            allocationStatus: qd.allocationStatus ?? null,
            currentGroupStart: qd.currentGroupStart ?? null,
            currentGroupEnd: qd.currentGroupEnd ?? null,
            estimatedWait: qd.estimatedWait ?? null,
            lastUpdated: (qd.lastUpdated || qd.timestamp).toISOString(),
          };

          // Add trend data for wait-time based queues (STANDBY, SINGLE_RIDER, etc.)
          if (
            qd.waitTime !== null &&
            qd.waitTime !== undefined &&
            qd.status === "OPERATING"
          ) {
            try {
              const trendData =
                await this.analyticsService.detectAttractionTrend(
                  attraction.id,
                  qd.queueType,
                  qd.waitTime,
                );
              queueDto.trend = {
                direction: trendData.trend,
                changeRate: trendData.changeRate,
                recentAverage: trendData.recentAverage,
                previousAverage: trendData.previousAverage,
              };
            } catch (error) {
              this.logger.warn(
                `Failed to calculate trend for ${qd.queueType}:`,
                error,
              );
            }
          }

          return queueDto;
        }),
      );

      dto.status = queueData[0].status;

      const primaryQueue =
        dto.queues?.find((q) => q.queueType === "STANDBY") || dto.queues?.[0];
      if (primaryQueue?.trend?.direction) {
        dto.trend =
          primaryQueue.trend.direction === "increasing"
            ? "up"
            : primaryQueue.trend.direction === "decreasing"
              ? "down"
              : "stable";
      } else {
        dto.trend = null;
      }
    }

    // --- Park status & effective status ---
    const parkStatus: "OPERATING" | "CLOSED" =
      parkStatusMap.get(attraction.parkId) || "CLOSED";

    // Free-flow attractions (playgrounds, water play areas) have no queue and are
    // reported CLOSED by the source. Override to OPERATING with 0 min wait.
    if (attraction.openWithPark && parkStatus === "OPERATING") {
      dto.status = "OPERATING";
      dto.queues = [
        {
          queueType: QueueType.STANDBY,
          status: LiveStatus.OPERATING,
          waitTime: 0,
          state: null,
          returnStart: null,
          returnEnd: null,
          price: null,
          allocationStatus: null,
          currentGroupStart: null,
          currentGroupEnd: null,
          estimatedWait: null,
          lastUpdated: new Date().toISOString(),
          trend: undefined,
        },
      ];
    }

    dto.effectiveStatus = parkStatus === "CLOSED" ? "CLOSED" : dto.status;

    // --- Forecasts ---
    if (forecasts.length > 0) {
      dto.forecasts = forecasts.map((f) => ({
        predictedTime: f.predictedTime.toISOString(),
        predictedWaitTime: f.predictedWaitTime,
        confidencePercentage: f.confidencePercentage,
        source: f.source,
      }));
    }

    // --- ML predictions (already fetched in batch, no extra health check needed) ---
    const enrichedPredictions = mlPredictionsRaw.map((p) => ({
      predictedTime: p.predictedTime,
      predictedWaitTime: p.predictedWaitTime,
      confidence: p.confidence,
      crowdLevel: p.crowdLevel,
      baseline: p.baseline,
      trend: p.trend || "stable",
    }));
    dto.hourlyForecast =
      mlPredictionsRaw.length > 0
        ? enrichedPredictions.map((p) => ({
            predictedTime: p.predictedTime,
            predictedWaitTime: p.predictedWaitTime,
            confidence: p.confidence,
            trend: p.trend,
          }))
        : [];

    // --- Crowd level + baseline: fetch P50 once, reuse for both ---
    const nowStr = new Date().toISOString().split(":")[0]; // "YYYY-MM-DDTHH"
    const currentPred = enrichedPredictions.find((p) =>
      p.predictedTime.startsWith(nowStr),
    );

    let crowdLevel: CrowdLevel | "closed" | null = null;
    if (dto.effectiveStatus === "CLOSED") {
      crowdLevel = "closed";
      dto.baseline = null;
      dto.comparison = null;
    } else {
      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null) {
        try {
          // P50 baseline (median, "typical wait"). Peak-vs-median ratio:
          // 100% = current wait matches typical, >150% = busier than
          // typical. No live 548-day PERCENTILE_CONT fallback — that
          // was the dominant cost on attraction-detail cache misses.
          // No P90 fallback either: both percentiles are produced from
          // the same daily PERCENTILE_CONT scan and written atomically,
          // so "P50 missing while P90 present" is not a real state.
          const baseline =
            await this.analyticsService.getAttractionP50BaselineFromCache(
              attraction.id,
            );

          crowdLevel =
            this.analyticsService.getAttractionCrowdLevel(wait, baseline) ||
            "moderate";

          if (baseline > 0) {
            const loadRating = this.analyticsService.getLoadRating(
              wait,
              baseline,
            );
            dto.baseline = loadRating.baseline;
            dto.comparison = this.analyticsService.getComparisonText(
              loadRating.rating,
            );
          } else {
            dto.baseline = null;
            dto.comparison = null;
          }
        } catch (error) {
          this.logger.warn(
            `Failed to get percentiles for crowd level/baseline:`,
            error,
          );
          crowdLevel = currentPred?.crowdLevel
            ? (currentPred.crowdLevel as CrowdLevel)
            : "moderate";
          dto.baseline = null;
          dto.comparison = null;
        }
      } else {
        crowdLevel = currentPred?.crowdLevel
          ? (currentPred.crowdLevel as CrowdLevel)
          : "very_low";
        dto.baseline = null;
        dto.comparison = null;
      }
    }
    dto.crowdLevel = crowdLevel;

    // --- Statistics, history, schedule: run in parallel (all need park timezone) ---
    const parkForUrl = park;
    if (park) {
      try {
        // Kick off the start-time lookup AND the history fetch in
        // parallel — history doesn't depend on startTime, only the
        // statistics call does. On a cold cache this turns a
        // (Redis+DB)-then-(stats+history+schedule) waterfall into
        // (Redis+DB || history || schedule) → (stats).
        const today = getStartOfDayInTimezone(park.timezone);
        const startDate = subDays(today, days);
        const startDateStr = formatInParkTimezone(startDate, park.timezone);
        const endDateStr = formatInParkTimezone(today, park.timezone);

        const historyPromise = this.calculateAttractionHistory(
          attraction.id,
          attraction.parkId,
          park.timezone,
          days,
        ).catch((err) => {
          this.logger.warn(
            `Failed to calculate history for attraction ${attraction.id}:`,
            err,
          );
          return [] as HistoryDayDto[];
        });

        const startTime = await this.analyticsService.getEffectiveStartTime(
          attraction.parkId,
          park.timezone,
        );

        const [statistics, history, scheduleEntries] = await Promise.all([
          this.analyticsService.getAttractionStatistics(
            attraction.id,
            startTime,
            park.timezone,
          ),
          historyPromise,
          this.scheduleEntryRepository
            .createQueryBuilder("schedule")
            .where("schedule.parkId = :parkId", { parkId: attraction.parkId })
            .andWhere("schedule.attractionId IS NULL")
            .andWhere("schedule.date >= :startDate", {
              startDate: startDateStr,
            })
            .andWhere("schedule.date <= :endDate", { endDate: endDateStr })
            .orderBy("schedule.date", "ASC")
            .addOrderBy("schedule.scheduleType", "ASC")
            .getMany()
            .catch((err) => {
              this.logger.warn(
                `Failed to fetch schedule for attraction ${attraction.id}:`,
                err,
              );
              return [] as ScheduleEntry[];
            }),
        ]);

        dto.statistics = {
          avgWaitToday: statistics.avgWaitToday,
          peakWaitToday: statistics.peakWaitToday,
          peakWaitTimestamp: statistics.peakWaitTimestamp
            ? statistics.peakWaitTimestamp.toISOString()
            : null,
          minWaitToday: statistics.minWaitToday,
          typicalWaitThisHour: statistics.typicalWaitThisHour,
          percentile95ThisHour: statistics.percentile95ThisHour,
          currentVsTypical: statistics.currentVsTypical,
          dataPoints: statistics.dataPoints,
          history: statistics.history || [],
          timestamp: statistics.timestamp.toISOString(),
        };

        dto.history = history;

        dto.schedule = scheduleEntries.map((entry) =>
          ScheduleItemDto.fromEntity(entry),
        );
        if (dto.schedule.length > 0) {
          await this.parkEnrichmentService.enrichScheduleWithHolidays(
            dto.schedule,
            park,
          );
        }

        // --- Rope-drop recommendation (read-through; UTC resolved vs today's opening) ---
        // Single read per attraction request (not N+1). Offsets are the portable
        // truth; the UTC instants are a convenience anchored to today's opening.
        const ropeDropStored = await this.analyticsService.getRopeDropFromCache(
          attraction.id,
        );
        if (ropeDropStored) {
          const rdTodayStr = getCurrentDateInTimezone(park.timezone);
          const rdTodayEntry = dto.schedule.find((s) => s.date === rdTodayStr);
          dto.ropeDrop = buildRopeDropInfo(
            ropeDropStored,
            rdTodayEntry?.openingTime ?? null,
          );
        }

        // --- Best visit times (today only, including current active 15-min slot) ---
        // Uses today's closing time from schedule so recommendations don't exceed operating hours.
        // Cached with the integrated response (expires at the next 5-min boundary).
        if (mlPredictionsRaw.length > 0) {
          const todayStr = getCurrentDateInTimezone(park.timezone);
          const todayEntry = dto.schedule.find((s) => s.date === todayStr);

          // For the current 15-min slot, substitute the actual live wait time so a
          // spike that the ML didn't predict can't appear as a "best time".
          const currentActualWait = dto.queues?.[0]?.waitTime;
          const slotStartMs = currentSlotStartMs();
          const predictionsForBestTimes =
            currentActualWait != null
              ? mlPredictionsRaw.map((p) =>
                  new Date(p.predictedTime).getTime() === slotStartMs
                    ? { ...p, predictedWaitTime: currentActualWait }
                    : p,
                )
              : mlPredictionsRaw;

          // Restrict to today's predictions in park-local time. Hourly predictions
          // span 24 h, so without this filter tomorrow's early-morning low-wait slots
          // appear as "optimal" recommendations when no closing time is available
          // (parks without full schedule integration, common for US parks).
          const todayOnlyPredictions = predictionsForBestTimes.filter(
            (p) =>
              formatInParkTimezone(new Date(p.predictedTime), park.timezone) ===
              todayStr,
          );

          dto.bestVisitTimes = computeBestVisitTimes(
            todayOnlyPredictions,
            todayEntry?.closingTime ?? null,
          );
        }
      } catch (error) {
        this.logger.error("Failed to fetch attraction statistics:", error);
        dto.statistics = null;
      }
    }

    // --- Prediction accuracy (already fetched in batch) ---
    if (predictionAccuracyResult) {
      dto.predictionAccuracy = {
        badge: predictionAccuracyResult.badge,
        last30Days: {
          mae: predictionAccuracyResult.last30Days.mae,
          comparedPredictions:
            predictionAccuracyResult.last30Days.comparedPredictions,
          totalPredictions:
            predictionAccuracyResult.last30Days.totalPredictions,
        },
        message: predictionAccuracyResult.message,
      };
    } else {
      dto.predictionAccuracy = null;
    }

    // --- URL ---
    if (
      parkForUrl &&
      parkForUrl.continentSlug &&
      parkForUrl.countrySlug &&
      parkForUrl.citySlug
    ) {
      dto.url = buildAttractionUrl(parkForUrl, attraction);
    } else {
      dto.url = null;
    }

    // Cache aligned to next 5-min boundary (+5s buffer) so the entry expires
    // at or just after a slot transition rather than at an arbitrary rolling offset.
    await this.redis.set(
      cacheKey,
      JSON.stringify(dto),
      "EX",
      ttlSecondsToNextBoundary(),
    );

    return dto;
  }

  /**
   * Calculate attraction history for a configurable time period
   *
   * Returns daily historical data including:
   * - Daily utilization (crowd level)
   * - Hourly P90 wait times
   * - Down count per day
   *
   * Only includes days when park was open (has OPERATING schedule or fallback detection).
   * Uses timezone-aware date calculations and park opening hours.
   *
   * @param attractionId - Attraction ID
   * @param parkId - Park ID
   * @param timezone - Park timezone (IANA format)
   * @param days - Number of days to include (including today)
   * @returns Array of history entries (only for days when park was open)
   */
  async calculateAttractionHistory(
    attractionId: string,
    parkId: string,
    timezone: string,
    days: number,
  ): Promise<HistoryDayDto[]> {
    // Check cache first
    const cacheKey = `attraction:history:${attractionId}:${days}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      try {
        const parsed = JSON.parse(cached);

        // Extract metadata and history data
        const calculatedAt = parsed.calculatedAt
          ? new Date(parsed.calculatedAt)
          : null;
        const historyData: HistoryDayDto[] = parsed.history || parsed; // Support old format (array) and new format (object with metadata)

        // Check if today should be in the result but isn't in cache
        // This handles the case where cache was created before today had any data
        const todayStr = getCurrentDateInTimezone(timezone);
        const hasTodayInCache = historyData.some(
          (day: HistoryDayDto) => day.date === todayStr,
        );

        // Only invalidate cache if:
        // 1. Today is missing from cache, AND
        // 2. Cache is older than 5 minutes (or has no timestamp - old format)
        // This prevents cache stampede when today has no data yet
        const cacheAgeMs = calculatedAt
          ? Date.now() - calculatedAt.getTime()
          : Infinity;
        const cacheIsStale = cacheAgeMs > 5 * 60 * 1000; // 5 minutes

        if (!hasTodayInCache && days > 0 && cacheIsStale) {
          this.logger.debug(
            `Cache stale for today (${todayStr}) in history for ${attractionId} (age: ${Math.round(cacheAgeMs / 1000)}s), recalculating...`,
          );
          // Don't return cached data - fall through to recalculate
        } else {
          // Apply 5-minute rounding to cached data (in case cache was created before rounding fix)
          const rounded = historyData.map((day: HistoryDayDto) => ({
            ...day,
            hourlyP90: day.hourlyP90.map((h) => ({
              ...h,
              value: roundToNearest5Minutes(h.value),
            })),
          }));
          this.logger.debug(
            `Using cached history for ${attractionId}: ${rounded.length} days`,
          );
          return rounded;
        }
      } catch (error) {
        this.logger.warn(
          `Failed to parse cached history for ${attractionId}:`,
          error,
        );
      }
    }

    try {
      // Calculate date range: today back to (today - days + 1) in park timezone
      // Example: days=30 means today + 29 past days = 30 days total
      const todayStr = getCurrentDateInTimezone(timezone);
      const today = fromZonedTime(`${todayStr}T00:00:00`, timezone);
      const startDate = subDays(today, days - 1);

      // Calculate end date: start of tomorrow in park timezone, converted to UTC
      // This ensures we include all of today's data (up to but not including tomorrow)
      const tomorrowStr = getTomorrowDateInTimezone(timezone);
      const endDate = fromZonedTime(`${tomorrowStr}T00:00:00`, timezone);

      this.logger.debug(
        `History query for attraction ${attractionId}: todayStr=${todayStr}, tomorrowStr=${tomorrowStr}, startDate=${startDate.toISOString()}, endDate=${endDate.toISOString()}, days=${days}, timezone=${timezone}`,
      );

      // Batch fetch schedules for all days in range
      const schedules = await this.parksService.getSchedule(
        parkId,
        startDate,
        endDate,
      );

      // Create schedule map: date string -> schedule entry
      const scheduleMap = new Map<string, ScheduleEntry>();
      for (const schedule of schedules) {
        if (schedule.scheduleType === ScheduleType.OPERATING) {
          // schedule.date is a PostgreSQL DATE column — TypeORM returns it as a
          // "YYYY-MM-DD" string, not a Date object. Use it directly as the key
          // to avoid UTC-midnight timezone shift for UTC+ parks.
          const dateStr =
            typeof schedule.date === "string"
              ? schedule.date
              : formatInParkTimezone(schedule.date, timezone);
          scheduleMap.set(dateStr, schedule);
        }
      }

      // Hybrid data source:
      // 1. Past days come from `attraction_hourly_history` (pre-aggregated
      //    nightly by AttractionHourlyHistoryProcessor) — one indexed SELECT.
      // 2. Today's still-in-progress slots are computed live against
      //    queue_data, but only ever for one day at a time.
      // This replaces the previous always-live PERCENTILE_CONT scan of the
      // entire requested window, which dominated cold-cache cost on the
      // attraction history endpoint.
      const fromDateStr = formatInParkTimezone(startDate, timezone);
      // History is up-to-but-not-including endDate (next-day-midnight in
      // park tz), so the latest *date* covered is today.
      const toDateStr = todayStr;

      const queueDataByDate = new Map<
        string,
        Array<{
          time_slot: string;
          p90: number;
          avgWait: number;
          sampleCount: number;
        }>
      >();
      const downCountMap = new Map<string, number>();

      const cachedHistory =
        await this.analyticsService.getAttractionHourlyHistory(
          attractionId,
          fromDateStr,
          toDateStr,
        );
      for (const [dateStr, row] of cachedHistory) {
        if (dateStr === todayStr) {
          // Skip cached "today" — we'll recompute it live so the chart
          // reflects the latest hour. Past days are settled and trusted.
          continue;
        }
        queueDataByDate.set(dateStr, row.slots || []);
        downCountMap.set(dateStr, row.downCount || 0);
      }

      // Live single-day compute for today (range may end before today, in
      // which case we skip this entirely and serve purely from the cache).
      if (endDate.getTime() > Date.now()) {
        const todayStart = fromZonedTime(`${todayStr}T00:00:00`, timezone);
        const todayEnd = new Date(todayStart.getTime() + 24 * 60 * 60 * 1000);

        const [todayRows, todayDown] = await Promise.all([
          this.dataSource.query(
            `
            SELECT
              (LPAD(EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $3)::text, 2, '0') ||
               ':' ||
               LPAD((FLOOR(EXTRACT(MINUTE FROM qd.timestamp AT TIME ZONE $3) / 15) * 15)::text, 2, '0')) as time_slot,
              PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as p90,
              AVG(qd."waitTime") as avg_wait,
              COUNT(*) as sample_count
            FROM queue_data qd
            WHERE qd."attractionId" = $1::uuid
              AND qd.timestamp >= $2
              AND qd.timestamp < $4
              AND qd.status = 'OPERATING'
              AND qd."queueType" = 'STANDBY'
              AND qd."waitTime" IS NOT NULL
              AND qd."waitTime" >= 5
            GROUP BY time_slot
            HAVING COUNT(*) >= 1
            ORDER BY time_slot
            `,
            [
              attractionId,
              todayStart.toISOString(),
              timezone,
              todayEnd.toISOString(),
            ],
          ),
          this.dataSource.query(
            `
            SELECT
              COUNT(DISTINCT DATE_TRUNC('hour', qd.timestamp AT TIME ZONE $3)) as down_count
            FROM queue_data qd
            WHERE qd."attractionId" = $1::uuid
              AND qd.timestamp >= $2
              AND qd.timestamp < $4
              AND qd.status = 'DOWN'
            `,
            [
              attractionId,
              todayStart.toISOString(),
              timezone,
              todayEnd.toISOString(),
            ],
          ),
        ]);

        const todaySlotsArr = todayRows.map(
          (row: {
            time_slot: string;
            p90: string;
            avg_wait: string;
            sample_count: string;
          }) => ({
            time_slot: row.time_slot,
            p90: roundToNearest5Minutes(parseFloat(row.p90)),
            avgWait: roundToNearest5Minutes(parseFloat(row.avg_wait)),
            sampleCount: parseInt(row.sample_count, 10) || 0,
          }),
        );
        if (todaySlotsArr.length > 0) {
          queueDataByDate.set(todayStr, todaySlotsArr);
        }
        const downCount = parseInt(todayDown[0]?.down_count || "0", 10) || 0;
        if (downCount > 0) downCountMap.set(todayStr, downCount);
      }

      // Median baseline (P50) for utilization — typical-day reference.
      // Per-day reading is the day's MAX hourly P90 (the day's actual peak),
      // so the ratio reads as "today's peak vs typical median" — a
      // peak-vs-median scale that calibrates against the existing threshold
      // ladder (100% = matches typical median, 150%+ = elevated, 200%+ =
      // extreme).
      const attractionBaseline =
        await this.analyticsService.getAttractionP50BaselineFromCache(
          attractionId,
        );

      // Build history entries for each day
      const history: HistoryDayDto[] = [];
      const currentDate = new Date(startDate);

      while (currentDate < endDate) {
        const dateStr = formatInParkTimezone(currentDate, timezone);

        // Check if we have queue data for this day
        // Only include days with actual data (either from schedule + data, or fallback with data)
        const schedule = scheduleMap.get(dateStr);
        const dayQueueData = queueDataByDate.get(dateStr) || [];

        // Only include days when we have queue data
        // This ensures we only show days when the park was actually operating and we have data
        if (dayQueueData.length > 0) {
          const downCount = downCountMap.get(dateStr) || 0;

          // Extract opening and closing hours from schedule if available.
          // Done up-front so the same operating-hours filter applies both
          // to the daily utilization (peak-vs-median ratio) and the
          // hourly chart array below.
          let openingTimeSlot: string | null = null;
          let closingTimeSlot: string | null = null;
          let openingHourVal: number | null = null;
          let closingHourVal: number | null = null;

          if (schedule && schedule.openingTime && schedule.closingTime) {
            const openingTimeStr = formatInTimeZone(
              schedule.openingTime,
              timezone,
              "HH:mm",
            );
            const [oH, oM] = openingTimeStr.split(":").map(Number);
            const oMRounded = Math.floor(oM / 15) * 15;
            openingTimeSlot = `${oH.toString().padStart(2, "0")}:${oMRounded.toString().padStart(2, "0")}`;
            openingHourVal = oH;

            const closingTimeStr = formatInTimeZone(
              schedule.closingTime,
              timezone,
              "HH:mm",
            );
            const [cH, cM] = closingTimeStr.split(":").map(Number);
            const cMRounded = Math.ceil(cM / 15) * 15;
            let finalCH = cH;
            let finalCM = cMRounded;
            if (finalCM === 60) {
              finalCH = (finalCH + 1) % 24;
              finalCM = 0;
            }
            closingTimeSlot = `${finalCH.toString().padStart(2, "0")}:${finalCM.toString().padStart(2, "0")}`;
            closingHourVal = finalCH;
          }

          // Slots within operating hours (drops off-hour outlier samples
          // that would otherwise distort the day's peak statistic).
          const inHoursSlots = dayQueueData.filter((h) => {
            if (openingTimeSlot && h.time_slot < openingTimeSlot) return false;
            if (closingTimeSlot && h.time_slot > closingTimeSlot) return false;
            return true;
          });

          // Daily utilization = peak-vs-median: P90 of in-hours slot P90s
          // (the day's representative peak hour, robust against a single
          // outlier slot) ÷ attraction P50 baseline. Reads as "how did
          // today's peak compare to a typical wait" — a busy day's peak
          // hits >150% (threshold "high"+); a quiet day sits around
          // 60-90% ("very_low" / "low").
          let utilization: CrowdLevel | "closed" = "closed";
          if (inHoursSlots.length > 0 && attractionBaseline > 0) {
            const p90Values = inHoursSlots
              .map((h) => h.p90)
              .sort((a, b) => a - b);
            const idx = (p90Values.length - 1) * 0.9;
            const lo = Math.floor(idx);
            const hi = Math.ceil(idx);
            const dayPeakWait =
              p90Values[lo] * (1 - (idx - lo)) + p90Values[hi] * (idx - lo);

            utilization = this.analyticsService.getAttractionCrowdLevel(
              dayPeakWait,
              attractionBaseline,
            ) as CrowdLevel;
          }

          // Build hourly P90 array (only for hours within operating hours)
          const hourlyP90: Array<{ hour: string; value: number }> = [];
          const hourDataMap = new Map<
            string,
            { p90: number; avgWait: number; sampleCount: number }
          >();

          // Create map of existing hour data
          for (const hourData of dayQueueData) {
            hourDataMap.set(hourData.time_slot, {
              p90: hourData.p90,
              avgWait: hourData.avgWait,
              sampleCount: hourData.sampleCount,
            });
          }

          // Add all existing hour data
          for (const hourData of dayQueueData) {
            // Filter by operating hours if available
            if (openingTimeSlot && hourData.time_slot < openingTimeSlot)
              continue;
            if (closingTimeSlot && hourData.time_slot > closingTimeSlot)
              continue;

            hourlyP90.push({
              hour: hourData.time_slot,
              value: roundToNearest5Minutes(hourData.p90),
            });
          }

          // Ensure we have opening hour if schedule available
          // openingHour is already in park timezone (from formatInTimeZone)
          // SMART PROJECTION: Only add opening hour if earliest actual data is within 2 hours
          // This prevents false projections when attraction opened late (e.g., 15:00 instead of 11:00)
          const earliestDataHour =
            hourDataMap.size > 0
              ? Math.min(
                  ...Array.from(hourDataMap.keys()).map((t) =>
                    parseInt(t.split(":")[0], 10),
                  ),
                )
              : null;

          // Only project opening hour if we have data AND earliest data is within 2 hours of opening
          const shouldProjectOpening =
            openingHourVal !== null &&
            earliestDataHour !== null &&
            earliestDataHour - openingHourVal <= 2 &&
            earliestDataHour >= openingHourVal;

          if (shouldProjectOpening && openingTimeSlot) {
            const hasOpeningSlot = hourlyP90.some(
              (h) => h.hour === openingTimeSlot,
            );
            if (!hasOpeningSlot) {
              const earliestSlot = Array.from(hourDataMap.keys()).sort()[0];
              const nearestValue = hourDataMap.get(earliestSlot)?.p90 || 0;
              hourlyP90.push({
                hour: openingTimeSlot,
                value: roundToNearest5Minutes(nearestValue),
              });
            }
          }

          // Ensure we have closing hour if schedule available
          // closingHour is already in park timezone (from formatInTimeZone)
          // SMART PROJECTION: Only add closing hour if latest actual data is within 2 hours
          // This prevents false projections when attraction closed early
          const latestDataHour =
            hourDataMap.size > 0
              ? Math.max(
                  ...Array.from(hourDataMap.keys()).map((t) =>
                    parseInt(t.split(":")[0], 10),
                  ),
                )
              : null;

          // Only project closing hour if we have data AND latest data is within 2 hours of closing
          const shouldProjectClosing =
            closingHourVal !== null &&
            latestDataHour !== null &&
            closingHourVal - latestDataHour <= 2 &&
            latestDataHour <= closingHourVal;

          if (shouldProjectClosing && closingTimeSlot) {
            const hasClosingSlot = hourlyP90.some(
              (h) => h.hour === closingTimeSlot,
            );
            if (!hasClosingSlot) {
              const latestSlot = Array.from(hourDataMap.keys())
                .sort()
                .reverse()[0];
              const nearestValue = hourDataMap.get(latestSlot)?.p90 || 0;
              hourlyP90.push({
                hour: closingTimeSlot,
                value: roundToNearest5Minutes(nearestValue),
              });
            }
          }

          // Sort by hour
          hourlyP90.sort((a, b) => a.hour.localeCompare(b.hour));

          // For today: Add current hour with current wait time
          // This ensures real-time data is visible in the chart
          if (dateStr === todayStr) {
            const now = new Date();
            const curHStr = formatInTimeZone(now, timezone, "HH");
            const curM = parseInt(formatInTimeZone(now, timezone, "mm"), 10);
            const curMSlot = (Math.floor(curM / 15) * 15)
              .toString()
              .padStart(2, "0");
            const currentSlotStr = `${curHStr}:${curMSlot}`;

            const hasCurrentSlot = hourlyP90.some(
              (h) => h.hour === currentSlotStr,
            );
            if (!hasCurrentSlot) {
              let currentValue = 0;
              if (hourDataMap.size > 0) {
                const latestSlot = Array.from(hourDataMap.keys())
                  .sort()
                  .reverse()[0];
                currentValue = hourDataMap.get(latestSlot)?.p90 || 0;
              }
              hourlyP90.push({
                hour: currentSlotStr,
                value: roundToNearest5Minutes(currentValue),
              });
              hourlyP90.sort((a, b) => a.hour.localeCompare(b.hour));
            }
          }

          history.push({
            date: dateStr,
            utilization: utilization || "closed",
            hourlyP90: hourlyP90.map((h) => ({
              ...h,
              value: roundToNearest5Minutes(h.value), // Ensure all values are rounded
            })),
            downCount,
          });
        }

        // Move to next day
        currentDate.setDate(currentDate.getDate() + 1);
      }

      // Determine TTL: shorter if today is included, longer for pure history
      const todayInRange = history.some((h) => h.date === todayStr);
      const ttl = todayInRange ? 5 * 60 : 24 * 60 * 60; // 5 min if today included, 24h for history only

      // Cache the result with metadata (calculatedAt for smart invalidation)
      const cachePayload = {
        calculatedAt: new Date().toISOString(),
        history,
      };
      await this.redis.set(cacheKey, JSON.stringify(cachePayload), "EX", ttl);

      return history;
    } catch (error) {
      this.logger.error(
        `Failed to calculate history for attraction ${attractionId}:`,
        error,
      );
      // Return empty array on error (don't fail entire response)
      return [];
    }
  }
}
