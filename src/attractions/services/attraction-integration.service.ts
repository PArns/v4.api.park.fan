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
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getTomorrowDateInTimezone,
  getStartOfDayInTimezone,
} from "../../common/utils/date.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { addDays, subDays } from "date-fns";
import { fromZonedTime, formatInTimeZone } from "date-fns-tz";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";
import { ParkEnrichmentService } from "../../parks/services/park-enrichment.service";

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
  private readonly TTL_INTEGRATED_RESPONSE = 5 * 60; // 5 minutes for real-time data

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

    // === PHASE 1: Parallel fetch of all independent data sources ===
    const [
      queueData,
      parkStatusResult,
      forecasts,
      parkForUrl,
      mlPredictionsResult,
      p50Baseline,
      p90Result,
    ] = await Promise.all([
      this.queueDataService
        .findCurrentStatusByAttraction(attraction.id)
        .catch(() => []),
      attraction.parkId
        ? this.parksService
            .getBatchParkStatus([attraction.parkId])
            .catch(() => new Map<string, "OPERATING" | "CLOSED">())
        : Promise.resolve(new Map<string, "OPERATING" | "CLOSED">()),
      this.queueDataService
        .findForecastsByAttraction(attraction.id, 24)
        .catch(() => []),
      attraction.parkId
        ? this.parkRepository
            .findOne({
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
            .catch(() => null)
        : Promise.resolve(null),
      this.mlService
        .isHealthy()
        .then((healthy) =>
          healthy
            ? this.mlService.getAttractionPredictionsWithFallback(
                attraction.id,
                "hourly",
              )
            : [],
        )
        .catch(() => []),
      this.analyticsService
        .getAttractionP50BaselineFromCache(attraction.id)
        .catch(() => 0),
      this.analyticsService
        .get90thPercentileWithConfidence(attraction.id, "attraction")
        .catch(() => ({ p50: 0, p90: 0 })),
    ]);

    // Resolve baseline once (used for crowd level + comparison)
    const baseline = p50Baseline || p90Result.p50 || p90Result.p90 || 0;

    // Process queue data
    if (queueData.length > 0) {
      const primaryQd =
        queueData.find((qd) => qd.queueType === "STANDBY") || queueData[0];

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

          if (
            qd === primaryQd &&
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

    // Park status
    const parkStatus: "OPERATING" | "CLOSED" =
      (attraction.parkId && parkStatusResult.get(attraction.parkId)) ||
      "CLOSED";
    dto.effectiveStatus = parkStatus === "CLOSED" ? "CLOSED" : dto.status;

    // Forecasts
    if (forecasts.length > 0) {
      dto.forecasts = forecasts.map((f) => ({
        predictedTime: f.predictedTime.toISOString(),
        predictedWaitTime: f.predictedWaitTime,
        confidencePercentage: f.confidencePercentage,
        source: f.source,
      }));
    }

    // ML predictions
    let enrichedPredictions: Array<{
      predictedTime: string;
      predictedWaitTime: number;
      confidence: number;
      crowdLevel?: string;
      baseline?: number;
      trend: string;
    }> = [];

    if (mlPredictionsResult.length > 0) {
      enrichedPredictions = mlPredictionsResult.map((p) => ({
        predictedTime: p.predictedTime,
        predictedWaitTime: p.predictedWaitTime,
        confidence: p.confidence,
        crowdLevel: p.crowdLevel,
        baseline: p.baseline,
        trend: p.trend || "stable",
      }));
      dto.hourlyForecast = enrichedPredictions.map((p) => ({
        predictedTime: p.predictedTime,
        predictedWaitTime: p.predictedWaitTime,
        confidence: p.confidence,
        trend: p.trend,
      }));
    } else {
      dto.hourlyForecast = [];
    }

    // Crowd level (uses pre-fetched baseline)
    let crowdLevel: CrowdLevel | "closed" | null = null;
    if (dto.effectiveStatus === "CLOSED") {
      crowdLevel = "closed";
    } else {
      const nowStr = new Date().toISOString().split(":")[0];
      const currentPred = enrichedPredictions.find((p) =>
        p.predictedTime.startsWith(nowStr),
      );

      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null && baseline > 0) {
        const calculatedLevel = this.analyticsService.getAttractionCrowdLevel(
          wait,
          baseline,
        );
        crowdLevel = calculatedLevel || "moderate";
      } else if (wait !== undefined && wait !== null) {
        crowdLevel = currentPred?.crowdLevel
          ? (currentPred.crowdLevel as CrowdLevel)
          : "moderate";
      } else {
        crowdLevel = currentPred?.crowdLevel
          ? (currentPred.crowdLevel as CrowdLevel)
          : "very_low";
      }
    }
    dto.crowdLevel = crowdLevel;

    // Baseline and comparison (uses pre-fetched baseline — no duplicate lookups)
    if (dto.effectiveStatus === "OPERATING") {
      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null && baseline > 0) {
        const loadRating = this.analyticsService.getLoadRating(wait, baseline);
        dto.baseline = loadRating.baseline;
        dto.comparison = this.analyticsService.getComparisonText(
          loadRating.rating,
        );
      } else {
        dto.baseline = null;
        dto.comparison = null;
      }
    } else {
      dto.baseline = null;
      dto.comparison = null;
    }

    // === PHASE 2: Statistics, history, schedule, accuracy (parallel where possible) ===
    try {
      if (!parkForUrl) {
        throw new Error(`Park not found for attraction ${attraction.id}`);
      }

      const startTime = await this.analyticsService.getEffectiveStartTime(
        attraction.parkId,
        parkForUrl.timezone,
      );

      const [statistics, history, accuracy] = await Promise.all([
        this.analyticsService.getAttractionStatistics(
          attraction.id,
          startTime,
          parkForUrl.timezone,
        ),
        parkForUrl.timezone
          ? this.calculateAttractionHistory(
              attraction.id,
              attraction.parkId,
              parkForUrl.timezone,
              days,
            ).catch((error) => {
              this.logger.warn(
                `Failed to calculate history for attraction ${attraction.id}:`,
                error,
              );
              return [] as HistoryDayDto[];
            })
          : Promise.resolve([] as HistoryDayDto[]),
        this.predictionAccuracyService
          .getAttractionAccuracyWithBadge(attraction.id, 30)
          .catch(() => null),
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

      if (accuracy) {
        dto.predictionAccuracy = {
          badge: accuracy.badge,
          last30Days: {
            comparedPredictions: accuracy.last30Days.comparedPredictions,
            totalPredictions: accuracy.last30Days.totalPredictions,
          },
          message: accuracy.message,
        };
      } else {
        dto.predictionAccuracy = null;
      }

      // Fetch park schedule for the same date range (aligned with history)
      if (parkForUrl.timezone) {
        try {
          // Calculate date range in park timezone: today back to (today - days)
          // Use getStartOfDayInTimezone to ensure "today" is correctly calculated in park timezone
          const today = getStartOfDayInTimezone(parkForUrl.timezone);
          const startDate = subDays(today, days); // today - 30 days for days=30
          // End date: today (inclusive) - already calculated in park timezone

          // IMPORTANT: Convert Date objects to date strings (YYYY-MM-DD) for TypeORM query
          // The schedule.date column is of type DATE (not TIMESTAMP), so we need to compare dates, not timestamps
          // This ensures correct timezone handling and prevents off-by-one errors
          const startDateStr = formatInParkTimezone(
            startDate,
            parkForUrl.timezone,
          );
          const endDateStr = formatInParkTimezone(today, parkForUrl.timezone);

          // Get schedule entries for the park (only park-level, not attraction-specific)
          const scheduleEntries = await this.scheduleEntryRepository
            .createQueryBuilder("schedule")
            .where("schedule.parkId = :parkId", { parkId: attraction.parkId })
            .andWhere("schedule.attractionId IS NULL") // Only park schedules
            .andWhere("schedule.date >= :startDate", {
              startDate: startDateStr,
            })
            .andWhere("schedule.date <= :endDate", { endDate: endDateStr })
            .orderBy("schedule.date", "ASC")
            .addOrderBy("schedule.scheduleType", "ASC")
            .getMany();

          // Convert to DTOs
          dto.schedule = scheduleEntries.map((entry) =>
            ScheduleItemDto.fromEntity(entry),
          );

          // Enrich schedule with holiday data (same logic as park endpoint, but different date range)
          if (dto.schedule && dto.schedule.length > 0 && parkForUrl) {
            await this.parkEnrichmentService.enrichScheduleWithHolidays(
              dto.schedule,
              parkForUrl,
            );
          }
        } catch (error) {
          // Log but don't fail - schedule is optional
          this.logger.warn(
            `Failed to fetch schedule for attraction ${attraction.id}:`,
            error,
          );
          dto.schedule = [];
        }
      }
    } catch (error) {
      this.logger.error("Failed to fetch attraction statistics:", error);
      dto.statistics = null;
    }

    // Set URL using geo route (park already fetched in phase 1)
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

    // Cache the complete response (5 minutes for real-time freshness)
    await this.redis.set(
      cacheKey,
      JSON.stringify(dto),
      "EX",
      this.TTL_INTEGRATED_RESPONSE,
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
          const dateStr = formatInParkTimezone(schedule.date, timezone);
          scheduleMap.set(dateStr, schedule);
        }
      }

      // Single batch query for all queue data in date range
      // Use raw SQL for efficient aggregation with timezone-aware date extraction
      // Note: Only 1 sample needed since we only store changes (not all values)
      // The WHERE clause uses UTC timestamps (stored in DB) compared to UTC Date objects
      // The GROUP BY extracts dates in park timezone for correct day boundaries
      // Round P90 and avg_wait to nearest 5 minutes in SQL for consistency
      const queueDataResults = await this.dataSource.query(
        `
        SELECT 
          DATE(qd.timestamp AT TIME ZONE $3) as date,
          EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $3) as hour,
          PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") as p90,
          AVG(qd."waitTime") as avg_wait,
          COUNT(*) as sample_count
        FROM queue_data qd
        WHERE qd."attractionId" = $1
          AND qd.timestamp >= $2
          AND qd.timestamp < $4
          AND qd.status = 'OPERATING'
          AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" IS NOT NULL
          AND qd."waitTime" > 0
        GROUP BY DATE(qd.timestamp AT TIME ZONE $3), 
                 EXTRACT(HOUR FROM qd.timestamp AT TIME ZONE $3)
        HAVING COUNT(*) >= 1
        ORDER BY date, hour
      `,
        [
          attractionId,
          startDate.toISOString(),
          timezone,
          endDate.toISOString(),
        ],
      );

      // Query down count separately (more efficient)
      const downCountResults = await this.dataSource.query(
        `
        SELECT 
          DATE(qd.timestamp AT TIME ZONE $3) as date,
          COUNT(DISTINCT 
            CASE 
              WHEN qd.status = 'DOWN' THEN 
                DATE_TRUNC('hour', qd.timestamp AT TIME ZONE $3)
              ELSE NULL
            END
          ) as down_count
        FROM queue_data qd
        WHERE qd."attractionId" = $1
          AND qd.timestamp >= $2
          AND qd.timestamp < $4
          AND qd.status = 'DOWN'
        GROUP BY DATE(qd.timestamp AT TIME ZONE $3)
      `,
        [
          attractionId,
          startDate.toISOString(),
          timezone,
          endDate.toISOString(),
        ],
      );

      // Group queue data by date
      // PostgreSQL DATE() returns a string in YYYY-MM-DD format, but ensure consistency
      const queueDataByDate = new Map<
        string,
        Array<{
          hour: number;
          p90: number;
          avgWait: number;
          sampleCount: number;
        }>
      >();
      for (const row of queueDataResults) {
        // Ensure date is in YYYY-MM-DD format (PostgreSQL DATE returns this format)
        const dateStr =
          typeof row.date === "string"
            ? row.date
            : new Date(row.date).toISOString().split("T")[0];
        if (!queueDataByDate.has(dateStr)) {
          queueDataByDate.set(dateStr, []);
        }
        queueDataByDate.get(dateStr)!.push({
          hour: parseInt(row.hour, 10),
          p90: roundToNearest5Minutes(parseFloat(row.p90)), // Ensure rounding (already done in SQL, but double-check)
          avgWait: roundToNearest5Minutes(parseFloat(row.avg_wait)), // Ensure rounding
          sampleCount: parseInt(row.sample_count, 10) || 0,
        });
      }

      // Debug: Log query results

      // Create down count map
      const downCountMap = new Map<string, number>();
      for (const row of downCountResults) {
        // Ensure date is in YYYY-MM-DD format
        const dateStr =
          typeof row.date === "string"
            ? row.date
            : new Date(row.date).toISOString().split("T")[0];
        downCountMap.set(dateStr, parseInt(row.down_count, 10) || 0);
      }

      // P50 baseline for utilization (same as crowd level); use once per attraction
      const attractionP50Baseline =
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

          // Calculate daily utilization (average wait vs P90 baseline)
          // Use weighted average (same as statistics.avgWaitToday) - each data point counts equally
          let utilization: CrowdLevel | "closed" = "closed";
          if (dayQueueData.length > 0) {
            // Calculate weighted average wait for the day
            // Weight by sample count to match statistics.avgWaitToday calculation
            const totalSamples = dayQueueData.reduce(
              (sum, h) => sum + h.sampleCount,
              0,
            );
            const totalAvgWait =
              totalSamples > 0
                ? dayQueueData.reduce(
                    (sum, h) => sum + h.avgWait * h.sampleCount,
                    0,
                  ) / totalSamples
                : dayQueueData.reduce((sum, h) => sum + h.avgWait, 0) /
                  dayQueueData.length; // Fallback if sample counts missing

            // Baseline: P50 when available, else average of hourly P90s for this day
            const avgP90 =
              totalSamples > 0
                ? dayQueueData.reduce(
                    (sum, h) => sum + h.p90 * h.sampleCount,
                    0,
                  ) / totalSamples
                : dayQueueData.reduce((sum, h) => sum + h.p90, 0) /
                  dayQueueData.length;
            const baseline =
              attractionP50Baseline > 0 ? attractionP50Baseline : avgP90;

            if (baseline > 0) {
              utilization = this.analyticsService.getAttractionCrowdLevel(
                totalAvgWait,
                baseline,
              ) as CrowdLevel;
            } else {
              utilization = "closed";
            }
          }

          // Build hourly P90 array (only for hours within operating hours)
          const hourlyP90: Array<{ hour: string; value: number }> = [];
          const hourDataMap = new Map<
            number,
            { p90: number; avgWait: number; sampleCount: number }
          >();

          // Create map of existing hour data
          for (const hourData of dayQueueData) {
            hourDataMap.set(hourData.hour, {
              p90: hourData.p90,
              avgWait: hourData.avgWait,
              sampleCount: hourData.sampleCount,
            });
          }

          // Extract opening and closing hours from schedule if available
          // Round times to nearest 5 minutes (0, 5, 10, 15, 20, etc.)
          let openingHour: number | null = null;
          let closingHour: number | null = null;

          if (schedule && schedule.openingTime && schedule.closingTime) {
            // Get time in park timezone and round to nearest 5 minutes
            // schedule.openingTime and schedule.closingTime are stored as UTC timestamps
            // We need to convert them to park timezone first
            const openingTimeStr = formatInTimeZone(
              schedule.openingTime,
              timezone,
              "HH:mm",
            );
            const [openingHourRaw, openingMinuteRaw] = openingTimeStr
              .split(":")
              .map(Number);
            // Round minutes to nearest 5 (0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55)
            const openingMinuteRounded = Math.round(openingMinuteRaw / 5) * 5;
            // If rounded to 60, increment hour and set minute to 0
            if (openingMinuteRounded === 60) {
              openingHour = (openingHourRaw + 1) % 24;
            } else {
              openingHour = openingHourRaw;
            }

            // Debug logging
            this.logger.debug(
              `Schedule for ${dateStr}: openingTime UTC=${schedule.openingTime.toISOString()}, ` +
                `park timezone=${openingTimeStr}, rounded hour=${openingHour}`,
            );

            // Extract closing hour in park timezone.
            // closingTime may be on the schedule date or the next calendar day (e.g. close at 00:30 or 01:00).
            // scheduleDateStr and closingDateStr are both in park timezone (formatInParkTimezone).
            const scheduleDateStr = formatInParkTimezone(
              schedule.date,
              timezone,
            );
            const closingDateStr = formatInParkTimezone(
              schedule.closingTime,
              timezone,
            );
            // Next calendar day (timezone-neutral UTC arithmetic so server TZ doesn't matter)
            const nextDayStr = addDays(
              new Date(scheduleDateStr + "T00:00:00.000Z"),
              1,
            )
              .toISOString()
              .slice(0, 10);
            const closingSameOrNextDay =
              closingDateStr === scheduleDateStr ||
              closingDateStr === nextDayStr;

            if (closingSameOrNextDay) {
              const closingTimeStr = formatInTimeZone(
                schedule.closingTime,
                timezone,
                "HH:mm",
              );
              const [closingHourRaw, closingMinuteRaw] = closingTimeStr
                .split(":")
                .map(Number);
              // Round minutes to nearest 5
              const closingMinuteRounded = Math.round(closingMinuteRaw / 5) * 5;
              // If rounded to 60, increment hour and set minute to 0
              if (closingMinuteRounded === 60) {
                closingHour = (closingHourRaw + 1) % 24;
              } else {
                closingHour = closingHourRaw;
              }
            } else {
              this.logger.warn(
                `Invalid closingTime for ${dateStr}: closingTime date (${closingDateStr}) doesn't match schedule date (${scheduleDateStr}) or next day (${nextDayStr})`,
              );
            }
          }

          // Add all existing hour data
          for (const hourData of dayQueueData) {
            const hour = hourData.hour;
            const hourStr = `${hour.toString().padStart(2, "0")}:00`;
            hourlyP90.push({
              hour: hourStr,
              value: roundToNearest5Minutes(hourData.p90),
            });
          }

          // Ensure we have opening hour if schedule available
          // openingHour is already in park timezone (from formatInTimeZone)
          // SMART PROJECTION: Only add opening hour if earliest actual data is within 2 hours
          // This prevents false projections when attraction opened late (e.g., 15:00 instead of 11:00)
          const earliestDataHour =
            hourDataMap.size > 0
              ? Math.min(...Array.from(hourDataMap.keys()))
              : null;

          // Only project opening hour if we have data AND earliest data is within 2 hours of opening
          const shouldProjectOpening =
            openingHour !== null &&
            earliestDataHour !== null &&
            earliestDataHour - openingHour <= 2 && // Earliest data within 2 hours of opening
            earliestDataHour >= openingHour; // Don't project if data is before opening

          if (shouldProjectOpening) {
            // Check if we already have this hour in the array
            const openingHourStr = `${openingHour!.toString().padStart(2, "0")}:00`;
            const hasOpeningHour = hourlyP90.some(
              (h) => h.hour === openingHourStr,
            );

            if (!hasOpeningHour) {
              // Use the earliest hour's data for opening
              const nearestValue = hourDataMap.get(earliestDataHour!)?.p90 || 0;

              hourlyP90.push({
                hour: openingHourStr,
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
              ? Math.max(...Array.from(hourDataMap.keys()))
              : null;

          // Only project closing hour if we have data AND latest data is within 2 hours of closing
          const shouldProjectClosing =
            closingHour !== null &&
            latestDataHour !== null &&
            closingHour - latestDataHour <= 2 && // Latest data within 2 hours of closing
            latestDataHour <= closingHour; // Don't project if data is after closing

          if (shouldProjectClosing) {
            // Check if we already have this hour in the array
            const closingHourStr = `${closingHour!.toString().padStart(2, "0")}:00`;
            const hasClosingHour = hourlyP90.some(
              (h) => h.hour === closingHourStr,
            );

            if (!hasClosingHour) {
              // Use the latest hour's data for closing
              const nearestValue = hourDataMap.get(latestDataHour!)?.p90 || 0;

              hourlyP90.push({
                hour: closingHourStr,
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
            const currentHourInTimezone = parseInt(
              formatInTimeZone(now, timezone, "HH"),
              10,
            );
            const currentHourStr = `${currentHourInTimezone.toString().padStart(2, "0")}:00`;

            // Check if we already have data for current hour
            const hasCurrentHour = hourlyP90.some(
              (h) => h.hour === currentHourStr,
            );

            if (!hasCurrentHour) {
              // Get the latest available data point (from hourDataMap or last entry)
              let currentValue = 0;

              if (hourDataMap.size > 0) {
                // Use the most recent hour's data
                const latestHour = Math.max(...Array.from(hourDataMap.keys()));
                currentValue = hourDataMap.get(latestHour)?.p90 || 0;
              }

              hourlyP90.push({
                hour: currentHourStr,
                value: roundToNearest5Minutes(currentValue),
              });

              // Re-sort after adding current hour
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
