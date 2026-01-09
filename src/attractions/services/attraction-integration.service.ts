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
import { subDays } from "date-fns";
import { fromZonedTime, formatInTimeZone } from "date-fns-tz";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";

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

    // Fetch current queue data (all queue types)
    // Uses park opening hours to determine valid data cutoff
    // Falls back to 6 hours if no schedule available
    const queueData = await this.queueDataService.findCurrentStatusByAttraction(
      attraction.id,
    );

    if (queueData.length > 0) {
      // Convert to DTOs and add trend data
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
              // If trend calculation fails, don't include it (optional field)
              this.logger.warn(
                `Failed to calculate trend for ${qd.queueType}:`,
                error,
              );
            }
          }

          return queueDto;
        }),
      );

      // Set overall status (use first queue's status as representative)
      dto.status = queueData[0].status;

      // Extract trend from primary queue (STANDBY or first available)
      const primaryQueue =
        dto.queues?.find((q) => q.queueType === "STANDBY") || dto.queues?.[0];
      if (primaryQueue?.trend?.direction) {
        // Map "increasing" -> "up", "decreasing" -> "down", "stable" -> "stable"
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

    // Check park status and calculate effectiveStatus
    // Attractions inherit park's closed status to prevent showing operating rides when park is closed
    let parkStatus: "OPERATING" | "CLOSED" = "CLOSED";
    if (attraction.parkId) {
      try {
        const statusMap = await this.parksService.getBatchParkStatus([
          attraction.parkId,
        ]);
        parkStatus = statusMap.get(attraction.parkId) || "CLOSED";
      } catch (error) {
        this.logger.warn(
          `Failed to fetch park status for attraction ${attraction.id}:`,
          error,
        );
        // Safe default: assume closed
        parkStatus = "CLOSED";
      }
    }

    // Calculate effective status
    // If park is CLOSED, attraction is effectively CLOSED regardless of queue data
    dto.effectiveStatus = parkStatus === "CLOSED" ? "CLOSED" : dto.status;

    // Fetch forecasts (ThemeParks.wiki - next 24 hours)
    const forecasts = await this.queueDataService.findForecastsByAttraction(
      attraction.id,
      24,
    );

    if (forecasts.length > 0) {
      dto.forecasts = forecasts.map((f) => ({
        predictedTime: f.predictedTime.toISOString(),
        predictedWaitTime: f.predictedWaitTime,
        confidencePercentage: f.confidencePercentage,
        source: f.source,
      }));
    }

    // Fetch ML predictions (our model - daily, up to 1 year)
    // Only if ML service is available
    let enrichedPredictions: Array<{
      predictedTime: string;
      predictedWaitTime: number;
      confidence: number;
      crowdLevel?: string;
      baseline?: number;
      trend: string;
    }> = [];
    try {
      const isHealthy = await this.mlService.isHealthy();
      if (isHealthy) {
        const predictions =
          await this.mlService.getAttractionPredictionsWithFallback(
            attraction.id,
            "hourly",
          );

        enrichedPredictions = predictions.map((p) => ({
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
      }
    } catch (error) {
      // ML service not available - log but don't fail
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      this.logger.warn("ML predictions unavailable:", errorMessage);
      dto.hourlyForecast = [];
    }

    // Calculate crowd level
    // Strategy: Use real-time wait time with P90 baseline if available,
    // otherwise fallback to ML prediction crowdLevel
    let crowdLevel: CrowdLevel | "closed" | null = null;
    if (dto.effectiveStatus === "CLOSED") {
      crowdLevel = "closed";
    } else {
      // Find current hour prediction for fallback
      const nowStr = new Date().toISOString().split(":")[0]; // "YYYY-MM-DDTHH"
      const currentPred = enrichedPredictions.find((p) =>
        p.predictedTime.startsWith(nowStr),
      );

      // 1. Try to use REAL-TIME Wait Time first (Ground Truth)
      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null) {
        // Get P90 baseline for relative crowd level (context-aware)
        try {
          const percentiles =
            await this.analyticsService.getAttractionPercentilesToday(
              attraction.id,
            );
          const p90 = percentiles?.p90 || 0;
          const { rating } = this.analyticsService.getLoadRating(wait, p90);
          crowdLevel = rating;
        } catch (error) {
          // If percentile lookup fails, fallback to ML prediction
          this.logger.warn(
            `Failed to get percentiles for crowd level, using fallback:`,
            error,
          );
          if (currentPred?.crowdLevel) {
            crowdLevel = currentPred.crowdLevel as any;
          } else {
            crowdLevel = "very_low";
          }
        }
      } else {
        // 2. Fallback to ML Prediction if no live data
        if (currentPred?.crowdLevel) {
          crowdLevel = currentPred.crowdLevel as any;
        } else {
          // 3. Last resort default
          crowdLevel = "very_low";
        }
      }
    }
    dto.crowdLevel = crowdLevel;

    // Calculate baseline and comparison (same logic as park-integration)
    if (dto.effectiveStatus === "OPERATING") {
      const wait = dto.queues?.[0]?.waitTime;
      if (wait !== undefined && wait !== null) {
        try {
          const percentiles =
            await this.analyticsService.getAttractionPercentilesToday(
              attraction.id,
            );
          const p90 = percentiles?.p90 || 0;

          if (p90 > 0) {
            const loadRating = this.analyticsService.getLoadRating(wait, p90);
            dto.baseline = loadRating.baseline;
            dto.comparison = this.analyticsService.getComparisonText(
              loadRating.rating,
            );
          } else {
            dto.baseline = null;
            dto.comparison = null;
          }
        } catch (error) {
          // If percentile lookup fails, set to null
          this.logger.warn(
            `Failed to get percentiles for baseline/comparison:`,
            error,
          );
          dto.baseline = null;
          dto.comparison = null;
        }
      } else {
        dto.baseline = null;
        dto.comparison = null;
      }
    } else {
      dto.baseline = null;
      dto.comparison = null;
    }

    // Fetch attraction statistics (requires park timezone for accurate daily filtering)
    // Also fetch park for URL generation
    let parkForUrl: Park | null = null;
    try {
      // Fetch park entity to get timezone and geo data
      const park = await this.parkRepository.findOne({
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
        ],
      });
      if (!park) {
        throw new Error(`Park not found for attraction ${attraction.id}`);
      }

      // Store park for URL generation later
      parkForUrl = park;

      const startTime = await this.analyticsService.getEffectiveStartTime(
        attraction.parkId,
        park.timezone,
      );

      const statistics = await this.analyticsService.getAttractionStatistics(
        attraction.id,
        startTime,
        park.timezone,
      );

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

      // Calculate historical data (utilization, hourly P90, down counts)
      if (parkForUrl && parkForUrl.timezone) {
        try {
          dto.history = await this.calculateAttractionHistory(
            attraction.id,
            attraction.parkId,
            parkForUrl.timezone,
            days,
          );
        } catch (error) {
          // Log but don't fail - history is optional
          this.logger.warn(
            `Failed to calculate history for attraction ${attraction.id}:`,
            error,
          );
          dto.history = [];
        }

        // Fetch park schedule for the same date range (aligned with history)
        // Schedule: from today back to (today - 30 days) = 31 days total
        try {
          // Calculate date range in park timezone: today back to (today - days)
          // Example: days=30 means today + 30 past days = 31 days total
          // Use getStartOfDayInTimezone to ensure "today" is correctly calculated in park timezone
          const today = getStartOfDayInTimezone(parkForUrl.timezone);
          const startDate = subDays(today, days); // today - 30 days for days=30
          // End date: today (inclusive) - already calculated in park timezone

          // Get schedule entries for the park (only park-level, not attraction-specific)
          const scheduleEntries = await this.scheduleEntryRepository
            .createQueryBuilder("schedule")
            .where("schedule.parkId = :parkId", { parkId: attraction.parkId })
            .andWhere("schedule.attractionId IS NULL") // Only park schedules
            .andWhere("schedule.date >= :startDate", { startDate })
            .andWhere("schedule.date <= :endDate", { endDate: today })
            .orderBy("schedule.date", "ASC")
            .addOrderBy("schedule.scheduleType", "ASC")
            .getMany();

          // Convert to DTOs
          dto.schedule = scheduleEntries.map((entry) =>
            ScheduleItemDto.fromEntity(entry),
          );
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

    // Fetch prediction accuracy
    try {
      const accuracy =
        await this.predictionAccuracyService.getAttractionAccuracyWithBadge(
          attraction.id,
          30, // Last 30 days
        );

      dto.predictionAccuracy = {
        badge: accuracy.badge,
        last30Days: {
          // Only expose counts to public, not technical metrics
          comparedPredictions: accuracy.last30Days.comparedPredictions,
          totalPredictions: accuracy.last30Days.totalPredictions,
        },
        message: accuracy.message,
      };
    } catch (error) {
      // Log error but don't fail - accuracy is optional
      this.logger.warn("Failed to fetch prediction accuracy:", error);
      dto.predictionAccuracy = null;
    }

    // Set URL using geo route (if park has geo data)
    // Try to use park from attraction relation first, or reuse park fetched for statistics
    if (!parkForUrl) {
      parkForUrl = attraction.park || null;
    }

    // If park relation not loaded or missing geo data, fetch it
    if (
      !parkForUrl ||
      !parkForUrl.continentSlug ||
      !parkForUrl.countrySlug ||
      !parkForUrl.citySlug
    ) {
      if (attraction.parkId) {
        // Fetch park with all fields (including geo slugs)
        parkForUrl = await this.parkRepository.findOne({
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
          ],
        });
      }
    }

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
        // Apply 5-minute rounding to cached data (in case cache was created before rounding fix)
        const rounded = parsed.map((day: HistoryDayDto) => ({
          ...day,
          hourlyP90: day.hourlyP90.map((h) => ({
            ...h,
            value: roundToNearest5Minutes(h.value),
          })),
        }));
        this.logger.log(
          `Using cached history for ${attractionId}: ${rounded.length} days`,
        );
        return rounded;
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

      // Debug logging
      this.logger.log(
        `History query for attraction ${attractionId}: ` +
          `todayStr=${todayStr}, tomorrowStr=${tomorrowStr}, ` +
          `startDate=${startDate.toISOString()}, endDate=${endDate.toISOString()}, ` +
          `days=${days}, timezone=${timezone}`,
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
      const distinctDates = Array.from(queueDataByDate.keys()).sort();
      console.log(
        `[DEBUG] History query for ${attractionId}: ` +
          `SQL returned ${queueDataResults.length} hour groups, ` +
          `${queueDataByDate.size} distinct dates: ${distinctDates.join(", ")}`,
      );
      this.logger.log(
        `History query returned ${queueDataResults.length} hour groups, ` +
          `covering ${queueDataByDate.size} distinct dates: ${distinctDates.join(", ")}`,
      );

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

            // Get P90 baseline (average of hourly P90s, weighted by sample count)
            const avgP90 =
              totalSamples > 0
                ? dayQueueData.reduce(
                    (sum, h) => sum + h.p90 * h.sampleCount,
                    0,
                  ) / totalSamples
                : dayQueueData.reduce((sum, h) => sum + h.p90, 0) /
                  dayQueueData.length; // Fallback if sample counts missing

            // Use analytics service to get crowd level
            if (avgP90 > 0) {
              utilization = this.analyticsService.getAttractionCrowdLevel(
                totalAvgWait,
                avgP90,
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

            // Extract closing hour in park timezone
            // Validate that closingTime is on the same day as the schedule date
            const scheduleDateStr = formatInParkTimezone(
              schedule.date,
              timezone,
            );
            const closingDateStr = formatInParkTimezone(
              schedule.closingTime,
              timezone,
            );

            // Only use closingTime if it's on the same day (handle data quality issues)
            if (scheduleDateStr === closingDateStr) {
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
              // Log warning but don't fail - use opening hour + reasonable default
              this.logger.warn(
                `Invalid closingTime for ${dateStr}: closingTime date (${closingDateStr}) doesn't match schedule date (${scheduleDateStr})`,
              );
              // Don't set closingHour - we'll just use opening hour
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
          if (openingHour !== null) {
            // Check if we already have this hour in the array
            const openingHourStr = `${openingHour.toString().padStart(2, "0")}:00`;
            const hasOpeningHour = hourlyP90.some(
              (h) => h.hour === openingHourStr,
            );

            if (!hasOpeningHour) {
              // Find nearest available hour data (prefer later hours)
              let nearestHour: number | null = null;
              let nearestValue: number | null = null;

              // Look for nearest hour with data
              for (const [hour, data] of hourDataMap.entries()) {
                if (hour >= openingHour) {
                  if (nearestHour === null || hour < nearestHour) {
                    nearestHour = hour;
                    nearestValue = data.p90;
                  }
                }
              }

              // If no later hour found, use earliest available
              if (nearestHour === null && hourDataMap.size > 0) {
                const sortedHours = Array.from(hourDataMap.keys()).sort(
                  (a, b) => a - b,
                );
                nearestHour = sortedHours[0];
                nearestValue = hourDataMap.get(nearestHour)!.p90;
              }

              // Add opening hour entry (format as HH:00 since we only track hours)
              const openingHourStr = `${openingHour.toString().padStart(2, "0")}:00`;
              hourlyP90.push({
                hour: openingHourStr,
                value:
                  nearestValue !== null
                    ? roundToNearest5Minutes(nearestValue)
                    : 0,
              });
            }
          }

          // Ensure we have closing hour if schedule available
          // closingHour is already in park timezone (from formatInTimeZone)
          if (closingHour !== null) {
            // Check if we already have this hour in the array
            const closingHourStr = `${closingHour.toString().padStart(2, "0")}:00`;
            const hasClosingHour = hourlyP90.some(
              (h) => h.hour === closingHourStr,
            );

            if (!hasClosingHour) {
              // Find nearest available hour data (prefer earlier hours)
              let nearestHour: number | null = null;
              let nearestValue: number | null = null;

              // Look for nearest hour with data
              for (const [hour, data] of hourDataMap.entries()) {
                if (hour <= closingHour) {
                  if (nearestHour === null || hour > nearestHour) {
                    nearestHour = hour;
                    nearestValue = data.p90;
                  }
                }
              }

              // If no earlier hour found, use latest available
              if (nearestHour === null && hourDataMap.size > 0) {
                const sortedHours = Array.from(hourDataMap.keys()).sort(
                  (a, b) => b - a,
                );
                nearestHour = sortedHours[0];
                nearestValue = hourDataMap.get(nearestHour)!.p90;
              }

              // Add closing hour entry (format as HH:00 since we only track hours)
              const closingHourStr = `${closingHour.toString().padStart(2, "0")}:00`;
              hourlyP90.push({
                hour: closingHourStr,
                value:
                  nearestValue !== null
                    ? roundToNearest5Minutes(nearestValue)
                    : 0,
              });
            }
          }

          // Sort by hour
          hourlyP90.sort((a, b) => a.hour.localeCompare(b.hour));

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

      // Cache the result
      await this.redis.set(cacheKey, JSON.stringify(history), "EX", ttl);

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
