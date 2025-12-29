import { Injectable, Logger, Inject } from "@nestjs/common";
import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ParkWithAttractionsDto } from "../dto/park-with-attractions.dto";
import { WeatherItemDto } from "../dto/weather-item.dto";
import { ScheduleItemDto } from "../dto/schedule-item.dto";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";
import { HolidaysService } from "../../holidays/holidays.service";

/**
 * Park Integration Service
 *
 * Dedicated service for building integrated park responses with live data.
 * Separates complex business logic from controller (NestJS best practice).
 *
 * Responsibilities:
 * - Fetches and integrates data from multiple sources (weather, schedule, queue, ML, analytics)
 * - Caches responses for performance
 * - Builds complete ParkWithAttractionsDto
 */
@Injectable()
export class ParkIntegrationService {
  private readonly logger = new Logger(ParkIntegrationService.name);

  // Multi-tier caching strategy aligned with actual update frequencies
  private readonly TTL_INTEGRATED_RESPONSE_OPERATING = 5 * 60; // 5 minutes (Push-based caching: Updated by background job every 5m)
  private readonly TTL_INTEGRATED_RESPONSE_CLOSED = 6 * 60 * 60; // 6 hours (no live data changes)
  private readonly TTL_ML_DAILY = 24 * 60 * 60; // 24 hours (daily predictions update at 1am)
  private readonly TTL_ML_HOURLY = 60 * 60; // 1 hour (hourly predictions update at :15)
  private readonly TTL_WEATHER_FORECAST = 6 * 60 * 60; // 6 hours (forecast updates every 12h)
  private readonly TTL_WEATHER_CURRENT = 6 * 60 * 60; // 6 hours (current updates every 12h)
  private readonly TTL_SCHEDULE = 12 * 60 * 60; // 12 hours (schedule updates daily at 4am)
  private readonly TTL_QUEUE_DATA = 5 * 60; // 5 minutes (matches update frequency)
  private readonly TTL_ANALYTICS_PERCENTILES = 12 * 60 * 60; // 12 hours (percentiles update daily at 2am)

  constructor(
    private readonly parksService: ParksService,
    private readonly weatherService: WeatherService,
    private readonly attractionsService: AttractionsService,
    private readonly showsService: ShowsService,
    private readonly restaurantsService: RestaurantsService,
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
    private readonly mlService: MLService,
    private readonly predictionAccuracyService: PredictionAccuracyService,
    private readonly predictionDeviationService: PredictionDeviationService,
    private readonly holidaysService: HolidaysService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Build integrated park response with live data
   *
   * Fetches and integrates:
   * - Current weather + 16-day forecast
   * - Today's schedule
   * - Current wait times for all attractions
   * - Analytics (occupancy + statistics + percentiles)
   * - ML predictions (hourly + daily)
   * - Show times
   * - Restaurant status
   *
   * Cached for 15 minutes (Push-based: warmed by queue-bootstrap/cache-warmup)
   *
   * @param park - Park entity
   * @param skipCache - If true, bypass cache read and force rebuild (used by CacheWarmup)
   * @returns Complete park DTO with all integrated live data
   */
  async buildIntegratedResponse(
    park: Park,
    skipCache: boolean = false,
  ): Promise<ParkWithAttractionsDto> {
    // Try cache first (unless skipped)
    const cacheKey = `park:integrated:${park.id}`;

    if (!skipCache) {
      const cached = await this.redis.get(cacheKey);

      if (cached) {
        const cachedDto = JSON.parse(cached) as ParkWithAttractionsDto;

        // Self-Healing: Check if cache is "corrupted" (missing relations that exist in DB)
        // This fixes the issue where a cold start cached an incomplete response
        const hasShowsInDb = park.shows && park.shows.length > 0;
        const hasRestaurantsInDb =
          park.restaurants && park.restaurants.length > 0;

        const missingShowsInCache =
          hasShowsInDb && (!cachedDto.shows || cachedDto.shows.length === 0);
        const missingRestaurantsInCache =
          hasRestaurantsInDb &&
          (!cachedDto.restaurants || cachedDto.restaurants.length === 0);

        if (missingShowsInCache || missingRestaurantsInCache) {
          this.logger.warn(
            `Detected incomplete cache for ${park.slug} (DB has shows/restaurants, cache empty). Force rebuilding.`,
          );
          // Fall through to rebuild logic below
        } else {
          // Implement stale-while-revalidate pattern
          const ttl = await this.redis.ttl(cacheKey);
          if (ttl < 60 && ttl > 0) {
            // Cache expires in less than 1 minute, refresh in background
            this.refreshCacheInBackground(park).catch((err) =>
              this.logger.warn(
                `Background cache refresh failed for ${park.slug}:`,
                err,
              ),
            );
          }
          return cachedDto;
        }
      }
    }

    // Start with base DTO
    const dto = ParkWithAttractionsDto.fromEntity(park);

    // Fetch weather (current + forecast)
    const weatherData = await this.weatherService.getCurrentAndForecast(
      park.id,
    );
    dto.weather = {
      current: weatherData.current
        ? WeatherItemDto.fromEntity(weatherData.current)
        : null,
      forecast: weatherData.forecast.map((w) => WeatherItemDto.fromEntity(w)),
    };

    // Fetch upcoming schedule (today + next 7 days)
    const schedule = await this.parksService.getUpcomingSchedule(park.id, 7);
    dto.schedule = schedule.map((s) => ScheduleItemDto.fromEntity(s));

    // Fetch current queue data for all attractions
    // ALWAYS fetch queue data to detect activity for parks without schedules
    const queueDataMap = await this.queueDataService.findCurrentStatusByPark(
      park.id,
    );

    // Collect ride status data for fallback logic (parks without schedules)
    const rideStatusData: import("../../common/utils/status-calculator.util").RideStatusData[] =
      [];
    for (const queueDataArray of queueDataMap.values()) {
      for (const qd of queueDataArray) {
        rideStatusData.push({
          status: qd.status,
          waitTime: qd.waitTime,
          lastUpdated: qd.timestamp,
        });
      }
    }

    // Determine overall park status using CENTRAL UTILITY
    // Hybrid strategy: Schedule-based (primary) + Ride-based fallback (for parks without schedules)
    const { isParkOpen } =
      await import("../../common/utils/status-calculator.util");
    const isOpen = isParkOpen(schedule, rideStatusData);
    dto.status = isOpen ? "OPERATING" : "CLOSED";

    // Fetch ML Predictions (Hourly for attractions, Daily for park)
    // IMPORTANT: Daily predictions limited to 16 days (like weather forecast)
    const hourlyPredictions: Record<string, any[]> = {};
    let dailyPredictions: import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] =
      [];

    try {
      const [hourlyRes, dailyRes] = await Promise.all([
        this.mlService.getParkPredictions(park.id, "hourly"),
        this.mlService.getParkPredictions(park.id, "daily", 16), // Limit to 16 days
      ]);

      // Filter hourly predictions:
      // - If park OPERATING: Show today's hourly predictions
      // - If park CLOSED: Show tomorrow's hourly predictions (trip planning)
      // Use park's timezone to determine "today" and "tomorrow"
      const todayInParkTz = park.timezone
        ? getCurrentDateInTimezone(park.timezone)
        : new Date().toISOString().split("T")[0];

      const tomorrowDate = new Date();
      tomorrowDate.setDate(tomorrowDate.getDate() + 1);
      const tomorrowInParkTz = park.timezone
        ? getCurrentDateInTimezone(park.timezone)
        : tomorrowDate.toISOString().split("T")[0];

      const targetDateStr =
        dto.status === "OPERATING" ? todayInParkTz : tomorrowInParkTz;

      if (hourlyRes && hourlyRes.predictions) {
        for (const p of hourlyRes.predictions) {
          // Check if prediction is for target date
          if (p.predictedTime.startsWith(targetDateStr)) {
            if (!hourlyPredictions[p.attractionId])
              hourlyPredictions[p.attractionId] = [];
            hourlyPredictions[p.attractionId].push(p);
          }
        }
      }

      if (dailyRes && dailyRes.predictions) {
        dailyPredictions = this.aggregateDailyPredictions(dailyRes.predictions);
      }
    } catch (error) {
      this.logger.warn(`ML Service unavailable for park ${park.slug}:`, error);
    }

    dto.crowdForecast = dailyPredictions;

    // Initialize counters for attraction status tracking
    let totalAttractionsCount = 0;
    let totalOperatingCount = 0;

    if (
      park.attractions &&
      park.attractions.length > 0 &&
      dto.attractions &&
      dto.attractions.length > 0
    ) {
      for (const attraction of dto.attractions) {
        totalAttractionsCount++;

        // Get current queue data for this attraction from the bulk result
        const queueData = queueDataMap.get(attraction.id) || [];

        if (queueData.length > 0) {
          // Convert to DTOs (removed timestamp field)
          attraction.queues = queueData.map((qd) => ({
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
          }));

          // Set overall status (use first queue's status as representative)
          attraction.status = queueData[0].status;
        } else {
          // Fallback if no live data found
          attraction.queues = [];

          // If park is OPERATING (via schedule or override), but this specific ride has no data,
          // we default to CLOSED per ride, unless we want to be smarter.
          // But existing logic was: if park closed, all closed.
          // If park open, no data -> closed. This works.
          attraction.status = "CLOSED";
        }

        // Calculate Effective Status
        // If Park is CLOSED, all attractions are effectively CLOSED
        // This prevents frontend from showing "Operating" rides when park is closed
        attraction.effectiveStatus =
          dto.status === "CLOSED" ? "CLOSED" : attraction.status;

        // Count operating attractions based on EFFECTIVE status
        // This ensures closed parks show 0 operating attractions
        if (attraction.effectiveStatus === "OPERATING") {
          totalOperatingCount++;
        }

        // Attach ML predictions
        const mlPreds = hourlyPredictions[attraction.id] || [];

        // Enrich predictions with deviation data (Confidence Downgrade)
        const enrichedPreds = await this.enrichPredictionsWithDeviations(
          attraction.id,
          mlPreds,
        );

        attraction.hourlyForecast = enrichedPreds.map((p) => ({
          predictedTime: p.predictedTime,
          predictedWaitTime: p.predictedWaitTime,
          confidencePercentage: p.confidenceAdjusted ?? p.confidence,
          trend: p.trend,
          currentWaitTime: p.currentWaitTime,
          deviationDetected: p.deviationDetected,
        }));

        // Attach Prediction Accuracy (Feedback Loop)
        try {
          const accuracy =
            await this.predictionAccuracyService.getAttractionAccuracyWithBadge(
              attraction.id,
            );

          // Map to public DTO, excluding technical metrics
          attraction.predictionAccuracy = {
            badge: accuracy.badge,
            last30Days: {
              comparedPredictions: accuracy.last30Days.comparedPredictions,
              totalPredictions: accuracy.last30Days.totalPredictions,
            },
            message: accuracy.message,
          };
        } catch (error) {
          this.logger.error(
            `Failed to fetch prediction accuracy for ${attraction.id}:`,
            error,
          );
          attraction.predictionAccuracy = null;
        }

        // Attach Current Load Rating (Relative Wait Time)
        // Only if we have live wait time data
        const standbyQueue = attraction.queues.find(
          (q) => q.queueType === "STANDBY" && q.waitTime !== null,
        );

        if (standbyQueue && typeof standbyQueue.waitTime === "number") {
          try {
            // Get 90th percentile baseline (1 year window)
            const hour = new Date().getHours();
            const day = new Date().getDay();

            const p90 = await this.analyticsService.get90thPercentileOneYear(
              attraction.id,
              hour,
              day,
              "attraction",
            );

            const ratingResult = this.analyticsService.getLoadRating(
              standbyQueue.waitTime,
              p90,
            );

            attraction.currentLoad = {
              crowdLevel: ratingResult.rating as any,
              baseline: ratingResult.baseline,
              currentWaitTime: standbyQueue.waitTime,
            };
          } catch (_error) {
            // specific logging if needed, or silent fail
          }
        } else {
          attraction.currentLoad = null;
        }
      }
    }

    // Calculate Park-level Current Load
    if (dto.status === "OPERATING" && totalOperatingCount > 0) {
      try {
        // Calculate current average wait time from the data we just fetched
        let totalWait = 0;
        let count = 0;

        for (const attr of dto.attractions) {
          const standby = attr.queues?.find(
            (q) => q.queueType === "STANDBY" && q.waitTime !== null,
          );
          if (standby && typeof standby.waitTime === "number") {
            totalWait += standby.waitTime;
            count++;
          }
        }

        if (count > 0) {
          const currentAvgWait = Math.round(totalWait / count);
          const hour = new Date().getHours();
          const day = new Date().getDay();

          const p90Park = await this.analyticsService.get90thPercentileOneYear(
            park.id,
            hour,
            day,
            "park",
          );

          const parkRating = this.analyticsService.getLoadRating(
            currentAvgWait,
            p90Park,
          );

          dto.currentLoad = {
            crowdLevel: parkRating.rating as any,
            baseline: parkRating.baseline,
            currentWaitTime: currentAvgWait,
          };
        }
      } catch (_error) {
        // Silent fail for optional metadata
      }
    }

    // Removed problematic fallback heuristic - timezone-aware status is reliable

    // Fetch current status for shows
    if (park.shows && park.shows.length > 0) {
      let showLiveDataMap = new Map<string, any>();
      // Always fetch to check for overrides?
      // For shows/restaurants, less critical, but let's be consistent:
      // If Park is now OPERATING (potentially via override), we fetch live data.
      if (dto.status === "OPERATING") {
        showLiveDataMap = await this.showsService.findCurrentStatusByPark(
          park.id,
        );
      } else {
        // Park remains CLOSED
        showLiveDataMap = await this.showsService.findTodayOperatingDataByPark(
          park.id,
          park.timezone,
        );
      }

      for (const show of dto.shows || []) {
        const liveData = showLiveDataMap.get(show.id);
        if (liveData) {
          // Keep operatingHours always (general schedule info)
          show.operatingHours = liveData.operatingHours || [];
          show.showtimes = liveData.showtimes || [];

          // If park is OPERATING, use live status. If CLOSED, force CLOSED but show times.
          show.status = dto.status === "OPERATING" ? liveData.status : "CLOSED";
          show.lastUpdated = liveData.lastUpdated?.toISOString();
        } else {
          // No live data available
          show.showtimes = [];
          show.operatingHours = [];
          show.status = "CLOSED";
          show.lastUpdated = undefined;
        }
      }

      // Filter out shows with empty showtimes regardless of park status
      // This hides "broken" shows that should be running but have no data (e.g. missing upstream)
      dto.shows = (dto.shows || []).filter(
        (s) => s.showtimes && s.showtimes.length > 0,
      );
    }

    // Fetch current status for restaurants
    if (park.restaurants && park.restaurants.length > 0) {
      let restaurantLiveDataMap = new Map<string, any>();

      if (dto.status === "OPERATING") {
        restaurantLiveDataMap =
          await this.restaurantsService.findCurrentStatusByPark(park.id);
      } else {
        // Park is CLOSED - Fetch "Today's" operating data
        restaurantLiveDataMap =
          await this.restaurantsService.findTodayOperatingDataByPark(
            park.id,
            park.timezone,
          );
      }

      for (const restaurant of dto.restaurants || []) {
        const liveData = restaurantLiveDataMap.get(restaurant.id);
        if (liveData) {
          // Keep operatingHours always (general schedule info)
          restaurant.operatingHours = liveData.operatingHours || [];

          // Only show wait times if park is OPERATING
          if (dto.status === "OPERATING") {
            restaurant.status = liveData.status;
            restaurant.waitTime = liveData.waitTime;
            restaurant.partySize = liveData.partySize;
          } else {
            restaurant.status = "CLOSED";
            restaurant.waitTime = null;
            restaurant.partySize = null;
          }
          restaurant.lastUpdated = liveData.lastUpdated?.toISOString();
        } else {
          // No live data available
          restaurant.operatingHours = [];

          // Improved Fallback: If park is OPERATING, assume restaurant is OPERATING
          // Many restaurants don't have real-time wait times but are open
          restaurant.status =
            dto.status === "OPERATING" ? "OPERATING" : "CLOSED";

          restaurant.waitTime = null;
          restaurant.partySize = null;
          restaurant.lastUpdated = undefined;
        }
      }
    }

    // Fetch analytics (occupancy + statistics + percentiles)
    // Only fetch live analytics if park is operating, otherwise return zeroed values
    if (dto.status === "OPERATING") {
      try {
        const [occupancy, statistics, percentiles] = await Promise.all([
          this.analyticsService.calculateParkOccupancy(park.id),
          this.analyticsService.getParkStatistics(park.id),
          this.analyticsService.getParkPercentilesToday(park.id),
        ]);

        dto.analytics = {
          occupancy: {
            current: occupancy.current,
            trend: occupancy.trend,
            comparedToTypical: occupancy.comparedToTypical,
            comparisonStatus: occupancy.comparisonStatus,
            baseline90thPercentile: occupancy.baseline90thPercentile,
            updatedAt: occupancy.updatedAt,
            breakdown: occupancy.breakdown,
          },
          statistics: {
            avgWaitTime: statistics.avgWaitTime,
            avgWaitToday: statistics.avgWaitToday,
            peakHour: statistics.peakHour,
            crowdLevel: statistics.crowdLevel,
            totalAttractions: statistics.totalAttractions,
            operatingAttractions: statistics.operatingAttractions,
            closedAttractions: statistics.closedAttractions,
            timestamp: statistics.timestamp.toISOString(),
          },
          percentiles: percentiles || undefined, // Only include if data available
        };
      } catch (error) {
        // Log error but don't fail the whole request
        this.logger.error("Failed to fetch analytics:", error);
        dto.analytics = null;
      }
    } else {
      // Park is CLOSED - Fetch today's historical analytics and provide "Typical" values for context
      try {
        const [statistics, percentiles] = await Promise.all([
          this.analyticsService.getParkStatistics(park.id),
          this.analyticsService.getParkPercentilesToday(park.id),
        ]);

        // Get typical rating for "right now" even if closed, to show what it would be like
        const hour = new Date().getHours();
        const day = new Date().getDay();
        const p90Park = await this.analyticsService.get90thPercentileOneYear(
          park.id,
          hour,
          day,
          "park",
        );
        // Note: We don't have a "current" wait, so we can't calculate a rating.
        // But we can populate baseline90thPercentile to show "Typical Wait: X min"

        dto.analytics = {
          occupancy: {
            current: 0, // No current occupancy
            trend: "stable",
            comparedToTypical: 0,
            comparisonStatus: "closed",
            baseline90thPercentile: p90Park || 0, // Show typical wait for this time
            updatedAt: new Date().toISOString(),
            breakdown: {
              currentAvgWait: 0,
              typicalAvgWait: p90Park || 0, // Use p90 as proxy for typical
              activeAttractions: 0,
            },
          },
          statistics: {
            avgWaitTime: 0, // No current wait
            avgWaitToday: statistics.avgWaitToday || 0, // Historical from when park was open today
            peakHour: statistics.peakHour || null, // Historical peak hour
            crowdLevel: "very_low", // Currently very low (closed)
            totalAttractions: totalAttractionsCount,
            operatingAttractions: 0,
            closedAttractions: totalAttractionsCount,
            timestamp: new Date().toISOString(),
          },
          percentiles: percentiles || undefined, // Historical percentiles for planning
        };
      } catch (error) {
        this.logger.error(
          "Failed to fetch historical analytics for closed park:",
          error,
        );
        // Fallback to fully zeroed if error
        dto.analytics = {
          occupancy: {
            current: 0,
            trend: "stable",
            comparedToTypical: 0,
            comparisonStatus: "closed",
            baseline90thPercentile: 0,
            updatedAt: new Date().toISOString(),
            breakdown: {
              currentAvgWait: 0,
              typicalAvgWait: 0,
              activeAttractions: 0,
            },
          },
          statistics: {
            avgWaitTime: 0,
            avgWaitToday: 0,
            peakHour: null,
            crowdLevel: "very_low",
            totalAttractions: totalAttractionsCount,
            operatingAttractions: 0,
            closedAttractions: totalAttractionsCount,
            timestamp: new Date().toISOString(),
          },
          percentiles: undefined,
        };
      }
    }

    // Enrich schedule with holiday data (covers weekends that might be missing in scraped data)
    if (dto.schedule && dto.schedule.length > 0) {
      try {
        // Find date range
        const dates = dto.schedule.map((s) => s.date).sort();
        const minDate = new Date(dates[0]);
        const maxDate = new Date(dates[dates.length - 1]);

        // Fetch all holidays for this range
        const holidays = await this.holidaysService.getHolidays(
          park.countryCode,
          minDate,
          maxDate,
        );

        // Map for fast lookup: "YYYY-MM-DD" -> Holiday
        const holidayMap = new Map<string, any>();
        for (const h of holidays) {
          // Filter by region: Only include if nationwide OR matches park region
          // Note: School holidays often have region set (e.g. DE-NW), need to match park region (e.g. NW)
          if (
            h.isNationwide ||
            !h.region ||
            h.region === park.regionCode ||
            (park.regionCode &&
              h.region &&
              h.region.endsWith(`-${park.regionCode}`))
          ) {
            // TypeORM might return date as string "YYYY-MM-DD" or Date object
            const dateStr =
              h.date instanceof Date
                ? h.date.toISOString().split("T")[0]
                : (h.date as unknown as string);

            // Prioritize public holidays over school holidays for naming if collision
            // But always keep record if any holiday exists
            if (
              !holidayMap.has(dateStr) ||
              h.holidayType === "public" ||
              h.holidayType === "bank"
            ) {
              holidayMap.set(dateStr, h);
            }
          }
        }
        // Apply to schedule items
        for (const item of dto.schedule) {
          const holiday = holidayMap.get(item.date);
          if (holiday) {
            item.isHoliday = true;
            // Only overwrite name if it's currently null or we have a better name (e.g. specific holiday vs generic)
            if (!item.holidayName) {
              item.holidayName = holiday.name;
            }
          }
        }
      } catch (error) {
        this.logger.warn(
          `Failed to enrich schedule with holidays for ${park.slug}: ${error}`,
        );
      }
    }

    // Fetch school holiday status for today
    try {
      dto.isSchoolVacation = await this.holidaysService.isSchoolHoliday(
        new Date(),
        park.countryCode,
        park.regionCode,
        park.timezone,
      );
    } catch (error) {
      this.logger.warn(
        `Failed to check school holiday for ${park.slug}:`,
        error,
      );
      dto.isSchoolVacation = false;
    }

    // Cache the complete response with dynamic TTL
    // For CLOSED parks: TTL expires ~5 min before next opening to ensure fresh data
    // For OPERATING parks: Short 3-minute TTL for live data
    const ttl = this.calculateDynamicTTL(dto.status, schedule, park.timezone);

    await this.redis.set(cacheKey, JSON.stringify(dto), "EX", ttl);

    return dto;
  }

  /**
   * Refresh cache in background (stale-while-revalidate pattern)
   * This ensures users always get fast cached responses
   */
  private async refreshCacheInBackground(park: Park): Promise<void> {
    // Clear the cache and rebuild
    const cacheKey = `park:integrated:${park.id}`;
    await this.redis.del(cacheKey);

    // Rebuild will automatically cache the result
    await this.buildIntegratedResponse(park);
  }

  /**
   * Calculate dynamic TTL based on park status and next opening time
   *
   * For OPERATING parks: Use short TTL (3 minutes) for fresh live data
   * For CLOSED parks: Calculate TTL to expire ~5 minutes before next opening
   *
   * This prevents the issue where an early morning request caches CLOSED status
   * for 6 hours, causing the park to appear closed even after it opens.
   *
   * @param status - Current park status ("OPERATING" or "CLOSED")
   * @param schedules - Park schedule entries
   * @param timezone - Park timezone (e.g., "Europe/Berlin")
   * @returns TTL in seconds
   */
  private calculateDynamicTTL(
    status: string,
    schedules: ScheduleEntry[],
    _timezone: string, // Unused but kept for backwards compatibility
  ): number {
    if (status === "OPERATING") {
      // Park is open -> use short TTL for fresh live data
      return this.TTL_INTEGRATED_RESPONSE_OPERATING; // 5 minutes
    }

    // Park is CLOSED - find next opening time
    // Use UTC for comparison (schedule times are stored as UTC)
    const now = new Date(); // Current time in UTC

    // Find next OPERATING schedule entry
    const nextOpening = schedules
      .filter(
        (s) =>
          s.scheduleType === "OPERATING" &&
          s.openingTime &&
          s.openingTime > now,
      )
      .sort((a, b) => a.openingTime!.getTime() - b.openingTime!.getTime())[0];

    if (nextOpening && nextOpening.openingTime) {
      // Calculate seconds until opening
      const secondsUntilOpening = Math.floor(
        (nextOpening.openingTime.getTime() - now.getTime()) / 1000,
      );

      // TTL = time until opening - 5 minute buffer (cache expires before opening)
      // This ensures fresh data when park opens
      const bufferSeconds = 5 * 60; // 5 minutes
      const ttl = Math.max(60, secondsUntilOpening - bufferSeconds);

      // Cap at 6 hours to avoid extremely long TTLs for off-season parks
      const cappedTTL = Math.min(ttl, this.TTL_INTEGRATED_RESPONSE_CLOSED);

      this.logger.debug(
        `Dynamic TTL for CLOSED park: ${Math.floor(cappedTTL / 60)} minutes ` +
          `(opens in ${Math.floor(secondsUntilOpening / 60)} minutes)`,
      );

      return cappedTTL;
    }

    // No next opening found (off-season or no schedule data)
    // Fall back to default CLOSED TTL
    // OPTIMIZED: Use shorter TTL (30 mins instead of 6 hours) to re-check status more frequently
    // This allows fast recovery if we missed an opening due to missing/late schedule
    const fallbackTTL = 30 * 60; // 30 minutes
    this.logger.debug(
      `No next opening time found, using fallback TTL (30 mins)`,
    );
    return fallbackTTL;
  }

  /**
   * Helper: Aggregate attraction daily predictions into park-level daily predictions
   * Public so it can be used by yearly predictions route
   */
  public aggregateDailyPredictions(
    predictions: any[],
  ): import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] {
    const datesMap = new Map<string, any[]>();

    // Group by date
    for (const p of predictions) {
      // predictedTime is ISO string, take YYYY-MM-DD
      const date = p.predictedTime.split("T")[0];
      if (!datesMap.has(date)) {
        datesMap.set(date, []);
      }
      datesMap.get(date)!.push(p);
    }

    const result: import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] =
      [];

    const crowdLevelMap = {
      closed: 0,
      very_low: 1,
      low: 2,
      moderate: 3,
      high: 4,
      very_high: 5,
      extreme: 6,
    };
    const reverseCrowdMap = [
      "closed",
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
    ];

    for (const [date, dailyPreds] of datesMap) {
      if (dailyPreds.length === 0) continue;

      // Filter for significant attractions (predicted wait > 10 min)
      // This ensures we don't dilute the rating with rides that have no wait
      const significantPreds = dailyPreds.filter(
        (p) => p.predictedWaitTime > 10,
      );

      // Use significant predictions if we have any, otherwise fall back to all
      // (e.g., if everything is < 10 min, the park is likely empty/very low crowds)
      const predsToUse =
        significantPreds.length > 0 ? significantPreds : dailyPreds;

      let totalScore = 0;
      let totalConfidence = 0;

      for (const p of predsToUse) {
        const level = p.crowdLevel as keyof typeof crowdLevelMap;
        totalScore += crowdLevelMap[level] || 0;
        totalConfidence += p.confidence || 0;
      }

      const avgScore = Math.round(totalScore / predsToUse.length);
      const avgConfidence = totalConfidence / predsToUse.length;

      const crowdLevel = reverseCrowdMap[avgScore] as any;

      let recommendation:
        | "highly_recommended"
        | "recommended"
        | "neutral"
        | "avoid"
        | "strongly_avoid"
        | "closed" = "neutral";
      if (crowdLevel === "closed") recommendation = "closed";
      else if (crowdLevel === "very_low" || crowdLevel === "low")
        recommendation = "highly_recommended";
      else if (crowdLevel === "moderate") recommendation = "recommended";
      else if (crowdLevel === "high") recommendation = "neutral";
      else if (crowdLevel === "very_high") recommendation = "avoid";
      else recommendation = "strongly_avoid"; // extreme

      result.push({
        date,
        crowdLevel,
        confidencePercentage: avgConfidence,
        recommendation,
        source: "ml", // Predictions come from our ML service
      });
    }

    return result.sort((a, b) => a.date.localeCompare(b.date));
  }

  /**
   * Enrich predictions with deviation data (Confidence Downgrade strategy)
   *
   * Checks Redis for deviation flags and adjusts confidence if detected
   *
   * @param attractionId - Attraction ID
   * @param predictions - Raw predictions from ML service
   * @returns Enriched predictions with deviation data
   */
  private async enrichPredictionsWithDeviations(
    attractionId: string,
    predictions: any[],
  ): Promise<any[]> {
    if (!predictions || predictions.length === 0) {
      return predictions;
    }

    try {
      // Get deviation flag from Redis
      const deviationFlag =
        await this.predictionDeviationService.getDeviationFlag(attractionId);

      if (deviationFlag) {
        // Deviation detected - enrich all predictions for this attraction
        return predictions.map((p) => ({
          ...p,
          currentWaitTime: deviationFlag.actualWaitTime,
          confidenceAdjusted: p.confidence * 0.5, // Halve confidence
          deviationDetected: true,
          deviationInfo: {
            message: `Current wait ${Math.abs(deviationFlag.deviation).toFixed(0)}min ${
              deviationFlag.deviation > 0 ? "higher" : "lower"
            } than predicted`,
            deviation: deviationFlag.deviation,
            percentageDeviation: deviationFlag.percentageDeviation,
            detectedAt: deviationFlag.detectedAt,
          },
        }));
      }

      // No deviation - return predictions unchanged
      return predictions;
    } catch (error) {
      // Don't fail request if enrichment fails
      this.logger.warn(
        `Failed to enrich predictions for ${attractionId}:`,
        error,
      );
      return predictions;
    }
  }
}
