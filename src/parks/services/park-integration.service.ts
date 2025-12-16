import { Injectable, Logger, Inject } from "@nestjs/common";
import { Park } from "../entities/park.entity";
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
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { toZonedTime } from "date-fns-tz";

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
  private readonly TTL_INTEGRATED_RESPONSE_OPERATING = 3 * 60; // 3 minutes (queue data updates every 5min)
  private readonly TTL_INTEGRATED_RESPONSE_CLOSED = 6 * 60 * 60; // 6 hours (no live data changes)
  private readonly TTL_ML_DAILY = 24 * 60 * 60; // 24 hours (daily predictions update at 1am)
  private readonly TTL_ML_HOURLY = 60 * 60; // 1 hour (hourly predictions update at :15)
  private readonly TTL_WEATHER_FORECAST = 6 * 60 * 60; // 6 hours (forecast updates every 12h)
  private readonly TTL_WEATHER_CURRENT = 6 * 60 * 60; // 6 hours (current updates every 12h)
  private readonly TTL_SCHEDULE = 12 * 60 * 60; // 12 hours (schedule updates daily at 4am)
  private readonly TTL_QUEUE_DATA = 3 * 60; // 3 minutes (updates every 5min, cache slightly less)
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
   * Cached for 5 minutes (balances freshness with performance)
   *
   * @param park - Park entity
   * @returns Complete park DTO with all integrated live data
   */
  async buildIntegratedResponse(park: Park): Promise<ParkWithAttractionsDto> {
    // Try cache first
    const cacheKey = `park:integrated:${park.id}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
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
      return JSON.parse(cached);
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

    // Determine overall park status based on schedule
    // CRITICAL: Use park's timezone to get current time, compare with UTC schedule times
    const nowInParkTz = toZonedTime(new Date(), park.timezone);
    const operatingSchedule = schedule.find(
      (s) =>
        s.scheduleType === "OPERATING" &&
        s.openingTime &&
        s.closingTime &&
        nowInParkTz >= s.openingTime &&
        nowInParkTz < s.closingTime,
    );
    dto.status = operatingSchedule ? "OPERATING" : "CLOSED";

    // Fetch ML Predictions (Hourly for attractions, Daily for park)
    const hourlyPredictions: Record<string, any[]> = {};
    let dailyPredictions: import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] =
      [];

    try {
      const [hourlyRes, dailyRes] = await Promise.all([
        this.mlService.getParkPredictions(park.id, "hourly"),
        this.mlService.getParkPredictions(park.id, "daily"),
      ]);

      // Filter hourly predictions:
      // - If park OPERATING: Show today's hourly predictions
      // - If park CLOSED: Show tomorrow's hourly predictions (trip planning)
      const targetDateStr =
        dto.status === "OPERATING"
          ? new Date().toISOString().split("T")[0] // Today
          : new Date(Date.now() + 24 * 60 * 60 * 1000)
              .toISOString()
              .split("T")[0]; // Tomorrow

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

    // Fetch current queue data for all attractions
    let totalAttractionsCount = 0;
    let totalOperatingCount = 0;

    if (
      park.attractions &&
      park.attractions.length > 0 &&
      dto.attractions &&
      dto.attractions.length > 0
    ) {
      // Only fetch live queue data if park is currently OPERATING
      if (dto.status === "OPERATING") {
        // OPTIMIZED: Fetch queue data for all attractions in a single bulk query
        const queueDataMap =
          await this.queueDataService.findCurrentStatusByPark(park.id);

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
            if (attraction.status === "OPERATING") {
              totalOperatingCount++;
            }
          } else {
            // Fallback if no live data found
            attraction.queues = [];
            attraction.status = "CLOSED";
          }

          // Attach ML predictions
          const mlPreds = hourlyPredictions[attraction.id] || [];
          attraction.hourlyForecast = mlPreds.map((p) => ({
            predictedTime: p.predictedTime,
            predictedWaitTime: p.predictedWaitTime,
            confidencePercentage: p.confidence,
            trend: p.trend,
            modelVersion: p.modelVersion,
          }));

          // Attach Prediction Accuracy (Feedback Loop)
          try {
            attraction.predictionAccuracy =
              await this.predictionAccuracyService.getAttractionAccuracyWithBadge(
                attraction.id,
              );
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
                crowdLevel: ratingResult.rating,
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
      } else {
        // Park is CLOSED - set all attractions to CLOSED without queue data
        for (const attraction of dto.attractions) {
          totalAttractionsCount++;
          attraction.queues = [];
          attraction.status = "CLOSED";
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
            crowdLevel: parkRating.rating,
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
      const showLiveData = await Promise.all(
        park.shows.map((show) =>
          this.showsService.findCurrentStatusByShow(show.id),
        ),
      );
      const showLiveDataMap = new Map(
        park.shows.map((show, index) => [show.id, showLiveData[index]]),
      );

      for (const show of dto.shows || []) {
        const liveData = showLiveDataMap.get(show.id);
        if (liveData) {
          // Keep operatingHours always (general schedule info)
          show.operatingHours = liveData.operatingHours || [];

          // Only show live showtimes if park is OPERATING
          if (dto.status === "OPERATING") {
            show.showtimes = liveData.showtimes || [];
            show.status = liveData.status;
            show.lastUpdated = liveData.lastUpdated?.toISOString();
          } else {
            // Park closed - no live showtimes, but keep schedule
            show.showtimes = [];
            show.status = "CLOSED";
            show.lastUpdated = undefined;
          }
        } else {
          // No live data available
          show.showtimes = [];
          show.operatingHours = [];
          show.status = undefined;
          show.lastUpdated = undefined;
        }
      }
    }

    // Fetch current status for restaurants
    if (park.restaurants && park.restaurants.length > 0) {
      const restaurantLiveData = await Promise.all(
        park.restaurants.map((restaurant) =>
          this.restaurantsService.findCurrentStatusByRestaurant(restaurant.id),
        ),
      );
      const restaurantLiveDataMap = new Map(
        park.restaurants.map((restaurant, index) => [
          restaurant.id,
          restaurantLiveData[index],
        ]),
      );

      for (const restaurant of dto.restaurants || []) {
        const liveData = restaurantLiveDataMap.get(restaurant.id);
        if (liveData) {
          // Keep operatingHours always (general schedule info)
          restaurant.operatingHours = liveData.operatingHours || [];

          // Only show live data if park is OPERATING
          if (dto.status === "OPERATING") {
            restaurant.status = liveData.status;
            restaurant.waitTime = liveData.waitTime;
            restaurant.partySize = liveData.partySize;
            restaurant.lastUpdated = liveData.lastUpdated?.toISOString();
          } else {
            // Park closed - no live data, but keep schedule
            restaurant.status = "CLOSED";
            restaurant.waitTime = null;
            restaurant.partySize = null;
            restaurant.lastUpdated = undefined;
          }
        } else {
          // No live data available
          restaurant.operatingHours = [];
          restaurant.status = undefined;
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
            comparisonStatus: "typical",
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
            comparisonStatus: "typical",
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

    // Cache the complete response with dynamic TTL based on park status
    const ttl =
      dto.status === "OPERATING"
        ? this.TTL_INTEGRATED_RESPONSE_OPERATING
        : this.TTL_INTEGRATED_RESPONSE_CLOSED;

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
   * Helper: Aggregate attraction daily predictions into park-level daily predictions
   */
  private aggregateDailyPredictions(
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
}
