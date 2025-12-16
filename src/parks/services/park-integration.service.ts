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
  private readonly TTL_INTEGRATED_RESPONSE = 5 * 60; // 5 minutes for real-time data

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

      // Filter hourly for TODAY only
      const todayStr = new Date().toISOString().split("T")[0];

      if (hourlyRes && hourlyRes.predictions) {
        for (const p of hourlyRes.predictions) {
          // Check if prediction is for today
          if (p.predictedTime.startsWith(todayStr)) {
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

    dto.dates = dailyPredictions;

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
        for (const attraction of dto.attractions) {
          totalAttractionsCount++;

          // Get current queue data for this attraction
          const queueData =
            await this.queueDataService.findCurrentStatusByAttraction(
              attraction.id,
            );

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
          attraction.predictions = mlPreds.map((p) => ({
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
                ...ratingResult,
                current: standbyQueue.waitTime,
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
            ...parkRating,
            current: currentAvgWait,
          };
        }
      } catch (_error) {
        // Silent fail for optional metadata
      }
    }

    // Removed problematic fallback heuristic - timezone-aware status is reliable

    // Fetch current status for shows
    if (park.shows && park.shows.length > 0) {
      for (const show of dto.shows || []) {
        const liveData = await this.showsService.findCurrentStatusByShow(
          show.id,
        );
        if (liveData) {
          show.status = liveData.status;
          show.showtimes = liveData.showtimes || [];
          show.operatingHours = liveData.operatingHours || [];
          show.lastUpdated = liveData.lastUpdated
            ? liveData.lastUpdated.toISOString()
            : new Date().toISOString();
        } else {
          show.status = "CLOSED";
          show.showtimes = [];
          show.operatingHours = [];
          show.lastUpdated = new Date().toISOString();
        }
      }
    }

    // Fetch current status for restaurants
    if (park.restaurants && park.restaurants.length > 0) {
      for (const restaurant of dto.restaurants || []) {
        const liveData =
          await this.restaurantsService.findCurrentStatusByRestaurant(
            restaurant.id,
          );
        if (liveData) {
          restaurant.status = liveData.status;
          restaurant.waitTime = liveData.waitTime ?? null;
          restaurant.partySize = liveData.partySize ?? null;
          restaurant.operatingHours = liveData.operatingHours || [];
          restaurant.lastUpdated = liveData.lastUpdated
            ? liveData.lastUpdated.toISOString()
            : new Date().toISOString();
        } else {
          restaurant.status = "CLOSED";
          restaurant.waitTime = null;
          restaurant.partySize = null;
          restaurant.operatingHours = [];
          restaurant.lastUpdated = new Date().toISOString();
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
            updatedAt: occupancy.updatedAt.toISOString(),
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
      // Park is CLOSED - return zeroed analytics with correct operatingAttractions count
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
          avgWaitToday: 0, // Could fetch historical if needed
          peakHour: null,
          crowdLevel: "very_low",
          totalAttractions: totalAttractionsCount,
          operatingAttractions: 0, // KEY FIX: Should be 0 when closed
          closedAttractions: totalAttractionsCount,
          timestamp: new Date().toISOString(),
        },
        percentiles: undefined,
      };
    }

    // Cache the complete response (5 minutes for real-time data freshness)
    await this.redis.set(
      cacheKey,
      JSON.stringify(dto),
      "EX",
      this.TTL_INTEGRATED_RESPONSE,
    );

    return dto;
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
