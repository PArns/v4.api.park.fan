import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In } from "typeorm";
import { Park } from "../entities/park.entity";
import { ScheduleEntry, ScheduleType } from "../entities/schedule-entry.entity";
import { ParkWithAttractionsDto } from "../dto/park-with-attractions.dto";
import { WeatherItemDto } from "../dto/weather-item.dto";
import { ScheduleItemDto } from "../dto/schedule-item.dto";
import { ParkWaitTimesResponse } from "../types/park-wait-times.type";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { MLService } from "../../ml/ml.service";
import { PredictionAccuracyService } from "../../ml/services/prediction-accuracy.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { AttractionAccuracyStats } from "../../ml/entities/attraction-accuracy-stats.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  getCurrentDateInTimezone,
  formatInParkTimezone,
} from "../../common/utils/date.util";
import { buildAttractionUrl } from "../../common/utils/url.util";
import { HolidaysService } from "../../holidays/holidays.service";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ShowLiveData } from "../../shows/entities/show-live-data.entity";
import { RestaurantLiveData } from "../../restaurants/entities/restaurant-live-data.entity";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { ParkStatus } from "../../common/types/status.type";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";

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
  private readonly TTL_INTEGRATED_RESPONSE_OPERATING = 15 * 60; // 15 minutes (Push-based caching: Updated by background job every 5m)
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
    private readonly parkEnrichmentService: ParkEnrichmentService,
    private readonly themeParksClient: ThemeParksClient,
    private readonly queueTimesClient: QueueTimesClient,
    private readonly wartezeitenClient: WartezeitenClient,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @InjectRepository(AttractionAccuracyStats)
    private readonly accuracyStatsRepository: Repository<AttractionAccuracyStats>,
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

        // Check if attractions have URLs (new feature - invalidate cache if missing)
        const hasAttractionsWithoutUrls =
          cachedDto.attractions &&
          cachedDto.attractions.length > 0 &&
          cachedDto.attractions.some((att) => !att.url);

        if (
          missingShowsInCache ||
          missingRestaurantsInCache ||
          hasAttractionsWithoutUrls
        ) {
          this.logger.warn(
            `Detected incomplete cache for ${park.slug} (missing shows/restaurants or attraction URLs). Force rebuilding.`,
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

    // Fetch weather, schedule, queue data, and ML predictions in parallel
    // These all only depend on park.id so they can run simultaneously
    // Queue data uses park opening hours to determine valid cutoff (not fixed 6 hours)

    const [weatherData, schedule, queueDataMap, mlPredictionsResult] =
      await Promise.all([
        this.weatherService.getCurrentAndForecast(park.id),
        this.parksService.getUpcomingSchedule(park.id, 7),
        this.queueDataService.findCurrentStatusByPark(park.id),
        Promise.all([
          this.mlService
            .getParkPredictions(park.id, "hourly")
            .catch(() => null),
          this.mlService
            .getParkPredictions(park.id, "daily", 16)
            .catch(() => null),
        ]),
      ]);
    const [hourlyRes, dailyRes] = mlPredictionsResult;

    dto.weather = {
      current: weatherData.current
        ? WeatherItemDto.fromEntity(weatherData.current)
        : null,
      forecast: weatherData.forecast.map((w) => WeatherItemDto.fromEntity(w)),
    };
    dto.schedule = schedule.map((s) => ScheduleItemDto.fromEntity(s));

    // 4. Determine Park Status & Hours
    let status: ParkStatus = "CLOSED";

    // A. Try Official Schedule First
    // Find today's schedule entry
    const todaySchedule = schedule.find((s) => {
      const scheduleDate = formatInParkTimezone(s.date, park.timezone);
      const todayDate = formatInParkTimezone(new Date(), park.timezone);
      return scheduleDate === todayDate;
    });

    if (
      todaySchedule &&
      todaySchedule.scheduleType === ScheduleType.OPERATING
    ) {
      status = "OPERATING";
    }

    // B. Fallback: Check Live Data for Operating Hours (if Schedule missing or Closed)
    // Sometimes the schedule API is empty/stale, but the Live API returns "Operating" with hours
    // Only attempt this if we are currently CLOSED (don't override valid Operating schedule)
    if (status === "CLOSED" && park.externalId) {
      try {
        const eid = park.externalId;

        // --- STRATEGY 1: Queue-Times (qt-) ---
        if (eid.startsWith("qt-")) {
          // Extract ID (e.g. "qt-51" -> 51)
          const qtIdStr = eid.replace("qt-", "");
          const qtId = parseInt(qtIdStr, 10);

          if (!isNaN(qtId)) {
            const qtData = await this.queueTimesClient.getParkQueueTimes(qtId);

            // If ANY ride is open, the park is operating
            // We iterate lands and rides
            const allRides = [
              ...qtData.rides,
              ...qtData.lands.flatMap((l) => l.rides),
            ];

            if (allRides.some((r) => r.is_open)) {
              status = "OPERATING";
              this.logger.debug(
                `Recovered OPERATING status from Queue-Times for ${park.name} (Active Rides found)`,
              );
              // Note: Queue-Times doesn't provide closing time, so we leave hours undefined (implied open)
            }
          }
        }

        // --- STRATEGY 2: Wartezeiten (wz-) ---
        else if (eid.startsWith("wz-")) {
          // Extract ID (e.g. "wz-phantasialand" -> "phantasialand")
          const wzId = eid.replace("wz-", "");

          const openingTimes =
            await this.wartezeitenClient.getOpeningTimes(wzId);

          if (
            openingTimes &&
            openingTimes.length > 0 &&
            openingTimes[0].opened_today
          ) {
            const ot = openingTimes[0];
            status = "OPERATING";
            // Check if we have times
            if (ot.open_from && ot.closed_from) {
              // Parse dates. API returns ISO strings like "2025-12-29T09:00:00+01:00"
              // We need to project them to Today string if needed?
              // Actually, getOpeningTimes returns "opened_today", so the dates should be for today.
              // Logic to extract HH:mm if needed, but for now just logging.
              // We don't have a place to put "Operating Hours" in the DTO unless we mock a schedule entry?
              // But 'status' OPERATING is enough for the client to show it as open.
              this.logger.debug(
                `Recovered OPERATING status from Wartezeiten for ${park.name}: ${ot.open_from} - ${ot.closed_from}`,
              );
            }
          }
        }

        // --- STRATEGY 3: ThemeParks.wiki (others) ---
        else {
          // Default behavior for pure ThemeParks IDs (UUIDs)
          // Fetch fresh live data (this is cached by the client usually, but ensures we get the live status)

          const liveDataList = await this.themeParksClient.getParkLiveData(
            park.externalId,
          );

          const liveData =
            liveDataList.find((x) => x.id === park.externalId) ||
            liveDataList[0];

          if (
            liveData &&
            liveData.status === "OPERATING" &&
            liveData.operatingHours &&
            liveData.operatingHours.length > 0
          ) {
            // Found live hours! Use them.
            // But first: Project stale dates to Today
            const now = new Date();
            const todayDateString = formatInParkTimezone(now, park.timezone); // YYYY-MM-DD

            // Pick the first operating rule (usually there's only one for the day)
            const rule = liveData.operatingHours[0];

            if (rule.startTime && rule.endTime) {
              // Project Start Time
              // Format: 2025-11-13T10:00:00+01:00 -> 2025-12-29T10:00:00+01:00
              const startIso = rule.startTime;
              const startTimePart = startIso.substring(11); // HH:mm:ss...
              const newStartIso = todayDateString + "T" + startTimePart;

              // Project End Time
              const endIso = rule.endTime;
              const endTimePart = endIso.substring(11);
              const newEndIso = todayDateString + "T" + endTimePart;

              status = "OPERATING";

              this.logger.debug(
                `Recovered operating hours from Live Data for ${park.name}: ${newStartIso} - ${newEndIso}`,
              );
            }
          }
        }
      } catch (err) {
        this.logger.warn(
          `Failed to fetch fallback live data for ${park.name}: ${err}`,
        );
      } finally {
      }
    }
    // Queue data already fetched in parallel above - just use it to detect activity

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

    // ML Predictions already fetched in parallel above - process results
    // IMPORTANT: Daily predictions limited to 16 days (like weather forecast)
    const hourlyPredictions: Record<string, PredictionDto[]> = {};
    let dailyPredictions: import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] =
      [];

    // Filter hourly predictions:
    // - If park OPERATING: Show today's hourly predictions
    // - If park CLOSED: Show tomorrow's hourly predictions (trip planning)
    // Use park's timezone to determine "today" and "tomorrow"
    const todayInParkTz = park.timezone
      ? getCurrentDateInTimezone(park.timezone)
      : new Date().toISOString().split("T")[0];

    // Get tomorrow's date in park's timezone (for hourly predictions when park is closed)
    const tomorrowDate = new Date();
    tomorrowDate.setDate(tomorrowDate.getDate() + 1);
    const tomorrowInParkTz = park.timezone
      ? (() => {
          const tomorrow = new Date();
          tomorrow.setDate(tomorrow.getDate() + 1);
          return formatInParkTimezone(tomorrow, park.timezone).split("T")[0];
        })()
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
      // Batch fetch P90 baselines for all attractions (for relative crowd calculation)
      const attractionIds = dto.attractions.map((a) => a.id);

      // Determine start time for analytics filtering
      const startTime = await this.analyticsService.getEffectiveStartTime(
        park.id,
        park.timezone,
      );

      // Batch fetch P90s, pre-aggregated accuracy stats, and deviation flags in parallel
      const [
        attractionP90s,
        accuracyStats,
        deviationMap,
        attractionStatsMap,
        attractionHistoryMap,
        trendsMap,
      ] = await Promise.all([
        this.analyticsService.getBatchAttractionP90s(attractionIds),
        this.accuracyStatsRepository.find({
          where: { attractionId: In(attractionIds) },
        }),
        this.predictionDeviationService.getBatchDeviationFlags(attractionIds),
        this.analyticsService.getBatchAttractionStatistics(
          attractionIds,
          startTime,
        ),
        this.analyticsService.getBatchAttractionWaitTimeHistory(
          attractionIds,
          startTime,
        ),
        this.analyticsService.getBatchAttractionTrends(attractionIds),
      ]);

      // Build accuracy map from pre-aggregated stats
      const accuracyMap = new Map<
        string,
        {
          badge: string;
          comparedPredictions: number;
          totalPredictions: number;
          message: string | null;
        }
      >();
      for (const stat of accuracyStats) {
        accuracyMap.set(stat.attractionId, {
          badge: stat.badge,
          comparedPredictions: stat.comparedPredictions,
          totalPredictions: stat.totalPredictions,
          message: stat.message,
        });
      }

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

          // Optimistic Fallback:
          // If the Park is OPERATING, but we have no data for this ride (filtered out as stale),
          // we assume the ride is OPERATING (unknown wait time) rather than CLOSED.
          // This prevents the "Park Open, All Rides Closed" issue when the live feed stops updating.
          if (dto.status === "OPERATING") {
            attraction.status = "OPERATING";
          } else {
            attraction.status = "CLOSED";
          }
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

        // Enrich predictions with deviation data (using pre-fetched map)
        const deviationFlag = deviationMap.get(attraction.id) || null;
        const enrichedPreds = this.enrichPredictionsWithDeviationsSync(
          mlPreds,
          deviationFlag,
        );

        attraction.hourlyForecast = enrichedPreds.map((p) => ({
          predictedTime: p.predictedTime,
          predictedWaitTime: p.predictedWaitTime,
          confidencePercentage: p.confidenceAdjusted ?? p.confidence,
          trend: p.trend,
          currentWaitTime: p.currentWaitTime,
          deviationDetected: p.deviationDetected,
        }));

        // Determine crowd level for this attraction
        let crowdLevel: CrowdLevel | "closed" = "closed";

        if (attraction.effectiveStatus === "CLOSED") {
          crowdLevel = "closed";
        } else {
          // Find current hour prediction
          const nowStr = new Date().toISOString().split(":")[0]; // "YYYY-MM-DDTHH"
          const currentPred = enrichedPreds.find((p) =>
            p.predictedTime.startsWith(nowStr),
          );

          // 1. Try to use REAL-TIME Wait Time first (Ground Truth)
          const wait = attraction.queues?.[0]?.waitTime;
          if (wait !== undefined && wait !== null) {
            // Use historical P90 baseline for relative crowd level (context-aware)
            const p90 = attractionP90s.get(attraction.id) || 0;
            const { rating } = this.analyticsService.getLoadRating(wait, p90);
            crowdLevel = rating;
          } else {
            // 2. Fallback to ML Prediction if no live data
            if (currentPred && currentPred.crowdLevel) {
              crowdLevel = currentPred.crowdLevel as any;
            } else {
              // 3. Last resort default
              crowdLevel = "very_low";
            }
          }
        }
        attraction.crowdLevel = crowdLevel;

        // Determine Trend
        // STRICT SEPARATION: Only use historical/live data.
        // Hybrid Approach:
        // 1. "Fast Trend": Compare Current Spot Wait vs Last Hour Average.
        // 2. "Slow Trend": Compare Last Hour Average vs Previous Hour Average (via AnalyticsService).
        let trend: "up" | "stable" | "down" | null = null;
        if (attraction.effectiveStatus === "OPERATING") {
          const historicalTrend = trendsMap.get(attraction.id);
          const currentWait = attraction.queues?.[0]?.waitTime;

          if (
            currentWait !== undefined &&
            currentWait !== null &&
            historicalTrend &&
            historicalTrend.recentAverage !== null
          ) {
            trend = this.analyticsService.computeTrend(
              currentWait,
              historicalTrend.recentAverage,
              historicalTrend.previousAverage,
            );
          } else if (historicalTrend) {
            trend =
              historicalTrend.trend === "increasing"
                ? "up"
                : historicalTrend.trend === "decreasing"
                  ? "down"
                  : "stable";
          }
        }
        attraction.trend = trend;

        // Determine Baseline and Comparison using AnalyticsService
        if (attraction.effectiveStatus === "OPERATING") {
          const wait = attraction.queues?.[0]?.waitTime;
          const p90 = attractionP90s.get(attraction.id) || 0;

          if (wait !== undefined && wait !== null && p90 > 0) {
            const loadRating = this.analyticsService.getLoadRating(wait, p90);

            // Use baseline from loadRating (consistent with analytics)
            attraction.baseline = loadRating.baseline;
            attraction.comparison = this.analyticsService.getComparisonText(
              loadRating.rating,
            );
          } else {
            attraction.baseline = null;
            attraction.comparison = null;
          }

          // Populate detailed statistics (peak today, history, etc.)
          const stats = attractionStatsMap.get(attraction.id);
          const history = attractionHistoryMap.get(attraction.id) || [];

          if (stats) {
            // Calculate current vs typical (if current wait exists)
            // Note: typicalWaitThisHour calculation is complex to batch, so we might skip it for the list view
            // or implement a simpler version. For now, we omit typicalWaitThisHour in the list to avoid N+1.
            // But we can approximate `currentVsTypical` percentage if we had typical.
            // Using p90 as a proxy for "busy day typical" or just 0 for now.

            attraction.statistics = {
              avgWaitToday: stats.avg,
              peakWaitToday: stats.max,
              peakWaitTimestamp: stats.maxTimestamp
                ? stats.maxTimestamp.toISOString()
                : null,
              minWaitToday: stats.min,
              dataPoints: stats.count,
              typicalWaitThisHour: null, // Expensive to calculate per-ride in list
              percentile95ThisHour: null,
              currentVsTypical: null,
              timestamp: new Date().toISOString(),
              history: history,
            };
          } else {
            // Fallback for operating rides with no data today (e.g. just opened)
            attraction.statistics = {
              avgWaitToday: 0,
              peakWaitToday: 0,
              peakWaitTimestamp: null,
              minWaitToday: 0,
              dataPoints: 0,
              typicalWaitThisHour: null,
              percentile95ThisHour: null,
              currentVsTypical: null,
              timestamp: new Date().toISOString(),
              history: [],
            };
          }
        } else {
          attraction.baseline = null;
          attraction.comparison = null;
          attraction.statistics = null;
        }

        // Attach Prediction Accuracy (from pre-aggregated stats table)
        const accuracy = accuracyMap.get(attraction.id);
        if (accuracy) {
          attraction.predictionAccuracy = {
            badge: accuracy.badge as
              | "excellent"
              | "good"
              | "fair"
              | "poor"
              | "insufficient_data",
            last30Days: {
              comparedPredictions: accuracy.comparedPredictions,
              totalPredictions: accuracy.totalPredictions,
            },
            message: accuracy.message ?? undefined,
          };
        } else {
          attraction.predictionAccuracy = null;
        }

        // Set URL using geo route (if park has geo data)
        // The park entity should have geo data since it was loaded via findByGeographicPath
        if (park.continentSlug && park.countrySlug && park.citySlug) {
          attraction.url = buildAttractionUrl(park, attraction);
        } else {
          // Fallback: try to build URL from park data in DTO if available
          // This handles cases where park entity might not have geo data loaded
          attraction.url = null;
        }
      }
    }

    // Removed problematic fallback heuristic - timezone-aware status is reliable

    // Fetch current status for shows
    if (park.shows && park.shows.length > 0) {
      let showLiveDataMap = new Map<string, ShowLiveData>();
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

          // FIX: Force project dates to Today if Operating (Fallback for ShowsService)
          if (
            show.status === "OPERATING" &&
            show.showtimes &&
            show.showtimes.length > 0
          ) {
            const now = new Date();
            const todayStr = formatInParkTimezone(now, park.timezone);

            show.showtimes = show.showtimes.map((st) => {
              if (!st.startTime) return st;

              // Project to Today
              const iso = st.startTime;
              const currentDatePart = iso.substring(0, 10);

              if (currentDatePart !== todayStr) {
                const newIso = todayStr + iso.substring(10);
                return {
                  ...st,
                  startTime: newIso,
                  endTime: st.endTime
                    ? todayStr + st.endTime.substring(10)
                    : st.endTime,
                };
              }
              return st;
            });
          }
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
      let restaurantLiveDataMap = new Map<string, RestaurantLiveData>();

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
        const startTime = await this.analyticsService.getEffectiveStartTime(
          park.id,
          park.timezone,
        );
        const [occupancy, statistics, percentiles] = await Promise.all([
          this.analyticsService.calculateParkOccupancy(park.id),
          this.analyticsService.getParkStatistics(
            park.id,
            park.timezone,
            startTime,
          ),
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
            peakWaitToday: statistics.peakWaitToday,
            peakHour: statistics.peakHour,
            crowdLevel: statistics.crowdLevel,
            totalAttractions: totalAttractionsCount,
            operatingAttractions: totalOperatingCount,
            closedAttractions: totalAttractionsCount - totalOperatingCount,
            timestamp:
              typeof statistics.timestamp === "string"
                ? statistics.timestamp
                : statistics.timestamp.toISOString(),
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
        const startTime = await this.analyticsService.getEffectiveStartTime(
          park.id,
          park.timezone,
        );
        const [statistics, percentiles] = await Promise.all([
          this.analyticsService.getParkStatistics(
            park.id,
            park.timezone,
            startTime,
          ),
          this.analyticsService.getParkPercentilesToday(park.id),
        ]);

        // Use park's local time for percentile lookup
        // Get typical rating for "right now" in park's timezone
        // Previously incorrectly used UTC time instead of park local time
        // Fallback: Get typical wait from historical P90 percentiles
        // Use park's local time for hour/day, not UTC
        const { toZonedTime } = await import("date-fns-tz");
        const nowInParkTz = park.timezone
          ? toZonedTime(new Date(), park.timezone)
          : new Date();
        const hour = nowInParkTz.getHours();
        const day = nowInParkTz.getDay();

        // Get P90 park-level baseline
        const p90Park = await this.analyticsService.get90thPercentileOneYear(
          park.id,
          hour,
          day,
          "park",
          park.timezone,
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
            peakWaitToday: statistics.peakWaitToday || 0,
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
            peakWaitToday: 0,
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
      await this.parkEnrichmentService.enrichScheduleWithHolidays(
        dto.schedule,
        park,
      );
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
      return this.TTL_INTEGRATED_RESPONSE_OPERATING; // 15 minutes
    }

    // Park is CLOSED - check if we *should* be open (Unexpected Closure)
    const now = new Date();
    const isScheduledOpen = schedules.some(
      (s) =>
        s.scheduleType === "OPERATING" &&
        s.openingTime &&
        s.closingTime &&
        s.openingTime <= now &&
        s.closingTime > now,
    );

    if (isScheduledOpen) {
      // Park is CLOSED but Schedule says OPEN -> Likely a temporary closure or data issue.
      // Use short TTL to recover quickly if it reopens.
      this.logger.debug(
        "Park is CLOSED but within operating hours. Using short TTL.",
      );
      return this.TTL_INTEGRATED_RESPONSE_OPERATING; // 15 minutes
    }

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
    predictions: PredictionDto[],
  ): import("../dto/park-daily-prediction.dto").ParkDailyPredictionDto[] {
    const datesMap = new Map<string, PredictionDto[]>();

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

      const predsWithWait = predsToUse.filter(
        (p) =>
          p.predictedWaitTime !== undefined && p.predictedWaitTime !== null,
      );
      const avgWaitTime =
        predsWithWait.length > 0
          ? Math.round(
              predsWithWait.reduce(
                (sum, p) => sum + (p.predictedWaitTime || 0),
                0,
              ) / predsWithWait.length,
            )
          : undefined;

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
        avgWaitTime,
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
    predictions: PredictionDto[],
  ): Promise<PredictionDto[]> {
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

  /**
   * Sync version of enrichPredictionsWithDeviations using pre-fetched deviation flag
   * OPTIMIZED: No Redis call - uses pre-fetched deviation map
   */
  private enrichPredictionsWithDeviationsSync(
    predictions: PredictionDto[],
    deviationFlag: {
      actualWaitTime: number;
      deviation: number;
      percentageDeviation: number;
      detectedAt: string;
    } | null,
  ): PredictionDto[] {
    if (!predictions || predictions.length === 0) {
      return predictions;
    }

    if (deviationFlag) {
      return predictions.map((p) => ({
        ...p,
        currentWaitTime: deviationFlag.actualWaitTime,
        confidenceAdjusted: p.confidence * 0.5,
        deviationDetected: true,
        deviationInfo: {
          message: `Current wait ${Math.abs(deviationFlag.deviation).toFixed(0)}min ${deviationFlag.deviation > 0 ? "higher" : "lower"} than predicted`,
          deviation: deviationFlag.deviation,
          percentageDeviation: deviationFlag.percentageDeviation,
          detectedAt: deviationFlag.detectedAt,
        },
      }));
    }

    return predictions;
  }

  /**
   * Get park wait times with timezone-aware park status check
   *
   * Unified method that replaces duplicated logic in controllers.
   * Ensures consistent behavior across all wait-times endpoints.
   *
   * CRITICAL Features:
   * - Timezone-aware park status check (uses getBatchParkStatus)
   * - Returns empty attractions array for CLOSED parks (no stale data)
   * - Uses QueueDataItemDto.fromEntity() for consistent DTO transformation
   * - Groups queue data by attraction
   *
   * @param park - Park entity (must have id, name, slug, timezone)
   * @param queueType - Optional queue type filter
   * @returns Park waittimes response with status and attractions
   */
  async getParkWaitTimesResponse(
    park: Park,
    queueType?: import("../../external-apis/themeparks/themeparks.types").QueueType,
  ): Promise<ParkWaitTimesResponse> {
    // Check timezone-aware park status
    const statusMap = await this.parksService.getBatchParkStatus([park.id]);
    const parkStatus = statusMap.get(park.id) || "CLOSED";

    // If park is CLOSED, return empty wait times (no stale data)
    if (parkStatus === "CLOSED") {
      return {
        park: {
          id: park.id,
          name: park.name,
          slug: park.slug,
          timezone: park.timezone,
          status: "CLOSED",
        },
        attractions: [],
      };
    }

    // Park is OPERATING - fetch live wait times
    const waitTimes = await this.queueDataService.findWaitTimesByPark(
      park.id,
      queueType,
    );

    // Group by attraction
    const attractionsMap = new Map<
      string,
      {
        attraction: { id: string; name: string; slug: string };
        queues: import("../../queue-data/dto/queue-data-item.dto").QueueDataItemDto[];
      }
    >();

    for (const queueData of waitTimes) {
      const attractionId = queueData.attraction.id;

      if (!attractionsMap.has(attractionId)) {
        attractionsMap.set(attractionId, {
          attraction: {
            id: queueData.attraction.id,
            name: queueData.attraction.name,
            slug: queueData.attraction.slug,
          },
          queues: [],
        });
      }

      // Use centralized DTO transformation
      const { QueueDataItemDto } =
        await import("../../queue-data/dto/queue-data-item.dto");
      const queueDto = QueueDataItemDto.fromEntity(queueData);

      attractionsMap.get(attractionId)!.queues.push(queueDto);
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
        status: parkStatus,
      },
      attractions: Array.from(attractionsMap.values()),
    };
  }
}
