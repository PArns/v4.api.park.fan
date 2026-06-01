import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { getCurrentDateInTimezone } from "../common/utils/date.util";
import { ParkWithAttractionsDto } from "../parks/dto/park-with-attractions.dto";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParksService } from "../parks/parks.service";
import { PopularityService } from "../popularity/popularity.service";
import {
  calculateHaversineDistance,
  sortByDistance,
  GeoCoordinate,
} from "../common/utils/distance.util";
import { buildParkUrl, buildAttractionUrl } from "../common/utils/url.util";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import {
  NearbyResponseDto,
  NearbyRidesDto,
  NearbyParksDto,
  RideWithDistanceDto,
  NearbyParkInfoDto,
} from "./dto/nearby-response.dto";
import { ParkWithDistanceDto } from "../common/dto/park-with-distance.dto";
import { CrowdLevel } from "../common/types/crowd-level.type";
import {
  formatTodaySchedule,
  formatNextSchedule,
} from "../common/utils/schedule.util";

/**
 * Location Service
 *
 * Handles location-based queries for parks and attractions.
 * Determines if user is inside a park or provides nearby parks.
 */
@Injectable()
export class LocationService {
  private readonly logger = new Logger(LocationService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
    private readonly popularityService: PopularityService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Find nearby parks or rides based on user location
   *
   * @param latitude - User latitude
   * @param longitude - User longitude
   * @param radiusInMeters - Radius to consider "in park" (default: 500m)
   * @returns Nearby response with rides or parks
   */
  async findNearby(
    latitude: number,
    longitude: number,
    radiusInMeters: number = 1000,
    limit: number = 6,
  ): Promise<NearbyResponseDto> {
    const userLocation: GeoCoordinate = { latitude, longitude };

    // Find if user is inside any park
    const nearbyPark = await this.findNearestPark(userLocation, radiusInMeters);

    if (nearbyPark) {
      // User is in a park - return rides
      // Calculate distance for logging
      const parkDistance = calculateHaversineDistance(
        userLocation,
        {
          latitude: Number(nearbyPark.latitude),
          longitude: Number(nearbyPark.longitude),
        },
        "m",
      );
      this.logger.log(
        `User is in park: ${nearbyPark.name} (${Math.round(parkDistance)}m away)`,
      );
      const ridesData = await this.findRidesInPark(nearbyPark.id, userLocation);

      // Count toward popularity so prewarm prioritizes this park.
      void this.popularityService.recordParkHit(nearbyPark.id);

      return {
        type: "in_park",
        userLocation: {
          latitude,
          longitude,
        },
        data: ridesData,
      };
    } else {
      // User is outside - return nearby parks
      this.logger.log(
        `User is outside all parks, finding nearest ${limit} parks`,
      );
      const parksData = await this.findNearbyParks(userLocation, limit);

      // Count returned parks toward popularity so prewarm prioritizes them.
      void this.popularityService.recordParkHits(
        parksData.parks.map((p) => p.id),
      );

      return {
        type: "nearby_parks",
        userLocation: {
          latitude,
          longitude,
        },
        data: parksData,
      };
    }
  }

  /**
   * Find nearest park within radius
   *
   * @param userLocation - User coordinates
   * @param radiusInMeters - Maximum distance to consider
   * @returns Park with distance or null if none found
   */
  private async findNearestPark(
    userLocation: GeoCoordinate,
    radiusInMeters: number,
  ): Promise<Park | null> {
    // Get all parks with coordinates
    const parks = await this.parkRepository.find({
      where: {
        latitude: Not(IsNull()),
        longitude: Not(IsNull()),
      },
    });

    if (parks.length === 0) {
      return null;
    }

    // Calculate distances and find nearest within radius
    const parksWithDistance = sortByDistance(
      parks.map((p) => ({
        ...p,
        latitude: Number(p.latitude),
        longitude: Number(p.longitude),
      })),
      userLocation,
    );

    const nearestPark = parksWithDistance[0];

    if (nearestPark && nearestPark.distance <= radiusInMeters) {
      // Return the original park (without the distance property)
      return parks.find((p) => p.id === nearestPark.id) || null;
    }

    return null;
  }

  /**
   * Find all rides in a park with distances from user
   *
   * @param parkId - Park ID
   * @param userLocation - User coordinates
   * @returns Rides data with park info
   */
  private async findRidesInPark(
    parkId: string,
    userLocation: GeoCoordinate,
  ): Promise<NearbyRidesDto> {
    // Fetch park and attractions in parallel
    const [park, attractions] = await Promise.all([
      this.parkRepository.findOne({ where: { id: parkId } }),
      this.attractionRepository.find({ where: { parkId } }),
    ]);

    if (!park) {
      throw new Error(`Park ${parkId} not found`);
    }

    const attractionIds = attractions.map((a) => a.id);

    // P90 baselines (cache-backed; daily-cron populated) drive the
    // peak-vs-peak crowd reading. P50 is fetched alongside as a fallback
    // for attractions that don't have a P90 row yet.
    const [
      statusMap,
      startTime,
      todaySchedule,
      nextSchedule,
      latestQueueData,
      p90Baselines,
      p50Baselines,
      analyticsMap,
      parkHasOperatingSchedule,
    ] = await Promise.all([
      this.parksService.getBatchParkStatus([parkId]),
      this.analyticsService.getEffectiveStartTime(park.id, park.timezone),
      this.parksService.getTodaySchedule(parkId).catch(() => []),
      this.parksService.getNextSchedule(parkId).catch(() => null),
      this.getLatestQueueData(attractionIds),
      this.analyticsService.getBatchAttractionP90Baselines(attractionIds),
      this.analyticsService.getBatchAttractionP50s(attractionIds),
      this.analyticsService.getBatchAttractionPercentilesToday(attractionIds),
      this.parksService.hasOperatingSchedule(parkId),
    ]);

    const parkAnalytics = await this.analyticsService
      .getParkStatistics(parkId, park.timezone, startTime)
      .catch(() => null);

    const parkStatus = statusMap.get(parkId) || "CLOSED";

    // Calculate distances to user (only for attractions with coordinates)
    const attractionsWithCoords = attractions.filter(
      (a) => a.latitude && a.longitude,
    );

    const attractionsWithDistance = sortByDistance(
      attractionsWithCoords.map((a) => ({
        ...a,
        latitude: Number(a.latitude),
        longitude: Number(a.longitude),
      })),
      userLocation,
    );

    // Build ride DTOs (no async needed inside map)
    const rides: RideWithDistanceDto[] = attractionsWithDistance.map(
      (attraction) => {
        const queueData = latestQueueData.get(attraction.id);
        const analytics = analyticsMap.get(attraction.id) || null;

        // Calculate crowd level
        const waitTime = queueData?.waitTime;
        let crowdLevel: CrowdLevel | null = null;

        if (
          waitTime !== null &&
          waitTime !== undefined &&
          queueData?.status === "OPERATING"
        ) {
          const baseline =
            p50Baselines.get(attraction.id) || p90Baselines.get(attraction.id);
          if (baseline && baseline > 0) {
            crowdLevel = this.analyticsService.getAttractionCrowdLevel(
              waitTime,
              baseline,
            );
          }
        }

        return {
          id: attraction.id,
          name: attraction.name,
          slug: attraction.slug,
          distance: Math.round(attraction.distance),
          waitTime:
            queueData?.waitTime !== null && queueData?.waitTime !== undefined
              ? roundToNearest5Minutes(queueData.waitTime)
              : null,
          status: queueData?.status || "CLOSED",
          crowdLevel,
          analytics: analytics
            ? {
                p50: analytics.p50,
                p90: analytics.p90,
              }
            : undefined,
          url: buildAttractionUrl(park, attraction) || "",
        };
      },
    );

    // Build park info
    const parkInfo: NearbyParkInfoDto = {
      id: park.id,
      name: park.name,
      slug: park.slug,
      distance: Math.round(
        calculateHaversineDistance(
          userLocation,
          {
            latitude: Number(park.latitude),
            longitude: Number(park.longitude),
          },
          "m",
        ),
      ),
      status: parkStatus,
      hasOperatingSchedule: parkHasOperatingSchedule,
      analytics: parkAnalytics
        ? {
            avgWaitTime: parkAnalytics.avgWaitTime,
            crowdLevel: parkAnalytics.crowdLevel,
            operatingAttractions: parkAnalytics.operatingAttractions,
          }
        : undefined,
      timezone: park.timezone,
      todaySchedule: formatTodaySchedule(todaySchedule),
      nextSchedule: formatNextSchedule(nextSchedule),
    };

    return {
      park: parkInfo,
      rides,
    };
  }

  /**
   * Find nearby parks (max 5)
   *
   * @param userLocation - User coordinates
   * @param limit - Maximum number of parks to return
   * @returns Parks data
   */
  private async findNearbyParks(
    userLocation: GeoCoordinate,
    limit: number = 6,
  ): Promise<NearbyParksDto> {
    // Get all parks with coordinates
    const parks = await this.parkRepository.find({
      where: {
        latitude: Not(IsNull()),
        longitude: Not(IsNull()),
      },
    });

    if (parks.length === 0) {
      return {
        parks: [],
        count: 0,
      };
    }

    // Sort by distance and take top N
    const sortedParks = sortByDistance(
      parks.map((p) => ({
        ...p,
        latitude: Number(p.latitude),
        longitude: Number(p.longitude),
      })),
      userLocation,
    ).slice(0, limit);

    // Fast path: read the prewarmed park:integrated cache (single MGET). All parks are
    // warmed every 5min, so this normally serves every nearby park directly from Redis —
    // the per-park batch status/occupancy/statistics/schedule round-trips below then only
    // run for genuine cache misses. Mirrors FavoritesService.enrichParks.
    const cachedRaw = await this.redis.mget(
      ...sortedParks.map((p) => `park:integrated:${p.id}`),
    );
    const integratedMap = new Map<string, ParkWithAttractionsDto>();
    const missedParks: typeof sortedParks = [];
    sortedParks.forEach((p, i) => {
      const raw = cachedRaw[i];
      if (raw) {
        try {
          integratedMap.set(p.id, JSON.parse(raw) as ParkWithAttractionsDto);
        } catch {
          missedParks.push(p);
        }
      } else {
        missedParks.push(p);
      }
    });

    // Slow path: batch queries cover ONLY cache misses (empty + near-free when all cached).
    const missedIds = missedParks.map((p) => p.id);
    const [statusMap, occupancyMap, schedules, operatingScheduleMap] =
      await Promise.all([
        this.parksService.getBatchParkStatus(missedIds),
        this.analyticsService["getBatchParkOccupancy"](missedIds),
        this.batchFetchSchedules(missedIds),
        this.parksService.getBatchHasOperatingSchedule(missedIds),
      ]);
    const context = new Map<string, { timezone: string; startTime: Date }>();
    if (missedIds.length > 0) {
      const startTimeMap =
        await this.analyticsService.getBatchEffectiveStartTime(
          missedParks.map((p) => ({ id: p.id, timezone: p.timezone || "UTC" })),
        );
      for (const park of missedParks) {
        context.set(park.id, {
          timezone: park.timezone || "UTC",
          startTime: startTimeMap.get(park.id)!,
        });
      }
    }
    const statisticsMap = await this.analyticsService.getBatchParkStatistics(
      missedIds,
      context,
    );

    // Build park DTOs (hits from the integrated cache, misses from the batch maps)
    const parkDtos: ParkWithDistanceDto[] = sortedParks.map((park) => {
      const distance = Math.round(park.distance);
      const integrated = integratedMap.get(park.id);

      if (integrated) {
        const today = getCurrentDateInTimezone(park.timezone || "UTC");
        const todayEntry = integrated.schedule?.find((s) => s.date === today);
        return {
          id: park.id,
          name: park.name,
          slug: park.slug,
          distance,
          city: park.city || null,
          country: park.country || null,
          status: integrated.status || "CLOSED",
          hasOperatingSchedule: integrated.hasOperatingSchedule ?? false,
          totalAttractions:
            integrated.analytics?.statistics?.totalAttractions || 0,
          operatingAttractions:
            integrated.analytics?.statistics?.operatingAttractions || 0,
          analytics: integrated.analytics
            ? {
                avgWaitTime:
                  integrated.analytics.occupancy?.breakdown?.currentAvgWait ||
                  0,
                crowdLevel: integrated.analytics.statistics?.crowdLevel,
                occupancy: integrated.analytics.occupancy?.current,
              }
            : undefined,
          url: integrated.url || buildParkUrl(park) || null,
          timezone: park.timezone,
          todaySchedule: todayEntry
            ? {
                openingTime: todayEntry.openingTime || "",
                closingTime: todayEntry.closingTime || "",
                scheduleType: todayEntry.scheduleType,
              }
            : undefined,
          nextSchedule: integrated.nextSchedule
            ? {
                openingTime: integrated.nextSchedule.openingTime,
                closingTime: integrated.nextSchedule.closingTime,
                scheduleType: integrated.nextSchedule.scheduleType,
              }
            : undefined,
        };
      }

      const occupancy = occupancyMap.get(park.id);
      const stats = statisticsMap.get(park.id);
      return {
        id: park.id,
        name: park.name,
        slug: park.slug,
        distance,
        city: park.city || null,
        country: park.country || null,
        status: statusMap.get(park.id) || "CLOSED",
        hasOperatingSchedule: operatingScheduleMap.get(park.id) || false,
        totalAttractions: stats?.totalAttractions || 0,
        operatingAttractions: stats?.operatingAttractions || 0,
        analytics: occupancy
          ? {
              avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
              crowdLevel: this.analyticsService.determineCrowdLevel(
                occupancy.current,
              ),
              occupancy: occupancy.current,
            }
          : undefined,
        url: buildParkUrl(park) || null,
        timezone: park.timezone,
        todaySchedule: formatTodaySchedule(schedules.today.get(park.id)),
        nextSchedule: formatNextSchedule(schedules.next.get(park.id)),
      };
    });

    return {
      parks: parkDtos,
      count: parkDtos.length,
    };
  }

  /**
   * Get latest queue data for multiple attractions (batch query)
   * Uses shared QueueDataService for consistent queue type prioritization
   *
   * @param attractionIds - Attraction IDs
   * @returns Map of attraction ID to latest queue data
   */
  private async getLatestQueueData(
    attractionIds: string[],
  ): Promise<Map<string, QueueData>> {
    if (attractionIds.length === 0) {
      return new Map();
    }

    // Get park ID from first attraction
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionIds[0] },
      select: ["id", "parkId"],
    });

    if (!attraction) {
      return new Map();
    }

    // Use shared service with STANDBY prioritization + fallback
    const allQueues = await this.queueDataService.findPrioritizedStatusByPark(
      attraction.parkId,
      30, // 30 minutes max age
    );

    // Filter to requested attractions only
    const result = new Map<string, QueueData>();
    for (const attractionId of attractionIds) {
      const queueData = allQueues.get(attractionId);
      if (queueData) {
        result.set(attractionId, queueData);
      }
    }

    return result;
  }

  /**
   * Batch fetch schedules for multiple parks
   * Returns both today's schedule and next schedule
   * @private
   */
  private async batchFetchSchedules(parkIds: string[]) {
    return this.parksService.getBatchSchedules(parkIds);
  }
}
