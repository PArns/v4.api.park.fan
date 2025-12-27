import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParksService } from "../parks/parks.service";
import {
  calculateHaversineDistance,
  sortByDistance,
  GeoCoordinate,
} from "../common/utils/distance.util";
import { buildParkUrl, buildAttractionUrl } from "../common/utils/url.util";
import {
  NearbyResponseDto,
  NearbyRidesDto,
  NearbyParksDto,
  RideWithDistanceDto,
  ParkWithDistanceDto,
  NearbyParkInfoDto,
} from "./dto/nearby-response.dto";

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
    @InjectRepository(QueueData)
    private readonly queueDataRepository: Repository<QueueData>,
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
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
    radiusInMeters: number = 500,
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
      this.logger.log("User is outside all parks, finding nearest parks");
      const parksData = await this.findNearbyParks(userLocation, 5);

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
    // Get park details
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
    });

    if (!park) {
      throw new Error(`Park ${parkId} not found`);
    }

    // Get all attractions in the park
    const attractions = await this.attractionRepository.find({
      where: { parkId },
    });

    // Get park status and analytics
    const [statusMap, parkAnalytics] = await Promise.all([
      this.parksService.getBatchParkStatus([parkId]),
      this.analyticsService.getParkStatistics(parkId).catch(() => null),
    ]);

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

    // Get latest queue data for all attractions (batch query)
    const attractionIds = attractions.map((a) => a.id);
    const latestQueueData = await this.getLatestQueueData(attractionIds);

    // Build ride DTOs
    const rides: RideWithDistanceDto[] = await Promise.all(
      attractionsWithDistance.map(async (attraction) => {
        const queueData = latestQueueData.get(attraction.id);
        const analytics = await this.analyticsService
          .getAttractionPercentilesToday(attraction.id)
          .catch(() => null);

        return {
          id: attraction.id,
          name: attraction.name,
          slug: attraction.slug,
          distance: Math.round(attraction.distance),
          waitTime: queueData?.waitTime || null,
          status: queueData?.status || "CLOSED",
          analytics: analytics
            ? {
                p50: analytics.p50,
                p90: analytics.p90,
              }
            : undefined,
          url: buildAttractionUrl(park, attraction) || "",
        };
      }),
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
      analytics: parkAnalytics
        ? {
            avgWaitTime: parkAnalytics.avgWaitTime,
            crowdLevel: parkAnalytics.crowdLevel,
            operatingAttractions: parkAnalytics.operatingAttractions,
          }
        : undefined,
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
    limit: number = 5,
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

    // Get park IDs for batch queries
    const parkIds = sortedParks.map((p) => p.id);

    // Batch fetch status and analytics
    const [statusMap, occupancyMap] = await Promise.all([
      this.parksService.getBatchParkStatus(parkIds),
      this.analyticsService["getBatchParkOccupancy"](parkIds),
    ]);

    // Get statistics for each park
    const statisticsMap = new Map<string, any>();
    await Promise.all(
      parkIds.map(async (parkId) => {
        try {
          const stats = await this.analyticsService.getParkStatistics(parkId);
          statisticsMap.set(parkId, stats);
        } catch (_e) {
          // Ignore errors
        }
      }),
    );

    // Build park DTOs
    const parkDtos: ParkWithDistanceDto[] = sortedParks.map((park) => {
      const status = statusMap.get(park.id) || "CLOSED";
      const occupancy = occupancyMap.get(park.id);
      const stats = statisticsMap.get(park.id);

      return {
        id: park.id,
        name: park.name,
        slug: park.slug,
        distance: Math.round(park.distance),
        city: park.city || "Unknown",
        country: park.country || "Unknown",
        status,
        totalAttractions: stats?.totalAttractions || 0,
        operatingAttractions: stats?.operatingAttractions || 0,
        analytics: occupancy
          ? {
              avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
              crowdLevel: this.mapCrowdLevel(occupancy.current),
              occupancy: occupancy.current,
            }
          : undefined,
        url: buildParkUrl(park) || "",
      };
    });

    return {
      parks: parkDtos,
      count: parkDtos.length,
    };
  }

  /**
   * Get latest queue data for multiple attractions (batch query)
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

    // Use 30 minutes to accommodate sync intervals
    const thirtyMinutesAgo = new Date(Date.now() - 30 * 60 * 1000);

    const latestData = await this.queueDataRepository
      .createQueryBuilder("qd")
      .select(["qd.attractionId", "qd.waitTime", "qd.status", "qd.timestamp"])
      .where("qd.attractionId IN (:...attractionIds)", { attractionIds })
      .andWhere("qd.timestamp >= :thirtyMinutesAgo", { thirtyMinutesAgo })
      .andWhere("qd.queueType = :queueType", { queueType: "STANDBY" })
      .orderBy("qd.timestamp", "DESC")
      .getMany();

    // Group by attraction ID and take the latest
    const resultMap = new Map<string, QueueData>();
    for (const data of latestData) {
      if (!resultMap.has(data.attractionId)) {
        resultMap.set(data.attractionId, data);
      }
    }

    return resultMap;
  }

  /**
   * Map occupancy percentage to crowd level
   */
  private mapCrowdLevel(occupancy: number): string {
    if (occupancy < 30) return "very_low";
    if (occupancy < 50) return "low";
    if (occupancy < 75) return "moderate";
    if (occupancy < 95) return "high";
    return "very_high";
  }
}
