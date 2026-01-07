import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ParksService } from "../parks/parks.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { AttractionResponseDto } from "../attractions/dto/attraction-response.dto";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import {
  FavoritesResponseDto,
  ParkWithDistanceDto,
  AttractionWithDistanceDto,
  ShowWithDistanceDto,
  RestaurantWithDistanceDto,
} from "./dto/favorites-response.dto";
import {
  calculateHaversineDistance,
  GeoCoordinate,
} from "../common/utils/distance.util";
import { buildParkUrl } from "../common/utils/url.util";

/**
 * Favorites Service
 *
 * Retrieves and enriches favorite entities (parks, attractions, shows, restaurants)
 * with full information including live data and optional distance calculations.
 */
@Injectable()
export class FavoritesService {
  private readonly logger = new Logger(FavoritesService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @InjectRepository(Show)
    private readonly showRepository: Repository<Show>,
    @InjectRepository(Restaurant)
    private readonly restaurantRepository: Repository<Restaurant>,
    private readonly parksService: ParksService,
    private readonly attractionsService: AttractionsService,
    private readonly attractionIntegrationService: AttractionIntegrationService,
    private readonly showsService: ShowsService,
    private readonly restaurantsService: RestaurantsService,
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
  ) {}

  /**
   * Get favorites with full information
   *
   * @param parkIds - Array of park IDs
   * @param attractionIds - Array of attraction IDs
   * @param showIds - Array of show IDs
   * @param restaurantIds - Array of restaurant IDs
   * @param userLocation - Optional user location for distance calculation
   * @returns Grouped favorites with full information
   */
  async getFavorites(
    parkIds: string[] = [],
    attractionIds: string[] = [],
    showIds: string[] = [],
    restaurantIds: string[] = [],
    userLocation?: GeoCoordinate,
  ): Promise<FavoritesResponseDto> {
    // Fetch all entities in parallel
    const [parks, attractions, shows, restaurants] = await Promise.all([
      this.fetchParks(parkIds),
      this.fetchAttractions(attractionIds),
      this.fetchShows(showIds),
      this.fetchRestaurants(restaurantIds),
    ]);

    // Enrich with live data and calculate distances
    const [
      enrichedParks,
      enrichedAttractions,
      enrichedShows,
      enrichedRestaurants,
    ] = await Promise.all([
      this.enrichParks(parks, userLocation),
      this.enrichAttractions(attractions, userLocation),
      this.enrichShows(shows, userLocation),
      this.enrichRestaurants(restaurants, userLocation),
    ]);

    return {
      parks: enrichedParks,
      attractions: enrichedAttractions,
      shows: enrichedShows,
      restaurants: enrichedRestaurants,
      userLocation: userLocation || null,
    };
  }

  /**
   * Fetch parks by IDs
   */
  private async fetchParks(ids: string[]): Promise<Park[]> {
    if (ids.length === 0) {
      return [];
    }
    return this.parkRepository.find({
      where: { id: In(ids) },
    });
  }

  /**
   * Fetch attractions by IDs
   */
  private async fetchAttractions(ids: string[]): Promise<Attraction[]> {
    if (ids.length === 0) {
      return [];
    }
    return this.attractionRepository.find({
      where: { id: In(ids) },
      relations: ["park"],
    });
  }

  /**
   * Fetch shows by IDs
   */
  private async fetchShows(ids: string[]): Promise<Show[]> {
    if (ids.length === 0) {
      return [];
    }
    return this.showRepository.find({
      where: { id: In(ids) },
      relations: ["park"],
    });
  }

  /**
   * Fetch restaurants by IDs
   */
  private async fetchRestaurants(ids: string[]): Promise<Restaurant[]> {
    if (ids.length === 0) {
      return [];
    }
    return this.restaurantRepository.find({
      where: { id: In(ids) },
      relations: ["park"],
    });
  }

  /**
   * Enrich parks with live data and calculate distances (similar to nearby endpoint)
   */
  private async enrichParks(
    parks: Park[],
    userLocation?: GeoCoordinate,
  ): Promise<ParkWithDistanceDto[]> {
    if (parks.length === 0) {
      return [];
    }

    const parkIds = parks.map((p) => p.id);

    // Pre-calculate context (timezone + startTime) for batch park statistics
    const context = new Map<string, { timezone: string; startTime: Date }>();
    await Promise.all(
      parks.map(async (park) => {
        const startTime = await this.analyticsService.getEffectiveStartTime(
          park.id,
          park.timezone,
        );
        context.set(park.id, { timezone: park.timezone, startTime });
      }),
    );

    // Batch fetch status, analytics, schedules, and statistics
    const [statusMap, occupancyMap, schedules, statisticsMap] =
      await Promise.all([
        this.parksService.getBatchParkStatus(parkIds),
        this.analyticsService["getBatchParkOccupancy"](parkIds),
        this.batchFetchSchedules(parkIds),
        this.analyticsService.getBatchParkStatistics(parkIds, context),
      ]);

    // Build park DTOs (similar to nearby endpoint)
    return parks.map((park) => {
      const status = statusMap.get(park.id) || "CLOSED";
      const occupancy = occupancyMap.get(park.id);
      const stats = statisticsMap.get(park.id);
      const todaySchedule = schedules.today.get(park.id);
      const nextSchedule = schedules.next.get(park.id);

      const dto: ParkWithDistanceDto = {
        id: park.id,
        name: park.name,
        slug: park.slug,
        distance:
          userLocation && park.latitude && park.longitude
            ? Math.round(
                calculateHaversineDistance(
                  userLocation,
                  {
                    latitude: Number(park.latitude),
                    longitude: Number(park.longitude),
                  },
                  "m",
                ),
              )
            : null,
        city: park.city || null,
        country: park.country || null,
        status,
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
        todaySchedule:
          todaySchedule && todaySchedule.length > 0
            ? {
                openingTime: todaySchedule[0].openingTime?.toISOString() || "",
                closingTime: todaySchedule[0].closingTime?.toISOString() || "",
                scheduleType: todaySchedule[0].scheduleType,
              }
            : undefined,
        nextSchedule: nextSchedule
          ? {
              openingTime: nextSchedule.openingTime?.toISOString() || "",
              closingTime: nextSchedule.closingTime?.toISOString() || "",
              scheduleType: nextSchedule.scheduleType,
            }
          : undefined,
      };

      return dto;
    });
  }

  /**
   * Enrich attractions with live data and calculate distances
   * Uses AttractionIntegrationService for complete data (queues, statistics, trends)
   */
  private async enrichAttractions(
    attractions: Attraction[],
    userLocation?: GeoCoordinate,
  ): Promise<AttractionWithDistanceDto[]> {
    if (attractions.length === 0) {
      return [];
    }

    // Build integrated responses for all attractions in parallel
    const integratedResponses = await Promise.all(
      attractions.map((attraction) =>
        this.attractionIntegrationService
          .buildIntegratedResponse(attraction)
          .catch(() => null),
      ),
    );

    // Build DTOs with distance and trend
    return attractions.map((attraction, index) => {
      const integrated = integratedResponses[index];

      // If integration failed, create minimal DTO
      if (!integrated) {
        const baseDto = AttractionResponseDto.fromEntity(attraction);
        const dto: AttractionWithDistanceDto = {
          ...baseDto,
          distance:
            userLocation && attraction.latitude && attraction.longitude
              ? Math.round(
                  calculateHaversineDistance(
                    userLocation,
                    {
                      latitude: Number(attraction.latitude),
                      longitude: Number(attraction.longitude),
                    },
                    "m",
                  ),
                )
              : null,
          trend: null,
        };
        return dto;
      }

      // Extract trend from primary queue (STANDBY or first available)
      const primaryQueue =
        integrated.queues?.find((q) => q.queueType === "STANDBY") ||
        integrated.queues?.[0];
      let trend: "up" | "down" | "stable" | null = null;
      if (primaryQueue?.trend?.direction) {
        // Map "increasing" -> "up", "decreasing" -> "down", "stable" -> "stable"
        trend =
          primaryQueue.trend.direction === "increasing"
            ? "up"
            : primaryQueue.trend.direction === "decreasing"
              ? "down"
              : "stable";
      }

      // Create DTO from integrated response with distance and trend
      const dto: AttractionWithDistanceDto = {
        ...integrated,
        distance:
          userLocation && attraction.latitude && attraction.longitude
            ? Math.round(
                calculateHaversineDistance(
                  userLocation,
                  {
                    latitude: Number(attraction.latitude),
                    longitude: Number(attraction.longitude),
                  },
                  "m",
                ),
              )
            : null,
        trend,
      };

      return dto;
    });
  }

  /**
   * Enrich shows with live data and calculate distances
   */
  private async enrichShows(
    shows: Show[],
    userLocation?: GeoCoordinate,
  ): Promise<ShowWithDistanceDto[]> {
    if (shows.length === 0) {
      return [];
    }

    // Fetch live data for all shows in parallel
    const liveDataPromises = shows.map((show) =>
      this.showsService.findCurrentStatusByShow(show.id).catch(() => null),
    );
    const liveDataArray = await Promise.all(liveDataPromises);

    // Build enriched DTOs (simplified like nearby)
    return shows.map((show, index) => {
      const liveData = liveDataArray[index];

      const dto: ShowWithDistanceDto = {
        id: show.id,
        name: show.name,
        slug: show.slug,
        distance:
          userLocation && show.latitude && show.longitude
            ? Math.round(
                calculateHaversineDistance(
                  userLocation,
                  {
                    latitude: Number(show.latitude),
                    longitude: Number(show.longitude),
                  },
                  "m",
                ),
              )
            : null,
        status: liveData?.status || "CLOSED",
        showtimes: liveData?.showtimes || null,
        url: show.park ? buildParkUrl(show.park) || null : null,
        park: show.park
          ? {
              id: show.park.id,
              name: show.park.name,
              slug: show.park.slug,
            }
          : null,
      };

      return dto;
    });
  }

  /**
   * Enrich restaurants with live data and calculate distances
   */
  private async enrichRestaurants(
    restaurants: Restaurant[],
    userLocation?: GeoCoordinate,
  ): Promise<RestaurantWithDistanceDto[]> {
    if (restaurants.length === 0) {
      return [];
    }

    // Fetch live data for all restaurants in parallel
    const liveDataPromises = restaurants.map((restaurant) =>
      this.restaurantsService
        .findCurrentStatusByRestaurant(restaurant.id)
        .catch(() => null),
    );
    const liveDataArray = await Promise.all(liveDataPromises);

    // Build enriched DTOs (simplified like nearby)
    return restaurants.map((restaurant, index) => {
      const liveData = liveDataArray[index];

      const dto: RestaurantWithDistanceDto = {
        id: restaurant.id,
        name: restaurant.name,
        slug: restaurant.slug,
        distance:
          userLocation && restaurant.latitude && restaurant.longitude
            ? Math.round(
                calculateHaversineDistance(
                  userLocation,
                  {
                    latitude: Number(restaurant.latitude),
                    longitude: Number(restaurant.longitude),
                  },
                  "m",
                ),
              )
            : null,
        status: liveData?.status || "CLOSED",
        waitTime: liveData?.waitTime || null,
        cuisineType: restaurant.cuisineType || null,
        url: restaurant.park ? buildParkUrl(restaurant.park) || null : null,
        park: restaurant.park
          ? {
              id: restaurant.park.id,
              name: restaurant.park.name,
              slug: restaurant.park.slug,
            }
          : null,
      };

      return dto;
    });
  }

  /**
   * Batch fetch schedules for multiple parks
   * Returns both today's schedule and next schedule
   */
  private async batchFetchSchedules(parkIds: string[]) {
    const [todayResults, nextResults] = await Promise.all([
      Promise.all(
        parkIds.map((id) =>
          this.parksService.getTodaySchedule(id).catch(() => []),
        ),
      ),
      Promise.all(
        parkIds.map((id) =>
          this.parksService.getNextSchedule(id).catch(() => null),
        ),
      ),
    ]);
    return {
      today: new Map(parkIds.map((id, i) => [id, todayResults[i]])),
      next: new Map(parkIds.map((id, i) => [id, nextResults[i]])),
    };
  }
}
