import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
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
  AttractionWithDistanceDto,
  ShowWithDistanceDto,
  RestaurantWithDistanceDto,
} from "./dto/favorites-response.dto";
import { ParkWithDistanceDto } from "../common/dto/park-with-distance.dto";
import {
  calculateHaversineDistance,
  GeoCoordinate,
} from "../common/utils/distance.util";
import {
  buildParkUrl,
  buildAttractionUrl,
  buildShowUrl,
  buildRestaurantUrl,
} from "../common/utils/url.util";

/**
 * Favorites Service
 *
 * Retrieves and enriches favorite entities (parks, attractions, shows, restaurants)
 * with full information including live data and optional distance calculations.
 */
@Injectable()
export class FavoritesService {
  private readonly logger = new Logger(FavoritesService.name);
  private readonly CACHE_TTL = 2 * 60; // 2 minutes - matches HTTP cache
  private readonly CACHE_STALE_THRESHOLD = 60; // Refresh if TTL < 1 minute

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
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Get favorites with full information
   *
   * Uses Redis caching for performance:
   * - Cache key based on sorted IDs + location hash
   * - TTL: 2 minutes (matches HTTP cache)
   * - Stale-while-revalidate: refreshes in background if TTL < 1 minute
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
    // Generate cache key from sorted IDs + location
    const cacheKey = this.buildCacheKey(
      parkIds,
      attractionIds,
      showIds,
      restaurantIds,
      userLocation,
    );

    // Try cache first
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) {
        const cachedResponse = JSON.parse(cached) as FavoritesResponseDto;

        // Stale-while-revalidate: refresh in background if cache expires soon
        const ttl = await this.redis.ttl(cacheKey);
        if (ttl < this.CACHE_STALE_THRESHOLD && ttl > 0) {
          this.refreshCacheInBackground(
            parkIds,
            attractionIds,
            showIds,
            restaurantIds,
            userLocation,
            cacheKey,
          ).catch((err) =>
            this.logger.warn(
              `Background cache refresh failed for favorites:`,
              err,
            ),
          );
        }

        return cachedResponse;
      }
    } catch (error) {
      this.logger.warn(`Cache read failed, falling back to DB:`, error);
    }

    // Cache miss - fetch and enrich data
    const response = await this.fetchAndEnrichFavorites(
      parkIds,
      attractionIds,
      showIds,
      restaurantIds,
      userLocation,
    );

    // Cache response
    try {
      await this.redis.set(
        cacheKey,
        JSON.stringify(response),
        "EX",
        this.CACHE_TTL,
      );
    } catch (error) {
      this.logger.warn(`Cache write failed:`, error);
    }

    return response;
  }

  /**
   * Build cache key from entity IDs and user location
   * Uses sorted IDs for consistent keys regardless of input order
   */
  private buildCacheKey(
    parkIds: string[],
    attractionIds: string[],
    showIds: string[],
    restaurantIds: string[],
    userLocation?: GeoCoordinate,
  ): string {
    // Sort IDs for consistent cache keys
    const sortedParkIds = [...parkIds].sort().join(",");
    const sortedAttractionIds = [...attractionIds].sort().join(",");
    const sortedShowIds = [...showIds].sort().join(",");
    const sortedRestaurantIds = [...restaurantIds].sort().join(",");

    // Create location hash if provided (rounded to ~100m precision)
    let locationHash = "";
    if (userLocation) {
      const roundedLat = Math.round(userLocation.latitude * 1000) / 1000;
      const roundedLng = Math.round(userLocation.longitude * 1000) / 1000;
      locationHash = `:${roundedLat}:${roundedLng}`;
    }

    // Build key: favorites:{parkIds}:{attractionIds}:{showIds}:{restaurantIds}:{locationHash}
    const keyParts = [
      "favorites",
      sortedParkIds || "none",
      sortedAttractionIds || "none",
      sortedShowIds || "none",
      sortedRestaurantIds || "none",
    ];

    if (locationHash) {
      keyParts.push(locationHash.substring(1)); // Remove leading colon
    }

    return keyParts.join(":");
  }

  /**
   * Refresh cache in background (stale-while-revalidate pattern)
   */
  private async refreshCacheInBackground(
    parkIds: string[],
    attractionIds: string[],
    showIds: string[],
    restaurantIds: string[],
    userLocation: GeoCoordinate | undefined,
    cacheKey: string,
  ): Promise<void> {
    const response = await this.fetchAndEnrichFavorites(
      parkIds,
      attractionIds,
      showIds,
      restaurantIds,
      userLocation,
    );

    try {
      await this.redis.set(
        cacheKey,
        JSON.stringify(response),
        "EX",
        this.CACHE_TTL,
      );
    } catch (error) {
      this.logger.warn(`Background cache refresh failed:`, error);
    }
  }

  /**
   * Fetch and enrich favorites data (core logic without caching)
   */
  private async fetchAndEnrichFavorites(
    parkIds: string[],
    attractionIds: string[],
    showIds: string[],
    restaurantIds: string[],
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

    // Pre-calculate context (timezone + startTime) for batch park statistics using batch method
    const startTimeMap = await this.analyticsService.getBatchEffectiveStartTime(
      parks.map((p) => ({ id: p.id, timezone: p.timezone || "UTC" })),
    );
    const context = new Map<string, { timezone: string; startTime: Date }>();
    for (const park of parks) {
      const startTime = startTimeMap.get(park.id)!;
      context.set(park.id, {
        timezone: park.timezone || "UTC",
        startTime,
      });
    }

    // Batch fetch status, analytics, schedules, and statistics
    const [statusMap, occupancyMap, schedules, statisticsMap] =
      await Promise.all([
        this.parksService.getBatchParkStatus(parkIds),
        this.analyticsService["getBatchParkOccupancy"](parkIds),
        this.parksService.getBatchSchedules(parkIds),
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
        // Add URL if park is available
        if (attraction.park) {
          dto.url = buildAttractionUrl(attraction.park, attraction) || null;
        }
        return dto;
      }

      // Create DTO from integrated response with distance
      // Trend is already included in integrated response from AttractionIntegrationService
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
        // Set URL if not already set and park is available
        url:
          integrated.url ||
          (attraction.park
            ? buildAttractionUrl(attraction.park, attraction) || null
            : null),
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

    // Batch fetch live data for all shows
    const showIds = shows.map((s) => s.id);
    const liveDataMap =
      await this.showsService.findBatchCurrentStatusByShows(showIds);

    // Build enriched DTOs (simplified like nearby)
    return shows.map((show) => {
      const liveData = liveDataMap.get(show.id) || null;

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
        url: show.park ? buildShowUrl(show.park, show) || null : null,
        park: show.park
          ? {
              id: show.park.id,
              name: show.park.name,
              slug: show.park.slug,
              timezone: show.park.timezone,
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

    // Batch fetch live data for all restaurants
    const restaurantIds = restaurants.map((r) => r.id);
    const liveDataMap =
      await this.restaurantsService.findBatchCurrentStatusByRestaurants(
        restaurantIds,
      );

    // Build enriched DTOs (simplified like nearby)
    return restaurants.map((restaurant) => {
      const liveData = liveDataMap.get(restaurant.id) || null;

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
        url: restaurant.park
          ? buildRestaurantUrl(restaurant.park, restaurant) || null
          : null,
        park: restaurant.park
          ? {
              id: restaurant.park.id,
              name: restaurant.park.name,
              slug: restaurant.park.slug,
              timezone: restaurant.park.timezone,
            }
          : null,
      };

      return dto;
    });
  }
}
