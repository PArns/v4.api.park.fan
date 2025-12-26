import { Injectable, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Brackets, Between } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../parks/entities/schedule-entry.entity";
import { SearchQueryDto } from "./dto/search-query.dto";
import { SearchResultDto, SearchResultItemDto } from "./dto/search-result.dto";
import { buildParkUrl, buildAttractionUrl } from "../common/utils/url.util";
import { ParksService } from "../parks/parks.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { ShowsService } from "../shows/shows.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

@Injectable()
export class SearchService {
  private readonly CACHE_TTL = 300; // 5 minutes

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @InjectRepository(Show)
    private readonly showRepository: Repository<Show>,
    @InjectRepository(Restaurant)
    private readonly restaurantRepository: Repository<Restaurant>,
    @InjectRepository(ScheduleEntry)
    private readonly scheduleRepository: Repository<ScheduleEntry>,
    private readonly parksService: ParksService,
    private readonly analyticsService: AnalyticsService,
    private readonly queueDataService: QueueDataService,
    private readonly showsService: ShowsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async search(query: SearchQueryDto): Promise<SearchResultDto> {
    const { q, type } = query;
    const limit = 5; // Fixed limit per type

    // Build cache key
    const typeKey = type && type.length > 0 ? type.join(",") : "all";
    const cacheKey = `search:${typeKey}:${q.toLowerCase()}`;

    // Try cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    // Determine which entity types to search
    const searchTypes =
      type && type.length > 0
        ? type
        : ["park", "attraction", "show", "restaurant"];

    const results: SearchResultItemDto[] = [];
    const counts = {
      park: { returned: 0, total: 0 },
      attraction: { returned: 0, total: 0 },
      show: { returned: 0, total: 0 },
      restaurant: { returned: 0, total: 0 },
    };

    // Search parks
    if (searchTypes.includes("park")) {
      const totalParks = await this.countParks(q);
      const parks = await this.searchParks(q, limit);
      const enrichedParks = await this.enrichParkResults(parks);
      results.push(...enrichedParks);
      counts.park = { returned: enrichedParks.length, total: totalParks };
    }

    // Search attractions
    if (searchTypes.includes("attraction")) {
      const totalAttractions = await this.countAttractions(q);
      const attractions = await this.searchAttractions(q, limit);
      const enrichedAttractions =
        await this.enrichAttractionResults(attractions);
      results.push(...enrichedAttractions);
      counts.attraction = {
        returned: enrichedAttractions.length,
        total: totalAttractions,
      };
    }

    // Search shows
    if (searchTypes.includes("show")) {
      const totalShows = await this.countShows(q);
      const shows = await this.searchShows(q, limit);
      const enrichedShows = await this.enrichShowResults(shows);
      results.push(...enrichedShows);
      counts.show = { returned: enrichedShows.length, total: totalShows };
    }

    // Search restaurants
    if (searchTypes.includes("restaurant")) {
      const totalRestaurants = await this.countRestaurants(q);
      const restaurants = await this.searchRestaurants(q, limit);
      const enrichedRestaurants =
        await this.enrichRestaurantResults(restaurants);
      results.push(...enrichedRestaurants);
      counts.restaurant = {
        returned: enrichedRestaurants.length,
        total: totalRestaurants,
      };
    }

    const response: SearchResultDto = {
      query: q,
      results,
      counts,
    };

    // Cache for 5 minutes
    await this.redis.set(
      cacheKey,
      JSON.stringify(response),
      "EX",
      this.CACHE_TTL,
    );

    return response;
  }

  /**
   * Count parks matching query
   */
  private async countParks(query: string): Promise<number> {
    return this.parkRepository
      .createQueryBuilder("park")
      .where(
        new Brackets((qb) => {
          qb.where("park.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .getCount();
  }

  /**
   * Search parks by name, city, country, or continent
   */
  private async searchParks(
    query: string,
    limit: number,
  ): Promise<
    Pick<
      Park,
      | "id"
      | "slug"
      | "name"
      | "latitude"
      | "longitude"
      | "continentSlug"
      | "countrySlug"
      | "citySlug"
      | "continent"
      | "country"
      | "countryCode"
      | "city"
      | "destination"
    >[]
  > {
    return this.parkRepository
      .createQueryBuilder("park")
      .leftJoinAndSelect("park.destination", "destination")
      .select([
        "park.id",
        "park.slug",
        "park.name",
        "park.latitude",
        "park.longitude",
        "park.continentSlug",
        "park.countrySlug",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.countryCode",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("park.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .orderBy(
        "CASE WHEN LOWER(park.name) = LOWER(:exactQuery) THEN 0 WHEN LOWER(park.name) LIKE LOWER(:startsWith) THEN 1 ELSE 2 END",
        "ASC",
      )
      .addOrderBy("park.name", "ASC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .limit(limit)
      .getMany();
  }

  /**
   * Count attractions matching query
   */
  private async countAttractions(query: string): Promise<number> {
    return this.attractionRepository
      .createQueryBuilder("attraction")
      .leftJoin("attraction.park", "park")
      .where(
        new Brackets((qb) => {
          qb.where("attraction.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .getCount();
  }

  /**
   * Search attractions by name OR by park location
   */
  private async searchAttractions(
    query: string,
    limit: number,
  ): Promise<
    (Pick<Attraction, "id" | "slug" | "name"> & {
      park?: Pick<
        Park,
        | "id"
        | "slug"
        | "name"
        | "latitude"
        | "longitude"
        | "continentSlug"
        | "countrySlug"
        | "citySlug"
        | "continent"
        | "country"
        | "countryCode"
        | "city"
        | "destination"
      >;
    })[]
  > {
    return this.attractionRepository
      .createQueryBuilder("attraction")
      .leftJoinAndSelect("attraction.park", "park")
      .leftJoinAndSelect("park.destination", "destination")
      .select([
        "attraction.id",
        "attraction.slug",
        "attraction.name",
        "park.id",
        "park.slug",
        "park.name",
        "park.latitude",
        "park.longitude",
        "park.continentSlug",
        "park.countrySlug",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.countryCode",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("attraction.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .orderBy(
        "CASE WHEN LOWER(attraction.name) = LOWER(:exactQuery) THEN 0 WHEN LOWER(attraction.name) LIKE LOWER(:startsWith) THEN 1 ELSE 2 END",
        "ASC",
      )
      .addOrderBy("attraction.name", "ASC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .limit(limit)
      .getMany();
  }

  /**
   * Count shows matching query
   */
  private async countShows(query: string): Promise<number> {
    return this.showRepository
      .createQueryBuilder("show")
      .leftJoin("show.park", "park")
      .where(
        new Brackets((qb) => {
          qb.where("show.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .getCount();
  }

  private async searchShows(
    query: string,
    limit: number,
  ): Promise<
    (Pick<Show, "id" | "slug" | "name"> & {
      park?: Pick<
        Park,
        | "id"
        | "slug"
        | "name"
        | "latitude"
        | "longitude"
        | "continentSlug"
        | "countrySlug"
        | "citySlug"
        | "continent"
        | "country"
        | "countryCode"
        | "city"
        | "destination"
      >;
    })[]
  > {
    return this.showRepository
      .createQueryBuilder("show")
      .leftJoinAndSelect("show.park", "park")
      .leftJoinAndSelect("park.destination", "destination")
      .select([
        "show.id",
        "show.slug",
        "show.name",
        "park.id",
        "park.slug",
        "park.name",
        "park.latitude",
        "park.longitude",
        "park.continentSlug",
        "park.countrySlug",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.countryCode",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("show.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .orderBy(
        "CASE WHEN LOWER(show.name) = LOWER(:exactQuery) THEN 0 WHEN LOWER(show.name) LIKE LOWER(:startsWith) THEN 1 ELSE 2 END",
        "ASC",
      )
      .addOrderBy("show.name", "ASC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .limit(limit)
      .getMany();
  }

  /**
   * Count restaurants matching query
   */
  private async countRestaurants(query: string): Promise<number> {
    return this.restaurantRepository
      .createQueryBuilder("restaurant")
      .leftJoin("restaurant.park", "park")
      .where(
        new Brackets((qb) => {
          qb.where("restaurant.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .getCount();
  }

  private async searchRestaurants(
    query: string,
    limit: number,
  ): Promise<
    (Pick<Restaurant, "id" | "slug" | "name"> & {
      park?: Pick<
        Park,
        | "id"
        | "slug"
        | "name"
        | "latitude"
        | "longitude"
        | "continentSlug"
        | "countrySlug"
        | "citySlug"
        | "continent"
        | "country"
        | "countryCode"
        | "city"
        | "destination"
      >;
    })[]
  > {
    return this.restaurantRepository
      .createQueryBuilder("restaurant")
      .leftJoinAndSelect("restaurant.park", "park")
      .leftJoinAndSelect("park.destination", "destination")
      .select([
        "restaurant.id",
        "restaurant.slug",
        "restaurant.name",
        "park.id",
        "park.slug",
        "park.name",
        "park.latitude",
        "park.longitude",
        "park.continentSlug",
        "park.countrySlug",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.countryCode",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("restaurant.name ILIKE :query", { query: `%${query}%` })
            .orWhere("park.city ILIKE :query")
            .orWhere("park.country ILIKE :query")
            .orWhere("park.continent ILIKE :query");
        }),
      )
      .orderBy(
        "CASE WHEN LOWER(restaurant.name) = LOWER(:exactQuery) THEN 0 WHEN LOWER(restaurant.name) LIKE LOWER(:startsWith) THEN 1 ELSE 2 END",
        "ASC",
      )
      .addOrderBy("restaurant.name", "ASC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .limit(limit)
      .getMany();
  }

  /**
   * Enrich park results with status and load from cached analytics data
   */
  private async enrichParkResults(
    parks: any[],
  ): Promise<SearchResultItemDto[]> {
    const parkIds = parks.map((p) => p.id);

    // Batch fetch status from existing cache (no extra DB queries!)
    const statusMap = await this.parksService.getBatchParkStatus(parkIds);

    // Batch fetch occupancy/load
    const loadMap = await this.getBatchLoadLevels(parkIds);

    // Batch fetch today's operating hours
    const hoursMap = await this.getBatchParkHours(parkIds);

    return parks.map((park) => ({
      type: "park" as const,
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),
      latitude: park.latitude ? parseFloat(park.latitude) : null,
      longitude: park.longitude ? parseFloat(park.longitude) : null,
      continent: park.continent || null,
      country: park.country || null,
      countryCode: park.countryCode || null,
      city: park.city || null,
      resort: park.destination?.name || null,
      status: statusMap.get(park.id) || "CLOSED",
      load: loadMap.get(park.id) || null,
      parkHours: hoursMap.get(park.id) || null,
    }));
  }

  /**
   * Enrich attraction results with parent park info, wait times, status, and load
   */
  private async enrichAttractionResults(
    attractions: any[],
  ): Promise<SearchResultItemDto[]> {
    // Batch fetch wait times and status for all attractions
    const attractionIds = attractions.map((a) => a.id);
    const waitTimesMap = await this.getBatchWaitTimes(attractionIds);
    const statusMap = await this.getBatchAttractionStatus(attractionIds);

    return attractions.map((attraction) => ({
      type: "attraction" as const,
      id: attraction.id,
      name: attraction.name,
      slug: attraction.slug,
      url: attraction.park
        ? buildAttractionUrl(attraction.park, { slug: attraction.slug })
        : null,
      latitude: attraction.park?.latitude
        ? parseFloat(attraction.park.latitude)
        : null,
      longitude: attraction.park?.longitude
        ? parseFloat(attraction.park.longitude)
        : null,
      continent: attraction.park?.continent || null,
      country: attraction.park?.country || null,
      countryCode: attraction.park?.countryCode || null,
      city: attraction.park?.city || null,
      resort: attraction.park?.destination?.name || null,
      status:
        (statusMap.get(attraction.id)?.status as
          | "OPERATING"
          | "CLOSED"
          | "DOWN"
          | "REFURBISHMENT"
          | null) || null,
      load: this.determineAttractionLoad(waitTimesMap.get(attraction.id)),
      waitTime: waitTimesMap.get(attraction.id) || null,
      parentPark: attraction.park
        ? {
            id: attraction.park.id,
            name: attraction.park.name,
            slug: attraction.park.slug,
            url: buildParkUrl(attraction.park),
          }
        : null,
    }));
  }

  /**
   * Enrich show results with parent park info and show times
   */
  private async enrichShowResults(
    shows: any[],
  ): Promise<SearchResultItemDto[]> {
    // Batch fetch today's show times
    const showIds = shows.map((s) => s.id);
    const showTimesMap = await this.getBatchShowTimes(showIds);
    return shows.map((show) => ({
      type: "show" as const,
      id: show.id,
      name: show.name,
      slug: show.slug,
      url: null,
      latitude: show.park?.latitude ? parseFloat(show.park.latitude) : null,
      longitude: show.park?.longitude ? parseFloat(show.park.longitude) : null,
      continent: show.park?.continent || null,
      country: show.park?.country || null,
      countryCode: show.park?.countryCode || null,
      city: show.park?.city || null,
      resort: show.park?.destination?.name || null,
      showTimes: showTimesMap.get(show.id) || null,
      parentPark: show.park
        ? {
            id: show.park.id,
            name: show.park.name,
            slug: show.park.slug,
            url: buildParkUrl(show.park),
          }
        : null,
    }));
  }

  /**
   * Enrich restaurant results with parent park info
   */
  private async enrichRestaurantResults(
    restaurants: any[],
  ): Promise<SearchResultItemDto[]> {
    return restaurants.map((restaurant) => ({
      type: "restaurant" as const,
      id: restaurant.id,
      name: restaurant.name,
      slug: restaurant.slug,
      url: null,
      latitude: restaurant.park?.latitude
        ? parseFloat(restaurant.park.latitude)
        : null,
      longitude: restaurant.park?.longitude
        ? parseFloat(restaurant.park.longitude)
        : null,
      continent: restaurant.park?.continent || null,
      country: restaurant.park?.country || null,
      countryCode: restaurant.park?.countryCode || null,
      city: restaurant.park?.city || null,
      resort: restaurant.park?.destination?.name || null,
      parentPark: restaurant.park
        ? {
            id: restaurant.park.id,
            name: restaurant.park.name,
            slug: restaurant.park.slug,
            url: buildParkUrl(restaurant.park),
          }
        : null,
    }));
  }

  /**
   * Batch fetch wait times for attractions
   */
  private async getBatchWaitTimes(
    attractionIds: string[],
  ): Promise<Map<string, number>> {
    const waitTimesMap = new Map<string, number>();

    await Promise.all(
      attractionIds.map(async (attractionId) => {
        try {
          // Get current status (most recent queue data for all queue types)
          const queueData =
            await this.queueDataService.findCurrentStatusByAttraction(
              attractionId,
            );
          // Find STANDBY queue (most common wait time)
          const standbyQueue = queueData.find((q) => q.queueType === "STANDBY");
          if (
            standbyQueue &&
            standbyQueue.waitTime !== null &&
            standbyQueue.waitTime !== undefined
          ) {
            waitTimesMap.set(attractionId, standbyQueue.waitTime);
          }
        } catch {
          // Skip attractions without wait time data
        }
      }),
    );

    return waitTimesMap;
  }

  /**
   * Batch fetch attraction status from queue data
   */
  private async getBatchAttractionStatus(
    attractionIds: string[],
  ): Promise<Map<string, { status: string }>> {
    const statusMap = new Map<string, { status: string }>();

    await Promise.all(
      attractionIds.map(async (attractionId) => {
        try {
          const queueData =
            await this.queueDataService.findCurrentStatusByAttraction(
              attractionId,
            );
          // Use first queue data status (usually STANDBY)
          if (queueData && queueData.length > 0 && queueData[0].status) {
            statusMap.set(attractionId, { status: queueData[0].status });
          }
        } catch {
          // Skip attractions without status data
        }
      }),
    );

    return statusMap;
  }

  /**
   * Determine attraction load level from wait time
   */
  private determineAttractionLoad(
    waitTime: number | undefined,
  ): "very_low" | "low" | "normal" | "higher" | "high" | "extreme" | null {
    if (!waitTime) return null;
    if (waitTime <= 10) return "very_low";
    if (waitTime <= 20) return "low";
    if (waitTime <= 40) return "normal";
    if (waitTime <= 60) return "higher";
    if (waitTime <= 90) return "high";
    return "extreme";
  }

  /**
   * Batch fetch today's operating hours for parks
   */
  private async getBatchParkHours(
    parkIds: string[],
  ): Promise<Map<string, { open: string; close: string; type: string }>> {
    const hoursMap = new Map<
      string,
      { open: string; close: string; type: string }
    >();

    // Get today's date range in local timezone
    const now = new Date();
    const todayStart = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
    );
    const todayEnd = new Date(todayStart);
    todayEnd.setDate(todayEnd.getDate() + 1);

    await Promise.all(
      parkIds.map(async (parkId) => {
        try {
          const schedule = await this.scheduleRepository.findOne({
            where: {
              parkId,
              date: Between(todayStart, todayEnd),
              scheduleType: ScheduleType.OPERATING,
            },
            order: { date: "ASC" },
          });

          if (schedule && schedule.openingTime && schedule.closingTime) {
            hoursMap.set(parkId, {
              open: schedule.openingTime.toISOString(),
              close: schedule.closingTime.toISOString(),
              type: schedule.scheduleType,
            });
          }
        } catch {
          // Skip parks without schedule data
        }
      }),
    );

    return hoursMap;
  }

  /**
   * Batch fetch today's show times for shows
   */
  private async getBatchShowTimes(
    showIds: string[],
  ): Promise<Map<string, string[]>> {
    const showTimesMap = new Map<string, string[]>();

    await Promise.all(
      showIds.map(async (showId) => {
        try {
          // Get current status which includes showtimes
          const liveData =
            await this.showsService.findCurrentStatusByShow(showId);
          if (liveData && liveData.showtimes && liveData.showtimes.length > 0) {
            // Extract start times from showtime objects
            const times = liveData.showtimes
              .map((st: any) => st.startTime)
              .filter(Boolean)
              .sort();
            if (times.length > 0) {
              showTimesMap.set(showId, times);
            }
          }
        } catch {
          // Skip shows without showtime data
        }
      }),
    );

    return showTimesMap;
  }

  /**
   * Batch fetch load levels for parks from analytics cache
   */
  private async getBatchLoadLevels(
    parkIds: string[],
  ): Promise<
    Map<string, "very_low" | "low" | "normal" | "higher" | "high" | "extreme">
  > {
    const loadMap = new Map<
      string,
      "very_low" | "low" | "normal" | "higher" | "high" | "extreme"
    >();

    await Promise.all(
      parkIds.map(async (parkId) => {
        try {
          const occupancy =
            await this.analyticsService.calculateParkOccupancy(parkId);
          loadMap.set(parkId, this.determineCrowdLevel(occupancy.current));
        } catch {
          // Skip parks without occupancy data
        }
      }),
    );

    return loadMap;
  }

  /**
   * Convert occupancy percentage to crowd level
   * (from AnalyticsService.determineCrowdLevel)
   */
  private determineCrowdLevel(
    occupancy: number,
  ): "very_low" | "low" | "normal" | "higher" | "high" | "extreme" {
    if (occupancy <= 20) return "very_low";
    if (occupancy <= 40) return "low";
    if (occupancy <= 60) return "normal";
    if (occupancy <= 80) return "higher";
    if (occupancy <= 95) return "high";
    return "extreme";
  }
}
