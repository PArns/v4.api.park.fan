import { Injectable, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Brackets } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { SearchQueryDto } from "./dto/search-query.dto";
import { SearchResultDto, SearchResultItemDto } from "./dto/search-result.dto";
import { buildParkUrl, buildAttractionUrl } from "../common/utils/url.util";
import { ParksService } from "../parks/parks.service";
import { AnalyticsService } from "../analytics/analytics.service";
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
    private readonly parksService: ParksService,
    private readonly analyticsService: AnalyticsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async search(query: SearchQueryDto): Promise<SearchResultDto> {
    const { q, type, limit = 5 } = query;

    // Build cache key
    const typeKey = type && type.length > 0 ? type.join(",") : "all";
    const cacheKey = `search:${typeKey}:${q.toLowerCase()}`;

    // Try cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }

    // Determine which entity types to search (max 5 per type)
    const searchTypes =
      type && type.length > 0
        ? type
        : ["park", "attraction", "show", "restaurant"];

    const results: SearchResultItemDto[] = [];
    let totalCount = 0;

    // Search parks
    if (searchTypes.includes("park")) {
      const parks = await this.searchParks(q, limit);
      const enrichedParks = await this.enrichParkResults(parks);
      results.push(...enrichedParks);
      totalCount += parks.length;
    }

    // Search attractions
    if (searchTypes.includes("attraction")) {
      const attractions = await this.searchAttractions(q, limit);
      const enrichedAttractions =
        await this.enrichAttractionResults(attractions);
      results.push(...enrichedAttractions);
      totalCount += attractions.length;
    }

    // Search shows
    if (searchTypes.includes("show")) {
      const shows = await this.searchShows(q, limit);
      const enrichedShows = await this.enrichShowResults(shows);
      results.push(...enrichedShows);
      totalCount += shows.length;
    }

    // Search restaurants
    if (searchTypes.includes("restaurant")) {
      const restaurants = await this.searchRestaurants(q, limit);
      const enrichedRestaurants =
        await this.enrichRestaurantResults(restaurants);
      results.push(...enrichedRestaurants);
      totalCount += restaurants.length;
    }

    const response: SearchResultDto = {
      results,
      total: totalCount,
      query: q,
      searchTypes,
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
      .orderBy("similarity(park.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .getMany();
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
      .orderBy("similarity(attraction.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .getMany();
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
      .orderBy("similarity(show.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .getMany();
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
      .orderBy("similarity(restaurant.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
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

    return parks.map((park) => ({
      type: "park" as const,
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),
      continent: park.continent || null,
      country: park.country || null,
      countryCode: park.countryCode || null,
      city: park.city || null,
      resort: park.destination?.name || null,
      status: statusMap.get(park.id) || "CLOSED",
      load: loadMap.get(park.id) || null,
    }));
  }

  /**
   * Enrich attraction results with parent park info
   */
  private async enrichAttractionResults(
    attractions: any[],
  ): Promise<SearchResultItemDto[]> {
    return attractions.map((attraction) => ({
      type: "attraction" as const,
      id: attraction.id,
      name: attraction.name,
      slug: attraction.slug,
      url: attraction.park
        ? buildAttractionUrl(attraction.park, { slug: attraction.slug })
        : null,
      continent: attraction.park?.continent || null,
      country: attraction.park?.country || null,
      countryCode: attraction.park?.countryCode || null,
      city: attraction.park?.city || null,
      resort: attraction.park?.destination?.name || null,
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
   * Enrich show results with parent park info
   */
  private async enrichShowResults(
    shows: any[],
  ): Promise<SearchResultItemDto[]> {
    return shows.map((show) => ({
      type: "show" as const,
      id: show.id,
      name: show.name,
      slug: show.slug,
      url: null,
      continent: show.park?.continent || null,
      country: show.park?.country || null,
      countryCode: show.park?.countryCode || null,
      city: show.park?.city || null,
      resort: show.park?.destination?.name || null,
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
