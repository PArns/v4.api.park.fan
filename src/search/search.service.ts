import { Injectable, Inject, OnModuleInit, Logger } from "@nestjs/common";
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
import { CrowdLevel } from "../common/types";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { SearchCounts } from "./types/search-counts.type";

@Injectable()
export class SearchService implements OnModuleInit {
  private readonly logger = new Logger(SearchService.name);
  private readonly CACHE_TTL = 60; // 1 minute (aligned with frontend revalidation)

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

  async onModuleInit() {
    // Initialize pg_trgm extension and indices for fuzzy search
    try {
      await this.parkRepository.query(
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
      );

      // Create indices concurrently if possible, but safe here without valid concurrently in transaction block usually
      // Park indices
      await this.parkRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_park_name_trgm ON parks USING gin (name gin_trgm_ops);",
      );
      await this.parkRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_park_city_trgm ON parks USING gin (city gin_trgm_ops);",
      );
      await this.parkRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_park_country_trgm ON parks USING gin (country gin_trgm_ops);",
      );

      // Attraction indices
      await this.attractionRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_attraction_name_trgm ON attractions USING gin (name gin_trgm_ops);",
      );
      await this.attractionRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_attraction_land_name_trgm ON attractions USING gin (land_name gin_trgm_ops);",
      );

      // Show indices
      await this.showRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_show_name_trgm ON shows USING gin (name gin_trgm_ops);",
      );

      // Restaurant indices
      await this.restaurantRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_restaurant_name_trgm ON restaurants USING gin (name gin_trgm_ops);",
      );

      this.logger.log("✅ Fuzzy search extensions and indices initialized.");
    } catch (error) {
      this.logger.warn("⚠️ Failed to initialize fuzzy search indices:", error);
    }
  }

  async search(query: SearchQueryDto): Promise<SearchResultDto> {
    const { q, type } = query;
    const limit = query.limit || 5; // Use generic limit or default

    if (!q || q.length < 2) {
      return {
        query: q,
        results: [],
        counts: {
          park: { returned: 0, total: 0 },
          attraction: { returned: 0, total: 0 },
          show: { returned: 0, total: 0 },
          restaurant: { returned: 0, total: 0 },
        } as SearchCounts,
      };
    }

    // Build cache key
    const typeKey = type && type.length > 0 ? type.join(",") : "all";
    const cacheKey = `search:fuzzy:v1:${typeKey}:${q.toLowerCase()}`;

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
    const counts: SearchCounts = {
      park: { returned: 0, total: 0 },
      attraction: { returned: 0, total: 0 },
      show: { returned: 0, total: 0 },
      restaurant: { returned: 0, total: 0 },
    };

    // Search parks
    if (searchTypes.includes("park")) {
      const parks = await this.searchParks(q, limit);
      // We do a separate count query or just use the length if we didn't hit limit?
      // For accurate "total" with fuzzy search, it's expensive.
      // Let's approximate total as length for now or do a count query if needed.
      // Given fuzzy nature, "total matches" is ambiguous (depends on threshold).
      // We'll use the returned length as total for now to save perf, or run a count if strictly needed.
      const enrichedParks = await this.enrichParkResults(
        parks as unknown as Park[],
      );
      results.push(...enrichedParks);
      counts.park = { returned: enrichedParks.length, total: parks.length };
    }

    // Search attractions
    if (searchTypes.includes("attraction")) {
      const attractions = await this.searchAttractions(q, limit);
      const enrichedAttractions =
        await this.enrichAttractionResults(attractions);
      results.push(...enrichedAttractions);
      counts.attraction = {
        returned: enrichedAttractions.length,
        total: attractions.length,
      };
    }

    // Search shows
    if (searchTypes.includes("show")) {
      const shows = await this.searchShows(q, limit);
      const enrichedShows = await this.enrichShowResults(shows);
      results.push(...enrichedShows);
      counts.show = { returned: enrichedShows.length, total: shows.length };
    }

    // Search restaurants
    if (searchTypes.includes("restaurant")) {
      const restaurants = await this.searchRestaurants(q, limit);
      const enrichedRestaurants =
        await this.enrichRestaurantResults(restaurants);
      results.push(...enrichedRestaurants);
      counts.restaurant = {
        returned: enrichedRestaurants.length,
        total: restaurants.length,
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
   * Search parks with fuzzy matching
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
      | "countryCode"
      | "citySlug"
      | "continent"
      | "country"
      | "city"
      | "destination"
    >[]
  > {
    const normalizedQuery = query.replace(/[^a-zA-Z0-9]/g, "");

    return (
      this.parkRepository
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
          "park.countryCode",
          "park.citySlug",
          "park.continent",
          "park.country",
          "park.city",
          "destination.id",
          "destination.name",
        ])
        .where(
          new Brackets((qb) => {
            // 1. Exact or Like Match
            qb.where("park.name ILIKE :query", { query: `%${query}%` })
              .orWhere("park.city ILIKE :query")
              .orWhere("park.country ILIKE :query")
              .orWhere("park.continent ILIKE :query")
              // 2. Normalized Match (ignores special chars)
              .orWhere(
                "REGEXP_REPLACE(park.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
                { normalizedQuery: `%${normalizedQuery}%` },
              )
              // 3. Fuzzy Match
              .orWhere("similarity(park.name, :query) > 0.3")
              .orWhere("similarity(park.city, :query) > 0.3")
              .orWhere("similarity(park.country, :query) > 0.3");
          }),
        )
        .orderBy(
          // Prioritize:
          // 0. Exact Name Match (e.g. "fly" == "fly")
          // 1. Normalized Exact Match (e.g. "F.L.Y." -> "fly" == "fly")
          // 2. Exact City Match (e.g. "Orlando")
          // 3. Prefix Match (e.g. "Flying..." starts with "fly")
          // 4. Others
          `CASE
          WHEN LOWER(park.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(park.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(park.city) = LOWER(:exactQuery) THEN 2
          WHEN LOWER(park.name) LIKE LOWER(:startsWith) THEN 3
          ELSE 4
        END`,
          "ASC",
        )
        // Secondary sort by similarity
        .addOrderBy("similarity(park.name, :query)", "DESC")
        .setParameter("exactQuery", query)
        .setParameter("startsWith", `${query}%`)
        .setParameter("normalizedQueryExact", normalizedQuery)
        .setParameter("query", query) // Ensure query parameter is available for similarity
        .limit(limit)
        .getMany()
    );
  }

  /**
   * Search attractions with fuzzy matching (including Land)
   */
  private async searchAttractions(
    query: string,
    limit: number,
  ): Promise<
    (Pick<Attraction, "id" | "slug" | "name" | "landName"> & {
      park?: Pick<
        Park,
        | "id"
        | "slug"
        | "name"
        | "latitude"
        | "longitude"
        | "continentSlug"
        | "countrySlug"
        | "countryCode"
        | "citySlug"
        | "continent"
        | "country"
        | "city"
        | "destination"
      >;
    })[]
  > {
    const normalizedQuery = query.replace(/[^a-zA-Z0-9]/g, "");

    return (
      this.attractionRepository
        .createQueryBuilder("attraction")
        .leftJoinAndSelect("attraction.park", "park")
        .leftJoinAndSelect("park.destination", "destination")
        .select([
          "attraction.id",
          "attraction.slug",
          "attraction.name",
          "attraction.landName",
          "park.id",
          "park.slug",
          "park.name",
          "park.latitude",
          "park.longitude",
          "park.continentSlug",
          "park.countrySlug",
          "park.countryCode",
          "park.citySlug",
          "park.continent",
          "park.country",
          "park.city",
          "destination.id",
          "destination.name",
        ])
        .where(
          new Brackets((qb) => {
            qb.where("attraction.name ILIKE :query", { query: `%${query}%` })
              // Land Name Match
              .orWhere("attraction.landName ILIKE :query")
              // Normalized Name Match
              .orWhere(
                "REGEXP_REPLACE(attraction.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
                { normalizedQuery: `%${normalizedQuery}%` },
              )
              // Normalized Land Match
              .orWhere(
                "REGEXP_REPLACE(attraction.landName, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
              )
              // Fuzzy Matches
              .orWhere("similarity(attraction.name, :query) > 0.3")
              .orWhere("similarity(attraction.landName, :query) > 0.3")
              // Parent Park Location Fuzzy Matches
              .orWhere("similarity(park.city, :query) > 0.3")
              .orWhere("similarity(park.country, :query) > 0.3");
          }),
        )
        .orderBy(
          `CASE
            WHEN LOWER(attraction.name) = LOWER(:exactQuery) THEN 0
            WHEN REGEXP_REPLACE(attraction.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
            WHEN LOWER(attraction.name) LIKE LOWER(:startsWith) THEN 2
            ELSE 3
          END`,
          "ASC",
        )
        .addOrderBy("similarity(attraction.name, :query)", "DESC")
        // Secondary sort: if searching for land, show land matches
        .addOrderBy("similarity(attraction.landName, :query)", "DESC")
        .setParameter("exactQuery", query)
        .setParameter("startsWith", `${query}%`)
        .setParameter("normalizedQueryExact", normalizedQuery)
        .setParameter("query", query)
        .limit(limit)
        .getMany()
    );
  }

  /**
   * Search shows with fuzzy matching
   */
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
        | "countryCode"
        | "citySlug"
        | "continent"
        | "country"
        | "city"
        | "destination"
      >;
    })[]
  > {
    const normalizedQuery = query.replace(/[^a-zA-Z0-9]/g, "");

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
        "park.countryCode",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("show.name ILIKE :query", { query: `%${query}%` })
            .orWhere(
              "REGEXP_REPLACE(show.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
              { normalizedQuery: `%${normalizedQuery}%` },
            )
            .orWhere("similarity(show.name, :query) > 0.1")
            .orWhere("similarity(park.city, :query) > 0.2")
            .orWhere("similarity(park.country, :query) > 0.2");
        }),
      )
      .orderBy(
        `CASE
          WHEN LOWER(show.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(show.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(show.name) LIKE LOWER(:startsWith) THEN 2
          ELSE 3
        END`,
        "ASC",
      )
      .addOrderBy("similarity(show.name, :query)", "DESC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .setParameter("normalizedQueryExact", normalizedQuery)
      .setParameter("query", query)
      .limit(limit)
      .getMany();
  }

  /**
   * Search restaurants with fuzzy matching
   */
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
        | "countryCode"
        | "citySlug"
        | "continent"
        | "country"
        | "city"
        | "destination"
      >;
    })[]
  > {
    const normalizedQuery = query.replace(/[^a-zA-Z0-9]/g, "");

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
        "park.countryCode",
        "park.citySlug",
        "park.continent",
        "park.country",
        "park.city",
        "destination.id",
        "destination.name",
      ])
      .where(
        new Brackets((qb) => {
          qb.where("restaurant.name ILIKE :query", { query: `%${query}%` })
            .orWhere(
              "REGEXP_REPLACE(restaurant.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
              { normalizedQuery: `%${normalizedQuery}%` },
            )
            .orWhere("similarity(restaurant.name, :query) > 0.1")
            .orWhere("similarity(park.city, :query) > 0.2")
            .orWhere("similarity(park.country, :query) > 0.2");
        }),
      )
      .orderBy(
        `CASE
          WHEN LOWER(restaurant.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(restaurant.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(restaurant.name) LIKE LOWER(:startsWith) THEN 2
          ELSE 3
        END`,
        "ASC",
      )
      .addOrderBy("similarity(restaurant.name, :query)", "DESC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .setParameter("normalizedQueryExact", normalizedQuery)
      .setParameter("query", query)
      .limit(limit)
      .getMany();
  }

  /**
   * Enrich park results with status and load from cached analytics data
   */
  private async enrichParkResults(
    parks: Park[],
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
      latitude: park.latitude ? Number(park.latitude) : null,
      longitude: park.longitude ? Number(park.longitude) : null,
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
    attractions: Array<
      Pick<Attraction, "id" | "name" | "slug" | "landName"> & {
        park?: Pick<
          Park,
          | "id"
          | "name"
          | "slug"
          | "latitude"
          | "longitude"
          | "continent"
          | "country"
          | "countryCode"
          | "city"
          | "continentSlug"
          | "countrySlug"
          | "citySlug"
          | "destination"
        >;
      }
    >,
  ): Promise<SearchResultItemDto[]> {
    // 1. Batch fetch park statuses first (to filter out closed parks)
    const parkStatusMap = await this.getParkStatusMap(attractions);

    // 2. Identify attractions in OPERATING parks
    const operatingAttractionIds: string[] = [];
    attractions.forEach((attraction) => {
      const parkStatus = attraction.park
        ? parkStatusMap.get(attraction.park.id)
        : "CLOSED";
      if (parkStatus === "OPERATING") {
        operatingAttractionIds.push(attraction.id);
      }
    });

    // 3. Batch fetch wait times, status, and P90s ONLY for operating attractions
    let waitTimesMap = new Map<string, number>();
    let statusMap = new Map<string, { status: string }>();
    let p90Map = new Map<string, number>();

    if (operatingAttractionIds.length > 0) {
      const [waitTimes, statuses, p90s] = await Promise.all([
        this.getBatchWaitTimes(operatingAttractionIds),
        this.getBatchAttractionStatus(operatingAttractionIds),
        this.analyticsService.getBatchAttractionP90s(operatingAttractionIds),
      ]);
      waitTimesMap = waitTimes;
      statusMap = statuses;
      p90Map = p90s;
    }

    return attractions.map((attraction) => {
      const parkStatus = attraction.park
        ? parkStatusMap.get(attraction.park.id)
        : "CLOSED";
      const isParkOpen = parkStatus === "OPERATING";

      // If park is closed, force attraction status to CLOSED and wait time to null
      const status = isParkOpen
        ? statusMap.get(attraction.id)?.status
        : "CLOSED";

      const waitTime = isParkOpen
        ? waitTimesMap.get(attraction.id) || null
        : null;

      const p90 = isParkOpen ? p90Map.get(attraction.id) : undefined;

      const load = isParkOpen
        ? this.determineAttractionLoad(waitTime ?? undefined, p90)
        : null;

      return {
        type: "attraction" as const,
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
        url: attraction.park
          ? buildAttractionUrl(attraction.park, { slug: attraction.slug })
          : null,
        latitude: attraction.park?.latitude
          ? Number(attraction.park.latitude)
          : null,
        longitude: attraction.park?.longitude
          ? Number(attraction.park.longitude)
          : null,
        continent: attraction.park?.continent || null,
        country: attraction.park?.country || null,
        countryCode: attraction.park?.countryCode || null,
        city: attraction.park?.city || null,
        resort: attraction.park?.destination?.name || null,
        status:
          (status as
            | "OPERATING"
            | "CLOSED"
            | "DOWN"
            | "REFURBISHMENT"
            | null) || "CLOSED",
        load,
        waitTime,
        parentPark: attraction.park
          ? {
              id: attraction.park.id,
              name: attraction.park.name,
              slug: attraction.park.slug,
              url: buildParkUrl(attraction.park),
            }
          : null,
      };
    });
  }

  /**
   * Enrich show results with parent park info and show times
   */
  private async enrichShowResults(
    shows: Array<
      Pick<Show, "id" | "name" | "slug"> & {
        park?: Pick<
          Park,
          | "id"
          | "name"
          | "slug"
          | "latitude"
          | "longitude"
          | "continent"
          | "country"
          | "countryCode"
          | "city"
          | "continentSlug"
          | "countrySlug"
          | "citySlug"
          | "destination"
        >;
      }
    >,
  ): Promise<SearchResultItemDto[]> {
    // 1. Batch fetch park statuses first (to filter out closed parks)
    const parkStatusMap = await this.getParkStatusMap(shows);

    // 2. Identify shows in OPERATING parks
    const operatingShowIds: string[] = [];
    shows.forEach((show) => {
      const parkStatus = show.park ? parkStatusMap.get(show.park.id) : "CLOSED";
      if (parkStatus === "OPERATING") {
        operatingShowIds.push(show.id);
      }
    });

    // 3. Batch fetch show times ONLY for operating shows
    let showTimesMap = new Map<string, string[]>();
    if (operatingShowIds.length > 0) {
      showTimesMap = await this.getBatchShowTimes(operatingShowIds);
    }

    return shows.map((show) => {
      const parkStatus = show.park ? parkStatusMap.get(show.park.id) : "CLOSED";
      const isParkOpen = parkStatus === "OPERATING";

      return {
        type: "show" as const,
        id: show.id,
        name: show.name,
        slug: show.slug,
        url: null,
        latitude: show.park?.latitude ? Number(show.park.latitude) : null,
        longitude: show.park?.longitude ? Number(show.park.longitude) : null,
        continent: show.park?.continent || null,
        country: show.park?.country || null,
        countryCode: show.park?.countryCode || null,
        city: show.park?.city || null,
        resort: show.park?.destination?.name || null,
        showTimes: isParkOpen ? showTimesMap.get(show.id) || null : null,
        parentPark: show.park
          ? {
              id: show.park.id,
              name: show.park.name,
              slug: show.park.slug,
              url: buildParkUrl(show.park),
            }
          : null,
      };
    });
  }

  /**
   * Enrich restaurant results with parent park info
   */
  private async enrichRestaurantResults(
    restaurants: Array<
      Pick<Restaurant, "id" | "name" | "slug"> & {
        park?: Pick<
          Park,
          | "id"
          | "name"
          | "slug"
          | "latitude"
          | "longitude"
          | "continent"
          | "country"
          | "countryCode"
          | "city"
          | "continentSlug"
          | "countrySlug"
          | "citySlug"
          | "destination"
        >;
      }
    >,
  ): Promise<SearchResultItemDto[]> {
    return restaurants.map((restaurant) => ({
      type: "restaurant" as const,
      id: restaurant.id,
      name: restaurant.name,
      slug: restaurant.slug,
      url: null,
      latitude: restaurant.park?.latitude
        ? Number(restaurant.park.latitude)
        : null,
      longitude: restaurant.park?.longitude
        ? Number(restaurant.park.longitude)
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

    if (attractionIds.length === 0) {
      return waitTimesMap;
    }

    try {
      // Get current status for all attractions in one query
      const queueDataMap =
        await this.queueDataService.findCurrentStatusByAttractionIds(
          attractionIds,
        );

      // Process each attraction
      for (const [attractionId, queueDataList] of queueDataMap.entries()) {
        const standbyQueue = queueDataList.find(
          (q) => q.queueType === "STANDBY",
        );
        if (
          standbyQueue &&
          standbyQueue.waitTime !== null &&
          standbyQueue.waitTime !== undefined
        ) {
          waitTimesMap.set(attractionId, standbyQueue.waitTime);
        }
      }
    } catch {
      // Skip attractions without wait time data
    }

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
   * REFACTORED: Delegates to AnalyticsService for consistent logic
   */
  private determineAttractionLoad(
    waitTime: number | undefined,
    p90: number | undefined,
  ): CrowdLevel | null {
    return this.analyticsService.getAttractionCrowdLevel(waitTime, p90);
  }

  /**
   * Determine park crowd level from occupancy percentage
   * REFACTORED: Delegates to AnalyticsService for consistent logic
   */
  private determineParkCrowdLevel(occupancyPercentage: number): CrowdLevel {
    return this.analyticsService.getParkCrowdLevel(occupancyPercentage);
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
          // Ignore missing schedule
        }
      }),
    );

    return hoursMap;
  }

  /**
   * Batch fetch park load levels (occupancy)
   */
  private async getBatchLoadLevels(
    parkIds: string[],
  ): Promise<Map<string, CrowdLevel>> {
    const loadMap = new Map<string, CrowdLevel>();

    try {
      // Use batch fetch for efficiency and consistent logic
      const occupancyMap =
        await this.analyticsService.getBatchParkOccupancy(parkIds);

      for (const [parkId, occupancy] of occupancyMap.entries()) {
        if (occupancy) {
          // Map occupancy percentage to CrowdLevel directly
          const crowdLevel = this.determineParkCrowdLevel(occupancy.current);
          loadMap.set(parkId, crowdLevel);
        }
      }
    } catch {
      // Ignore errors
    }

    return loadMap;
  }

  /**
   * Batch fetch show times for operating shows
   */
  private async getBatchShowTimes(
    showIds: string[],
  ): Promise<Map<string, string[]>> {
    const showTimesMap = new Map<string, string[]>();

    await Promise.all(
      showIds.map(async (showId) => {
        try {
          const liveData =
            await this.showsService.findCurrentStatusByShow(showId);

          if (liveData && liveData.showtimes) {
            const times = liveData.showtimes
              .map((st) => st.startTime)
              .filter((t) => !!t) as string[];

            if (times.length > 0) {
              showTimesMap.set(showId, times);
            }
          }
        } catch {
          // Ignore errors
        }
      }),
    );

    return showTimesMap;
  }

  /**
   * Helper to get batch park status for a list of items with park property
   */
  private async getParkStatusMap(
    items: Array<{ park?: { id: string } | null }>,
  ): Promise<Map<string, string>> {
    const parkIds = new Set<string>();
    items.forEach((item) => {
      if (item.park && item.park.id) {
        parkIds.add(item.park.id);
      }
    });

    if (parkIds.size === 0) {
      return new Map();
    }

    return this.parksService.getBatchParkStatus(Array.from(parkIds));
  }
}
