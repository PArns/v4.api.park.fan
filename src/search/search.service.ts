import { Injectable, Inject, OnModuleInit, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Brackets } from "typeorm";
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
import {
  buildParkUrl,
  buildAttractionUrl,
  buildCountryDiscoveryUrl,
  buildCityDiscoveryUrl,
} from "../common/utils/url.util";
import { getCurrentDateInTimezone } from "../common/utils/date.util";
import { cleanSlugSuffix } from "../common/utils/slug.util";
import { ParksService } from "../parks/parks.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { ShowsService } from "../shows/shows.service";
import { CrowdLevel } from "../common/types";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { SearchCounts } from "./types/search-counts.type";
import { PopularityService } from "../popularity/popularity.service";

@Injectable()
export class SearchService implements OnModuleInit {
  private readonly logger = new Logger(SearchService.name);
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
    private readonly popularityService: PopularityService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async onModuleInit() {
    // Initialize pg_trgm extension and indices for fuzzy search
    try {
      await this.parkRepository.query(
        "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
      );
      await this.parkRepository.query(
        "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;",
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
      await this.parkRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_park_name_dmetaphone ON parks (dmetaphone(name));",
      );
      await this.parkRepository
        .query(
          "CREATE INDEX IF NOT EXISTS idx_park_name_normalized ON parks USING gin (REGEXP_REPLACE(name, '[^a-zA-Z0-9]', '', 'g') gin_trgm_ops);",
        )
        .catch(() => {});

      // Attraction indices
      await this.attractionRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_attraction_name_trgm ON attractions USING gin (name gin_trgm_ops);",
      );
      await this.attractionRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_attraction_land_name_trgm ON attractions USING gin (land_name gin_trgm_ops);",
      );
      await this.attractionRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_attraction_name_dmetaphone ON attractions (dmetaphone(name));",
      );
      await this.attractionRepository
        .query(
          "CREATE INDEX IF NOT EXISTS idx_attraction_name_normalized ON attractions USING gin (REGEXP_REPLACE(name, '[^a-zA-Z0-9]', '', 'g') gin_trgm_ops);",
        )
        .catch(() => {});

      // Word similarity index for partial matches (e.g. "phantasia" matching "phantasialand")
      await this.attractionRepository
        .query(
          "CREATE INDEX IF NOT EXISTS idx_park_name_word_trgm ON parks USING gist (name gist_trgm_ops);",
        )
        .catch(() => {}); // gist might not be available depending on postgres version/extensions

      // Show indices
      await this.showRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_show_name_trgm ON shows USING gin (name gin_trgm_ops);",
      );

      // Restaurant indices
      await this.restaurantRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_restaurant_name_trgm ON restaurants USING gin (name gin_trgm_ops);",
      );

      // Park continent index (used in searchParks ILIKE and searchLocations)
      await this.parkRepository.query(
        "CREATE INDEX IF NOT EXISTS idx_park_continent_trgm ON parks USING gin (continent gin_trgm_ops);",
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
          location: { returned: 0, total: 0 },
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
        : ["park", "attraction", "show", "restaurant", "location"];

    // Step 1: Run all search queries in parallel
    const [rawParks, rawAttractions, rawShows, rawRestaurants, locations] =
      await Promise.all([
        searchTypes.includes("park")
          ? this.searchParks(q, limit)
          : Promise.resolve([] as Awaited<ReturnType<typeof this.searchParks>>),
        searchTypes.includes("attraction")
          ? this.searchAttractions(q, limit)
          : Promise.resolve(
              [] as Awaited<ReturnType<typeof this.searchAttractions>>,
            ),
        searchTypes.includes("show")
          ? this.searchShows(q, limit)
          : Promise.resolve([] as Awaited<ReturnType<typeof this.searchShows>>),
        searchTypes.includes("restaurant")
          ? this.searchRestaurants(q, limit)
          : Promise.resolve(
              [] as Awaited<ReturnType<typeof this.searchRestaurants>>,
            ),
        searchTypes.includes("location")
          ? this.searchLocations(q, limit)
          : Promise.resolve([] as SearchResultItemDto[]),
      ]);

    // Step 2: Enrich all result sets in parallel
    const [
      enrichedParks,
      enrichedAttractions,
      enrichedShows,
      enrichedRestaurants,
    ] = await Promise.all([
      this.enrichParkResults(rawParks as unknown as Park[]),
      this.enrichAttractionResults(rawAttractions),
      this.enrichShowResults(rawShows),
      this.enrichRestaurantResults(rawRestaurants),
    ]);

    // Step 3: Deduplicate results by name and parent park
    // This prevents showing the same attraction twice if it exists under different IDs (e.g. from multiple sources)
    const deduplicatedResults: SearchResultItemDto[] = [];
    const seen = new Set<string>();

    // Priority sort: OPERATING entities first, then others
    const allEnriched = [
      ...enrichedParks,
      ...enrichedAttractions,
      ...enrichedShows,
      ...enrichedRestaurants,
    ].sort((a, b) => {
      if (a.status === "OPERATING" && b.status !== "OPERATING") return -1;
      if (a.status !== "OPERATING" && b.status === "OPERATING") return 1;
      return 0;
    });

    for (const res of allEnriched) {
      // Key: name + parentParkId (if available) + type
      const parentId = res.parentPark?.id || res.id;
      const key = `${res.type}:${res.name.toLowerCase().trim()}:${parentId}`;

      if (!seen.has(key)) {
        seen.add(key);
        deduplicatedResults.push(res);
      }
    }

    const results: SearchResultItemDto[] = [
      ...deduplicatedResults,
      ...locations,
    ];
    const counts: SearchCounts = {
      park: {
        returned: deduplicatedResults.filter((r) => r.type === "park").length,
        total: rawParks.length,
      },
      attraction: {
        returned: deduplicatedResults.filter((r) => r.type === "attraction")
          .length,
        total: rawAttractions.length,
      },
      show: {
        returned: deduplicatedResults.filter((r) => r.type === "show").length,
        total: rawShows.length,
      },
      restaurant: {
        returned: deduplicatedResults.filter((r) => r.type === "restaurant")
          .length,
        total: rawRestaurants.length,
      },
      location: { returned: locations.length, total: locations.length },
    };

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
    const topParkIds = await this.popularityService.getTopParks(20);

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
            // All clauses below must be backed by an index to avoid a full
            // table scan when combined via OR. Trigram operators (% and <%)
            // and ILIKE all use the pg_trgm GIN indexes; pg_trgm is
            // case-insensitive, so LOWER() wrapping is removed (it would
            // defeat the index built on the raw column).
            qb.where("park.name ILIKE :likeQuery", { likeQuery: `%${query}%` })
              .orWhere("park.city ILIKE :likeQuery")
              .orWhere("park.country ILIKE :likeQuery")
              .orWhere("park.continent ILIKE :likeQuery")
              // Normalized Match (fixes "fly" -> "F.L.Y.") — uses functional
              // GIN trgm index on REGEXP_REPLACE(name, ...)
              .orWhere(
                "REGEXP_REPLACE(park.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
                { normalizedQuery: `%${normalizedQuery}%` },
              )
              // Phonetic Match — uses functional B-tree index on dmetaphone(name)
              .orWhere("dmetaphone(park.name) = dmetaphone(:query)")
              .orWhere("dmetaphone(park.city) = dmetaphone(:query)")
              .orWhere("dmetaphone(park.country) = dmetaphone(:query)")
              // Trigram similarity / word similarity — both use the GIN trgm
              // index. These replace the previous unindexable levenshtein()
              // and similarity()>X checks that forced a sequential scan.
              .orWhere("park.name % :query")
              .orWhere(":query <% park.name");
          }),
        )
        .orderBy(
          // Priority Ranking (Lower is better):
          // 0: Exact Name Match
          // 1: Normalized Exact Match (e.g. "fly" matches "F.L.Y.")
          // 2: Prefix Name Match (e.g. "Phan" matches "Phantasialand")
          // 3: Popularity Boost
          // 4: City Match
          // 5: Others
          `CASE
          WHEN LOWER(park.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(park.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(park.name) LIKE LOWER(:startsWith) THEN 2
          WHEN park.id IN (:...topIds) THEN 3
          WHEN LOWER(park.city) = LOWER(:exactQuery) THEN 4
          ELSE 5
        END`,
          "ASC",
        )
        // Secondary sort by similarity
        .addOrderBy("similarity(LOWER(park.name), LOWER(:query))", "DESC")
        .setParameter("exactQuery", query)
        .setParameter("startsWith", `${query}%`)
        .setParameter("normalizedQueryExact", normalizedQuery)
        .setParameter("query", query)
        .setParameter(
          "topIds",
          topParkIds.length > 0
            ? topParkIds
            : ["00000000-0000-0000-0000-000000000000"],
        )
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
    const topAttractionIds = await this.popularityService.getTopAttractions(50);

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
            // Index-only WHERE: every clause must hit a trigram, dmetaphone
            // or functional index so the OR chain does not collapse to a
            // sequential scan. levenshtein() and similarity()>X were removed
            // for that reason — trigram % handles typo tolerance.
            qb.where("attraction.name ILIKE :likeQuery", {
              likeQuery: `%${query}%`,
            })
              .orWhere("attraction.landName ILIKE :likeQuery")
              .orWhere(
                "REGEXP_REPLACE(attraction.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
                { normalizedQuery: `%${normalizedQuery}%` },
              )
              .orWhere("dmetaphone(attraction.name) = dmetaphone(:query)")
              .orWhere("attraction.name % :query")
              .orWhere(":query <% attraction.name")
              .orWhere("park.city % :query")
              .orWhere("park.country % :query");
          }),
        )
        .orderBy(
          `CASE
            WHEN LOWER(attraction.name) = LOWER(:exactQuery) THEN 0
            WHEN REGEXP_REPLACE(attraction.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
            WHEN LOWER(attraction.name) LIKE LOWER(:startsWith) THEN 2
            WHEN attraction.id IN (:...topIds) THEN 3
            WHEN dmetaphone(attraction.name) = dmetaphone(:query) THEN 4
            ELSE 5
          END`,
          "ASC",
        )
        .addOrderBy("similarity(LOWER(attraction.name), LOWER(:query))", "DESC")
        // Secondary sort: if searching for land, show land matches
        .addOrderBy(
          "similarity(LOWER(attraction.landName), LOWER(:query))",
          "DESC",
        )
        .setParameter("exactQuery", query)
        .setParameter("startsWith", `${query}%`)
        .setParameter("normalizedQueryExact", normalizedQuery)
        .setParameter("query", query)
        .setParameter(
          "topIds",
          topAttractionIds.length > 0
            ? topAttractionIds
            : ["00000000-0000-0000-0000-000000000000"],
        )
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
          // Index-only WHERE — see searchParks comment.
          qb.where("show.name ILIKE :likeQuery", {
            likeQuery: `%${query}%`,
          })
            .orWhere(
              "REGEXP_REPLACE(show.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
              { normalizedQuery: `%${normalizedQuery}%` },
            )
            .orWhere("dmetaphone(show.name) = dmetaphone(:query)")
            .orWhere("dmetaphone(park.city) = dmetaphone(:query)")
            .orWhere("dmetaphone(park.country) = dmetaphone(:query)")
            .orWhere("show.name % :query")
            .orWhere(":query <% show.name")
            .orWhere("park.city % :query")
            .orWhere("park.country % :query");
        }),
      )
      .orderBy(
        `CASE
          WHEN LOWER(show.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(show.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(show.name) LIKE LOWER(:startsWith) THEN 2
          WHEN dmetaphone(show.name) = dmetaphone(:query) THEN 3
          ELSE 4
        END`,
        "ASC",
      )
      .addOrderBy("similarity(LOWER(show.name), LOWER(:query))", "DESC")
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
          // Index-only WHERE — see searchParks comment.
          qb.where("restaurant.name ILIKE :likeQuery", {
            likeQuery: `%${query}%`,
          })
            .orWhere(
              "REGEXP_REPLACE(restaurant.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQuery",
              { normalizedQuery: `%${normalizedQuery}%` },
            )
            .orWhere("dmetaphone(restaurant.name) = dmetaphone(:query)")
            .orWhere("dmetaphone(park.city) = dmetaphone(:query)")
            .orWhere("dmetaphone(park.country) = dmetaphone(:query)")
            .orWhere("restaurant.name % :query")
            .orWhere(":query <% restaurant.name")
            .orWhere("park.city % :query")
            .orWhere("park.country % :query");
        }),
      )
      .orderBy(
        `CASE
          WHEN LOWER(restaurant.name) = LOWER(:exactQuery) THEN 0
          WHEN REGEXP_REPLACE(restaurant.name, '[^a-zA-Z0-9]', '', 'g') ILIKE :normalizedQueryExact THEN 1
          WHEN LOWER(restaurant.name) LIKE LOWER(:startsWith) THEN 2
          WHEN dmetaphone(restaurant.name) = dmetaphone(:query) THEN 3
          ELSE 4
        END`,
        "ASC",
      )
      .addOrderBy("similarity(LOWER(restaurant.name), LOWER(:query))", "DESC")
      .setParameter("exactQuery", query)
      .setParameter("startsWith", `${query}%`)
      .setParameter("normalizedQueryExact", normalizedQuery)
      .setParameter("query", query)
      .limit(limit)
      .getMany();
  }

  /**
   * Search distinct cities and countries
   */
  private async searchLocations(
    query: string,
    limit: number,
  ): Promise<SearchResultItemDto[]> {
    // We search for parks that match the location query, then aggregate distinct locations
    // This is a bit inefficient but leverages existing indices on parks table
    // A better approach would be to have a materialized view of locations, but distinct query works for now.

    const properties = [
      "park.continent",
      "park.continentSlug",
      "park.country",
      "park.countrySlug",
      "park.countryCode",
      "park.city",
      "park.citySlug",
    ];

    const parks = await this.parkRepository
      .createQueryBuilder("park")
      .select(properties)
      .distinct(true) // Ensure distinct rows
      .where(
        new Brackets((qb) => {
          // Index-only WHERE — see searchParks comment.
          qb.where("park.city ILIKE :likeQuery", {
            likeQuery: `%${query}%`,
          })
            .orWhere("dmetaphone(park.city) = dmetaphone(:query)")
            .orWhere("park.city % :query")
            .orWhere("park.country ILIKE :likeQuery")
            .orWhere("dmetaphone(park.country) = dmetaphone(:query)")
            .orWhere("park.country % :query");
        }),
      )
      .setParameter("query", query)
      .limit(limit * 2) // Fetch more to allow for filtering after distinct logic if needed
      .getRawMany();

    // Map to result items and deduplicate based on unique location (City+Country or just Country)
    const results: SearchResultItemDto[] = [];
    const seenLocations = new Set<string>();

    for (const p of parks) {
      // Check City Match
      // Ideally we should score them, but for now if it matches query or is similar, we check
      // For simplicity, we just return the valid locations found in the returned rows

      // Process City
      if (p.park_city && p.park_citySlug) {
        // Simple client-side re-check to see if this city actually matches (since OR condition returns rows matching either city OR country)
        const cityMatch =
          p.park_city.toLowerCase().includes(query.toLowerCase()) ||
          this.isSimilar(p.park_city, query);

        if (cityMatch) {
          const key = `city:${p.park_citySlug}`;
          if (!seenLocations.has(key)) {
            seenLocations.add(key);
            results.push({
              type: "location",
              id: key,
              name: p.park_city,
              slug: p.park_citySlug,
              url: buildCityDiscoveryUrl(
                p.park_continentSlug,
                p.park_countrySlug,
                p.park_citySlug,
              ),
              continent: p.park_continent,
              country: p.park_country,
              countryCode: p.park_countryCode,
              city: p.park_city,
              status: null,
              load: null,
            });
          }
        }
      }

      // Process Country
      if (p.park_country && p.park_countrySlug) {
        const countryMatch =
          p.park_country.toLowerCase().includes(query.toLowerCase()) ||
          this.isSimilar(p.park_country, query);

        if (countryMatch) {
          const key = `country:${p.park_countrySlug}`;
          if (!seenLocations.has(key)) {
            seenLocations.add(key);
            results.push({
              type: "location",
              id: key,
              name: p.park_country,
              slug: p.park_countrySlug,
              url: buildCountryDiscoveryUrl(
                p.park_continentSlug,
                p.park_countrySlug,
              ),
              continent: p.park_continent,
              country: p.park_country,
              countryCode: p.park_countryCode,
              status: null,
              load: null,
            });
          }
        }
      }
    }

    return results.slice(0, limit);
  }

  /**
   * Helper for in-memory Similarity check (since we select many rows, we verify which field matched)
   */
  private isSimilar(text: string, query: string): boolean {
    if (!text) return false;
    // We can't easily access pg_trgm functions here without DB query, so we use a simple JS check
    // or rely on the fact that the DB returned it.
    // Given the DB query: WHERE city LIKE ... OR country LIKE ...
    // If a row is returned, ONE of them matched.
    // If we want to know WHICH one, strictly speaking we should check.
    // For now, simple includes is OK, or we assume if it's in the row it's a candidate.
    // Let's rely on simple includes for now to distinguish if it was the city or country that matched.
    return text.toLowerCase().includes(query.toLowerCase());
  }

  /**
   * Enrich park results with status and load from cached analytics data
   */
  private async enrichParkResults(
    parks: Park[],
  ): Promise<SearchResultItemDto[]> {
    const parkIds = parks.map((p) => p.id);

    // Park status comes from a LATERAL subquery over every attraction of
    // every park (parks.service.ts:getBatchParkStatus). It is identical
    // for every search hitting the same parks within a short window, so
    // we cache the result in Redis for 60s.
    const statusMap = await this.getCachedParkStatusMap(parkIds);

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

    // 3. Batch fetch queue data once (single roundtrip yields both wait time
    //    and status — previously we fetched the same data twice in parallel).
    //    P90 is intentionally NOT fetched here: it runs a PERCENTILE_CONT over
    //    548 days of queue_data and dominates the request on cache miss. P50
    //    is pre-baked in a baseline table and covers the same baseline role;
    //    attractions without a P50 fall through to the "moderate" default.
    let waitTimesMap = new Map<string, number>();
    let statusMap = new Map<string, { status: string }>();
    let p50Map = new Map<string, number>();

    if (operatingAttractionIds.length > 0) {
      const [queueDataMap, p50s] = await Promise.all([
        this.queueDataService.findCurrentStatusByAttractionIds(
          operatingAttractionIds,
        ),
        this.analyticsService.getBatchAttractionP50s(operatingAttractionIds),
      ]);

      for (const [attractionId, queueDataList] of queueDataMap.entries()) {
        if (queueDataList.length === 0) continue;
        if (queueDataList[0].status) {
          statusMap.set(attractionId, { status: queueDataList[0].status });
        }
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

      p50Map = p50s;
    }

    return attractions.map((attraction) => {
      const parkStatus = attraction.park
        ? parkStatusMap.get(attraction.park.id)
        : "CLOSED";
      const isParkOpen = parkStatus === "OPERATING";

      const status = isParkOpen
        ? statusMap.get(attraction.id)?.status
        : "CLOSED";

      const waitTime = isParkOpen
        ? waitTimesMap.get(attraction.id) || null
        : null;

      // P50 only (pre-baked baseline). P90 fallback is omitted on the search
      // path for latency reasons — missing P50 falls through to "moderate".
      const baseline = isParkOpen ? p50Map.get(attraction.id) : undefined;

      const load = isParkOpen
        ? this.getCrowdLevelForSearch(waitTime ?? undefined, baseline)
        : null;

      return {
        type: "attraction" as const,
        id: attraction.id,
        name: attraction.name,
        slug: cleanSlugSuffix(attraction.slug),
        url: attraction.park
          ? buildAttractionUrl(attraction.park, {
              slug: cleanSlugSuffix(attraction.slug),
            })
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
        waitTime:
          waitTime !== null && waitTime !== undefined
            ? roundToNearest5Minutes(waitTime)
            : null,
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
   * Determine attraction load level from wait time
   * REFACTORED: Delegates to AnalyticsService for consistent logic (P50 baseline when available).
   */
  private getCrowdLevelForSearch(
    waitTime: number | undefined,
    baseline: number | undefined,
  ): CrowdLevel | null {
    const level = this.analyticsService.getAttractionCrowdLevel(
      waitTime,
      baseline,
    );
    return level || "moderate";
  }

  /**
   * Determine park crowd level from occupancy percentage
   * REFACTORED: Delegates to AnalyticsService for consistent logic
   */
  private determineParkCrowdLevel(occupancyPercentage: number): CrowdLevel {
    return this.analyticsService.getParkCrowdLevel(occupancyPercentage);
  }

  /**
   * Batch fetch today's operating hours for parks.
   * Uses each park's timezone for "today" (never server date).
   * Single bulk query instead of N individual queries.
   */
  private async getBatchParkHours(
    parkIds: string[],
  ): Promise<Map<string, { open: string; close: string; type: string }>> {
    const hoursMap = new Map<
      string,
      { open: string; close: string; type: string }
    >();

    if (parkIds.length === 0) return hoursMap;

    const parks = await this.parkRepository.find({
      where: parkIds.map((id) => ({ id })),
      select: ["id", "timezone"],
    });

    // Build map of parkId -> today's date string
    const parkDateMap = new Map<string, string>();
    for (const park of parks) {
      parkDateMap.set(
        park.id,
        getCurrentDateInTimezone(park.timezone || "UTC"),
      );
    }

    // Single query using CASE to handle different "today" dates per park
    try {
      const qb = this.scheduleRepository
        .createQueryBuilder("s")
        .select([
          "s.parkId",
          "s.openingTime",
          "s.closingTime",
          "s.scheduleType",
        ])
        .where("s.parkId IN (:...parkIds)", { parkIds })
        .andWhere("s.scheduleType = :type", { type: ScheduleType.OPERATING });

      // Add a dynamic where condition for each park's date
      // For performance with many parks, we group by date
      const byDate = new Map<string, string[]>();
      for (const [id, date] of parkDateMap.entries()) {
        if (!byDate.has(date)) byDate.set(date, []);
        byDate.get(date)!.push(id);
      }

      qb.andWhere(
        new Brackets((inner) => {
          for (const [date, ids] of byDate.entries()) {
            inner.orWhere(
              new Brackets((inner2) => {
                inner2
                  .where("s.date = :date_" + date.replace(/-/g, ""), {
                    ["date_" + date.replace(/-/g, "")]: date,
                  })
                  .andWhere(
                    "s.parkId IN (:...ids_" + date.replace(/-/g, "") + ")",
                    {
                      ["ids_" + date.replace(/-/g, "")]: ids,
                    },
                  );
              }),
            );
          }
        }),
      );

      const schedules = await qb.getMany();

      for (const schedule of schedules) {
        if (schedule.openingTime && schedule.closingTime) {
          hoursMap.set(schedule.parkId, {
            open: schedule.openingTime.toISOString(),
            close: schedule.closingTime.toISOString(),
            type: schedule.scheduleType,
          });
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to fetch batch park hours: ${error}`);
    }

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
   * Batch fetch show times for operating shows.
   * Uses findBatchCurrentStatusByShows (single DISTINCT ON query) instead of N individual queries.
   */
  private async getBatchShowTimes(
    showIds: string[],
  ): Promise<Map<string, string[]>> {
    const showTimesMap = new Map<string, string[]>();

    if (showIds.length === 0) return showTimesMap;

    try {
      const liveDataMap =
        await this.showsService.findBatchCurrentStatusByShows(showIds);

      for (const [showId, liveData] of liveDataMap.entries()) {
        if (liveData && liveData.showtimes) {
          const times = liveData.showtimes
            .map((st) => st.startTime)
            .filter((t) => !!t) as string[];

          if (times.length > 0) {
            showTimesMap.set(showId, times);
          }
        }
      }
    } catch {
      // Ignore errors
    }

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

    return this.getCachedParkStatusMap(Array.from(parkIds));
  }

  /**
   * Cached wrapper around parksService.getBatchParkStatus(). The underlying
   * query runs a LATERAL subquery over every attraction of every requested
   * park; on the search hot path we hit it repeatedly with overlapping park
   * sets. Per-park Redis cache (60s) collapses these to one DB call per
   * minute per park.
   */
  private async getCachedParkStatusMap(
    parkIds: string[],
  ): Promise<Map<string, "OPERATING" | "CLOSED">> {
    const result = new Map<string, "OPERATING" | "CLOSED">();
    if (parkIds.length === 0) return result;

    const cacheKeys = parkIds.map((id) => `search:parkstatus:${id}`);
    const cached = await this.redis.mget(...cacheKeys);

    const uncachedIds: string[] = [];
    parkIds.forEach((id, i) => {
      const v = cached[i];
      if (v === "OPERATING" || v === "CLOSED") {
        result.set(id, v);
      } else {
        uncachedIds.push(id);
      }
    });

    if (uncachedIds.length === 0) return result;

    const fresh = await this.parksService.getBatchParkStatus(uncachedIds);
    const pipeline = this.redis.pipeline();
    for (const id of uncachedIds) {
      const status = fresh.get(id) || "CLOSED";
      result.set(id, status);
      pipeline.set(`search:parkstatus:${id}`, status, "EX", 60);
    }
    await pipeline.exec();

    return result;
  }

  /**
   * Pre-warm search cache using actual park names from the DB.
   * Called from CacheWarmupService after wait-times sync.
   * Extracts unique first words from all park names so the most common
   * user queries (typing "disney", "universal", etc.) hit cache.
   */
  async warmupSearch(): Promise<void> {
    try {
      const [parks, popularParkIds] = await Promise.all([
        this.parkRepository.find({ select: ["id", "name", "city", "country"] }),
        this.popularityService.getTopParks(30),
      ]);

      const termSet = new Set<string>();

      // 1. Add common global terms
      const commonTerms = [
        "disney",
        "universal",
        "six flags",
        "europa",
        "phantasia",
        "efteling",
        "cedar",
        "alton",
        "thorpe",
        "walibi",
        "gardaland",
        "portaventura",
        "knott",
        "busch",
        "seaworld",
        "lego",
        "merlin",
      ];
      commonTerms.forEach((t) => termSet.add(t));

      // 2. Add prefixes for popular parks (search-as-you-type support)
      const popularSet = new Set(popularParkIds);
      const prioritizedParks = parks.filter((p) => popularSet.has(p.id));

      for (const park of prioritizedParks) {
        const name = park.name.toLowerCase().replace(/[^a-z0-9 ]/g, "");
        const words = name.split(" ");

        // Warm first 3 and 4 chars of the name and individual words
        words.forEach((word) => {
          if (word.length >= 3) termSet.add(word.substring(0, 3));
          if (word.length >= 4) termSet.add(word.substring(0, 4));
          if (word.length >= 3) termSet.add(word);
        });
      }

      const terms = Array.from(termSet);
      this.logger.verbose(
        `🔥 Warming search cache for ${terms.length} prioritized terms...`,
      );

      // Process in small batches to avoid DB/Redis spikes
      const batchSize = 10;
      for (let i = 0; i < terms.length; i += batchSize) {
        const batch = terms.slice(i, i + batchSize);
        await Promise.all(
          batch.map((term) =>
            this.search({ q: term, limit: 10 } as any).catch(() => null),
          ),
        );
        // Small pause between batches
        if (i + batchSize < terms.length) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }

      this.logger.log(`✅ Search cache warmed for ${terms.length} terms`);
    } catch (error) {
      this.logger.warn("Search warmup failed", error);
    }
  }
}
