import { Injectable, Inject, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Redis } from "ioredis";
import { Park } from "../parks/entities/park.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  GeoStructureDto,
  ContinentDto,
  CountryDto,
  CityDto,
  ParkReferenceDto,
  AttractionReferenceDto,
} from "./dto/geo-structure.dto";

/**
 * Discovery Service
 *
 * Builds hierarchical geographic structures for route generation.
 * Heavily cached (24h TTL) since structure changes infrequently.
 */
@Injectable()
export class DiscoveryService {
  private readonly logger = new Logger(DiscoveryService.name);
  private readonly CACHE_KEY = "discovery:geo:structure:v2"; // v2: includes attractions
  private readonly CACHE_TTL = 24 * 60 * 60; // 24 hours
  private readonly LIVE_STATS_CACHE_KEY = "discovery:live_stats:v1";
  private readonly LIVE_STATS_TTL = 5 * 60; // 5 minutes

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
  ) {}

  /**
   * Get complete geographic structure
   *
   * Returns hierarchical structure: continents → countries → cities → parks
   * Cached for 24 hours to improve performance.
   *
   * @returns Complete geo structure
   */
  async getGeoStructure(): Promise<GeoStructureDto> {
    // Try cache first
    const cached = await this.redis.get(this.CACHE_KEY);
    if (cached) {
      this.logger.debug("Returning cached geo structure");
      return JSON.parse(cached);
    }

    this.logger.log("Building geo structure from database");

    // Fetch all parks with complete geographic data and attractions
    const parks = await this.parkRepository.find({
      where: {
        continent: Not(IsNull()),
        country: Not(IsNull()),
        city: Not(IsNull()),
        continentSlug: Not(IsNull()),
        countrySlug: Not(IsNull()),
        citySlug: Not(IsNull()),
      },
      relations: ["attractions"],
      select: [
        "id",
        "name",
        "slug",
        "continent",
        "continentSlug",
        "country",
        "countrySlug",
        "countryCode",
        "city",
        "citySlug",
      ],
      order: {
        continent: "ASC",
        country: "ASC",
        city: "ASC",
        name: "ASC",
      },
    });

    // Build hierarchical structure
    const continentMap = new Map<string, ContinentDto>();

    for (const park of parks) {
      const continentKey = park.continentSlug;

      // Get or create continent
      let continent = continentMap.get(continentKey);
      if (!continent) {
        continent = {
          name: park.continent,
          slug: park.continentSlug,
          countries: [],
          countryCount: 0,
          parkCount: 0,
        };
        continentMap.set(continentKey, continent);
      }

      // Get or create country
      // Match by countryCode (primary) or Name (fallback)
      let country = continent.countries.find(
        (c) =>
          (park.countryCode && c.code === park.countryCode) ||
          c.name === park.country,
      );

      if (country) {
        // If we found a match, check if we should upgrade to better metadata
        // We prefer longer slugs (e.g. 'france' > 'fr')
        if (
          park.countrySlug &&
          country.slug &&
          park.countrySlug.length > country.slug.length
        ) {
          country.name = park.country;
          country.slug = park.countrySlug;
        }
        // Ensure code is populated if missing
        if (!country.code && park.countryCode) {
          country.code = park.countryCode;
        }
      } else {
        country = {
          name: park.country,
          slug: park.countrySlug,
          code: park.countryCode || "",
          cities: [],
          cityCount: 0,
          parkCount: 0,
        };
        continent.countries.push(country);
      }

      // Get or create city
      let city = country.cities.find((c) => c.slug === park.citySlug);
      if (!city) {
        city = {
          name: park.city,
          slug: park.citySlug,
          parks: [],
          parkCount: 0,
        };
        country.cities.push(city);
      }

      // Add park reference with attractions
      const parkBaseUrl = `/v1/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`;

      const attractions: AttractionReferenceDto[] = (park.attractions || [])
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((attraction) => ({
          id: attraction.id,
          name: attraction.name,
          slug: attraction.slug,
          url: `${parkBaseUrl}/${attraction.slug}`,
        }));

      const parkRef: ParkReferenceDto = {
        id: park.id,
        name: park.name,
        slug: park.slug,
        url: parkBaseUrl,
        attractions,
        attractionCount: attractions.length,
      };
      city.parks.push(parkRef);
      city.parkCount++;
      country.parkCount++;
      continent.parkCount++;
    }

    // Update counts
    const continents = Array.from(continentMap.values());
    for (const continent of continents) {
      continent.countryCount = continent.countries.length;
      for (const country of continent.countries) {
        country.cityCount = country.cities.length;
      }
    }

    // Build final response
    const totalAttractions = continents.reduce(
      (sum, continent) =>
        sum +
        continent.countries.reduce(
          (countrySum, country) =>
            countrySum +
            country.cities.reduce(
              (citySum, city) =>
                citySum +
                city.parks.reduce(
                  (parkSum, park) => parkSum + park.attractionCount,
                  0,
                ),
              0,
            ),
          0,
        ),
      0,
    );

    const structure: GeoStructureDto = {
      continents,
      continentCount: continents.length,
      countryCount: continents.reduce((sum, c) => sum + c.countries.length, 0),
      cityCount: continents.reduce(
        (sum, c) =>
          sum + c.countries.reduce((s, co) => s + co.cities.length, 0),
        0,
      ),
      parkCount: parks.length,
      attractionCount: totalAttractions,
      generatedAt: new Date().toISOString(),
    };

    // Merge live statistics
    const liveStats = await this.getLiveStats();

    for (const continent of structure.continents) {
      let continentOpenCount = 0;
      let continentTotalWait = 0;
      let continentWaitCount = 0;

      for (const country of continent.countries) {
        let countryOpenCount = 0;
        let countryTotalWait = 0;
        let countryWaitCount = 0;

        for (const city of country.cities) {
          let cityOpenCount = 0;
          let cityTotalWait = 0;
          let cityWaitCount = 0;

          for (const park of city.parks) {
            const stats = liveStats.get(park.id);
            if (stats?.isOpen) {
              cityOpenCount++;
              if (stats.avgWait > 0) {
                cityTotalWait += stats.avgWait;
                cityWaitCount++;
              }
            }
          }

          city.openParkCount = cityOpenCount;
          city.averageWaitTime =
            cityWaitCount > 0
              ? Math.round(cityTotalWait / cityWaitCount)
              : undefined;

          countryOpenCount += cityOpenCount;
          countryTotalWait += cityTotalWait;
          countryWaitCount += cityWaitCount;
        }

        country.openParkCount = countryOpenCount;
        country.averageWaitTime =
          countryWaitCount > 0
            ? Math.round(countryTotalWait / countryWaitCount)
            : undefined;

        continentOpenCount += countryOpenCount;
        continentTotalWait += continentTotalWait;
        continentWaitCount += countryWaitCount;
      }

      continent.openParkCount = continentOpenCount;
      continent.averageWaitTime =
        continentWaitCount > 0
          ? Math.round(continentTotalWait / continentWaitCount)
          : undefined;
    }

    // Cache the result
    await this.redis.setex(
      this.CACHE_KEY,
      this.CACHE_TTL,
      JSON.stringify(structure),
    );

    this.logger.log(
      `Built geo structure: ${structure.continentCount} continents, ${structure.countryCount} countries, ${structure.cityCount} cities, ${structure.parkCount} parks, ${structure.attractionCount} attractions`,
    );

    return structure;
  }

  /**
   * Get all continents
   *
   * @returns List of continents with basic info
   */
  async getContinents(): Promise<ContinentDto[]> {
    const structure = await this.getGeoStructure();
    return structure.continents;
  }

  /**
   * Get countries in a specific continent
   *
   * @param continentSlug - Continent slug
   * @returns List of countries or null if continent not found
   */
  async getCountriesInContinent(
    continentSlug: string,
  ): Promise<CountryDto[] | null> {
    const structure = await this.getGeoStructure();
    const continent = structure.continents.find(
      (c) => c.slug === continentSlug,
    );
    return continent ? continent.countries : null;
  }

  /**
   * Get cities in a specific country
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @returns List of cities or null if not found
   */
  async getCitiesInCountry(
    continentSlug: string,
    countrySlug: string,
  ): Promise<CityDto[] | null> {
    const structure = await this.getGeoStructure();
    const continent = structure.continents.find(
      (c) => c.slug === continentSlug,
    );
    if (!continent) return null;

    const country = continent.countries.find((c) => c.slug === countrySlug);
    return country ? country.cities : null;
  }

  /**
   * Get live park statistics (open counts + wait times)
   * Cached for 5 minutes, performant single query
   *
   * @returns Map of Park ID -> { isOpen, avgWait }
   */
  private async getLiveStats(): Promise<
    Map<string, { isOpen: boolean; avgWait: number }>
  > {
    const cached = await this.redis.get(this.LIVE_STATS_CACHE_KEY);
    if (cached) {
      this.logger.debug("Returning cached live stats");
      return new Map(JSON.parse(cached));
    }

    this.logger.log("Fetching live park statistics");

    // ONE performant query for all parks
    const result = await this.parkRepository.query(`
      WITH park_status AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      park_waits AS (
        SELECT 
          a."parkId",
          AVG(qd."waitTime") as avg_wait
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        WHERE qd.timestamp > NOW() - INTERVAL '20 minutes'
          AND qd.status = 'OPERATING'
          AND qd."waitTime" IS NOT NULL
        GROUP BY a."parkId"
      )
      SELECT 
        p.id,
        CASE WHEN ps."parkId" IS NOT NULL THEN true ELSE false END as is_open,
        COALESCE(pw.avg_wait, 0) as avg_wait
      FROM parks p
      LEFT JOIN park_status ps ON ps."parkId" = p.id
      LEFT JOIN park_waits pw ON pw."parkId" = p.id
    `);

    const stats = new Map<string, { isOpen: boolean; avgWait: number }>();
    for (const row of result) {
      stats.set(row.id, {
        isOpen: row.is_open,
        avgWait: Math.round(parseFloat(row.avg_wait || 0)),
      });
    }

    // Cache for 5 minutes
    await this.redis.setex(
      this.LIVE_STATS_CACHE_KEY,
      this.LIVE_STATS_TTL,
      JSON.stringify(Array.from(stats.entries())),
    );

    this.logger.log(
      `Fetched live stats for ${stats.size} parks (${Array.from(stats.values()).filter((s) => s.isOpen).length} open)`,
    );

    return stats;
  }

  /**
   * Invalidate cache (for manual refresh or on data changes)
   */
  async invalidateCache(): Promise<void> {
    await this.redis.del(this.CACHE_KEY);
    this.logger.log("Geo structure cache invalidated");
  }
}
