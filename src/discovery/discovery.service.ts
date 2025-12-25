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
      let country = continent.countries.find(
        (c) => c.slug === park.countrySlug,
      );
      if (!country) {
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
   * Invalidate cache (for manual refresh or on data changes)
   */
  async invalidateCache(): Promise<void> {
    await this.redis.del(this.CACHE_KEY);
    this.logger.log("Geo structure cache invalidated");
  }
}
