import { Injectable, Logger, Inject } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import {
  GoogleGeocodingResponse,
  GeographicData,
  GeocodingResult,
} from "./google-geocoding.types";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Google Geocoding API Client
 *
 * Performs reverse geocoding to get city, country, and continent from lat/lng coordinates.
 * Uses the locality type to get major cities instead of small localities.
 *
 * API Docs: https://developers.google.com/maps/documentation/geocoding/requests-reverse-geocoding
 * Free Tier: 10,000 requests/month
 */
@Injectable()
export class GoogleGeocodingClient {
  private readonly logger = new Logger(GoogleGeocodingClient.name);
  private readonly httpClient: AxiosInstance;
  private readonly apiKey: string;
  private readonly baseUrl =
    "https://maps.googleapis.com/maps/api/geocode/json";

  // Very long TTL for geocoding - coordinates NEVER change
  // 90 days - minimizes expensive Google API calls
  private readonly TTL_GEOCODING = 90 * 24 * 60 * 60; // 90 days

  constructor(
    private readonly configService: ConfigService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    this.apiKey =
      this.configService.get<string>("GOOGLE_API_KEY") ||
      this.configService.get<string>("GEOCODING_API_KEY") ||
      "";

    if (!this.apiKey) {
      this.logger.warn(
        "GOOGLE_API_KEY (or GEOCODING_API_KEY) not found in environment variables. Geocoding will fail.",
      );
    }

    this.httpClient = axios.create({
      timeout: 10000, // 10 seconds
    });
  }

  /**
   * Reverse geocode coordinates to get geographic data
   *
   * IMPORTANT: Aggressively cached for 90 days to minimize Google API costs!
   * Coordinates never change, so we can cache indefinitely.
   *
   * @param latitude - Latitude coordinate
   * @param longitude - Longitude coordinate
   * @returns Geographic data (continent, country, city) or null if not found
   */
  async reverseGeocode(
    latitude: number,
    longitude: number,
  ): Promise<GeographicData | null> {
    // Create cache key from coordinates (rounded to 6 decimals for precision)
    const latRounded = latitude.toFixed(6);
    const lngRounded = longitude.toFixed(6);
    const cacheKey = `geocoding:${latRounded}:${lngRounded}`;

    // Try cache first - ALWAYS check cache before calling Google API!
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      const data = JSON.parse(cached) as GeographicData;

      // Smart Cache Check:
      // ONLY re-fetch if ESSENTIAL fields are missing (countryCode, city, country, continent)
      // Region/regionCode are OPTIONAL - not all countries have them
      // This prevents expensive API re-calls for parks where region data doesn't exist
      const hasEssentialFields =
        data.countryCode && data.city && data.country && data.continent;

      if (hasEssentialFields) {
        // Cache is good enough - return it
        return data;
      }

      // Missing essential fields - need to upgrade
      this.logger.warn(
        `Cache missing essential fields for: ${latRounded}, ${lngRounded} - upgrading`,
      );
    }

    // Cache miss or upgrade needed - call Google API (costs money!)
    this.logger.log(
      `Calling Google Geocoding API for: ${latRounded}, ${lngRounded}`,
    );
    const result = await this.reverseGeocodeWithRetry(latitude, longitude);

    // Cache the result (even if null) to avoid repeated failed attempts
    if (result) {
      await this.redis.set(
        cacheKey,
        JSON.stringify(result),
        "EX",
        this.TTL_GEOCODING,
      );
    } else {
      // Cache null results for 7 days to avoid repeated API calls for bad coordinates
      await this.redis.set(
        cacheKey,
        JSON.stringify(null),
        "EX",
        7 * 24 * 60 * 60, // 7 days for failed lookups
      );
      this.logger.debug(
        `Cached null result for 7 days: ${latRounded}, ${lngRounded}`,
      );
    }

    return result;
  }

  private async reverseGeocodeWithRetry(
    latitude: number,
    longitude: number,
    attempt = 0,
  ): Promise<GeographicData | null> {
    try {
      const response = await this.httpClient.get<GoogleGeocodingResponse>(
        this.baseUrl,
        {
          params: {
            latlng: `${latitude},${longitude}`,
            key: this.apiKey,
            language: "en",
          },
        },
      );

      // Handle Google API specific status codes
      if (response.data.status === "OVER_QUERY_LIMIT") {
        if (attempt >= 5) {
          this.logger.error(
            `Google Geocoding API rate limit exceeded after 5 attempts.`,
          );
          return null;
        }

        const delay = 1000 * Math.pow(2, attempt);
        this.logger.warn(
          `Google Geocoding API rate limit hit (OVER_QUERY_LIMIT). Retrying in ${delay}ms (Attempt ${
            attempt + 1
          }/5)`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.reverseGeocodeWithRetry(latitude, longitude, attempt + 1);
      }

      if (response.data.status !== "OK") {
        this.logger.error(
          `Google Geocoding API error: ${response.data.status} - ${
            response.data.error_message || "Unknown error"
          }`,
        );
        return null;
      }

      if (!response.data.results || response.data.results.length === 0) {
        this.logger.warn("No results returned from Google Geocoding API");
        return null;
      }

      // Extract geographic data from results
      const geodata = this.extractGeographicData(response.data.results);

      if (!geodata) {
        this.logger.warn(
          `Could not extract geographic data for coordinates: ${latitude}, ${longitude}`,
        );
        return null;
      }

      return geodata;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        // Handle HTTP 429
        if (error.response?.status === 429) {
          if (attempt >= 5) {
            this.logger.error(`HTTP 429 Rate limit exceeded after 5 attempts.`);
            return null;
          }
          const delay = 1000 * Math.pow(2, attempt);
          this.logger.warn(
            `HTTP 429 Rate limit hit. Retrying in ${delay}ms (Attempt ${
              attempt + 1
            }/5)`,
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          return this.reverseGeocodeWithRetry(latitude, longitude, attempt + 1);
        }

        this.logger.error(
          `HTTP error during geocoding: ${error.message}`,
          error.stack,
        );
      } else {
        this.logger.error(
          `Unexpected error during geocoding: ${error}`,
          error instanceof Error ? error.stack : undefined,
        );
      }
      return null;
    }
  }

  /**
   * Extract geographic data from Google Geocoding API results
   * Includes region/state extraction for holiday filtering
   */
  private extractGeographicData(
    results: GeocodingResult[],
  ): GeographicData | null {
    let city: string | null = null;
    let country: string | null = null;
    let countryCode: string | null = null;
    let region: string | null = null;
    let regionCode: string | null = null;

    // ... (city extraction logic same as before) ...
    // Known metropolitan areas that should override specific locality names
    const knownMetroAreas = [
      "Tokyo",
      "Osaka",
      "Greater London",
      "Greater Paris",
      "Greater Los Angeles",
      "Greater New York",
      "San Francisco Bay Area",
    ];

    // First, check for major metropolitan areas (administrative_area_level_2)
    for (const result of results) {
      const metroComponent = result.address_components.find((component) =>
        component.types.includes("administrative_area_level_2"),
      );
      if (
        metroComponent &&
        knownMetroAreas.some((metro) =>
          metroComponent.long_name.includes(metro),
        )
      ) {
        const matchedMetro = knownMetroAreas.find((metro) =>
          metroComponent.long_name.includes(metro),
        );
        if (matchedMetro) {
          city = matchedMetro;
          break;
        }
      }
    }

    // If no metro area found, try locality (city)
    if (!city) {
      const localityResult = results.find((result) =>
        result.types.includes("locality"),
      );

      if (localityResult) {
        const localityComponent = localityResult.address_components.find(
          (component) => component.types.includes("locality"),
        );
        if (localityComponent) {
          city = localityComponent.long_name;
        }
      }
    }

    // If no locality found, try administrative_area_level_3 or sublocality
    if (!city) {
      for (const result of results) {
        const fallbackComponent = result.address_components.find(
          (component) =>
            component.types.includes("administrative_area_level_3") ||
            component.types.includes("sublocality") ||
            component.types.includes("postal_town"),
        );
        if (fallbackComponent) {
          city = fallbackComponent.long_name;
          break;
        }
      }
    }

    // Extract Country & Region from the most specific result containing them
    for (const result of results) {
      // Extract Country
      if (!country) {
        const countryComponent = result.address_components.find((component) =>
          component.types.includes("country"),
        );
        if (countryComponent) {
          country = countryComponent.long_name;
          countryCode = countryComponent.short_name; // ISO 3166-1 alpha-2
        }
      }

      // Extract Region (State/Province/Bundesland)
      if (!region) {
        const regionComponent = result.address_components.find((component) =>
          component.types.includes("administrative_area_level_1"),
        );
        if (regionComponent) {
          region = regionComponent.long_name; // e.g. "Florida"
          regionCode = regionComponent.short_name; // e.g. "FL"
        }
      }

      if (country && region) break;
    }

    // Validate we have essential data
    if (!city || !country || !countryCode) {
      this.logger.warn(
        `Incomplete geographic data: city=${city}, country=${country}, code=${countryCode}`,
      );
      return null;
    }

    // Map country to continent
    const continent = this.mapCountryToContinent(country);

    if (!continent) {
      this.logger.warn(`Could not map country to continent: ${country}`);
      return null;
    }

    return {
      city,
      country,
      countryCode,
      continent,
      region: region || undefined,
      regionCode: regionCode || undefined,
    };
  }

  /**
   * Map country name to continent
   *
   * This is a comprehensive mapping of countries to continents.
   * Google Geocoding API does not provide continent information directly.
   *
   * @param country - Country name (long_name from Google API)
   * @returns Continent name or null if not found
   */
  private mapCountryToContinent(country: string): string | null {
    const countryToContinent: Record<string, string> = {
      // North America
      "United States": "North America",
      "United States of America": "North America",
      Canada: "North America",
      Mexico: "North America",
      "Costa Rica": "North America",
      Panama: "North America",
      Guatemala: "North America",
      Honduras: "North America",
      "El Salvador": "North America",
      Nicaragua: "North America",
      Belize: "North America",
      Jamaica: "North America",
      "Dominican Republic": "North America",
      "Puerto Rico": "North America",

      // South America
      Brazil: "South America",
      Argentina: "South America",
      Chile: "South America",
      Colombia: "South America",
      Peru: "South America",
      Venezuela: "South America",
      Ecuador: "South America",
      Bolivia: "South America",
      Paraguay: "South America",
      Uruguay: "South America",

      // Europe
      France: "Europe",
      Germany: "Europe",
      Italy: "Europe",
      Spain: "Europe",
      "United Kingdom": "Europe",
      Netherlands: "Europe",
      Belgium: "Europe",
      Switzerland: "Europe",
      Austria: "Europe",
      Sweden: "Europe",
      Norway: "Europe",
      Denmark: "Europe",
      Finland: "Europe",
      Poland: "Europe",
      Portugal: "Europe",
      Greece: "Europe",
      Ireland: "Europe",
      Russia: "Europe", // Western Russia is considered Europe
      "Czech Republic": "Europe",
      Hungary: "Europe",
      Romania: "Europe",
      Ukraine: "Europe",

      // Asia
      China: "Asia",
      Japan: "Asia",
      "South Korea": "Asia",
      India: "Asia",
      Thailand: "Asia",
      Singapore: "Asia",
      Malaysia: "Asia",
      Indonesia: "Asia",
      Philippines: "Asia",
      Vietnam: "Asia",
      Taiwan: "Asia",
      "Hong Kong": "Asia",
      Macau: "Asia",
      "United Arab Emirates": "Asia",
      "Saudi Arabia": "Asia",
      Turkey: "Asia",
      Israel: "Asia",
      Pakistan: "Asia",
      Bangladesh: "Asia",
      "Sri Lanka": "Asia",

      // Oceania
      Australia: "Oceania",
      "New Zealand": "Oceania",
      Fiji: "Oceania",

      // Africa
      "South Africa": "Africa",
      Egypt: "Africa",
      Morocco: "Africa",
      Kenya: "Africa",
      Nigeria: "Africa",
      Tanzania: "Africa",
      Uganda: "Africa",
    };

    return countryToContinent[country] || null;
  }
}
