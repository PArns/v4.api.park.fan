import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import {
  NagerPublicHoliday,
  NagerCountryInfo,
  NagerLongWeekend,
} from "./nager-date.types";

/**
 * Nager.Date API Client
 *
 * API: https://date.nager.at/
 * Documentation: https://date.nager.at/Api
 *
 * Free public holiday API:
 * - No API key required
 * - No rate limits
 * - Supports 100+ countries
 * - Data updated annually
 *
 * Usage:
 * - Fetch public holidays for countries where parks exist
 * - Use for ML predictions (holidays = higher attendance)
 * - Update annually via scheduled job
 */
@Injectable()
export class NagerDateClient {
  private readonly logger = new Logger(NagerDateClient.name);
  private readonly client: AxiosInstance;
  private readonly baseUrl = "https://date.nager.at/api/v3";

  constructor() {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 10000,
      headers: {
        "User-Agent": "park.fan API v4 (https://park.fan)",
      },
    });
  }

  /**
   * Get all available countries
   *
   * GET /api/v3/AvailableCountries
   */
  async getAvailableCountries(): Promise<NagerCountryInfo[]> {
    try {
      const response = await this.client.get<NagerCountryInfo[]>(
        "/AvailableCountries",
      );
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`Failed to fetch available countries: ${errorMessage}`);
      throw new Error(`Nager.Date API error: ${errorMessage}`);
    }
  }

  /**
   * Get public holidays for a specific country and year
   *
   * GET /api/v3/PublicHolidays/{year}/{countryCode}
   *
   * @param year - Year (e.g., 2025)
   * @param countryCode - ISO 3166-1 alpha-2 code (e.g., "US", "DE")
   */
  async getPublicHolidays(
    year: number,
    countryCode: string,
  ): Promise<NagerPublicHoliday[]> {
    try {
      const response = await this.client.get<NagerPublicHoliday[]>(
        `/PublicHolidays/${year}/${countryCode}`,
      );
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to fetch holidays for ${countryCode} ${year}: ${errorMessage}`,
      );
      throw new Error(`Nager.Date API error: ${errorMessage}`);
    }
  }

  /**
   * Get long weekends for a specific country and year
   *
   * GET /api/v3/LongWeekend/{year}/{countryCode}
   *
   * Long weekends are periods where public holidays create extended weekends.
   * These are particularly important for theme parks (high attendance).
   *
   * @param year - Year (e.g., 2025)
   * @param countryCode - ISO 3166-1 alpha-2 code (e.g., "US", "DE")
   */
  async getLongWeekends(
    year: number,
    countryCode: string,
  ): Promise<NagerLongWeekend[]> {
    try {
      const response = await this.client.get<NagerLongWeekend[]>(
        `/LongWeekend/${year}/${countryCode}`,
      );
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Failed to fetch long weekends for ${countryCode} ${year}: ${errorMessage}`,
      );
      // Don't throw - long weekends are optional enhancement
      return [];
    }
  }

  /**
   * Check if a specific date is a public holiday
   *
   * GET /api/v3/IsTodayPublicHoliday/{countryCode}
   * GET /api/v3/IsTomorrowPublicHoliday/{countryCode}
   *
   * Note: We don't use this - we fetch all holidays and check locally.
   */

  /**
   * Get holidays for multiple years (for ML training data)
   *
   * @param countryCode - ISO 3166-1 alpha-2 code
   * @param startYear - Start year (inclusive)
   * @param endYear - End year (inclusive)
   */
  async getHolidaysForYears(
    countryCode: string,
    startYear: number,
    endYear: number,
  ): Promise<NagerPublicHoliday[]> {
    const allHolidays: NagerPublicHoliday[] = [];

    for (let year = startYear; year <= endYear; year++) {
      try {
        const holidays = await this.getPublicHolidays(year, countryCode);
        allHolidays.push(...holidays);

        // Small delay to be respectful to the API
        await this.sleep(100);
      } catch (_error) {
        this.logger.warn(
          `Failed to fetch holidays for ${countryCode} ${year}, continuing...`,
        );
      }
    }

    return allHolidays;
  }

  /**
   * Helper: Sleep for a given number of milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
