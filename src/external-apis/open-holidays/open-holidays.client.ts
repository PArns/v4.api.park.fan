import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { OpenHolidaysEntry } from "./open-holidays.types";

/**
 * OpenHolidays API Client
 *
 * API: https://openholidaysapi.org
 * Swagger: https://openholidaysapi.org/swagger/index.html
 *
 * Provides school holiday data for European countries.
 */
@Injectable()
export class OpenHolidaysClient {
  private readonly logger = new Logger(OpenHolidaysClient.name);
  private readonly client: AxiosInstance;
  private readonly baseUrl = "https://openholidaysapi.org";

  constructor() {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 15000,
      headers: {
        "User-Agent": "park.fan API v4 (https://park.fan)",
        Accept: "application/json",
      },
    });
  }

  /**
   * Get school holidays for a specific country and date range
   *
   * @param countryIsoCode - ISO 3166-1 alpha-2 code (e.g., "DE", "NL")
   * @param languageIsoCode - Language for descriptions (e.g., "DE", "EN")
   * @param validFrom - Start date (YYYY-MM-DD)
   * @param validTo - End date (YYYY-MM-DD)
   * @param subdivisionCode - Optional: Filter by region (e.g. "DE-BW") - API supports filtering by subdivision code directly?
   *                        The API doc shows /SchoolHolidays?countryIsoCode=...
   *                        Usage: GET /SchoolHolidays
   */
  async getSchoolHolidays(
    countryIsoCode: string,
    validFrom: string,
    validTo: string,
    languageIsoCode?: string,
  ): Promise<OpenHolidaysEntry[]> {
    try {
      this.logger.debug(
        `Fetching school holidays for ${countryIsoCode} (${validFrom} to ${validTo}, lang: ${languageIsoCode})`,
      );
      const params: {
        countryIsoCode: string;
        validFrom: string;
        validTo: string;
        languageIsoCode?: string;
      } = {
        countryIsoCode,
        validFrom,
        validTo,
      };
      if (languageIsoCode) {
        params.languageIsoCode = languageIsoCode;
      }
      const response = await this.client.get<OpenHolidaysEntry[]>(
        "/SchoolHolidays",
        { params },
      );
      this.logger.debug(
        `Fetched ${response.data.length} entries for ${countryIsoCode}`,
      );
      return response.data;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to fetch school holidays for ${countryIsoCode}: ${errorMessage}`,
      );
      // Return empty array to avoid breaking the whole sync process
      return [];
    }
  }
}
