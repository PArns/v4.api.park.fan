import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";
import { logExternalApiError } from "../../common/utils/file-logger.util";
import { parseMinHeightInches } from "./six-flags.parser";

const TIMEOUT_MS = 20_000;

/**
 * Reads ride facts from sixflags.com.
 *
 * There is no API — the numbers are scraped out of the server-rendered ride
 * page. That is only worth doing because ThemeParks.wiki carries no
 * minimumHeight for any of these parks, leaving ~1400 rides with nothing.
 *
 * Deliberately one request at a time with a pause between: heights change a
 * few times a year, so there is no reason to hit the site hard, and this
 * project's convention is that only DB and Redis work gets batched, never the
 * fetching.
 */
@Injectable()
export class SixFlagsClient {
  private readonly logger = new Logger(SixFlagsClient.name);
  private readonly http: AxiosInstance;

  constructor() {
    this.http = axios.create({
      baseURL: "https://www.sixflags.com",
      timeout: TIMEOUT_MS,
      headers: BROWSER_HEADERS,
      // A missing ride page is expected (our slug may differ from theirs),
      // so 404 is data rather than an error.
      validateStatus: (status) => status === 200 || status === 404,
    });
  }

  /**
   * Minimum height in inches, or null when the ride has no requirement, the
   * page does not exist, or the request failed. Callers cannot distinguish
   * those and should not: all three mean "nothing to record".
   */
  async fetchMinHeightInches(
    parkSiteSlug: string,
    rideSlug: string,
  ): Promise<number | null> {
    const path = `/${parkSiteSlug}/attractions/${rideSlug}`;
    try {
      const response = await this.http.get<string>(path, {
        responseType: "text",
        transformResponse: (data: string) => data,
      });
      if (response.status === 404) return null;
      return parseMinHeightInches(response.data);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Six Flags fetch failed for ${path}: ${message}`);
      logExternalApiError("six-flags", path, message);
      return null;
    }
  }
}
