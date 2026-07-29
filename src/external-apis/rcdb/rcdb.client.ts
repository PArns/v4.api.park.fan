import { Injectable, Logger } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";
import { logExternalApiError } from "../../common/utils/file-logger.util";
import { parseRideStats, type RcdbRideStats } from "./rcdb.parser";

const TIMEOUT_MS = 20_000;

/**
 * Reads a ride's measurements off rcdb.com.
 *
 * There is no API, and nothing upstream carries these numbers: ThemeParks.wiki
 * gives us rider heights and nothing else, so length, drop, speed and duration
 * exist nowhere in this system. The Roller Coaster DataBase has them for the
 * ~500 rides we already store an `rcdbId` for.
 *
 * One request at a time with a pause between, like {@link SixFlagsClient}: a
 * coaster's length does not change, so this runs rarely and has no business
 * hitting a volunteer-run reference site hard. Only DB and Redis work gets
 * batched in this project, never the fetching.
 */
@Injectable()
export class RcdbClient {
  private readonly logger = new Logger(RcdbClient.name);
  private readonly http: AxiosInstance;

  constructor() {
    this.http = axios.create({
      baseURL: "https://rcdb.com",
      timeout: TIMEOUT_MS,
      headers: BROWSER_HEADERS,
      // A retired or renumbered id is expected — our seed outlives their
      // catalogue — so 404 is data rather than an error.
      validateStatus: (status) => status === 200 || status === 404,
    });
  }

  /**
   * The ride's stats, or `null` when the id is gone, points at a manufacturer's
   * model page rather than a ride, or the request failed. Callers cannot tell
   * those apart and should not need to: all three mean "nothing to record".
   */
  async fetchRideStats(rcdbId: number): Promise<RcdbRideStats | null> {
    const path = `/${rcdbId}.htm`;
    try {
      const response = await this.http.get<string>(path, {
        responseType: "text",
        transformResponse: (data: string) => data,
      });
      if (response.status === 404) return null;
      return parseRideStats(response.data);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(`RCDB fetch failed for ${path}: ${message}`);
      logExternalApiError("rcdb", path, message);
      return null;
    }
  }
}
