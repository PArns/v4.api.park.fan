import { Injectable, Logger, Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

import {
  DestinationsApiResponse,
  EntityResponse,
  EntityChildrenResponse,
  EntityLiveResponse,
} from "./themeparks.types";

/**
 * ThemeParks.wiki API Client
 *
 * Wrapper around direct HTTP calls to ThemeParks.wiki API.
 * Note: The 'themeparks' npm package exists but we use direct HTTP for more control.
 *
 * Rate Limiting: 60 req/min (token bucket)
 * API Docs: https://api.themeparks.wiki/docs/v1/
 */
@Injectable()
export class ThemeParksClient {
  private readonly logger = new Logger(ThemeParksClient.name);
  private readonly baseUrl = "https://api.themeparks.wiki/v1";

  // Redis key for distributed rate limiting
  private readonly BLOCKED_KEY = "ratelimit:themeparks:blocked";

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Helper to fetch with retry on 429
   *
   * IMPORTANT: This method checks for blocks before making requests to prevent
   * calls during a block, which would extend the lock duration.
   */
  private async fetchWithRetry(url: string, attempt = 0): Promise<Response> {
    // 1. Check Distributed Rate Limit
    // Only check on first attempt to avoid redundant checks during retry loop
    // But also check during retries if a block becomes active
    const blockedUntil = await this.redis.get(this.BLOCKED_KEY);
    if (blockedUntil) {
      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      const nextRetrySeconds = ttl > 0 ? ttl : 0;
      const nextRetryDate = new Date(Date.now() + nextRetrySeconds * 1000);

      // Only log on first attempt to avoid duplicate logs
      if (attempt === 0) {
        this.logger.warn(
          `⏳ Global Rate Limit active. Blocked for ${nextRetrySeconds}s. Next retry at ${nextRetryDate.toISOString()}`,
        );
      }
      // CRITICAL: Throw error BEFORE any API call to prevent extending the lock
      throw new Error(`ThemeParks API: Global Rate Limit (blocked)`);
    }

    const response = await fetch(url);

    // Check for 429 (Too Many Requests) OR 5xx (Server Errors)
    if (
      response.status === 429 ||
      (response.status >= 500 && response.status < 600)
    ) {
      // If 429, Set Distributed Lock
      if (response.status === 429) {
        const retryAfter = response.headers.get("Retry-After");
        let unlockTime = 5; // Default 5s if unknown
        if (retryAfter) {
          const seconds = parseInt(retryAfter, 10);
          if (!isNaN(seconds)) {
            unlockTime = seconds;
          }
        }
        await this.redis.set(this.BLOCKED_KEY, "true", "EX", unlockTime);
      }

      if (attempt >= 5) {
        throw new Error(
          `Request failed after 5 attempts for ${url} (Status: ${response.status})`,
        );
      }

      const retryAfter = response.headers.get("Retry-After");
      let delay = 1000 * Math.pow(2, attempt); // Default exponential backoff

      if (retryAfter) {
        const seconds = parseInt(retryAfter, 10);
        if (!isNaN(seconds)) {
          delay = seconds * 1000;
        }
      }

      this.logger.warn(
        `Rate limit or Server Error (${response.status}) for ${url}. Retrying in ${delay}ms (Attempt ${attempt + 1}/5)`,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
      return this.fetchWithRetry(url, attempt + 1);
    }

    return response;
  }

  /**
   * GET /v1/destinations
   *
   * Fetches all destinations with their parks.
   */
  async getDestinations(): Promise<DestinationsApiResponse> {
    const response = await this.fetchWithRetry(`${this.baseUrl}/destinations`);

    if (!response.ok) {
      throw new Error(
        `Failed to fetch destinations: ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  /**
   * GET /v1/entity/{id}
   *
   * Fetches full entity data (park, attraction, etc.)
   */
  async getEntity(entityId: string): Promise<EntityResponse> {
    const response = await this.fetchWithRetry(
      `${this.baseUrl}/entity/${entityId}`,
    );

    if (!response.ok) {
      throw new Error(
        `Failed to fetch entity ${entityId}: ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  /**
   * GET /v1/entity/{id}/children
   *
   * Fetches child entities (e.g., attractions for a park)
   */
  async getEntityChildren(entityId: string): Promise<EntityChildrenResponse> {
    const response = await this.fetchWithRetry(
      `${this.baseUrl}/entity/${entityId}/children`,
    );

    if (!response.ok) {
      throw new Error(
        `Failed to fetch children for ${entityId}: ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  /**
   * GET /v1/entity/{id}/live
   *
   * Fetches live data for an entity (wait times, status, etc.)
   */
  async getLiveData(entityId: string): Promise<EntityLiveResponse> {
    const url = `${this.baseUrl}/entity/${entityId}/live`;
    const startTime = Date.now();
    const response = await this.fetchWithRetry(url);
    const duration = Date.now() - startTime;

    if (!response.ok) {
      this.logger.error(
        `❌ API Error: ${response.status} ${response.statusText} for ${entityId} (took ${duration}ms)`,
      );
      const errorBody = await response
        .text()
        .catch(() => "Unable to read error body");
      this.logger.error(`Error details: ${errorBody}`);
      throw new Error(
        `Failed to fetch live data for ${entityId}: ${response.status} ${response.statusText}`,
      );
    }

    const rawData = await response.json();

    // Extract live data from the liveData array
    // API structure: { liveData: [{ queue, status, forecast, ... }] }
    const data =
      rawData.liveData && rawData.liveData.length > 0
        ? rawData.liveData[0]
        : rawData;

    return data;
  }

  /**
   * GET /v1/entity/{parkId}/live
   *
   * Fetches ALL live data for a park (all attractions at once)
   * Returns the complete liveData array instead of just the first element
   *
   * OPTIMIZATION: Use this for parks to get all attractions in one API call!
   */
  async getParkLiveData(parkId: string): Promise<EntityLiveResponse[]> {
    const url = `${this.baseUrl}/entity/${parkId}/live`;
    const startTime = Date.now();
    const response = await this.fetchWithRetry(url);
    const duration = Date.now() - startTime;

    if (!response.ok) {
      this.logger.error(
        `❌ API Error: ${response.status} ${response.statusText} for ${parkId} (took ${duration}ms)`,
      );
      const errorBody = await response
        .text()
        .catch(() => "Unable to read error body");
      this.logger.error(`Error details: ${errorBody}`);
      throw new Error(
        `Failed to fetch park live data for ${parkId}: ${response.status} ${response.statusText}`,
      );
    }

    const rawData = await response.json();

    // Return the complete liveData array (all child entities)
    // API structure: { liveData: [{ id, status, queue, ... }, ...] }
    return rawData.liveData || [];
  }

  /**
   * GET /v1/entity/{id}/schedule
   *
   * Fetches schedule data for a park (operating hours, events, etc.)
   * Returns schedule for the next 30 days by default.
   */
  async getSchedule(entityId: string): Promise<{ schedule: any[] }> {
    const response = await this.fetchWithRetry(
      `${this.baseUrl}/entity/${entityId}/schedule`,
    );

    if (!response.ok) {
      throw new Error(
        `Failed to fetch schedule for ${entityId}: ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  /**
   * GET /v1/entity/{id}/schedule/{year}/{month}
   *
   * Fetches schedule data for a specific month.
   * Month must be zero-padded (e.g., "03" not "3").
   */
  async getScheduleForMonth(
    entityId: string,
    year: number,
    month: number,
  ): Promise<{ schedule: any[] }> {
    const monthStr = month.toString().padStart(2, "0");
    const response = await this.fetchWithRetry(
      `${this.baseUrl}/entity/${entityId}/schedule/${year}/${monthStr}`,
    );

    if (!response.ok) {
      throw new Error(
        `Failed to fetch schedule for ${entityId} (${year}/${monthStr}): ${response.status} ${response.statusText}`,
      );
    }

    return response.json();
  }

  /**
   * Fetches schedule data for a range of months
   *
   * Always requests each of the next monthsAhead months via the month-specific endpoint
   * (e.g. /entity/{id}/schedule/2026/05 for May). Month is zero-padded (05 not 5) per Wiki API.
   * Some parks (e.g. Efteling) only expose data when the source has published that month.
   *
   * Optionally merges with the generic /schedule endpoint (~30 days) for the near term.
   *
   * @param entityId - Park entity ID
   * @param monthsAhead - Number of months to fetch ahead (default: 12)
   * @returns Combined schedule data from all months
   */
  async getScheduleExtended(
    entityId: string,
    monthsAhead: number = 12,
  ): Promise<{ schedule: any[] }> {
    const now = new Date();
    const allSchedules: any[] = [];

    // Optional: try generic endpoint first for near-term data (~30 days)
    try {
      const genericResponse = await this.getSchedule(entityId);
      if (genericResponse.schedule && genericResponse.schedule.length > 0) {
        allSchedules.push(...genericResponse.schedule);
        this.logger.log(
          `✅ Generic schedule returned ${genericResponse.schedule.length} entries for ${entityId}`,
        );
      }
    } catch (error: any) {
      this.logger.warn(
        `Generic schedule endpoint failed for ${entityId}: ${error.message}`,
      );
    }

    // Always fetch each of the next monthsAhead months via month endpoint (YYYY/MM with MM zero-padded)
    this.logger.log(
      `Fetching ${monthsAhead} months (month-by-month) for ${entityId}...`,
    );
    for (let i = 0; i < monthsAhead; i++) {
      const iterDate = new Date(now.getFullYear(), now.getMonth() + i, 1);
      const year = iterDate.getFullYear();
      const month = iterDate.getMonth() + 1; // 1–12; getScheduleForMonth pads to "01".."12"

      try {
        const monthResponse = await this.getScheduleForMonth(
          entityId,
          year,
          month,
        );
        if (monthResponse.schedule && monthResponse.schedule.length > 0) {
          allSchedules.push(...monthResponse.schedule);
          this.logger.debug(
            `Fetched ${monthResponse.schedule.length} entries for ${year}/${String(month).padStart(2, "0")}`,
          );
        }
      } catch (error: any) {
        // Far-future months may be empty or 404 until the park publishes
        this.logger.verbose(
          `No schedule for ${entityId} ${year}/${String(month).padStart(2, "0")}: ${error.message}`,
        );
      }
    }

    this.logger.log(
      `📅 Fetched total ${allSchedules.length} schedule entries for ${entityId}`,
    );

    return { schedule: allSchedules };
  }
}
