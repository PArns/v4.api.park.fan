import { Injectable, Logger, Inject } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";

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
  private readonly client: AxiosInstance;

  // Redis key for distributed rate limiting
  private readonly BLOCKED_KEY = "ratelimit:themeparks:blocked";

  // Retry policy for transient failures (5xx / network / timeout). 4xx are
  // client errors and never retried; 429 sets the distributed block instead.
  private readonly maxRetries = 3;
  private readonly retryBackoffMs = 1000;

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 20000,
      headers: { ...BROWSER_HEADERS },
    });
  }

  /**
   * GET an API path and return the parsed JSON body, with retry on transient
   * failures and a distributed 429 rate-limit block.
   *
   * IMPORTANT: This method checks for an active block BEFORE making requests so
   * we never call during a block, which would extend the lock duration.
   */
  private async request<T>(path: string): Promise<T> {
    // 1. Check distributed rate-limit block
    const blockedUntil = await this.redis.get(this.BLOCKED_KEY);
    if (blockedUntil) {
      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      const nextRetrySeconds = ttl > 0 ? ttl : 0;
      throw new Error(
        `ThemeParks API: Global Rate Limit (blocked for ${nextRetrySeconds}s)`,
      );
    }

    let lastError: any;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      if (attempt > 0) {
        const delay = this.retryBackoffMs * Math.pow(2, attempt - 1);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      try {
        const response = await this.client.get<T>(path);
        return response.data;
      } catch (err: any) {
        lastError = err;
        const status = axios.isAxiosError(err)
          ? err.response?.status
          : undefined;

        // 429 → set the distributed block (honouring Retry-After) and stop.
        if (status === 429) {
          const retryAfter = axios.isAxiosError(err)
            ? (err.response?.headers?.["retry-after"] as string | undefined)
            : undefined;
          let unlockTime = 10; // Default 10s if unknown
          const seconds = retryAfter ? parseInt(retryAfter, 10) : NaN;
          if (!isNaN(seconds) && seconds > 0) unlockTime = seconds;
          await this.redis.set(this.BLOCKED_KEY, "true", "EX", unlockTime);
          throw new Error(
            `ThemeParks API: Rate limit exceeded (blocked for ${unlockTime}s)`,
          );
        }

        // Other 4xx are client errors — don't retry (e.g. far-future 404s).
        if (status !== undefined && status >= 400 && status < 500) {
          throw new Error(
            `Failed to fetch ${path}: ${status} ${
              err.response?.statusText ?? ""
            }`.trim(),
          );
        }

        // 5xx / network / timeout → fall through and retry.
      }
    }

    this.logger.warn(
      `ThemeParks API fetch failed for ${path}: ${lastError?.message}`,
    );
    throw lastError;
  }

  /**
   * GET /v1/destinations
   *
   * Fetches all destinations with their parks.
   */
  async getDestinations(): Promise<DestinationsApiResponse> {
    return this.request<DestinationsApiResponse>("/destinations");
  }

  /**
   * GET /v1/entity/{id}
   *
   * Fetches full entity data (park, attraction, etc.)
   */
  async getEntity(entityId: string): Promise<EntityResponse> {
    return this.request<EntityResponse>(`/entity/${entityId}`);
  }

  /**
   * GET /v1/entity/{id}/children
   *
   * Fetches child entities (e.g., attractions for a park)
   */
  async getEntityChildren(entityId: string): Promise<EntityChildrenResponse> {
    return this.request<EntityChildrenResponse>(`/entity/${entityId}/children`);
  }

  /**
   * GET /v1/entity/{id}/live
   *
   * Fetches live data for an entity (wait times, status, etc.)
   */
  async getLiveData(entityId: string): Promise<EntityLiveResponse> {
    const rawData = await this.request<any>(`/entity/${entityId}/live`);

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
    const rawData = await this.request<any>(`/entity/${parkId}/live`);

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
    return this.request<{ schedule: any[] }>(`/entity/${entityId}/schedule`);
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
    return this.request<{ schedule: any[] }>(
      `/entity/${entityId}/schedule/${year}/${monthStr}`,
    );
  }

  /**
   * Fetches schedule data for a range of months
   *
   * Requests the previous month plus each of the next monthsAhead months via the month-specific endpoint
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

    // Always fetch previous month + next monthsAhead months via month endpoint (YYYY/MM with MM zero-padded)
    this.logger.log(
      `Fetching previous month + ${monthsAhead} months (month-by-month) for ${entityId}...`,
    );
    for (let i = -1; i < monthsAhead; i++) {
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
