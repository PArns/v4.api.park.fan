import { Injectable, Logger, Inject } from "@nestjs/common";
import axios, { AxiosInstance, AxiosError } from "axios";
import {
  WartezeitenParkResponse,
  WartezeitenWaitTimeResponse,
  WartezeitenOpeningTimeResponse,
  WartezeitenCrowdLevelResponse,
} from "./wartezeiten.types";

/**
 * Wartezeiten.app API Client
 *
 * Official API: https://api.wartezeiten.app
 * Rate Limit: 100 requests per minute
 * Rate Limit Penalty: 15 minute block
 *
 * Cache TTLs (per API docs):
 * - Parks: 24 hours
 * - Wait Times: 5 minutes
 * - Opening Times: 24 hours
 * - Crowd Level: 5-10 minutes
 */
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

// ... (imports)

@Injectable()
export class WartezeitenClient {
  private readonly logger = new Logger(WartezeitenClient.name);
  private readonly client: AxiosInstance;
  private readonly baseURL = "https://api.wartezeiten.app";

  // Redis keys
  private readonly BLOCKED_KEY = "ratelimit:wartezeiten:blocked";

  // Rate limiting state
  private requestCount = 0;
  private requestWindow = Date.now();
  private readonly maxRequestsPerMinute = 90; // Set to 90 to stay under 100 limit

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {
    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: 10000,
      headers: {
        "User-Agent": "park.fan/1.0",
      },
    });

    // Add response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => this.handleError(error),
    );
  }

  /**
   * Enforce rate limiting (90 req/min local + global Redis check)
   *
   * IMPORTANT: This method MUST be called before any API request to prevent
   * calls during a block, which would extend the lock duration.
   */
  private async enforceRateLimit(): Promise<void> {
    // 1. Check Global Redis Block
    const blockedUntil = await this.redis.get(this.BLOCKED_KEY);
    if (blockedUntil) {
      // Get TTL to determine when block expires
      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      const nextRetrySeconds = ttl > 0 ? ttl : 0;
      const nextRetryDate = new Date(Date.now() + nextRetrySeconds * 1000);

      this.logger.warn(
        `⏳ Global Rate Limit active. Blocked for ${nextRetrySeconds}s. Next retry at ${nextRetryDate.toISOString()}`,
      );
      // CRITICAL: Throw error BEFORE any API call to prevent extending the lock
      throw new Error(`Wartezeiten API: Global Rate Limit (blocked)`);
    }

    const now = Date.now();
    const windowDuration = 60 * 1000; // 1 minute

    // Reset window if more than 1 minute has passed
    if (now - this.requestWindow > windowDuration) {
      this.requestCount = 0;
      this.requestWindow = now;
    }

    // Check if we've hit the limit
    if (this.requestCount >= this.maxRequestsPerMinute) {
      const waitTime = windowDuration - (now - this.requestWindow);
      this.logger.warn(
        `⏳ Local Rate limit reached (${this.requestCount}/${this.maxRequestsPerMinute}). Waiting ${Math.ceil(waitTime / 1000)}s...`,
      );
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      // Reset after waiting
      this.requestCount = 0;
      this.requestWindow = Date.now();
    }

    this.requestCount++;
  }

  /**
   * Handle API errors
   */
  private async handleError(error: AxiosError): Promise<never> {
    if (error.response) {
      const status = error.response.status;
      const data = error.response.data as any;

      switch (status) {
        case 400:
          throw new Error(
            `Wartezeiten API: Invalid parameters - ${data?.message || "Unknown error"}`,
          );
        case 404:
          throw new Error(
            `Wartezeiten API: No data for park - ${data?.message || "Not found"}`,
          );
        case 405:
          throw new Error("Wartezeiten API: Method not allowed");
        case 429:
          this.logger.error(
            "🚨 Rate limit exceeded! Setting global block for 15 minutes.",
          );
          // Set global block in Redis for 15 minutes
          await this.redis.set(this.BLOCKED_KEY, "true", "EX", 15 * 60);

          throw new Error(
            "Wartezeiten API: Rate limit exceeded (blocked for 15 minutes)",
          );
        default:
          throw new Error(
            `Wartezeiten API error (${status}): ${data?.message || error.message}`,
          );
      }
    } else if (error.request) {
      throw new Error(
        `Wartezeiten API: No response received - ${error.message}`,
      );
    } else {
      throw new Error(`Wartezeiten API request failed: ${error.message}`);
    }
  }

  /**
   * Execute request with retry logic for 5xx and network errors
   * NOTE: Does NOT retry 429s because of the severe 15-minute block penalty.
   *
   * IMPORTANT: This method assumes enforceRateLimit() was called before.
   * However, we check again here to prevent calls during retries if a block
   * becomes active (e.g., from another process).
   */
  private async requestWithRetry<T>(
    url: string,
    config: any,
    retries = 3,
    delay = 1000,
  ): Promise<T> {
    // Double-check block status before making request (prevents calls during retries)
    // Only log if this is a retry (retries < 3), otherwise enforceRateLimit() already logged
    const blockedUntil = await this.redis.get(this.BLOCKED_KEY);
    if (blockedUntil) {
      // Only log if we're in a retry loop (not the initial call)
      if (retries < 3) {
        const ttl = await this.redis.ttl(this.BLOCKED_KEY);
        const nextRetrySeconds = ttl > 0 ? ttl : 0;
        const nextRetryDate = new Date(Date.now() + nextRetrySeconds * 1000);

        this.logger.warn(
          `⏳ Block detected during retry. Blocked for ${nextRetrySeconds}s. Next retry at ${nextRetryDate.toISOString()}`,
        );
      }
      // CRITICAL: Throw error BEFORE any API call to prevent extending the lock
      throw new Error(`Wartezeiten API: Global Rate Limit (blocked)`);
    }

    try {
      const response = await this.client.get<T>(url, config);
      return response.data;
    } catch (error: any) {
      if (axios.isAxiosError(error)) {
        // Handle 5xx Server Errors
        if (
          error.response &&
          error.response.status >= 500 &&
          error.response.status < 600
        ) {
          if (retries > 0) {
            this.logger.warn(
              `Wartezeiten Server Error (${error.response.status}). Retrying in ${delay}ms...`,
            );
            await new Promise((resolve) => setTimeout(resolve, delay));
            return this.requestWithRetry<T>(
              url,
              config,
              retries - 1,
              delay * 2,
            );
          }
        }

        // Handle Network Errors (ECONNRESET, etc.)
        if (
          error.code === "ECONNRESET" ||
          error.code === "ETIMEDOUT" ||
          error.code === "ENOTFOUND"
        ) {
          if (retries > 0) {
            this.logger.warn(
              `Wartezeiten Network Error (${error.code}). Retrying in ${delay}ms...`,
            );
            await new Promise((resolve) => setTimeout(resolve, delay));
            return this.requestWithRetry<T>(
              url,
              config,
              retries - 1,
              delay * 2,
            );
          }
        }
      }

      // If not retryable or retries exhausted, let the existing interceptor handle it
      throw error;
    }
  }

  /**
   * Get all parks
   *
   * @param language - Display language ('en' or 'de')
   * @returns List of parks
   *
   * Cache: 24 hours (per API docs)
   */
  async getParks(
    language: "en" | "de" = "en",
  ): Promise<WartezeitenParkResponse[]> {
    await this.enforceRateLimit();

    this.logger.debug(`Fetching parks (language: ${language})`);

    const data = await this.requestWithRetry<WartezeitenParkResponse[]>(
      "/v1/parks",
      {
        headers: {
          language,
        },
      },
    );

    this.logger.log(`✅ Fetched ${data.length} parks`);
    return data;
  }

  /**
   * Get wait times for a park
   *
   * @param parkId - Park ID or UUID
   * @param language - Display language ('en' or 'de')
   * @returns List of attraction wait times
   *
   * Cache: 5 minutes (per API docs)
   */
  async getWaitTimes(
    parkId: string,
    language: "en" | "de" = "en",
  ): Promise<WartezeitenWaitTimeResponse[]> {
    await this.enforceRateLimit();

    this.logger.debug(`Fetching wait times for park: ${parkId}`);

    const data = await this.requestWithRetry<WartezeitenWaitTimeResponse[]>(
      "/v1/waitingtimes",
      {
        headers: {
          park: parkId,
          language,
        },
      },
    );

    this.logger.debug(
      `✅ Fetched ${data.length} wait times for park: ${parkId}`,
    );
    return data;
  }

  /**
   * Get opening times for a park
   *
   * @param parkId - Park ID or UUID
   * @returns Opening times (single object in array)
   *
   * Cache: 24 hours (per API docs)
   */
  async getOpeningTimes(
    parkId: string,
  ): Promise<WartezeitenOpeningTimeResponse[]> {
    await this.enforceRateLimit();

    this.logger.debug(`Fetching opening times for park: ${parkId}`);

    const data = await this.requestWithRetry<WartezeitenOpeningTimeResponse[]>(
      "/v1/openingtimes",
      {
        headers: {
          park: parkId,
        },
      },
    );

    this.logger.debug(
      `✅ Fetched opening times for park: ${parkId} (opened_today: ${data[0]?.opened_today})`,
    );
    return data;
  }

  /**
   * Get crowd level for a park
   *
   * @param parkId - Park ID or UUID
   * @returns Crowd level data
   *
   * Cache: 5-10 minutes (per API docs)
   */
  async getCrowdLevel(parkId: string): Promise<WartezeitenCrowdLevelResponse> {
    await this.enforceRateLimit();

    this.logger.debug(`Fetching crowd level for park: ${parkId}`);

    const data = await this.requestWithRetry<WartezeitenCrowdLevelResponse>(
      "/v1/crowdlevel",
      {
        headers: {
          park: parkId,
        },
      },
    );

    this.logger.debug(
      `✅ Fetched crowd level for park: ${parkId} (${data.crowd_level})`,
    );
    return data;
  }

  /**
   * Health check - verify API is accessible
   */
  async isHealthy(): Promise<boolean> {
    try {
      await this.enforceRateLimit();
      // Use parks endpoint as health check (cached 24h, so minimal impact)
      await this.client.get("/v1/parks", {
        headers: { language: "en" },
        timeout: 5000,
      });
      return true;
    } catch (error) {
      this.logger.error(`Health check failed: ${error}`);
      return false;
    }
  }
}
