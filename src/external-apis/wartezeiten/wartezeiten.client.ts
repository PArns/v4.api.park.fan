import { Injectable, Logger, Inject } from "@nestjs/common";
import axios, { AxiosInstance, AxiosError } from "axios";
import {
  WartezeitenParkResponse,
  WartezeitenWaitTimeResponse,
  WartezeitenOpeningTimeResponse,
} from "./wartezeiten.types";
import { logRateLimitBlock } from "../../common/utils/file-logger.util";

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
  private readonly COUNTER_KEY = "ratelimit:wartezeiten:counter";
  private readonly SLOT_KEY = "ratelimit:wartezeiten:slot";
  private readonly LAST_429_KEY = "ratelimit:wartezeiten:last429";

  // Rate-limit strategy. The API (Cloudflare-fronted) 429s on BURSTS — many
  // requests in the same second via Promise.all — not on the per-minute total:
  // we get 429'd with only ~13 requests in the window, and a single live probe
  // returns 200 with no Retry-After header. So we (a) SPACE requests evenly to
  // avoid presenting a same-second burst, and (b) on a 429 apply a SHORT
  // cooldown (honouring Retry-After if the API ever sends one) instead of the
  // old 15-minute self-lockout, which was a self-inflicted over-reaction.
  private readonly requestSpacingMs = 750; // min gap between WZ requests (≈80/min)
  private readonly maxSpacingWaitMs = 20000; // skip a request rather than stall longer
  private readonly cooldownSeconds = 30; // self-cooldown after a 429 (tunable)

  // Atomic even-spacing reservation (shared across concurrent callers): returns
  // the ms to wait so this request starts ≥ previous slot + spacing. The PX
  // expiry resets the slot after an idle gap so we never carry a stale backlog.
  private readonly reserveSlotLua = `
    local now = tonumber(ARGV[1])
    local interval = tonumber(ARGV[2])
    local last = tonumber(redis.call('GET', KEYS[1]) or '0')
    local slot = now
    if last + interval > now then slot = last + interval end
    redis.call('SET', KEYS[1], slot, 'PX', 60000)
    return slot - now
  `;

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {
    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: 20000, // 20 seconds (increased to reduce timeout errors)
    });

    // Add response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => this.handleError(error),
    );
  }

  /**
   * Enforce rate limiting (Distributed Redis check)
   *
   * IMPORTANT: This method MUST be called before any API request to prevent
   * calls during a block, which would extend the lock duration.
   */
  private async enforceRateLimit(): Promise<void> {
    // 1. Active self-cooldown from a recent 429 — fail fast (no HTTP call, so it
    //    can't extend anything). Message includes "rate limit" so the
    //    orchestrator treats it as an expected backoff, not an error.
    const cooling = await this.redis.get(this.BLOCKED_KEY);
    if (cooling) {
      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      throw new Error(
        `Wartezeiten API: rate limit cooling down (${ttl > 0 ? ttl : 0}s left)`,
      );
    }

    // 2. Observability counter (per-minute) — NOT used for limiting anymore (the
    //    spacer below does that); kept so the 429 log can report how many
    //    requests were in the window when a burst tripped Cloudflare.
    const windowCount = await this.redis.incr(this.COUNTER_KEY);
    if (windowCount === 1) await this.redis.expire(this.COUNTER_KEY, 60);

    // 3. Even-spacing reservation — spreads concurrent Promise.all requests so
    //    we never present Cloudflare with a same-second burst (the actual 429
    //    trigger). Atomic across callers via the Lua slot.
    const waitMs = Number(
      await this.redis.eval(
        this.reserveSlotLua,
        1,
        this.SLOT_KEY,
        Date.now().toString(),
        this.requestSpacingMs.toString(),
      ),
    );
    if (waitMs > this.maxSpacingWaitMs) {
      // Too many requests queued this cycle — skip rather than stall the sync.
      throw new Error(
        `Wartezeiten API: rate limit spacing budget exceeded (would wait ${Math.ceil(
          waitMs / 1000,
        )}s)`,
      );
    }
    if (waitMs > 0) {
      // DEBUG: per-request spacing applied — lets us analyse queue/timing.
      this.logger.debug(
        `⏱️  WZ spacing: wait ${waitMs}ms (windowCount=${windowCount}, spacing=${this.requestSpacingMs}ms)`,
      );
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
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
        case 429: {
          const headers = (error.response.headers ?? {}) as Record<
            string,
            unknown
          >;
          const retryAfterRaw = headers["retry-after"];
          const retryAfterSeconds = retryAfterRaw
            ? parseInt(String(retryAfterRaw), 10)
            : NaN;
          // Honour the API's Retry-After if it ever sends one (capped at 15min),
          // else a SHORT self-cooldown — a live probe shows the API recovers
          // immediately, so the old hard 15-min lockout was self-inflicted.
          const cooldown =
            Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
              ? Math.min(retryAfterSeconds, 900)
              : this.cooldownSeconds;

          await this.redis.set(this.BLOCKED_KEY, "true", "EX", cooldown);

          // DEBUG timing data: window count, time since the previous 429
          // (cooldown→resume→429 cadence), and the actual rate-limit response
          // headers — so we can finally see whether the API signals a real
          // penalty (Retry-After / X-RateLimit-*) or just bursts us on Cloudflare.
          const now = Date.now();
          const windowCount = await this.redis.get(this.COUNTER_KEY);
          const prev429 = await this.redis.get(this.LAST_429_KEY);
          await this.redis.set(this.LAST_429_KEY, now.toString(), "EX", 3600);
          const secSinceLast429 = prev429
            ? Math.round((now - parseInt(prev429, 10)) / 1000)
            : null;

          this.logger.warn(
            `⏳ Wartezeiten 429 (burst). Cooling down ${cooldown}s. ` +
              `retry-after=${retryAfterRaw ?? "none"} window=${windowCount ?? 0} ` +
              `sinceLast429=${secSinceLast429 ?? "n/a"}s`,
          );

          logRateLimitBlock(
            "wartezeiten.app",
            Math.round(cooldown / 60),
            "429 Too Many Requests",
            {
              cooldownSeconds: cooldown,
              requestsThisWindow: windowCount ? parseInt(windowCount, 10) : 0,
              requestSpacingMs: this.requestSpacingMs,
              secondsSinceLast429: secSinceLast429,
              retryAfterHeader: retryAfterRaw ?? null,
              rateLimitHeaders: {
                limit:
                  headers["x-ratelimit-limit"] ??
                  headers["ratelimit-limit"] ??
                  null,
                remaining:
                  headers["x-ratelimit-remaining"] ??
                  headers["ratelimit-remaining"] ??
                  null,
                reset:
                  headers["x-ratelimit-reset"] ??
                  headers["ratelimit-reset"] ??
                  null,
              },
              cfRay: headers["cf-ray"] ?? null,
            },
          );

          throw new Error(
            `Wartezeiten API: 429 rate limit (cooling down ${cooldown}s)`,
          );
        }
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
