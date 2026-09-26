import { Injectable, Logger, Inject } from "@nestjs/common";
import axios, { AxiosInstance, AxiosResponse } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";
import { incrementWithWindow } from "../../common/redis/rate-limit.util";
import { ThemeParksRateLimitError } from "./themeparks.errors";

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
 * Rate limiting: the upstream publishes its own budget on every response that
 * reaches its origin — `RateLimit-Policy: 300;w=60`, so 300 requests per fixed
 * 60-second window (measured from celestrial on 2026-09-26; this docstring used
 * to claim 60 req/min, which nothing in the code enforced either). We space
 * requests {@link requestSpacingMs} apart, which caps us at 240 per window
 * wherever the upstream's window boundary happens to fall, and we WAIT OUT a
 * cooldown instead of failing the caller.
 *
 * API Docs: https://api.themeparks.wiki/docs/v1/
 */
@Injectable()
export class ThemeParksClient {
  private readonly logger = new Logger(ThemeParksClient.name);
  private readonly baseUrl = "https://api.themeparks.wiki/v1";
  private readonly client: AxiosInstance;

  // Redis keys — all three are shared by every job that talks to the Wiki,
  // because the upstream budget is per sender and not per job. Pacing only the
  // schedule sync while the 5-minute live poll stays unthrottled would move the
  // problem rather than fix it.
  private readonly BLOCKED_KEY = "ratelimit:themeparks:blocked";
  private readonly COUNTER_KEY = "ratelimit:themeparks:counter";
  private readonly SLOT_KEY = "ratelimit:themeparks:slot";

  // Retry policy for transient failures (5xx / network / timeout). 4xx are
  // client errors and never retried; 429 sets the distributed block and the
  // next attempt waits it out.
  private readonly maxRetries = 3;
  private readonly retryBackoffMs = 1000;

  // The upstream window, as published in `RateLimit-Policy: 300;w=60`.
  private readonly windowSeconds = 60;
  private readonly windowLimit = 300;

  /**
   * Minimum gap between two requests: 250 ms, so at most 241 land in any
   * 60-second stretch.
   *
   * Even spacing rather than a per-window counter on purpose. A counter has to
   * pick a window boundary, and ours would not be the upstream's — 240 requests
   * crammed into the end of our window plus 240 into the start of the next puts
   * 480 into one of theirs. Spacing makes the cap hold under every alignment.
   *
   * The cost is that the bulk schedule sync (195 parks × 14 requests) takes
   * ~11.5 minutes instead of the 7.5 it used to attempt, and the 5-minute live
   * poll spreads over ~50 s instead of 11.5. Both fit their cadence; neither
   * fits inside the upstream's budget without this.
   */
  private readonly requestSpacingMs = 250;

  /**
   * Bounds on waiting. Generous, because for this client a late answer beats no
   * answer: an immediate failure is what let a 58-second cooldown hand 87 of
   * 200 parks an empty schedule in a single run (PAR-480). Exceeding either
   * bound is a real backlog and raises {@link ThemeParksRateLimitError}.
   */
  private readonly maxSpacingWaitMs = 120_000;
  private readonly maxBlockWaitMs = 120_000;

  /**
   * Pause when the upstream says this few slots are left in the current window.
   * Stopping one step before the 429 keeps us off the penalty path entirely.
   */
  private readonly remainingFloor = 25;

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
      baseURL: this.baseUrl,
      timeout: 20000,
      headers: { ...BROWSER_HEADERS },
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Wait until the cooldown set by a 429 (or by {@link noteBudget}) has run out.
   *
   * This used to throw, which cost no time at all: a caller looping over parks
   * kept asking, every request failed instantly, and the whole cooldown was
   * spent handing out empty results at full speed. Waiting makes the same
   * cooldown slow the run down, which is what a cooldown is for.
   */
  private async waitOutBlock(path: string): Promise<void> {
    let waitedMs = 0;

    for (;;) {
      const blocked = await this.redis.get(this.BLOCKED_KEY);
      if (!blocked) return;

      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      if (ttl < 0) {
        // -2: it expired between GET and TTL. -1: it has no expiry at all,
        // which no writer here produces — drop it rather than wait forever.
        if (ttl === -1) await this.redis.del(this.BLOCKED_KEY);
        return;
      }

      const waitMs = ttl * 1000;
      if (waitedMs + waitMs > this.maxBlockWaitMs) {
        throw new ThemeParksRateLimitError(
          `ThemeParks API: still cooling down after ${Math.round(
            waitedMs / 1000,
          )}s, giving up on ${path} (${ttl}s left)`,
          ttl,
        );
      }

      this.logger.warn(
        `⏳ ThemeParks cooldown: waiting ${ttl}s before ${path}`,
      );
      await this.sleep(waitMs);
      waitedMs += waitMs;
    }
  }

  /**
   * Wait until this request is allowed to go out: past any cooldown, and at
   * least {@link requestSpacingMs} after the previous one.
   *
   * @returns the number of requests counted in the current observability window
   */
  private async enforceRateLimit(path: string): Promise<number> {
    await this.waitOutBlock(path);

    // Observability only — the spacer below is the limiter. This is the number
    // the 429 log needs to say how busy the window was when we were penalised.
    const windowCount = await incrementWithWindow(
      this.redis,
      this.COUNTER_KEY,
      this.windowSeconds,
    );

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
      throw new ThemeParksRateLimitError(
        `ThemeParks API: pacing backlog of ${Math.ceil(
          waitMs / 1000,
        )}s for ${path} exceeds the ${Math.round(
          this.maxSpacingWaitMs / 1000,
        )}s budget`,
        Math.ceil(waitMs / 1000),
      );
    }

    if (waitMs > 0) await this.sleep(waitMs);

    return windowCount;
  }

  /** Read a header as a non-negative integer, or undefined if it is absent. */
  private readIntHeader(
    headers: Record<string, unknown> | undefined,
    name: string,
  ): number | undefined {
    const raw = headers?.[name];
    if (raw === undefined || raw === null) return undefined;
    const value = parseInt(String(raw), 10);
    return Number.isFinite(value) && value >= 0 ? value : undefined;
  }

  /**
   * Pause before the upstream has to say no.
   *
   * Only for answers that actually reached the origin. A Cloudflare cache HIT
   * repeats the `RateLimit-*` values of whichever request filled the cache —
   * measured on 2026-09-26, a HIT with `age: 212` still reported
   * `remaining: 260, reset: 29`, numbers that were four minutes old. Acting on
   * them would pause on someone else's window, and a cached answer costs no
   * budget anyway because it never reaches the limiter.
   */
  private async noteBudget(
    response: AxiosResponse<unknown>,
    path: string,
  ): Promise<void> {
    const headers = response.headers as Record<string, unknown> | undefined;

    const cacheStatus = String(
      headers?.["cf-cache-status"] ?? "",
    ).toUpperCase();
    const age = this.readIntHeader(headers, "age");
    if (cacheStatus.startsWith("HIT") || (age !== undefined && age > 0)) return;

    const remaining = this.readIntHeader(headers, "ratelimit-remaining");
    if (remaining === undefined || remaining > this.remainingFloor) return;

    const reset = this.readIntHeader(headers, "ratelimit-reset");
    const pauseSeconds =
      reset !== undefined && reset > 0
        ? Math.min(reset, this.windowSeconds)
        : this.windowSeconds;

    await this.redis.set(this.BLOCKED_KEY, "true", "EX", pauseSeconds);
    this.logger.warn(
      `⏳ ThemeParks budget nearly spent after ${path}: ${remaining} of ${
        this.readIntHeader(headers, "ratelimit-limit") ?? this.windowLimit
      } left — pausing ${pauseSeconds}s`,
    );
  }

  /**
   * GET an API path and return the parsed JSON body, paced against the upstream
   * budget, with retry on transient failures.
   *
   * A 429 sets a distributed cooldown and the next attempt waits it out, so a
   * penalty costs latency rather than data. Once the retries are used up the
   * caller gets a {@link ThemeParksRateLimitError}, which is how it can tell
   * "we never asked" from "the source has nothing".
   */
  private async request<T>(path: string): Promise<T> {
    let lastError: any;
    let rateLimitError: ThemeParksRateLimitError | undefined;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      if (attempt > 0) {
        const delay = this.retryBackoffMs * Math.pow(2, attempt - 1);
        await this.sleep(delay);
      }

      // Throws ThemeParksRateLimitError if the cooldown or the pacing backlog
      // outlasts its budget — the caller must not read that as an empty source.
      const windowCount = await this.enforceRateLimit(path);

      try {
        const response = await this.client.get<T>(path);
        await this.noteBudget(response, path);
        return response.data;
      } catch (err: any) {
        lastError = err;
        const status = axios.isAxiosError(err)
          ? err.response?.status
          : undefined;

        // 429 → set the distributed cooldown (honouring Retry-After) and let
        // the next attempt wait it out.
        if (status === 429) {
          const retryAfter = axios.isAxiosError(err)
            ? (err.response?.headers?.["retry-after"] as string | undefined)
            : undefined;
          let unlockTime = 10; // Default 10s if unknown
          const seconds = retryAfter ? parseInt(retryAfter, 10) : NaN;
          if (!isNaN(seconds) && seconds > 0) {
            unlockTime = Math.min(seconds, 900);
          }
          await this.redis.set(this.BLOCKED_KEY, "true", "EX", unlockTime);

          this.logger.warn(
            `ThemeParks API 429 on ${path} with ${windowCount} request(s) in the window — cooling down ${unlockTime}s`,
          );

          rateLimitError = new ThemeParksRateLimitError(
            `ThemeParks API: rate limit exceeded on ${path} (cooling down ${unlockTime}s)`,
            unlockTime,
          );
          continue;
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

    if (rateLimitError) throw rateLimitError;

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
   * **An empty result means the source published nothing.** Where the requests
   * were throttled instead, this raises {@link ThemeParksRateLimitError} rather
   * than returning an empty list, because the caller writes an empty list as
   * "no change" and logs it as a successful fetch. That is what made 87 of 200
   * parks look like parks without a schedule (PAR-480).
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
    let throttled = false;

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
      if (error instanceof ThemeParksRateLimitError) {
        // Do not walk 13 months into a closed door. Each month would fail the
        // same way and the empty list they add up to is indistinguishable from
        // a park the source knows nothing about.
        throw new ThemeParksRateLimitError(
          `Schedule fetch for ${entityId} was throttled before it started: ${error.message}`,
          error.retryAfterSeconds,
        );
      }
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
        if (error instanceof ThemeParksRateLimitError) {
          // The cooldown outlasted its wait budget. Stop asking for this park
          // rather than spend that budget again on each remaining month.
          throttled = true;
          this.logger.warn(
            `Schedule fetch for ${entityId} throttled at ${year}/${String(month).padStart(2, "0")}: ${error.message}`,
          );
          break;
        }
        // Far-future months may be empty or 404 until the park publishes
        this.logger.verbose(
          `No schedule for ${entityId} ${year}/${String(month).padStart(2, "0")}: ${error.message}`,
        );
      }
    }

    if (throttled && allSchedules.length === 0) {
      throw new ThemeParksRateLimitError(
        `Schedule fetch for ${entityId} returned nothing because it was throttled`,
        0,
      );
    }

    if (allSchedules.length === 0) {
      // Not throttled and still nothing: this one IS the source's answer.
      this.logger.warn(
        `📅 Fetched 0 schedule entries for ${entityId} — the source published none for the requested months (no request was throttled)`,
      );
    } else {
      this.logger.log(
        `📅 Fetched total ${allSchedules.length} schedule entries for ${entityId}${
          throttled ? " (partial — the rest of the months were throttled)" : ""
        }`,
      );
    }

    return { schedule: allSchedules };
  }
}
