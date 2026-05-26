import { Injectable, Logger, Inject } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { logRateLimitBlock } from "../../common/utils/file-logger.util";

/**
 * Open-Meteo Weather API Client
 *
 * Documentation: https://open-meteo.com/en/docs
 *
 * Features:
 * - Free, no API key required
 * - No rate limits
 * - Historical data (1940 - present)
 * - 16-day forecast
 * - Hourly and daily data
 *
 * Daily Variables:
 * - temperature_2m_max, temperature_2m_min
 * - precipitation_sum, rain_sum, snowfall_sum
 * - weathercode (WMO code)
 * - windspeed_10m_max
 */
@Injectable()
export class OpenMeteoClient {
  private readonly logger = new Logger(OpenMeteoClient.name);
  private readonly client: AxiosInstance;
  private readonly baseUrl = "https://api.open-meteo.com/v1";
  // Redis key for distributed rate limiting
  private readonly BLOCKED_KEY = "ratelimit:openmeteo:blocked";
  // Circuit breaker: opened when 5xx/network failures exhaust their retries, so
  // callers fail fast during an upstream outage instead of each burning retries.
  private readonly CIRCUIT_KEY = "ratelimit:openmeteo:circuit";
  private readonly CIRCUIT_COOLDOWN = 30; // seconds the circuit stays open
  // Hourly forecast is stable across hours; nowcast covers real-time. Refresh
  // every ~6h with jitter to respect the quota and avoid synchronized expiry.
  private readonly CACHE_TTL = 6 * 60 * 60; // 6 hours
  private readonly CACHE_TTL_JITTER = 60 * 60; // up to +1h random spread
  // Nowcast is served per-park from a dedicated 30-min cache; the client cache
  // matches it to keep upstream request volume low (~half Open-Meteo's ~15-min
  // refresh cadence).
  private readonly NOWCAST_CACHE_TTL = 30 * 60; // 30 minutes
  // Neutral browser-like User-Agent. The default "axios/x" UA is an obvious bot
  // signature that can get the shared free-tier IP rate-limited/blocked.
  private readonly USER_AGENT =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
  // In-flight upstream requests keyed by cache key, for singleflight dedup.
  private readonly inflight = new Map<string, Promise<unknown>>();

  constructor(
    private readonly configService: ConfigService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 20000, // 20 seconds (increased to reduce timeout errors)
      headers: {
        "User-Agent": this.USER_AGENT,
      },
    });
  }

  /**
   * Singleflight: coalesce concurrent calls for the same cache key into one
   * upstream request. Without this, a cache miss under concurrency (e.g. the
   * 5-min warmup hitting a park's park-level AND attraction-level predictions at
   * once, all resolving to the same coordinates) would fire N identical
   * Open-Meteo requests instead of one. In-flight callers share one promise;
   * once it settles the entry is cleared so the next miss re-fetches.
   */
  private dedupe<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const existing = this.inflight.get(key) as Promise<T> | undefined;
    if (existing) return existing;

    const promise = fn().finally(() => this.inflight.delete(key));
    this.inflight.set(key, promise);
    return promise;
  }

  /**
   * Execute request with retry logic for 429s and 5xx errors
   *
   * IMPORTANT: This method checks for blocks before making requests to prevent
   * calls during a block, which would extend the lock duration.
   */
  private async requestWithRetry<T>(
    url: string,
    config: any,
    retries = 3,
    delay = 1000,
  ): Promise<T> {
    // Check global rate-limit block and circuit breaker before making a request
    // (both fail fast). Only log on the first attempt (retries === 3).
    const [blockedUntil, circuitOpen] = await this.redis.mget(
      this.BLOCKED_KEY,
      this.CIRCUIT_KEY,
    );
    if (blockedUntil) {
      const ttl = await this.redis.ttl(this.BLOCKED_KEY);
      const nextRetrySeconds = ttl > 0 ? ttl : 0;
      const nextRetryDate = new Date(Date.now() + nextRetrySeconds * 1000);

      // Only log on first attempt to avoid duplicate logs
      if (retries === 3) {
        this.logger.warn(
          `⏳ Global Rate Limit active. Blocked for ${nextRetrySeconds}s. Next retry at ${nextRetryDate.toISOString()}`,
        );
      }
      // CRITICAL: Throw error BEFORE any API call to prevent extending the lock
      throw new Error(`Open-Meteo API: Global Rate Limit (blocked)`);
    }
    if (circuitOpen) {
      if (retries === 3) {
        const ttl = await this.redis.ttl(this.CIRCUIT_KEY);
        this.logger.warn(
          `⚡ Open-Meteo circuit open (upstream degraded). Failing fast for ${ttl > 0 ? ttl : 0}s.`,
        );
      }
      throw new Error(`Open-Meteo API: circuit open (upstream degraded)`);
    }

    try {
      const response = await this.client.get<T>(url, config);

      return response.data;
    } catch (error: any) {
      if (axios.isAxiosError(error) && error.response) {
        const status = error.response.status;

        // Handle 429 Rate Limit
        if (status === 429) {
          // Set Global Block (60s default)
          await this.redis.set(this.BLOCKED_KEY, "true", "EX", 60);

          // Log to dedicated file
          logRateLimitBlock(
            "open-meteo.com",
            1,
            "429 Too Many Requests - API rate limit exceeded",
            {
              url,
              retriesLeft: retries,
            },
          );

          if (retries > 0) {
            this.logger.warn(
              `⏸️ Open-Meteo Rate Limit (429). Retrying in 60s... (Attempts left: ${retries})`,
            );
            await new Promise((resolve) => setTimeout(resolve, 60000));
            return this.requestWithRetry<T>(url, config, retries - 1, delay);
          }
        }

        // Handle 5xx Server Errors
        if (status >= 500 && status < 600) {
          if (retries > 0) {
            this.logger.warn(
              `Open-Meteo Server Error (${status}). Retrying in ${delay}ms...`,
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

      // Handle Network Errors (ECONNRESET, etc.)
      const isNetworkError =
        error.code === "ECONNRESET" ||
        error.code === "ETIMEDOUT" ||
        error.code === "ENOTFOUND";
      if (isNetworkError) {
        if (retries > 0) {
          this.logger.warn(
            `Open-Meteo Network Error (${error.code}). Retrying in ${delay}ms...`,
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          return this.requestWithRetry<T>(url, config, retries - 1, delay * 2);
        }
      }

      // Reaching here means retries are exhausted (or the error was
      // non-retryable). If a 5xx or network failure burned through all retries
      // the upstream is degraded → open the circuit so concurrent/subsequent
      // callers fail fast instead of each retrying. Auto-closes after cooldown.
      const isServerError =
        axios.isAxiosError(error) &&
        error.response != null &&
        error.response.status >= 500 &&
        error.response.status < 600;
      if (isServerError || isNetworkError) {
        await this.redis
          .set(this.CIRCUIT_KEY, "true", "EX", this.CIRCUIT_COOLDOWN)
          .catch(() => {});
        this.logger.warn(
          `⚡ Open-Meteo circuit opened for ${this.CIRCUIT_COOLDOWN}s after repeated upstream failures`,
        );
      }

      throw error;
    }
  }

  /**
   * Fetch daily weather forecast for a location.
   * Results are cached by lat/lng/forecastDays for 2 hours to avoid redundant
   * API calls for parks that share coordinates (e.g. multiple parks in one resort).
   *
   * @param latitude - Location latitude
   * @param longitude - Location longitude
   * @param forecastDays - Number of forecast days (max 16)
   * @returns Daily weather data (current + forecast)
   */
  async getDailyWeather(
    latitude: number,
    longitude: number,
    forecastDays: number = 16,
  ): Promise<DailyWeatherResponse> {
    // Round to 2 decimal places (~1km precision) for cache key deduplication
    const latR = Math.round(latitude * 100) / 100;
    const lonR = Math.round(longitude * 100) / 100;
    const cacheKey = `weather:daily:${latR}:${lonR}:${forecastDays}`;

    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    } catch {
      // Cache miss is fine
    }

    return this.dedupe(cacheKey, async () => {
      try {
        const data = await this.requestWithRetry<OpenMeteoResponse>(
          "/forecast",
          {
            params: {
              latitude,
              longitude,
              forecast_days: forecastDays,
              daily: [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "rain_sum",
                "snowfall_sum",
                "weathercode",
                "windspeed_10m_max",
              ].join(","),
              current: [
                "temperature_2m",
                "apparent_temperature",
                "relative_humidity_2m",
                "weather_code",
                "wind_speed_10m",
                "is_day",
              ].join(","),
              timezone: "auto",
            },
          },
        );

        const result = this.transformResponse(data);

        try {
          await this.redis.set(cacheKey, JSON.stringify(result), "EX", 7200); // 2h TTL
        } catch {
          // Cache write failure is non-critical
        }

        return result;
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.error(
          `Failed to fetch weather data: ${errorMessage}`,
          error instanceof Error ? error.stack : undefined,
        );
        throw new Error(`Open-Meteo API error: ${errorMessage}`);
      }
    });
  }

  async getHourlyForecast(
    latitude: number,
    longitude: number,
    forecastDays: number = 2, // Default to 2 days (48h) enough for hourly prediction
  ): Promise<HourlyWeatherResponse> {
    // Check cache first
    const cacheKey = `weather:hourly:${latitude}:${longitude}`; // Simple key
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) {
        // this.logger.debug(`Cache hit for weather at ${latitude},${longitude}`); // Too noisy
        return JSON.parse(cached);
      }
    } catch (err: any) {
      this.logger.warn(`Redis cache error: ${err.message}`);
    }

    return this.dedupe(cacheKey, async () => {
      try {
        const data = await this.requestWithRetry<OpenMeteoResponse>(
          "/forecast",
          {
            params: {
              latitude,
              longitude,
              forecast_days: forecastDays,
              hourly: [
                "temperature_2m",
                "precipitation",
                "rain",
                "snowfall",
                "weathercode",
                "windspeed_10m",
              ].join(","),
              timezone: "auto",
            },
          },
        );

        const result = this.transformHourlyResponse(data);

        // Cache the successful result (jittered TTL to avoid synchronized expiry)
        try {
          const ttl =
            this.CACHE_TTL + Math.floor(Math.random() * this.CACHE_TTL_JITTER);
          await this.redis.set(cacheKey, JSON.stringify(result), "EX", ttl);
        } catch (err: any) {
          this.logger.warn(`Failed to cache weather response: ${err.message}`);
        }

        return result;
      } catch (error: unknown) {
        // Enhanced error logging with context
        if (axios.isAxiosError(error)) {
          const details = {
            url: error.config?.url,
            params: { latitude, longitude, forecast_days: forecastDays },
            status: error.response?.status,
            statusText: error.response?.statusText,
            code: error.code,
            message: error.message,
          };

          this.logger.error(
            `Open-Meteo API request failed: ${JSON.stringify(details)}`,
          );

          // More specific error messages
          if (error.code === "ENOTFOUND" || error.code === "EAI_AGAIN") {
            throw new Error(
              `Open-Meteo API: DNS resolution failed (lat: ${latitude}, lon: ${longitude})`,
            );
          } else if (
            error.code === "ETIMEDOUT" ||
            error.code === "ECONNABORTED"
          ) {
            throw new Error(
              `Open-Meteo API: Request timeout (lat: ${latitude}, lon: ${longitude})`,
            );
          } else if (error.response?.status) {
            throw new Error(
              `Open-Meteo API: HTTP ${error.response.status} ${error.response.statusText}`,
            );
          } else {
            throw new Error(
              `Open-Meteo API: ${error.message} (lat: ${latitude}, lon: ${longitude})`,
            );
          }
        }

        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.error(
          `Failed to fetch hourly weather data: ${errorMessage}`,
          error instanceof Error ? error.stack : undefined,
        );
        throw new Error(`Open-Meteo API error: ${errorMessage}`);
      }
    });
  }

  /**
   * Fetch minutely (15-min resolution) nowcast for a location.
   *
   * Returns precipitation in 15-minute steps for the next few hours.
   * Used to derive short-term alerts ("rain starts in X min, ends in Y min").
   *
   * The result is cached for 15 min, matching Open-Meteo's upstream update
   * cadence and the per-park nowcast cache.
   *
   * @param latitude - Location latitude
   * @param longitude - Location longitude
   * @param steps - Number of 15-min steps (default 24 = 6 hours)
   * @returns Minutely nowcast data
   */
  async getMinutelyNowcast(
    latitude: number,
    longitude: number,
    steps: number = 24,
  ): Promise<MinutelyNowcastResponse> {
    const latR = Math.round(latitude * 100) / 100;
    const lonR = Math.round(longitude * 100) / 100;
    const cacheKey = `weather:nowcast:${latR}:${lonR}:${steps}`;

    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    } catch {
      // Cache miss is fine
    }

    return this.dedupe(cacheKey, async () => {
      try {
        const data = await this.requestWithRetry<OpenMeteoResponse>(
          "/forecast",
          {
            params: {
              latitude,
              longitude,
              minutely_15: [
                "precipitation",
                "precipitation_probability",
                "snowfall",
                "weather_code",
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_gusts_10m",
                "visibility",
              ].join(","),
              forecast_minutely_15: steps,
              current: [
                "temperature_2m",
                "apparent_temperature",
                "relative_humidity_2m",
                "precipitation",
                "weather_code",
                "is_day",
                "wind_speed_10m",
                "wind_gusts_10m",
              ].join(","),
              daily: ["temperature_2m_max", "temperature_2m_min"].join(","),
              forecast_days: 1,
              timezone: "auto",
            },
          },
        );

        const result = this.transformNowcastResponse(data);

        try {
          await this.redis.set(
            cacheKey,
            JSON.stringify(result),
            "EX",
            this.NOWCAST_CACHE_TTL,
          );
        } catch {
          // Cache write failure is non-critical
        }

        return result;
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.error(
          `Failed to fetch nowcast data: ${errorMessage}`,
          error instanceof Error ? error.stack : undefined,
        );
        throw new Error(`Open-Meteo API error: ${errorMessage}`);
      }
    });
  }

  /**
   * Transform Open-Meteo API response to our format
   */
  private transformResponse(data: OpenMeteoResponse): DailyWeatherResponse {
    const { daily, current } = data;

    if (!daily || !daily.time) {
      throw new Error("Invalid Open-Meteo response: missing daily data");
    }

    const days: DailyWeather[] = daily.time.map((date, index) => ({
      date,
      temperatureMax: daily.temperature_2m_max?.[index] ?? null,
      temperatureMin: daily.temperature_2m_min?.[index] ?? null,
      precipitationSum: daily.precipitation_sum?.[index] ?? null,
      rainSum: daily.rain_sum?.[index] ?? null,
      snowfallSum: daily.snowfall_sum?.[index] ?? null,
      weatherCode: daily.weathercode?.[index] ?? null,
      windSpeedMax: daily.windspeed_10m_max?.[index] ?? null,
    }));

    const currentConditions: CurrentConditions | null = current
      ? {
          temperature: current.temperature_2m ?? null,
          apparentTemperature: current.apparent_temperature ?? null,
          humidity:
            current.relative_humidity_2m != null
              ? Math.round(current.relative_humidity_2m)
              : null,
          weatherCode: current.weather_code ?? null,
          windSpeed: current.wind_speed_10m ?? null,
          isDay: current.is_day != null ? current.is_day === 1 : null,
          observedAt: current.time,
        }
      : null;

    return { days, current: currentConditions };
  }

  private transformNowcastResponse(
    data: OpenMeteoResponse,
  ): MinutelyNowcastResponse {
    const { minutely_15: m, current, daily } = data;

    if (!m || !m.time) {
      throw new Error("Invalid Open-Meteo response: missing minutely_15 data");
    }

    const steps: NowcastStep[] = m.time.map((time, index) => ({
      time,
      precipitation: m.precipitation?.[index] ?? null,
      precipitationProbability: m.precipitation_probability?.[index] ?? null,
      snowfall: m.snowfall?.[index] ?? null,
      weatherCode: m.weather_code?.[index] ?? null,
      windSpeed: m.wind_speed_10m?.[index] ?? null,
      windDirection: m.wind_direction_10m?.[index] ?? null,
      windGusts: m.wind_gusts_10m?.[index] ?? null,
      visibility: m.visibility?.[index] ?? null,
    }));

    // `daily` is requested with forecast_days=1 → exactly one entry for today.
    const todayMax = daily?.temperature_2m_max?.[0] ?? null;
    const todayMin = daily?.temperature_2m_min?.[0] ?? null;

    return {
      steps,
      current: current
        ? {
            time: current.time,
            temperature: current.temperature_2m ?? null,
            apparentTemperature: current.apparent_temperature ?? null,
            humidity:
              current.relative_humidity_2m != null
                ? Math.round(current.relative_humidity_2m)
                : null,
            precipitation: current.precipitation ?? null,
            weatherCode: current.weather_code ?? null,
            isDay: current.is_day != null ? current.is_day === 1 : null,
            windSpeed: current.wind_speed_10m ?? null,
            windGusts: current.wind_gusts_10m ?? null,
          }
        : null,
      daily: { temperatureMax: todayMax, temperatureMin: todayMin },
    };
  }

  private transformHourlyResponse(
    data: OpenMeteoResponse,
  ): HourlyWeatherResponse {
    const { hourly } = data;

    if (!hourly || !hourly.time) {
      throw new Error("Invalid Open-Meteo response: missing hourly data");
    }

    const hours: HourlyWeather[] = hourly.time.map((time, index) => ({
      time,
      temperature: hourly.temperature_2m?.[index] ?? null,
      precipitation: hourly.precipitation?.[index] ?? null,
      rain: hourly.rain?.[index] ?? null,
      snowfall: hourly.snowfall?.[index] ?? null,
      weatherCode: hourly.weathercode?.[index] ?? null,
      windSpeed: hourly.windspeed_10m?.[index] ?? null,
    }));

    return { hours };
  }
}

/**
 * Open-Meteo API Response
 */
interface OpenMeteoResponse {
  latitude: number;
  longitude: number;
  timezone: string;
  current?: {
    time: string;
    temperature_2m?: number | null;
    apparent_temperature?: number | null;
    relative_humidity_2m?: number | null;
    weather_code?: number | null;
    wind_speed_10m?: number | null;
    wind_gusts_10m?: number | null;
    is_day?: number | null; // 0 or 1
    precipitation?: number | null;
  };
  minutely_15?: {
    time: string[];
    precipitation?: (number | null)[];
    precipitation_probability?: (number | null)[];
    snowfall?: (number | null)[];
    weather_code?: (number | null)[];
    wind_speed_10m?: (number | null)[];
    wind_direction_10m?: (number | null)[];
    wind_gusts_10m?: (number | null)[];
    visibility?: (number | null)[];
  };
  daily: {
    time: string[];
    temperature_2m_max?: (number | null)[];
    temperature_2m_min?: (number | null)[];
    precipitation_sum?: (number | null)[];
    rain_sum?: (number | null)[];
    snowfall_sum?: (number | null)[];
    weathercode?: (number | null)[];
    windspeed_10m_max?: (number | null)[];
  };
  hourly: {
    time: string[];
    temperature_2m?: (number | null)[];
    precipitation?: (number | null)[];
    rain?: (number | null)[];
    snowfall?: (number | null)[];
    weathercode?: (number | null)[];
    windspeed_10m?: (number | null)[];
  };
}

/**
 * Daily Weather Data (our format)
 */
export interface DailyWeather {
  date: string; // YYYY-MM-DD
  temperatureMax: number | null;
  temperatureMin: number | null;
  precipitationSum: number | null;
  rainSum: number | null;
  snowfallSum: number | null;
  weatherCode: number | null;
  windSpeedMax: number | null;
}

export interface CurrentConditions {
  temperature: number | null;
  apparentTemperature: number | null;
  humidity: number | null;
  weatherCode: number | null;
  windSpeed: number | null;
  isDay: boolean | null;
  observedAt: string; // ISO datetime from Open-Meteo
}

export interface DailyWeatherResponse {
  days: DailyWeather[];
  current: CurrentConditions | null;
}

export interface HourlyWeather {
  time: string; // ISO 8601
  temperature: number | null;
  precipitation: number | null;
  rain: number | null;
  snowfall: number | null;
  weatherCode: number | null;
  windSpeed: number | null;
}

export interface HourlyWeatherResponse {
  hours: HourlyWeather[];
}

export interface NowcastStep {
  time: string; // ISO 8601 — start of the 15-min interval, in park's local timezone
  precipitation: number | null; // mm in this 15-min slot
  precipitationProbability: number | null; // 0-100
  snowfall: number | null; // cm in this 15-min slot
  weatherCode: number | null; // WMO code
  windSpeed: number | null; // km/h, sustained
  windDirection: number | null; // degrees (0-360, direction wind comes FROM)
  windGusts: number | null; // km/h, gusts
  visibility: number | null; // meters
}

export interface NowcastCurrent {
  time: string;
  /** Air temperature in °C. */
  temperature: number | null;
  /** "Feels like" temperature in °C. */
  apparentTemperature: number | null;
  /** Relative humidity 0-100 (rounded to whole percent). */
  humidity: number | null;
  precipitation: number | null;
  weatherCode: number | null;
  isDay: boolean | null;
  windSpeed: number | null;
  windGusts: number | null;
}

/** Daily summary for "today" in the park's local timezone. */
export interface NowcastDaily {
  temperatureMax: number | null;
  temperatureMin: number | null;
}

export interface MinutelyNowcastResponse {
  steps: NowcastStep[];
  current: NowcastCurrent | null;
  daily: NowcastDaily;
}
