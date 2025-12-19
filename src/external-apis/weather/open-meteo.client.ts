import { Injectable, Logger, Inject } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

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
  private rateLimitedUntil: Date | null = null; // Track when rate limit expires
  private readonly CACHE_TTL = 3600; // 1 hour

  constructor(
    private readonly configService: ConfigService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 10000,
    });
  }

  /**
   * Fetch daily weather forecast for a location
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
    try {
      const response = await this.client.get<OpenMeteoResponse>("/forecast", {
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
          timezone: "auto",
        },
      });

      return this.transformResponse(response.data);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to fetch weather data: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw new Error(`Open-Meteo API error: ${errorMessage}`);
    }
  }

  /**
   * Fetch historical weather data
   *
   * @param latitude - Location latitude
   * @param longitude - Location longitude
   * @param startDate - Start date (YYYY-MM-DD)
   * @param endDate - End date (YYYY-MM-DD)
   * @returns Historical daily weather data
   */
  async getHistoricalWeather(
    latitude: number,
    longitude: number,
    startDate: string,
    endDate: string,
  ): Promise<DailyWeatherResponse> {
    try {
      const response = await this.client.get<OpenMeteoResponse>("/archive", {
        params: {
          latitude,
          longitude,
          start_date: startDate,
          end_date: endDate,
          daily: [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "weathercode",
            "windspeed_10m_max",
          ].join(","),
          timezone: "auto",
        },
      });

      return this.transformResponse(response.data);
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to fetch historical weather data: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw new Error(`Open-Meteo API error: ${errorMessage}`);
    }
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

    // Skip if we're currently rate limited (don't spam the logdur)
    if (this.rateLimitedUntil && new Date() < this.rateLimitedUntil) {
      throw new Error(
        `Open-Meteo API: Rate limited until ${this.rateLimitedUntil.toISOString()}`,
      );
    }

    try {
      const response = await this.client.get<OpenMeteoResponse>("/forecast", {
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
      });

      // Reset rate limit on successful request
      this.rateLimitedUntil = null;

      const result = this.transformHourlyResponse(response.data);

      // Cache the successful result
      try {
        await this.redis.set(
          cacheKey,
          JSON.stringify(result),
          "EX",
          this.CACHE_TTL,
        );
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

        // Special handling for rate limits (429)
        if (error.response?.status === 429) {
          // Set rate limit expiry (60 seconds from now)
          this.rateLimitedUntil = new Date(Date.now() + 60 * 1000);

          this.logger.warn(
            `⏸️  Open-Meteo API rate limited (HTTP 429). Skipping weather requests for 60s.`,
          );

          throw new Error(`Open-Meteo API: HTTP 429 Too Many Requests`);
        }

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
  }

  /**
   * Transform Open-Meteo API response to our format
   */
  private transformResponse(data: OpenMeteoResponse): DailyWeatherResponse {
    const { daily } = data;

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

    return { days };
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

export interface DailyWeatherResponse {
  days: DailyWeather[];
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
