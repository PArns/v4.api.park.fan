import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { WeatherData } from "./entities/weather-data.entity";
import { Park } from "./entities/park.entity";
import {
  CurrentConditions,
  DailyWeather,
  MinutelyNowcastResponse,
  NowcastStep,
  OpenMeteoClient,
} from "../external-apis/weather/open-meteo.client";
import { WeatherForecastItemDto } from "../ml/dto/prediction-request.dto";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import { addDays } from "date-fns";
import { fromZonedTime } from "date-fns-tz";

/**
 * Weather Service
 *
 * Handles weather data storage and retrieval.
 */
/** Qualitative rain intensity bucket derived from mm-per-15-min. */
export type RainIntensity = "light" | "moderate" | "heavy";

export interface ParkNowcast {
  /**
   * ISO timestamp when the upstream forecast was last fetched. Acts as
   * the data-freshness indicator and as the reference time for every
   * `current*` field below. Stable for the lifetime of a cache entry.
   */
  observedAt: string;
  /**
   * ISO timestamp at which the cached forecast expires and a fresh
   * fetch will be performed (= `observedAt` + 15 min). Clients render
   * this as the "next update" time.
   */
  nextUpdateAt: string;

  // ---- Snapshot at `observedAt` -----------------------------------------
  // All `current*` fields describe the state at the moment the upstream
  // forecast was fetched (`observedAt`), not the moment a cached
  // response is read. Clients that need true real-time state can derive
  // it from the absolute event timestamps below or from `steps`.

  /** Whether the park was raining at `observedAt`. */
  currentlyRaining: boolean;
  /** Air temperature in °C at `observedAt`. */
  currentTemperatureC: number | null;
  /** "Feels like" temperature in °C at `observedAt` (combines wind chill / humidity). */
  currentApparentTemperatureC: number | null;
  /** Relative humidity 0-100 (whole percent) at `observedAt`. */
  currentHumidity: number | null;
  /** Precipitation in mm for the slot containing `observedAt`. */
  currentPrecipitationMm: number | null;
  /** Qualitative bucket for the rain falling right now, classified from `currentPrecipitationMm`. Null when not raining. Unlike `rainStartsIntensity` this is populated during active rain. */
  currentRainIntensity: RainIntensity | null;
  /** WMO weather code for the slot containing `observedAt`. Use this to pick a weather icon. */
  currentWeatherCode: number | null;
  /** Whether it is daytime at `observedAt`. Drives day/night icon variants. */
  isDay: boolean | null;
  /** Sustained wind speed in km/h at `observedAt`. */
  currentWindSpeedKmh: number | null;
  /** Wind direction in degrees (0-360, the direction the wind blows FROM) at `observedAt`. */
  currentWindDirectionDeg: number | null;
  /** Wind gusts in km/h at `observedAt`. */
  currentWindGustsKmh: number | null;
  /** Snowfall in cm for the slot containing `observedAt`. */
  currentSnowfallCm: number | null;
  /** Horizontal visibility in metres at `observedAt`. Low values indicate fog/haze. */
  currentVisibilityM: number | null;

  // ---- Today's daily summary --------------------------------------------

  /** Forecast high for "today" in the park's local timezone, in °C. */
  temperatureMaxC: number | null;
  /** Forecast low for "today" in the park's local timezone, in °C. */
  temperatureMinC: number | null;

  // ---- Absolute event timestamps (stable across the cache window) -------

  /** ISO timestamp when rain is next expected to start. Null if already raining at `observedAt` or no rain in the window. */
  rainStartsAt: string | null;
  /** Precipitation in mm for the first rainy slot (intensity at start). */
  rainStartsIntensityMm: number | null;
  /** Qualitative bucket for the starting rain intensity. */
  rainStartsIntensity: RainIntensity | null;
  /** ISO timestamp when rain is expected to stop. Null if no rain or rain continues beyond the window. */
  rainEndsAt: string | null;

  /** ISO timestamp of the next thunderstorm slot (WMO 95/96/99). Null if none in window. */
  thunderstormStartsAt: string | null;
  /** ISO timestamp when the thunderstorm block is expected to clear. Null if none in window or it continues beyond the window. */
  thunderstormEndsAt: string | null;

  /** ISO timestamp of the next hail slot (WMO 96/99). Null if no hail in window. */
  hailStartsAt: string | null;
  /** ISO timestamp when the hail block is expected to clear. Null if none in window or it continues beyond the window. */
  hailEndsAt: string | null;

  /** ISO timestamp of the next slot with storm-force wind gusts (≥ 75 km/h, Beaufort 9). Null if none in window. */
  stormStartsAt: string | null;
  /** ISO timestamp when storm-force wind gusts are expected to die down. Null if none in window or they continue beyond the window. */
  stormEndsAt: string | null;

  /** Peak wind gust forecasted within the nowcast window, in km/h. */
  peakWindGustsKmh: number | null;
  /** Raw 15-min forecast series for clients that want to render their own chart. */
  steps: NowcastStep[];
}

@Injectable()
export class WeatherService {
  private readonly logger = new Logger(WeatherService.name);
  private readonly CACHE_TTL_SECONDS = 2 * 60 * 60; // 2 hours (weather sync runs every 12h)
  // Hourly forecast barely shifts hour-to-hour and live conditions are served
  // by the nowcast (15-min cache), so we refresh only every ~6h to respect the
  // Open-Meteo quota. Jitter spreads expiry so all parks don't refetch at once.
  private readonly HOURLY_CACHE_TTL = 6 * 60 * 60; // 6 hours
  private readonly HOURLY_CACHE_JITTER = 60 * 60; // up to +1h random spread
  private readonly NOWCAST_CACHE_TTL = 15 * 60; // 15 minutes

  /** Precipitation threshold (mm per 15-min slot) considered "raining". */
  private static readonly RAIN_THRESHOLD_MM = 0.1;
  /**
   * Rain intensity buckets in mm per 15-min slot. Derived from the standard
   * meteorological mm/h thresholds (light < 2.5, moderate < 7.6, heavy ≥ 7.6)
   * divided by 4 to match a 15-min window.
   */
  private static readonly RAIN_INTENSITY_MODERATE_MM = 2.5 / 4; // 0.625 mm / 15 min
  private static readonly RAIN_INTENSITY_HEAVY_MM = 7.6 / 4; // 1.9 mm / 15 min
  /** WMO codes representing thunderstorms (95 = no hail, 96/99 = with hail). */
  private static readonly THUNDERSTORM_CODES = new Set([95, 96, 99]);
  /** WMO codes representing hail (subset of thunderstorms). */
  private static readonly HAIL_CODES = new Set([96, 99]);
  /**
   * Wind-gust threshold (km/h) that triggers a storm alert.
   * 75 km/h ≈ Beaufort 9 ("strong gale") — most outdoor rides close well
   * below this, so this is a conservative ceiling.
   */
  private static readonly STORM_GUST_KMH = 75;

  constructor(
    @InjectRepository(WeatherData)
    private weatherDataRepository: Repository<WeatherData>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    private openMeteoClient: OpenMeteoClient,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Get short-term nowcast alert for a park.
   *
   * Returns an actionable summary derived from Open-Meteo's `minutely_15`
   * forecast: whether it was raining at fetch time, when rain is
   * expected to start/stop, and whether a thunderstorm / hail / storm
   * is imminent (with a matching ends-time).
   *
   * Cache strategy: the entire derived response is cached per-park for
   * 15 minutes (matches Open-Meteo's data resolution). This is safe
   * because every "event" field is an absolute ISO timestamp that does
   * not drift with wall-clock time, and `current*` snapshot fields are
   * explicitly defined as "the state at `observedAt`" — not "right
   * now". Clients that need the live state can compute it from the
   * absolute timestamps or from `steps[]`.
   *
   * @param parkId - Park ID
   * @returns Nowcast result, or `null` if coordinates are missing
   */
  async getNowcast(parkId: string): Promise<ParkNowcast | null> {
    const cacheKey = `weather:nowcast:park:${parkId}`;

    // Cache hit → the cached response is the response. No re-derive.
    try {
      const cachedStr = await this.redis.get(cacheKey);
      if (cachedStr) return JSON.parse(cachedStr) as ParkNowcast;
    } catch (err) {
      this.logger.warn(`Redis cache error: ${err}`);
    }

    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["latitude", "longitude", "timezone"],
    });

    if (!park || park.latitude == null || park.longitude == null) {
      this.logger.warn(
        `Cannot fetch nowcast for park ${parkId}: Missing coordinates`,
      );
      return null;
    }

    let raw: MinutelyNowcastResponse;
    try {
      raw = await this.openMeteoClient.getMinutelyNowcast(
        park.latitude,
        park.longitude,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Failed to fetch nowcast for park ${parkId}: ${errorMessage}`,
      );
      return null;
    }

    const result = this.deriveNowcastAlert(
      raw,
      park.timezone || "UTC",
      new Date().toISOString(),
    );

    try {
      await this.redis.set(
        cacheKey,
        JSON.stringify(result),
        "EX",
        this.NOWCAST_CACHE_TTL,
      );
    } catch (err) {
      this.logger.warn(`Failed to cache nowcast response: ${err}`);
    }

    return result;
  }

  /**
   * Derive an actionable nowcast alert from the raw 15-min step series.
   *
   * `fetchedAt` is the single time reference for everything in the
   * returned object: the slot containing `fetchedAt` is treated as
   * "current", and all event lookups (rainStartsAt, rainEndsAt, …)
   * search the steps that are still future relative to `fetchedAt`.
   * That makes the response immutable for the cache TTL — no field
   * changes meaning depending on when it is read.
   */
  private deriveNowcastAlert(
    raw: MinutelyNowcastResponse,
    timezone: string,
    fetchedAt: string,
  ): ParkNowcast {
    const SLOT_MS = 15 * 60 * 1000;
    const fetchedMs = new Date(fetchedAt).getTime();

    const steps = raw.steps.map((s) => ({
      ...s,
      // Open-Meteo returns naive local-time strings ("2026-05-21T14:00"); treat
      // them as instants in the park's timezone for correct cross-timezone math.
      timeMs: this.parseLocalIso(s.time, timezone),
    }));

    const isRain = (s: NowcastStep) =>
      (s.precipitation ?? 0) >= WeatherService.RAIN_THRESHOLD_MM;
    const isThunder = (s: NowcastStep) =>
      s.weatherCode != null &&
      WeatherService.THUNDERSTORM_CODES.has(s.weatherCode);
    const isHail = (s: NowcastStep) =>
      s.weatherCode != null && WeatherService.HAIL_CODES.has(s.weatherCode);
    const isStorm = (s: NowcastStep) =>
      (s.windGusts ?? 0) >= WeatherService.STORM_GUST_KMH;

    const future = steps.filter((s) => s.timeMs + SLOT_MS > fetchedMs);

    // The "current" slot is the one whose 15-min window contains `fetchedAt`.
    const currentSlot = steps.find(
      (s) => s.timeMs <= fetchedMs && fetchedMs < s.timeMs + SLOT_MS,
    );

    const currentlyRaining = currentSlot ? isRain(currentSlot) : false;

    let rainStartsAt: string | null = null;
    let rainEndsAt: string | null = null;
    let rainStartsIntensityMm: number | null = null;
    let rainStartsIntensity: RainIntensity | null = null;

    if (currentlyRaining) {
      // Find first future slot that is dry.
      const dry = future.find((s) => !isRain(s));
      rainEndsAt = dry ? new Date(dry.timeMs).toISOString() : null;
    } else {
      // Find first future slot that has rain.
      const rainy = future.find((s) => isRain(s));
      if (rainy) {
        rainStartsAt = new Date(rainy.timeMs).toISOString();
        rainStartsIntensityMm = rainy.precipitation ?? null;
        rainStartsIntensity = this.classifyRainIntensity(rainy.precipitation);
        // And when it stops again (first dry slot after rainy).
        const dryAgain = future.find(
          (s) => s.timeMs > rainy.timeMs && !isRain(s),
        );
        rainEndsAt = dryAgain ? new Date(dryAgain.timeMs).toISOString() : null;
      }
    }

    /**
     * For thunderstorm/hail/storm: find the first matching slot and then
     * the first non-matching slot after it within the same window so we
     * can show "starts at X, ends at Y".
     */
    const findBlock = (
      predicate: (s: NowcastStep & { timeMs: number }) => boolean,
    ): { startsAt: string | null; endsAt: string | null } => {
      const start = future.find(predicate);
      if (!start) return { startsAt: null, endsAt: null };
      const end = future.find((s) => s.timeMs > start.timeMs && !predicate(s));
      return {
        startsAt: new Date(start.timeMs).toISOString(),
        endsAt: end ? new Date(end.timeMs).toISOString() : null,
      };
    };

    const thunder = findBlock(isThunder);
    const hail = findBlock(isHail);
    const storm = findBlock(isStorm);

    const peakWindGustsKmh = future.reduce<number | null>((max, s) => {
      if (s.windGusts == null) return max;
      return max == null || s.windGusts > max ? s.windGusts : max;
    }, null);

    return {
      observedAt: new Date(fetchedMs).toISOString(),
      nextUpdateAt: new Date(
        fetchedMs + this.NOWCAST_CACHE_TTL * 1000,
      ).toISOString(),
      currentlyRaining,
      currentTemperatureC: raw.current?.temperature ?? null,
      currentApparentTemperatureC: raw.current?.apparentTemperature ?? null,
      currentHumidity: raw.current?.humidity ?? null,
      currentPrecipitationMm: currentSlot?.precipitation ?? null,
      currentRainIntensity: this.classifyRainIntensity(
        currentSlot?.precipitation,
      ),
      currentWeatherCode:
        currentSlot?.weatherCode ?? raw.current?.weatherCode ?? null,
      isDay: raw.current?.isDay ?? null,
      currentWindSpeedKmh:
        currentSlot?.windSpeed ?? raw.current?.windSpeed ?? null,
      currentWindDirectionDeg: currentSlot?.windDirection ?? null,
      currentWindGustsKmh:
        currentSlot?.windGusts ?? raw.current?.windGusts ?? null,
      currentSnowfallCm: currentSlot?.snowfall ?? null,
      currentVisibilityM: currentSlot?.visibility ?? null,
      temperatureMaxC: raw.daily?.temperatureMax ?? null,
      temperatureMinC: raw.daily?.temperatureMin ?? null,
      rainStartsAt,
      rainStartsIntensityMm,
      rainStartsIntensity,
      rainEndsAt,
      thunderstormStartsAt: thunder.startsAt,
      thunderstormEndsAt: thunder.endsAt,
      hailStartsAt: hail.startsAt,
      hailEndsAt: hail.endsAt,
      stormStartsAt: storm.startsAt,
      stormEndsAt: storm.endsAt,
      peakWindGustsKmh,
      steps: steps.map(({ timeMs: _ignored, ...rest }) => rest),
    };
  }

  /**
   * Map a 15-min-slot precipitation amount to a qualitative bucket.
   * Returns `null` when below the rain threshold.
   */
  private classifyRainIntensity(
    precipitationMm: number | null | undefined,
  ): RainIntensity | null {
    if (
      precipitationMm == null ||
      precipitationMm < WeatherService.RAIN_THRESHOLD_MM
    ) {
      return null;
    }
    if (precipitationMm >= WeatherService.RAIN_INTENSITY_HEAVY_MM)
      return "heavy";
    if (precipitationMm >= WeatherService.RAIN_INTENSITY_MODERATE_MM)
      return "moderate";
    return "light";
  }

  /**
   * Open-Meteo returns naive local timestamps like "2026-05-21T14:00".
   * Convert them to absolute epoch ms in the given IANA timezone.
   */
  private parseLocalIso(local: string, timezone: string): number {
    // Use Intl to figure out the UTC offset that applies to `local` in `timezone`.
    // We construct a UTC instant from the literal, then ask: at that instant, what
    // wall-clock time does the zone show? The difference is the offset to subtract.
    const asUtc = new Date(`${local}Z`).getTime();
    const dtf = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    const parts = dtf
      .formatToParts(new Date(asUtc))
      .reduce<Record<string, string>>((acc, p) => {
        if (p.type !== "literal") acc[p.type] = p.value;
        return acc;
      }, {});
    const wallMs = Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      Number(parts.hour) === 24 ? 0 : Number(parts.hour),
      Number(parts.minute),
      Number(parts.second),
    );
    const offsetMs = wallMs - asUtc;
    return asUtc - offsetMs;
  }

  /**
   * Get hourly weather forecast for a park
   * Used by ML Service for predictions
   *
   * cached by parkId to reduce API calls
   */
  async getHourlyForecast(parkId: string): Promise<WeatherForecastItemDto[]> {
    const cacheKey = `weather:hourly:park:${parkId}`;

    // Try cache first
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached) {
        return JSON.parse(cached);
      }
    } catch (err) {
      this.logger.warn(`Redis cache error: ${err}`);
      // Continue to fetch
    }

    try {
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["latitude", "longitude"],
      });

      if (!park || !park.latitude || !park.longitude) {
        this.logger.warn(
          `Cannot fetch weather for park ${parkId}: Missing coordinates`,
        );
        return [];
      }

      const forecast = await this.openMeteoClient.getHourlyForecast(
        park.latitude,
        park.longitude,
      );

      // Map to DTO
      const mappedForecast: WeatherForecastItemDto[] = forecast.hours.map(
        (h) => ({
          time: h.time,
          temperature: h.temperature,
          precipitation: h.precipitation,
          rain: h.rain,
          snowfall: h.snowfall,
          weatherCode: h.weatherCode,
          windSpeed: h.windSpeed,
        }),
      );

      // Cache result (jittered TTL so parks don't all expire in the same minute)
      const ttl =
        this.HOURLY_CACHE_TTL +
        Math.floor(Math.random() * this.HOURLY_CACHE_JITTER);
      await this.redis.set(cacheKey, JSON.stringify(mappedForecast), "EX", ttl);

      return mappedForecast;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Failed to fetch hourly weather for park ${parkId}: ${errorMessage}. Attempting DB fallback...`,
      );

      // Fallback: Try to synthesize hourly data from stored daily data (use park timezone)
      try {
        const park = await this.parkRepository.findOne({
          where: { id: parkId },
          select: ["timezone"],
        });
        const tz = park?.timezone || "UTC";
        const today = getCurrentDateInTimezone(tz);
        const todayStart = fromZonedTime(`${today}T00:00:00`, tz);
        const next7Days = addDays(todayStart, 7);

        const dailyData = await this.weatherDataRepository.find({
          where: {
            parkId,
            date: Between(todayStart, next7Days),
          },
          order: { date: "ASC" },
        });

        if (dailyData.length > 0) {
          const synthesizedForecast: WeatherForecastItemDto[] = [];

          for (const day of dailyData) {
            // Create 24 hours for each day
            const dateStr =
              day.date instanceof Date
                ? day.date.toISOString().split("T")[0]
                : day.date; // Handle string/date discrepancies

            for (let hour = 0; hour < 24; hour++) {
              // Simple sinusoidal interpolation for temperature
              // Min at 4am, Max at 2pm (14:00)
              const minTemp = Number(day.temperatureMin || 15);
              const maxTemp = Number(day.temperatureMax || 25);
              const tempRange = maxTemp - minTemp;

              // Shift curve so peak is at 14:00
              // cos((h - 14) / 12 * PI) gives peak at 14, trough at 2/26
              // simplified interpolation
              const normalizedTime = ((hour - 14) / 12) * Math.PI;
              const temp =
                Math.round(
                  ((Math.cos(normalizedTime) * -0.5 + 0.5) * tempRange +
                    minTemp) *
                    10,
                ) / 10;

              synthesizedForecast.push({
                time: `${dateStr}T${hour.toString().padStart(2, "0")}:00`,
                temperature: temp,
                precipitation: Number(day.precipitationSum || 0) / 24, // Distribute evenly (naive)
                rain: Number(day.rainSum || 0) / 24,
                snowfall: Number(day.snowfallSum || 0) / 24,
                weatherCode: day.weatherCode || 0,
                windSpeed: Number(day.windSpeedMax || 0) / 2, // Assume avg is half max
              });
            }
          }

          this.logger.log(
            `✓ Bootstrapped ${synthesizedForecast.length} hourly weather points from DB for park ${parkId}`,
          );
          return synthesizedForecast;
        }
      } catch (dbError) {
        this.logger.warn(`DB fallback failed: ${dbError}`);
      }

      return [];
    }
  }

  /**
   * Save weather data for a park
   *
   * Uses upsert strategy: Updates existing record or creates new one.
   * This allows updating "current" day weather as it changes throughout the day.
   *
   * @param parkId - Park ID
   * @param weatherData - Daily weather data from API
   * @param dataType - Type of data (historical, current, forecast)
   * @returns Number of records saved
   */
  async saveWeatherData(
    parkId: string,
    weatherData: DailyWeather[],
    dataType: "historical" | "current" | "forecast",
    currentConditions?: CurrentConditions | null,
  ): Promise<number> {
    let savedCount = 0;

    // Get park timezone for correct date interpretation
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park || !park.timezone) {
      this.logger.error(
        `Cannot save weather data: Park ${parkId} not found or has no timezone`,
      );
      return 0;
    }

    // day.date is "YYYY-MM-DD" in the park's local timezone (Open-Meteo uses
    // timezone:"auto"). Store as noon UTC so the PostgreSQL DATE column always
    // extracts the correct calendar day regardless of the session timezone.
    // Midnight UTC would shift European parks (UTC+N) to the previous calendar day.
    const liveFields =
      dataType === "current" && currentConditions
        ? {
            temperatureCurrent: currentConditions.temperature,
            apparentTemperature: currentConditions.apparentTemperature,
            humidity: currentConditions.humidity,
            isDay: currentConditions.isDay,
          }
        : {};

    const records = weatherData.map((day) => ({
      parkId,
      date: new Date(`${day.date}T12:00:00Z`),
      dataType,
      temperatureMax: day.temperatureMax,
      temperatureMin: day.temperatureMin,
      precipitationSum: day.precipitationSum,
      rainSum: day.rainSum,
      snowfallSum: day.snowfallSum,
      weatherCode: day.weatherCode,
      windSpeedMax: day.windSpeedMax,
      ...liveFields,
    }));

    try {
      await this.weatherDataRepository.upsert(records, {
        conflictPaths: ["parkId", "date"],
        skipUpdateIfNoValuesChanged: true,
      });
      savedCount = records.length;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to save weather data for park ${parkId}: ${errorMessage}`,
      );
    }

    return savedCount;
  }

  /**
   * Get weather data for a park within a date range.
   * @param timezone - Optional; when provided (e.g. from calendar with park already loaded), skips park lookup.
   */
  async getWeatherData(
    parkId: string,
    startDate: Date,
    endDate: Date,
    timezone?: string,
  ): Promise<WeatherData[]> {
    let tz = timezone;
    if (!tz) {
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["id", "timezone"],
      });
      if (!park || !park.timezone) {
        this.logger.warn(
          `Cannot query weather data correctly: Park ${parkId} has no timezone`,
        );
        return this.weatherDataRepository
          .createQueryBuilder("weather")
          .where("weather.parkId = :parkId", { parkId })
          .andWhere("weather.date >= :startDate", { startDate })
          .andWhere("weather.date <= :endDate", { endDate })
          .orderBy("weather.date", "ASC")
          .getMany();
      }
      tz = park.timezone;
    }

    const startStr = formatInParkTimezone(startDate, tz);
    const endStr = formatInParkTimezone(endDate, tz);

    // Direct DATE comparison — see getCurrentAndForecast for explanation of
    // why AT TIME ZONE must not be used on a DATE column.
    return this.weatherDataRepository
      .createQueryBuilder("weather")
      .where("weather.parkId = :parkId", { parkId })
      .andWhere("weather.date >= :start", { start: startStr })
      .andWhere("weather.date <= :end", { end: endStr })
      .orderBy("weather.date", "ASC")
      .getMany();
  }

  /**
   * Check if historical data exists for a park
   */
  async hasHistoricalData(parkId: string): Promise<boolean> {
    const count = await this.weatherDataRepository.count({
      where: {
        parkId,
        dataType: "historical",
      },
    });

    return count > 0;
  }

  /**
   * Mark past weather data as historical
   *
   * Updates all weather_data records where:
   * - date < today (in each park's timezone)
   * - dataType != 'historical'
   *
   * This ensures that past data is properly categorized for ML training.
   *
   * @returns Number of records updated
   */
  async markPastDataAsHistorical(): Promise<number> {
    try {
      const parks = await this.parkRepository.find({
        select: ["id", "timezone"],
      });
      let totalUpdated = 0;

      for (const park of parks) {
        const todayStr = getCurrentDateInTimezone(park.timezone || "UTC");
        const result = await this.weatherDataRepository
          .createQueryBuilder()
          .update(WeatherData)
          .set({ dataType: "historical" })
          .where("parkId = :parkId", { parkId: park.id })
          .andWhere("date < :todayStr", { todayStr })
          .andWhere("dataType != :historical", { historical: "historical" })
          .execute();

        totalUpdated += result.affected || 0;
      }

      if (totalUpdated > 0) {
        this.logger.log(
          `✅ Marked ${totalUpdated} past weather records as historical`,
        );
      }

      return totalUpdated;
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to mark past data as historical: ${errorMessage}`,
      );
      throw error;
    }
  }

  /**
   * Get current weather and forecast for a park
   *
   * Returns today's weather + 16-day forecast.
   * Optimized for integrated park endpoint.
   *
   * @param parkId - Park ID
   * @returns Object with current and forecast weather
   */
  async getCurrentAndForecast(parkId: string): Promise<{
    current: WeatherData | null;
    forecast: WeatherData[];
  }> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    const cacheKey = `weather:forecast:${parkId}`;

    // Try cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      // Deserialize dates properly
      const parsed = JSON.parse(cached) as { current: any; forecast: any[] };
      if (parsed.current) parsed.current.date = new Date(parsed.current.date);
      parsed.forecast = parsed.forecast.map((f: any) => ({
        ...f,
        date: new Date(f.date),
      }));
      return parsed as { current: WeatherData | null; forecast: WeatherData[] };
    }

    if (!park) {
      return { current: null, forecast: [] };
    }

    // "today" in park's local timezone (YYYY-MM-DD)
    const todayStr = getCurrentDateInTimezone(park.timezone);

    // +16 days as a date string — no timezone conversion needed since weather.date
    // is a plain DATE column stored via noon-UTC (see saveWeatherData).
    const todayNoon = new Date(`${todayStr}T12:00:00Z`);
    const futureStr = new Date(todayNoon.getTime() + 16 * 24 * 60 * 60 * 1000)
      .toISOString()
      .split("T")[0];

    // Direct DATE comparison using string params — PostgreSQL auto-casts 'YYYY-MM-DD'
    // strings to DATE, so no ::text cast is needed. This avoids the AT TIME ZONE trap
    // on a DATE column: PostgreSQL would cast DATE to midnight-UTC timestamptz first,
    // shifting western parks (UTC-N) back one calendar day and making today disappear.
    const allWeather = await this.weatherDataRepository
      .createQueryBuilder("weather")
      .where("weather.parkId = :parkId", { parkId })
      .andWhere("weather.date >= :start", { start: todayStr })
      .andWhere("weather.date <= :end", { end: futureStr })
      .orderBy("weather.date", "ASC")
      .getMany();

    // weather.date is a DATE column; TypeORM maps it to a midnight-UTC Date object.
    // toISOString().split("T")[0] reliably extracts YYYY-MM-DD regardless of the park
    // timezone because DATE values are stored as noon-UTC (no tz ambiguity).
    const toWeatherDateStr = (d: Date | string): string =>
      d instanceof Date ? d.toISOString().split("T")[0] : String(d);

    const current =
      allWeather.find((w) => toWeatherDateStr(w.date) === todayStr) || null;

    // Filter forecast (future dates) - dates are already in correct timezone from query
    const forecast = allWeather.filter(
      (w) => toWeatherDateStr(w.date) > todayStr,
    );

    const result = { current, forecast };

    // Cache result
    await this.redis.set(
      cacheKey,
      JSON.stringify(result),
      "EX",
      this.CACHE_TTL_SECONDS,
    );

    return result;
  }
}
