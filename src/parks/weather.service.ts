import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { WeatherData } from "./entities/weather-data.entity";
import { Park } from "./entities/park.entity";
import {
  DailyWeather,
  OpenMeteoClient,
} from "../external-apis/weather/open-meteo.client";
import { WeatherForecastItemDto } from "../ml/dto/prediction-request.dto";

/**
 * Weather Service
 *
 * Handles weather data storage and retrieval.
 */
@Injectable()
export class WeatherService {
  private readonly logger = new Logger(WeatherService.name);
  private readonly CACHE_TTL_SECONDS = 30 * 60; // 30 minutes
  private readonly HOURLY_CACHE_TTL = 60 * 60; // 1 hour

  constructor(
    @InjectRepository(WeatherData)
    private weatherDataRepository: Repository<WeatherData>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    private openMeteoClient: OpenMeteoClient,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

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

      // Cache result
      await this.redis.set(
        cacheKey,
        JSON.stringify(mappedForecast),
        "EX",
        this.HOURLY_CACHE_TTL,
      );

      return mappedForecast;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Failed to fetch hourly weather for park ${parkId}: ${errorMessage}. Attempting DB fallback...`,
      );

      // Fallback: Try to synthesize hourly data from stored daily data
      try {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const next7Days = new Date(today);
        next7Days.setDate(next7Days.getDate() + 7);

        const dailyData = await this.weatherDataRepository.find({
          where: {
            parkId,
            date: Between(today, next7Days),
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

          this.logger.debug(
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
  ): Promise<number> {
    let savedCount = 0;

    for (const day of weatherData) {
      try {
        // Check if record exists
        const existing = await this.weatherDataRepository.findOne({
          where: {
            parkId,
            date: new Date(day.date),
          },
        });

        if (existing) {
          // Update existing record using composite key
          await this.weatherDataRepository.update(
            { parkId, date: existing.date },
            {
              dataType,
              temperatureMax: day.temperatureMax,
              temperatureMin: day.temperatureMin,
              precipitationSum: day.precipitationSum,
              rainSum: day.rainSum,
              snowfallSum: day.snowfallSum,
              weatherCode: day.weatherCode,
              windSpeedMax: day.windSpeedMax,
            },
          );
        } else {
          // Create new record
          await this.weatherDataRepository.save({
            parkId,
            date: new Date(day.date),
            dataType,
            temperatureMax: day.temperatureMax,
            temperatureMin: day.temperatureMin,
            precipitationSum: day.precipitationSum,
            rainSum: day.rainSum,
            snowfallSum: day.snowfallSum,
            weatherCode: day.weatherCode,
            windSpeedMax: day.windSpeedMax,
          });
        }

        savedCount++;
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.error(
          `Failed to save weather data for ${day.date}: ${errorMessage}`,
        );
      }
    }

    return savedCount;
  }

  /**
   * Get weather data for a park within a date range
   */
  async getWeatherData(
    parkId: string,
    startDate: Date,
    endDate: Date,
  ): Promise<WeatherData[]> {
    return this.weatherDataRepository
      .createQueryBuilder("weather")
      .where("weather.parkId = :parkId", { parkId })
      .andWhere("weather.date >= :startDate", { startDate })
      .andWhere("weather.date <= :endDate", { endDate })
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
   * - date < today
   * - dataType != 'historical'
   *
   * This ensures that past data is properly categorized for ML training.
   *
   * @returns Number of records updated
   */
  async markPastDataAsHistorical(): Promise<number> {
    const today = new Date();
    today.setHours(0, 0, 0, 0); // Start of today

    try {
      const result = await this.weatherDataRepository
        .createQueryBuilder()
        .update(WeatherData)
        .set({ dataType: "historical" })
        .where("date < :today", { today })
        .andWhere("dataType != :historical", { historical: "historical" })
        .execute();

      const updatedCount = result.affected || 0;

      if (updatedCount > 0) {
        this.logger.log(
          `✅ Marked ${updatedCount} past weather records as historical`,
        );
      } else {
      }

      return updatedCount;
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

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const futureDate = new Date(today);
    futureDate.setDate(futureDate.getDate() + 16);

    const allWeather = await this.weatherDataRepository
      .createQueryBuilder("weather")
      .where("weather.parkId = :parkId", { parkId })
      .andWhere("weather.date >= :today", { today })
      .andWhere("weather.date <= :futureDate", { futureDate })
      .orderBy("weather.date", "ASC")
      .getMany();

    // Separate current (today) from forecast (future)
    // Ensure date is converted to Date object if it's a string
    const current =
      allWeather.find((w) => {
        const weatherDate = new Date(w.date);
        return (
          weatherDate.toISOString().split("T")[0] ===
          today.toISOString().split("T")[0]
        );
      }) || null;

    const forecast = allWeather.filter((w) => {
      const weatherDate = new Date(w.date);
      return weatherDate > today;
    });

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
