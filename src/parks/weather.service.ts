import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { WeatherData } from "./entities/weather-data.entity";
import { DailyWeather } from "../external-apis/weather/open-meteo.client";

/**
 * Weather Service
 *
 * Handles weather data storage and retrieval.
 */
@Injectable()
export class WeatherService {
  private readonly logger = new Logger(WeatherService.name);
  private readonly CACHE_TTL_SECONDS = 30 * 60; // 30 minutes

  constructor(
    @InjectRepository(WeatherData)
    private weatherDataRepository: Repository<WeatherData>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

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
