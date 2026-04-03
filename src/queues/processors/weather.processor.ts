import { Processor, Process } from "@nestjs/bull";
import { Inject, Logger } from "@nestjs/common";
import { Job } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParksService } from "../../parks/parks.service";
import { WeatherService } from "../../parks/weather.service";
import { OpenMeteoClient } from "../../external-apis/weather/open-meteo.client";

/**
 * Weather Processor
 *
 * Fetches weather data from Open-Meteo API for all parks.
 *
 * Strategy:
 * - current-only (hourly): today's record + live conditions (temperature, humidity, isDay)
 * - full (every 12h): today + 16-day forecast
 *
 * Data stored:
 * - Current: Today's weather + live conditions (updated hourly)
 * - Forecast: Next 16 days (updated every 12 hours)
 */
@Processor("weather")
export class WeatherProcessor {
  private readonly logger = new Logger(WeatherProcessor.name);

  constructor(
    private parksService: ParksService,
    private weatherService: WeatherService,
    private openMeteoClient: OpenMeteoClient,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("fetch-weather")
  async handleSyncWeather(job: Job<{ currentOnly?: boolean }>): Promise<void> {
    const currentOnly = job.data?.currentOnly === true;
    this.logger.log(
      currentOnly
        ? "🌤️  Starting weather sync (current + live conditions only)..."
        : "🌤️  Starting weather sync (full: current + 16-day forecast)...",
    );

    try {
      const parks = await this.parksService.findAll();

      if (parks.length === 0) {
        this.logger.warn("No parks found. Run park-metadata sync first.");
        return;
      }

      const parksWithCoords = parks.filter(
        (park) => park.latitude !== null && park.longitude !== null,
      );

      this.logger.log(
        `Fetching weather data for ${parksWithCoords.length} parks...`,
      );

      let totalCurrent = 0;
      let totalForecast = 0;
      const total = parksWithCoords.length;

      for (let idx = 0; idx < parksWithCoords.length; idx++) {
        const park = parksWithCoords[idx];

        try {
          const forecastData = await this.openMeteoClient.getDailyWeather(
            park.latitude!,
            park.longitude!,
            currentOnly ? 1 : 16, // 1 = today only, 16 = full forecast
          );

          const currentDay = forecastData.days.slice(0, 1);
          const forecast = forecastData.days.slice(1);

          if (currentDay.length > 0) {
            const savedCurrent = await this.weatherService.saveWeatherData(
              park.id,
              currentDay,
              "current",
              forecastData.current,
            );
            totalCurrent += savedCurrent;
          }

          if (!currentOnly && forecast.length > 0) {
            const savedForecast = await this.weatherService.saveWeatherData(
              park.id,
              forecast,
              "forecast",
            );
            totalForecast += savedForecast;
          }

          await this.redis.del(`weather:forecast:${park.id}`);
        } catch (error: unknown) {
          const errorMessage =
            error instanceof Error ? error.message : String(error);
          const errorStack = error instanceof Error ? error.stack : undefined;
          this.logger.error(
            `❌ Failed to process weather for ${park.name}: ${errorMessage}`,
          );
          if (errorStack) {
            this.logger.error(`Stack trace: ${errorStack}`);
          }
        }

        if ((idx + 1) % 10 === 0 || idx + 1 === total) {
          const percent = Math.round(((idx + 1) / total) * 100);
          this.logger.log(
            `Weather sync progress: ${idx + 1}/${total} (${percent}%) - Last: ${park.name}`,
          );
        }

        // Shorter delay for current-only runs (lighter API call, no DB forecast saves)
        const delay = currentOnly ? 300 : 1000;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      this.logger.log(
        `✅ Weather sync complete! Saved ${totalCurrent} current, ${totalForecast} forecast records`,
      );
    } catch (error: unknown) {
      this.logger.error("❌ Weather sync failed", error);
      throw error;
    }
  }
}
