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

      // Deduplicate by rounded coordinates (~1km precision) so parks sharing a
      // resort location (e.g. all WDW parks) share one API call via the client cache.
      // We still save weather for every park individually (same data, fast DB writes).
      const coordKey = (p: {
        latitude: number | null;
        longitude: number | null;
      }) =>
        `${Math.round(p.latitude! * 100) / 100},${Math.round(p.longitude! * 100) / 100}`;

      const uniqueCoordParks = new Map<string, (typeof parksWithCoords)[0]>();
      for (const park of parksWithCoords) {
        const key = coordKey(park);
        if (!uniqueCoordParks.has(key)) uniqueCoordParks.set(key, park);
      }

      const uniqueCount = uniqueCoordParks.size;
      const total = parksWithCoords.length;
      this.logger.log(
        `Deduped ${total} parks to ${uniqueCount} unique locations for API calls`,
      );

      // Fetch weather for unique locations first (populates client-side cache)
      let apiIdx = 0;
      for (const [, repPark] of uniqueCoordParks) {
        try {
          await this.openMeteoClient.getDailyWeather(
            repPark.latitude!,
            repPark.longitude!,
            currentOnly ? 1 : 16,
          );
        } catch {
          // Errors will surface again per-park below; ignore here
        }

        apiIdx++;
        if (apiIdx % 10 === 0 || apiIdx === uniqueCount) {
          this.logger.log(
            `API fetch progress: ${apiIdx}/${uniqueCount} unique locations`,
          );
        }

        // Delay between unique API calls (cache hits for same-coord parks skip this)
        const delay = currentOnly ? 500 : 1500;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      // Now save weather for every park (cache hit for shared-coord parks = no extra API call)
      for (const park of parksWithCoords) {
        try {
          const forecastData = await this.openMeteoClient.getDailyWeather(
            park.latitude!,
            park.longitude!,
            currentOnly ? 1 : 16,
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
          this.logger.error(
            `❌ Failed to process weather for ${park.name}: ${errorMessage}`,
          );
        }
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
