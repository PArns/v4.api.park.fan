import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ParksService } from "../../parks/parks.service";
import { WeatherService } from "../../parks/weather.service";
import { OpenMeteoClient } from "../../external-apis/weather/open-meteo.client";

/**
 * Weather Processor
 *
 * Fetches weather data from Open-Meteo API for all parks.
 *
 * Strategy:
 * - Fetch current day + 16-day forecast
 * - Update every 12 hours (0:00 and 12:00)
 *
 * Data stored:
 * - Current: Today's weather (updated throughout day)
 * - Forecast: Next 16 days (updated every 12 hours)
 */
@Processor("weather")
export class WeatherProcessor {
  private readonly logger = new Logger(WeatherProcessor.name);

  constructor(
    private parksService: ParksService,
    private weatherService: WeatherService,
    private openMeteoClient: OpenMeteoClient,
  ) {}

  @Process("fetch-weather")
  async handleSyncWeather(_job: Job): Promise<void> {
    this.logger.log("🌤️  Starting weather sync...");

    try {
      const parks = await this.parksService.findAll();

      if (parks.length === 0) {
        this.logger.warn("No parks found. Run park-metadata sync first.");
        return;
      }

      // Filter parks with valid coordinates
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
          // Fetch current day + 16-day forecast (total 17 days)
          const forecastData = await this.openMeteoClient.getDailyWeather(
            park.latitude!,
            park.longitude!,
            16, // 16 forecast days (includes today)
          );

          // Split into current day and forecast
          const currentDay = forecastData.days.slice(0, 1); // Today
          const forecast = forecastData.days.slice(1); // Next 15 days

          // Save current day
          if (currentDay.length > 0) {
            const savedCurrent = await this.weatherService.saveWeatherData(
              park.id,
              currentDay,
              "current",
            );
            totalCurrent += savedCurrent;
          }

          // Save forecast
          if (forecast.length > 0) {
            const savedForecast = await this.weatherService.saveWeatherData(
              park.id,
              forecast,
              "forecast",
            );
            totalForecast += savedForecast;
          }
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
          // Continue with next park
        }

        // Log progress every 10 parks or at the end
        if ((idx + 1) % 10 === 0 || idx + 1 === total) {
          const percent = Math.round(((idx + 1) / total) * 100);
          this.logger.log(
            `Weather sync progress: ${idx + 1}/${total} (${percent}%) - Last: ${park.name}`,
          );
        }

        // Rate limiting: Wait 1 second between parks (Open-Meteo is free, be nice)
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      this.logger.log(
        `✅ Weather sync complete! Saved ${totalCurrent} current, ${totalForecast} forecast records`,
      );
    } catch (error: unknown) {
      this.logger.error("❌ Weather sync failed", error);
      throw error; // Bull will retry
    }
  }
}
