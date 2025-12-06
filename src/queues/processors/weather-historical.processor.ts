import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { WeatherService } from "../../parks/weather.service";

/**
 * Weather Historical Processor
 *
 * Marks past weather data as 'historical' for proper ML training categorization.
 *
 * Strategy:
 * - Runs daily at 5am
 * - Updates all weather_data records where date < today AND dataType != 'historical'
 * - Sets dataType = 'historical' for these records
 *
 * Why needed:
 * - Weather data is initially saved as 'current' or 'forecast'
 * - Once the date passes, it should be marked as 'historical' for ML training
 * - This ensures clear separation between training data and predictions
 */
@Processor("weather-historical")
export class WeatherHistoricalProcessor {
  private readonly logger = new Logger(WeatherHistoricalProcessor.name);

  constructor(private weatherService: WeatherService) {}

  @Process("mark-historical")
  async handleSyncHistoricalWeather(_job: Job): Promise<void> {
    this.logger.log("🕰️  Starting weather historical marking...");

    try {
      const updatedCount = await this.weatherService.markPastDataAsHistorical();

      if (updatedCount > 0) {
        this.logger.log(
          `✅ Weather historical marking complete! Updated ${updatedCount} records`,
        );
      } else {
        this.logger.log(
          "✅ Weather historical marking complete! No updates needed",
        );
      }
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Weather historical marking failed: ${errorMessage}`,
      );
      throw error; // Bull will retry
    }
  }
}
