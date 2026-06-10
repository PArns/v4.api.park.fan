import { Processor, Process } from "@nestjs/bull";
import { CacheKeys } from "../../common/cache/cache-keys";
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
    const syncType = currentOnly ? "current" : "full";
    const doneKey = `weather:sync:done:${syncType}`;

    // Idempotency guard: skip if a sync of this type already completed within
    // its interval. A crash mid-sync leaves the marker unset, so Bull's
    // stalled-job recovery re-runs it on restart; but a crash AFTER completion
    // finds the marker and skips — no redundant Open-Meteo calls.
    const alreadyDone = await this.redis.get(doneKey);
    if (alreadyDone) {
      const ttl = await this.redis.ttl(doneKey);
      this.logger.log(
        `⏭️  Skipping ${syncType} weather sync — already completed at ${alreadyDone} (next eligible in ${ttl}s).`,
      );
      return;
    }

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

      // Fetch weather for unique locations first (populates client-side cache).
      // Batched Promise.all instead of strictly sequential calls: with ~300
      // unique locations the old 1 call + fixed delay loop took several
      // minutes of wall time. The batch delay keeps the request rate well
      // under Open-Meteo's limit; the client additionally handles 429s.
      const API_BATCH_SIZE = 5;
      const apiBatchDelay = currentOnly ? 1000 : 2000;
      const uniqueParks = [...uniqueCoordParks.values()];
      for (let i = 0; i < uniqueParks.length; i += API_BATCH_SIZE) {
        const batch = uniqueParks.slice(i, i + API_BATCH_SIZE);
        await Promise.all(
          batch.map((repPark) =>
            this.openMeteoClient
              .getDailyWeather(
                repPark.latitude!,
                repPark.longitude!,
                currentOnly ? 1 : 16,
              )
              // Errors will surface again per-park below; ignore here
              .catch(() => undefined),
          ),
        );

        const fetched = Math.min(i + API_BATCH_SIZE, uniqueCount);
        if (fetched % 50 === 0 || fetched === uniqueCount) {
          this.logger.log(
            `API fetch progress: ${fetched}/${uniqueCount} unique locations`,
          );
        }

        if (i + API_BATCH_SIZE < uniqueParks.length) {
          await new Promise((resolve) => setTimeout(resolve, apiBatchDelay));
        }
      }

      // Now save weather for every park (cache hit for shared-coord parks = no
      // extra API call). DB-bound, so a larger batch size is fine.
      const SAVE_BATCH_SIZE = 10;
      for (let i = 0; i < parksWithCoords.length; i += SAVE_BATCH_SIZE) {
        const batch = parksWithCoords.slice(i, i + SAVE_BATCH_SIZE);
        await Promise.all(
          batch.map(async (park) => {
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

              await this.redis.del(CacheKeys.weatherForecast(park.id));
            } catch (error: unknown) {
              const errorMessage =
                error instanceof Error ? error.message : String(error);
              this.logger.error(
                `❌ Failed to process weather for ${park.name}: ${errorMessage}`,
              );
            }
          }),
        );
      }

      this.logger.log(
        `✅ Weather sync complete! Saved ${totalCurrent} current, ${totalForecast} forecast records`,
      );

      // Mark this sync type as done. TTL sits just under the cron interval
      // (full=12h, current=6h) so the next scheduled run always proceeds, but a
      // restart in between skips the redundant re-sync.
      const doneTtl = currentOnly ? 5 * 60 * 60 : 11 * 60 * 60;
      await this.redis.set(doneKey, new Date().toISOString(), "EX", doneTtl);
    } catch (error: unknown) {
      this.logger.error("❌ Weather sync failed", error);
      throw error;
    }
  }
}
