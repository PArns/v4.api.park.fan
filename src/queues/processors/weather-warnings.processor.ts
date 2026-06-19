import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { WeatherWarningsService } from "../../parks/weather-warnings.service";

/**
 * Weather-warnings processor.
 *
 * Every ~15 minutes, syncs severe-weather warnings (MeteoGate → DWD/MeteoAlarm)
 * for every park whose country is covered, matching each park to the affected
 * area(s). Warnings are non-critical, so a failure is logged and swallowed
 * (no Bull retry storm) — the next cron run recovers.
 */
@Processor("weather-warnings")
export class WeatherWarningsProcessor {
  private readonly logger = new Logger(WeatherWarningsProcessor.name);

  constructor(
    private readonly weatherWarningsService: WeatherWarningsService,
  ) {}

  @Process("sync-warnings")
  async handleSyncWarnings(): Promise<void> {
    const start = Date.now();
    try {
      const r = await this.weatherWarningsService.syncWarnings();
      this.logger.log(
        `🌩️  Weather-warnings sync done in ${((Date.now() - start) / 1000).toFixed(1)}s: ` +
          `${r.activeWarnings} active across ${r.countries} countries → ${r.parkRows} park rows`,
      );
    } catch (err) {
      this.logger.error(
        `Weather-warnings sync failed: ${(err as Error)?.message ?? err}`,
      );
    }
  }
}
