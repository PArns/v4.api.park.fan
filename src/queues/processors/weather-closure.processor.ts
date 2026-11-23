import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { WeatherClosureService } from "../../parks/services/weather-closure.service";

/**
 * Weekly: relearns, per ride, whether cold or snow shuts it.
 *
 * Its own queue, not `downtime`: Bull starts one fetch loop per handler, so a
 * second handler there would let two reconstructions run at once. Scheduled
 * at 05:45, after the 05:00 reconstruction usually finishes.
 */
@Processor("weather-closure")
export class WeatherClosureProcessor {
  private readonly logger = new Logger(WeatherClosureProcessor.name);

  constructor(private readonly weatherClosure: WeatherClosureService) {}

  @Process("rebuild-weather-closure-profiles")
  async handleRebuild(): Promise<void> {
    const totals = await this.weatherClosure.rebuildAll();
    if (totals.parks === 0 && totals.failed > 0) {
      // Every park failed: the archive is down or blocked. Fail the job so the
      // failing-jobs monitor sees it rather than a quiet success.
      throw new Error(
        `Weather-closure rebuild failed for all ${totals.failed} parks`,
      );
    }
    this.logger.log(`Weather-closure rebuild done: ${JSON.stringify(totals)}`);
  }
}
