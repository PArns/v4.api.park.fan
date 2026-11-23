import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { WeatherClosureService } from "../../parks/services/weather-closure.service";

/**
 * Weekly: relearns, per ride, whether cold or snow shuts it.
 *
 * On the `downtime` queue because it reads the same year of `queue_data` the
 * nightly reconstruction reads, and one queue runs one job at a time; the two
 * never decompress the same chunks at once.
 */
@Processor("downtime")
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
