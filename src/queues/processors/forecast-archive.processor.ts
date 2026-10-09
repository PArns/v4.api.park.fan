import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ForecastArchiveService } from "../../parks/services/forecast-archive.service";
import { ForecastArchiveScoringService } from "../../parks/services/forecast-archive-scoring.service";

/**
 * The forward archive of served intraday curves (PAR-831).
 *
 * - `capture` runs every hour at :05 and captures each park whose own clock
 *   shows an origin hour (06 daily; 10–18 every two hours intraday). Why it is
 *   hourly and not one job per timezone: the origin is a park-local time, and
 *   one hourly job reading every park's clock covers all offsets.
 * - `score` runs daily at 12:00 UTC, when yesterday (UTC) is over in every
 *   park's timezone, scores it against queue_data truth and prunes the archive.
 */
@Processor("forecast-archive")
export class ForecastArchiveProcessor {
  private readonly logger = new Logger(ForecastArchiveProcessor.name);

  constructor(
    private readonly archiveService: ForecastArchiveService,
    private readonly scoringService: ForecastArchiveScoringService,
  ) {}

  @Process("capture")
  async handleCapture(_job: Job): Promise<void> {
    const result = await this.archiveService.captureDue(new Date());
    // Quiet on the hours no park is at an origin.
    if (result.parks > 0 || result.failed > 0) {
      this.logger.log(
        `🗄️  Forward archive: ${result.parks} park(s), ${result.curves} curve rows, ` +
          `${result.parkDays} park-days` +
          (result.failed > 0 ? `, ${result.failed} failed` : ""),
      );
    }
  }

  @Process("score")
  async handleScore(_job: Job): Promise<void> {
    try {
      const { dates } = await this.scoringService.scoreDue(new Date());
      if (dates.length > 0) {
        this.logger.log(`📐 Forward archive scored: ${dates.join(", ")}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Forward archive scoring failed: ${message}`);
      throw error;
    } finally {
      // Retention runs whether or not scoring did: the archive must not grow
      // because yesterday could not be scored.
      try {
        const pruned = await this.archiveService.pruneExpired(new Date());
        if (pruned.curves > 0 || pruned.parkDays > 0) {
          this.logger.log(
            `🧹 Forward archive: pruned ${pruned.curves} curve rows, ${pruned.parkDays} park-days`,
          );
        }
      } catch (error) {
        // 55P03 / 57014: a lock or statement deadline fired — tomorrow retries.
        const message = error instanceof Error ? error.message : String(error);
        this.logger.warn(`Forward archive retention deferred: ${message}`);
      }
    }
  }
}
