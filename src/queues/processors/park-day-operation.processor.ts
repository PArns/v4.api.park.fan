import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { subDays } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { ParksService } from "../../parks/parks.service";
import { ParkDayOperationService } from "../../parks/services/park-day-operation.service";

/**
 * Park Day Operation Processor
 *
 * Takes the measured-operation verdict for finished park-local days and stores
 * it in `park_day_operations`: did ride activity say the park operated, against
 * a schedule entry that says it was shut (PAR-697). The rule and its four
 * thresholds are in `src/parks/utils/measured-operation.gate.ts`; this
 * processor only decides WHICH days get judged.
 *
 * Schedule: daily at 4:45 AM. It has to run after the
 * `attraction-hourly-history` rollup at 4:30, because the derived hours stored
 * beside the verdict are read from that rollup — judging yesterday before 4:30
 * would store a verdict without the hours it reconstructed. It also has to be
 * done before `downtime` at 5:00, which reads the same `queue_data` chunks and
 * would decompress them a second time concurrently.
 *
 * Fill of the past: `backfill-park-day-operation` judges an explicit range. The
 * one-time run that seeds the history after this deploy goes through it in
 * portions — a Coolify deploy renews the Postgres container and kills whatever
 * is writing, so the portion size is the damage radius (G-134).
 */
@Processor("park-day-operation")
export class ParkDayOperationProcessor {
  private readonly logger = new Logger(ParkDayOperationProcessor.name);

  constructor(
    private readonly parksService: ParksService,
    private readonly parkDayOperationService: ParkDayOperationService,
  ) {}

  /**
   * Daily job — judges yesterday for every park.
   *
   * Yesterday is read in each park's own timezone, so a park at UTC+13 and one
   * at UTC−7 each get their own last finished calendar day rather than a shared
   * UTC one.
   */
  @Process("calculate-yesterday-park-day-operation")
  async handleYesterday(_job: Job): Promise<void> {
    this.logger.log("📅 Taking yesterday's measured-operation verdicts...");
    const startTime = Date.now();

    const parks = await this.parksService.findAll();
    let parksProcessed = 0;
    let daysJudged = 0;
    let operatingDays = 0;

    for (const park of parks) {
      const yesterday = formatInTimeZone(
        subDays(new Date(), 1),
        park.timezone,
        "yyyy-MM-dd",
      );
      try {
        const result = await this.parkDayOperationService.computeRange(
          park,
          yesterday,
          yesterday,
        );
        daysJudged += result.daysJudged;
        operatingDays += result.operatingDays;
        parksProcessed++;
      } catch (e) {
        // One park's day is one row. A feed that makes its aggregate fail must
        // not cost the other parks their verdict, so this is a warning and the
        // loop goes on — the missing row leaves that day on the operator's
        // entry, which is the documented fallback.
        this.logger.warn(
          `Measured-operation verdict failed for park ${park.slug} on ${yesterday}: ${
            e instanceof Error ? e.message : e
          }`,
        );
      }
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    this.logger.log(
      `✅ Measured-operation verdicts: ${daysJudged} park-days across ${parksProcessed} parks, ${operatingDays} measured as operating, in ${duration}s`,
    );
  }

  /**
   * Fill — judges an explicit park-local date range, optionally for one park.
   *
   * Used to seed the history after the deploy and to re-judge a window after a
   * threshold changes. The caller chooses the portion: one job per slice of
   * days, so an interrupted run leaves a known prefix behind rather than an
   * unknown one.
   */
  @Process("backfill-park-day-operation")
  async handleBackfill(
    job: Job<{ parkId?: string; fromDate: string; toDate: string }>,
  ): Promise<void> {
    const { parkId, fromDate, toDate } = job.data;
    this.logger.log(
      `🔄 Filling measured-operation verdicts ${fromDate} → ${toDate}${
        parkId ? ` (park ${parkId})` : " (all parks)"
      }`,
    );

    const parks = parkId
      ? [await this.parksService.findById(parkId)].filter((p) => p !== null)
      : await this.parksService.findAll();

    let daysJudged = 0;
    let operatingDays = 0;
    for (const park of parks) {
      if (!park) continue;
      try {
        const result = await this.parkDayOperationService.computeRange(
          park,
          fromDate,
          toDate,
        );
        daysJudged += result.daysJudged;
        operatingDays += result.operatingDays;
      } catch (e) {
        this.logger.warn(
          `Fill failed for park ${park.slug} ${fromDate}…${toDate}: ${
            e instanceof Error ? e.message : e
          }`,
        );
      }
    }

    this.logger.log(
      `✅ Fill complete: ${daysJudged} park-days, ${operatingDays} measured as operating`,
    );
  }
}
