import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AnalyticsService } from "../../analytics/analytics.service";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { subDays } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";

/**
 * Attraction Hourly History Processor
 *
 * Daily rollup of the per-attraction 15-min-slot P90/avg/sampleCount
 * breakdown that the attraction history endpoint used to compute live
 * on every cache miss. Persists one row per attraction per day into
 * `attraction_hourly_history`; the read path can then serve a 30-day
 * chart with one SELECT instead of N PERCENTILE_CONT scans of raw
 * queue_data.
 *
 * Schedule: daily at 4:30 AM (after the P50/P90 baseline crons at 3 AM
 * parks and 4 AM attractions). Yesterday's data is fully settled by
 * then — today's slots are still produced live by the history endpoint
 * for the in-progress day.
 *
 * Backfill: the `backfill-attraction-hourly-history` job re-runs the
 * computation for a given date range (or attraction subset) when the
 * historical window needs to be extended after a deploy.
 */
@Processor("attraction-hourly-history")
export class AttractionHourlyHistoryProcessor {
  private readonly logger = new Logger(AttractionHourlyHistoryProcessor.name);

  constructor(
    private readonly analyticsService: AnalyticsService,
    private readonly parksService: ParksService,
    private readonly attractionsService: AttractionsService,
  ) {}

  /**
   * Daily job — pre-aggregates yesterday's per-attraction hourly slots
   * for every park in one query per park (GROUP BY attractionId +
   * time_slot), then bulk-upserts. Idempotent: re-running the same day
   * just refreshes the JSONB blob.
   */
  @Process("calculate-yesterday-hourly-history")
  async handleYesterdayHourlyHistory(_job: Job): Promise<void> {
    this.logger.log(
      "📊 Computing yesterday's per-attraction hourly history...",
    );
    const startTime = Date.now();

    try {
      const parks = await this.parksService.findAll();
      let parksProcessed = 0;
      let rowsWritten = 0;

      for (const park of parks) {
        try {
          const yesterdayStr = formatInTimeZone(
            subDays(new Date(), 1),
            park.timezone,
            "yyyy-MM-dd",
          );

          const written = await this.aggregateParkForDate(
            park.id,
            yesterdayStr,
            park.timezone,
          );
          rowsWritten += written;
          parksProcessed++;
        } catch (e) {
          this.logger.warn(
            `Failed to compute hourly history for park ${park.id}: ${
              e instanceof Error ? e.message : e
            }`,
          );
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(
        `✅ Hourly-history rollup complete: ${rowsWritten} attraction-days across ${parksProcessed} parks in ${duration}s`,
      );
    } catch (error) {
      this.logger.error(
        `Hourly-history rollup failed`,
        error instanceof Error ? error.stack : String(error),
      );
      throw error;
    }
  }

  /**
   * Backfill — runs the rollup for an explicit date range. Used to seed
   * the table after deploy or to repair missing rows. The cron job above
   * only handles "yesterday"; everything else goes through this entry
   * point.
   */
  @Process("backfill-attraction-hourly-history")
  async handleBackfill(
    job: Job<{ parkId?: string; fromDate: string; toDate: string }>,
  ): Promise<void> {
    const { parkId, fromDate, toDate } = job.data;
    this.logger.log(
      `🔄 Backfilling hourly history ${fromDate} → ${toDate}${
        parkId ? ` (park ${parkId})` : " (all parks)"
      }`,
    );

    const parks = parkId
      ? [await this.parksService.findById(parkId)].filter((p) => p)
      : await this.parksService.findAll();

    const dates = enumerateDates(fromDate, toDate);
    let rowsWritten = 0;
    for (const park of parks) {
      if (!park) continue;
      for (const date of dates) {
        try {
          rowsWritten += await this.aggregateParkForDate(
            park.id,
            date,
            park.timezone,
          );
        } catch (e) {
          this.logger.warn(
            `Backfill failed for park ${park.id} on ${date}: ${
              e instanceof Error ? e.message : e
            }`,
          );
        }
      }
    }

    this.logger.log(`✅ Backfill complete: ${rowsWritten} attraction-days`);
  }

  /**
   * Shared aggregation step: pulls one park's worth of attractions, runs
   * the per-park hourly + down-count queries, joins them in memory, and
   * bulk-upserts. Returns the number of rows written so the caller can
   * report progress.
   *
   * Attractions with no rows in either query still get an empty-slots
   * record written — that lets the read path tell "we processed this
   * day, there was just nothing" apart from "we never processed it".
   */
  private async aggregateParkForDate(
    parkId: string,
    date: string,
    timezone: string,
  ): Promise<number> {
    const { data: attractions } = await this.attractionsService.findByParkId(
      parkId,
      1,
      10000,
    );
    if (attractions.length === 0) return 0;

    const [slotsByAttraction, downCounts] = await Promise.all([
      this.analyticsService.computeParkHourlyHistoryForDate(
        parkId,
        date,
        timezone,
      ),
      this.analyticsService.computeParkDownCountForDate(parkId, date, timezone),
    ]);

    const rows = attractions.map((a) => ({
      attractionId: a.id,
      parkId,
      date,
      slots: slotsByAttraction.get(a.id) || [],
      downCount: downCounts.get(a.id) || 0,
    }));

    await this.analyticsService.saveAttractionHourlyHistoryBatch(rows);
    return rows.length;
  }
}

/**
 * Enumerate the inclusive list of YYYY-MM-DD dates between two endpoints.
 * Kept local to this processor — the only other place that needs a date
 * range walk is the history endpoint, which iterates in park timezone
 * via its own loop.
 */
function enumerateDates(fromDate: string, toDate: string): string[] {
  const out: string[] = [];
  const start = new Date(`${fromDate}T00:00:00Z`);
  const end = new Date(`${toDate}T00:00:00Z`);
  const cursor = new Date(start);
  while (cursor <= end) {
    out.push(cursor.toISOString().split("T")[0]);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}
