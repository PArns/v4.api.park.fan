import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { DataSource } from "typeorm";
import {
  OUTAGE_EXPOSURE_SQL,
  OUTAGE_FINGERPRINT_VERSION,
  OUTAGE_INTERVALS_SQL,
  OUTAGE_SCAN_START_SQL,
} from "../../analytics/utils/outage-reconstruction.sql";
import { DowntimeProfileService } from "../../analytics/downtime-profile.service";
import { AttractionOutage } from "../../analytics/entities/attraction-outage.entity";
import { AttractionExposureDay } from "../../analytics/entities/attraction-exposure-day.entity";

/**
 * Rebuild `attraction_outages` and `attraction_exposure_days`, then the profiles.
 *
 * ## One statement per time chunk, across every park
 *
 * Not one per park. `queue_data` compresses after 30 days with no `segmentby`,
 * so a per-park loop decompresses the same chunks two hundred times over; the
 * shape used here is the one `refreshOperatingDayRollup` settled on for the same
 * reason. The park filter exists for a targeted repair, not for the nightly run.
 *
 * ## Delete-then-insert, per covered range
 *
 * An outage is an interval that can GROW between two runs. Upserting on
 * `(attractionId, startedAt)` would leave yesterday's shorter copy behind
 * whenever the stitch merged two runs into one, and the event count would drift
 * upward every night. So the covered range is deleted first and rewritten whole.
 *
 * ## The scan starts where the data says, not where the calendar does
 *
 * An interval still open, or one that began before the requested window, is
 * re-read from its own beginning. `OUTAGE_SCAN_START_SQL` finds that point;
 * without it every long outage would come back truncated at the window edge with
 * a wrong duration.
 */
@Processor("downtime")
export class DowntimeReconstructionProcessor {
  private readonly logger = new Logger(DowntimeReconstructionProcessor.name);

  /** Default lookback for the nightly run. */
  private readonly DEFAULT_WINDOW_DAYS = 120;

  constructor(
    private readonly dataSource: DataSource,
    private readonly profiles: DowntimeProfileService,
  ) {}

  @Process("reconstruct-downtime")
  async handleReconstruct(
    job: Job<{ parkIds?: string[]; windowDays?: number }>,
  ): Promise<void> {
    const parkIds = job.data?.parkIds?.length ? job.data.parkIds : null;
    const windowDays = Math.min(
      Math.max(job.data?.windowDays ?? this.DEFAULT_WINDOW_DAYS, 1),
      400,
    );

    const asOf = new Date();
    const windowFrom = new Date(
      asOf.getTime() - windowDays * 24 * 60 * 60 * 1000,
    );

    this.logger.log(
      `🔧 Reconstructing downtime over ${windowDays}d` +
        `${parkIds ? ` for ${parkIds.length} park(s)` : " (all parks)"}`,
    );
    const startedAt = Date.now();

    let scanStart = windowFrom;
    try {
      const rows: Array<{ scan_start: Date | null }> =
        await this.dataSource.query(OUTAGE_SCAN_START_SQL, [
          windowFrom,
          parkIds,
        ]);
      if (rows[0]?.scan_start) scanStart = new Date(rows[0].scan_start);
    } catch (error) {
      // An empty or unreadable outage table is the first-run case. Falling back
      // to the requested window is correct there and merely conservative later.
      this.logger.warn(
        `Scan-start lookup failed, using the requested window: ${message(error)}`,
      );
    }

    const [intervals, exposure] = await Promise.all([
      this.dataSource.query(OUTAGE_INTERVALS_SQL, [
        parkIds,
        scanStart,
        asOf,
        asOf,
      ]) as Promise<IntervalRow[]>,
      this.dataSource.query(OUTAGE_EXPOSURE_SQL, [
        parkIds,
        scanStart,
        asOf,
        asOf,
      ]) as Promise<ExposureRow[]>,
    ]);

    // Keyed on the interval's OPERATING day, which statement 1 resolves from the
    // window rather than from the calendar: a park closing at 02:00 files a
    // 00:30 outage under the previous operating day, and a calendar key would
    // point at a day the exposure table has no row for.
    const starts = new Map<string, number>();
    for (const row of intervals) {
      if (row.likelyWorksPeriod || !row.startOpDay) continue;
      const key = `${row.attractionId}|${row.startOpDay}`;
      starts.set(key, (starts.get(key) ?? 0) + 1);
    }

    let suspectDays = 0;

    await this.dataSource.transaction(async (manager) => {
      await manager.query(
        `DELETE FROM attraction_outages
          WHERE started_at >= $1
            AND ($2::uuid[] IS NULL OR "parkId" = ANY($2::uuid[]))`,
        [scanStart, parkIds],
      );
      await manager.query(
        `DELETE FROM attraction_exposure_days
          WHERE op_day >= $1::date
            AND ($2::uuid[] IS NULL OR "parkId" = ANY($2::uuid[]))`,
        [scanStart, parkIds],
      );

      for (const batch of chunked(intervals, 500)) {
        await manager
          .createQueryBuilder()
          .insert()
          .into(AttractionOutage)
          .values(
            batch.map((row) => ({
              attractionId: row.attractionId,
              parkId: row.parkId,
              startedAt: row.startedAt,
              endedAt: row.endedAt,
              operatingMinutes: row.operatingMinutes,
              observedOperatingMinutes: row.observedOperatingMinutes,
              wallMinutes: row.wallMinutes,
              operatingDays: row.operatingDays,
              endReason: row.endReason as AttractionOutage["endReason"],
              durationUsable: isDurationUsable(row),
              likelyWorksPeriod: row.likelyWorksPeriod,
              rowsInSpell: row.rowsInSpell,
              heartbeatRows: row.heartbeatRows,
              startCensored: row.startCensored,
              fingerprintVersion: OUTAGE_FINGERPRINT_VERSION,
              computedAt: asOf,
            })),
          )
          .orIgnore()
          .execute();
      }

      for (const batch of chunked(exposure, 500)) {
        const rows = batch.map((row) => {
          const accounted =
            row.operatingMinutes +
            row.downMinutes +
            row.closedMinutes +
            row.refurbishmentMinutes +
            row.absentMinutes +
            row.unobservedMinutes;
          // Kept and flagged, never silently NULLed: dropping the day would hide
          // the arithmetic bug AND shrink the denominator at the same time.
          const suspect = Math.abs(accounted - row.parkOpenMinutes) > 1;
          if (suspect) suspectDays++;
          return {
            attractionId: row.attractionId,
            parkId: row.parkId,
            opDay: row.opDay,
            parkOpenMinutes: row.parkOpenMinutes,
            operatingMinutes: row.operatingMinutes,
            downMinutes: row.downMinutes,
            downMinutesObserved: row.downMinutesObserved,
            closedMinutes: row.closedMinutes,
            refurbishmentMinutes: row.refurbishmentMinutes,
            absentMinutes: row.absentMinutes,
            unobservedMinutes: row.unobservedMinutes,
            outageStarts: starts.get(`${row.attractionId}|${row.opDay}`) ?? 0,
            suspect,
            computedAt: asOf,
          };
        });
        await manager
          .createQueryBuilder()
          .insert()
          .into(AttractionExposureDay)
          .values(rows)
          .orIgnore()
          .execute();
      }
    });

    if (suspectDays > 0) {
      // Monitored rather than swallowed: the invariant is the only thing that
      // notices a segment-arithmetic bug, and a silent one shrinks every
      // denominator on the site.
      this.logger.warn(
        `⚠️  ${suspectDays} exposure day(s) failed the minute invariant ` +
          `(operating + down + closed + refurbishment + absent + unobserved ` +
          `!= parkOpenMinutes). Rows kept and flagged suspect.`,
      );
    }

    this.logger.log(
      `✅ Downtime reconstruction: ${intervals.length} interval(s), ` +
        `${exposure.length} exposure day(s) in ${Date.now() - startedAt} ms`,
    );

    await this.profiles.rebuild(parkIds);
  }
}

interface IntervalRow {
  attractionId: string;
  parkId: string;
  startedAt: Date;
  endedAt: Date | null;
  operatingMinutes: number;
  observedOperatingMinutes: number;
  wallMinutes: number;
  operatingDays: number;
  endReason: string;
  startCensored: boolean;
  likelyWorksPeriod: boolean;
  rowsInSpell: number;
  heartbeatRows: number;
  /** Park-local operating day the interval started on, or null if outside every window. */
  startOpDay: string | null;
}

interface ExposureRow {
  attractionId: string;
  parkId: string;
  opDay: string;
  parkOpenMinutes: number;
  operatingMinutes: number;
  downMinutes: number;
  downMinutesObserved: number;
  closedMinutes: number;
  refurbishmentMinutes: number;
  absentMinutes: number;
  unobservedMinutes: number;
}

/**
 * Whether this interval's LENGTH may be counted.
 *
 * Only an observed end qualifies, and only when our own writer did not supply
 * most of it. Everything else contributes to the event count alone — a censored
 * interval is a lower bound, and a median built out of lower bounds is not a
 * median.
 */
export function isDurationUsable(row: {
  endReason: string;
  likelyWorksPeriod: boolean;
  startCensored: boolean;
  operatingMinutes: number;
  observedOperatingMinutes: number;
}): boolean {
  if (row.likelyWorksPeriod || row.startCensored) return false;
  if (row.endReason !== "recovered" && row.endReason !== "reclassified") {
    return false;
  }
  if (row.operatingMinutes <= 0) return false;
  return row.observedOperatingMinutes / row.operatingMinutes >= 0.5;
}

function* chunked<T>(items: T[], size: number): Generator<T[]> {
  for (let i = 0; i < items.length; i += size) yield items.slice(i, i + size);
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
