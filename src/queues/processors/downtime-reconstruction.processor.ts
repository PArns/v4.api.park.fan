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
import { CLOSURE_GAP_INTERVALS_SQL } from "../../common/utils/closure-gap.sql";
import { DowntimeRecoveryService } from "../../analytics/downtime-recovery.service";
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
 * ## Delete-then-insert, per covered range AND per covered ride
 *
 * An outage is an interval that can GROW between two runs. Upserting on
 * `(attractionId, startedAt)` would leave yesterday's shorter copy behind
 * whenever the stitch merged two runs into one, and the event count would drift
 * upward every night. So the covered range is deleted first and rewritten whole.
 *
 * **Covered is a set of rides, not a park.** Keyed on `parkId` alone, the delete
 * erases the whole park's window and the insert only replaces what the
 * statements returned — so a ride that left the `tracked` population since
 * yesterday loses its reconstructed history permanently. The delete therefore
 * names the rides this run actually covered; see `coveredIds` at the call site
 * for how that set is read off the results already in hand, and for why an empty
 * run now deletes nothing instead of everything.
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

  /**
   * Default lookback for the nightly run.
   *
   * **30, not 120, and the reason is measured.** The two statements cost 0.43 s
   * over one day, 3.6 s over seven and 22 s over thirty — but ~7 minutes over
   * 180, so the cost is strongly super-linear, and the temp spill grows with it
   * (30 days already writes ~700 MB for statement 1). They run under one
   * `Promise.all`, so a 120-day nightly run spills several GB concurrently every
   * night to recompute intervals that have not changed since yesterday.
   *
   * A rolling 30 days is enough for the nightly job because the table
   * accumulates: an interval written last month stays written, and the profile
   * window (90 days) reads `attraction_outages` rather than the reconstruction.
   * `OUTAGE_SCAN_START_SQL` still walks back past the window edge for any spell
   * that is open or started earlier, so a long outage is not truncated.
   *
   * The FIRST fill is a different job and is not this one: it needs the whole
   * history and belongs in 30-day stages, run by hand. See `todo.md`.
   */
  private readonly DEFAULT_WINDOW_DAYS = 30;

  /**
   * How long a reconstructed row is kept.
   *
   * These five tables are plain heap tables — not hypertables, no compression,
   * no TimescaleDB retention policy — and the nightly job only ever rewrites
   * its own rolling window, so anything older than that window is written once
   * and then never touched again. Left alone they grow forever: measured at
   * ~6800 exposure rows and ~1000 intervals per day, that is roughly 2.5 M rows
   * and ~730 MB of `attraction_exposure_days` per year.
   *
   * 400 days is the longest window anything reads, with slack: profiles look
   * back 90 days and the recovery curves 180. A row older than this cannot
   * affect a published figure.
   */
  private readonly RETENTION_DAYS = 400;

  constructor(
    private readonly dataSource: DataSource,
    private readonly profiles: DowntimeProfileService,
    private readonly recovery: DowntimeRecoveryService,
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
          // Hard floor on how far back the scan may reach, whatever it finds
          // still open. Twice the requested window: enough slack for a genuinely
          // long outage to keep its start, bounded enough that the statement
          // cannot grow without limit.
          new Date(asOf.getTime() - windowDays * 2 * 24 * 60 * 60 * 1000),
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

    // Third statement, for the parks the first two cannot see. It restricts
    // itself to parks whose feed never emits DOWN, so it cannot double-count.
    // Its own try/catch: it is an addition for parks that would otherwise have
    // no history at all, and its failure must not cost the reconstruction.
    let closureGaps: ClosureGapRow[] = [];
    try {
      closureGaps = (await this.dataSource.query(CLOSURE_GAP_INTERVALS_SQL, [
        parkIds,
        scanStart,
        asOf,
      ])) as ClosureGapRow[];
    } catch (error) {
      this.logger.warn(
        `Closure-gap reconstruction failed, DOWN intervals kept: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

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

    // The rides THIS run covered, which is what the deletes below may clear.
    //
    // The deletes used to be keyed on `parkId` alone, so they erased every row
    // of the park in the window and re-inserted only what the statements
    // returned. A ride that left the `tracked` population between two runs — a
    // merge (`last_merged_at` moves inside the scan window), `retired_at`, a
    // flip to `open_with_park`, the park losing its schedule or its
    // `wiki_entity_id` — was therefore deleted and never rewritten. Not stale:
    // gone, and gone for good, because the reconstruction only ever rewrites
    // its own rolling window and nothing else writes these two tables.
    //
    // The exposure table loses the same way and hurts twice over: it is the
    // DENOMINATOR of everything published about downtime, and of the
    // duty-cycle share in `closure-gap.sql`, where a missing operating day
    // moves the ratio in the direction that publishes a timetable as a fault.
    //
    // The population needs no query of its own. `OUTAGE_EXPOSURE_SQL` selects
    // `FROM tracked t JOIN park_open po`, one row per tracked ride per
    // operating day its park published in the window — so the ids in
    // `exposure` ARE this run's `tracked` set, minus rides whose park had no
    // operating day at all, which can produce nothing to write either.
    // `intervals` and `closureGaps` are added because a ride may be written
    // without being in that set: the closure-gap statement serves `blind_parks`
    // and, unlike `tracked`, does not exclude free-flow rides.
    //
    // The direction of the remaining gap is deliberate. A ride still in the
    // population that produced nothing this run keeps what it had, rather than
    // having it deleted — a stale row a later run can still correct, against a
    // loss no run can undo.
    //
    // `idx_attraction_outages_ride` is (`attractionId`, `started_at`), and the
    // exposure table's primary key is (`attractionId`, `op_day`), so the
    // narrower predicate is the one both indexes are built for.
    const coveredIds = [
      ...new Set([
        ...exposure.map((row) => row.attractionId),
        ...intervals.map((row) => row.attractionId),
        ...closureGaps.map((row) => row.attractionId),
      ]),
    ];

    await this.dataSource.transaction(async (manager) => {
      await manager.query(
        `DELETE FROM attraction_outages
          WHERE started_at >= $1
            AND ($2::uuid[] IS NULL OR "parkId" = ANY($2::uuid[]))
            AND "attractionId" = ANY($3::uuid[])`,
        [scanStart, parkIds, coveredIds],
      );
      await manager.query(
        `DELETE FROM attraction_exposure_days
          WHERE op_day >= $1::date
            AND ($2::uuid[] IS NULL OR "parkId" = ANY($2::uuid[]))
            AND "attractionId" = ANY($3::uuid[])`,
        [scanStart, parkIds, coveredIds],
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
              signal: "down" as const,
              fingerprintVersion: OUTAGE_FINGERPRINT_VERSION,
              computedAt: asOf,
            })),
          )
          .orIgnore()
          .execute();
      }

      // A closure gap is a complete, observed interval by construction: the
      // statement only returns one once the ride has come back, so it always
      // has both edges and never needs the censoring machinery. Its minutes are
      // wall minutes measured inside opening hours, which for a same-day gap is
      // the same number as operating minutes.
      for (const batch of chunked(closureGaps, 500)) {
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
              operatingMinutes: row.wallMinutes,
              observedOperatingMinutes: row.wallMinutes,
              wallMinutes: row.wallMinutes,
              operatingDays: 1,
              endReason: "recovered" as AttractionOutage["endReason"],
              durationUsable: true,
              likelyWorksPeriod: false,
              rowsInSpell: 1,
              heartbeatRows: 0,
              startCensored: false,
              signal: "closed_gap" as const,
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
        `${closureGaps.length} closure gap(s), ` +
        `${exposure.length} exposure day(s) in ${Date.now() - startedAt} ms`,
    );

    await this.pruneOldRows(asOf);

    // Guarded for the same reason the curve rebuild below is, and the omission
    // was pure asymmetry: an unguarded throw here fails the BullMQ job, Bull
    // retries the whole handler, and the reconstruction — the strongly
    // super-linear part with the ~700 MB temp spill — runs again from the top.
    // A deterministic error burns all three attempts and leaves
    // `attraction_downtime_profiles`, the table every ride page reads, silently
    // frozen. This codebase has been bitten by that shape twice already.
    try {
      await this.profiles.rebuild(parkIds);
    } catch (error) {
      this.logger.error(
        `Profile rebuild failed, reconstruction kept: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    // The curves last, and always over every park regardless of `parkIds`: the
    // pooled row is the fallback every park's serving path reads, so rebuilding
    // it from a single park's intervals would quietly narrow the population
    // behind ~40 000 ride pages.
    //
    // Logged and swallowed rather than thrown. The curves are an addition to a
    // reconstruction that has already succeeded and been written; letting them
    // fail the job made BullMQ retry the whole thing, so a `delete({})` that
    // TypeORM rejects cost three full reconstructions (3 x 35 s, 200 000
    // exposure rows each) every night — and the error still never reached the
    // log, so the run looked healthy while the table stayed empty.
    try {
      await this.recovery.rebuild();
    } catch (error) {
      this.logger.error(
        `Recovery curves failed, reconstruction kept: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Delete what nothing reads any more.
   *
   * Its own try/catch and after the write: housekeeping must never cost a
   * reconstruction that already succeeded. Both deletes are range scans on the
   * time column each table is keyed by.
   */
  private async pruneOldRows(asOf: Date): Promise<void> {
    const cutoff = new Date(
      asOf.getTime() - this.RETENTION_DAYS * 24 * 60 * 60 * 1000,
    );
    try {
      const outages = await this.dataSource.query(
        `DELETE FROM attraction_outages WHERE started_at < $1`,
        [cutoff],
      );
      const exposure = await this.dataSource.query(
        `DELETE FROM attraction_exposure_days WHERE op_day < $1::date`,
        [cutoff],
      );
      const removed =
        (Array.isArray(outages) ? 0 : (outages?.[1] ?? 0)) +
        (Array.isArray(exposure) ? 0 : (exposure?.[1] ?? 0));
      if (removed > 0) {
        this.logger.log(`🧹 Pruned rows older than ${this.RETENTION_DAYS}d`);
      }
    } catch (error) {
      this.logger.warn(
        `Retention prune failed, reconstruction kept: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
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

interface ClosureGapRow {
  attractionId: string;
  parkId: string;
  startedAt: Date;
  endedAt: Date;
  startOpDay: string;
  wallMinutes: number;
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
