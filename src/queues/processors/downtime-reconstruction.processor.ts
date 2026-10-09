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
import {
  CLOSURE_GAP_INTERVALS_SQL,
  CLOSURE_GAP_PLANNER_SETTINGS,
} from "../../common/utils/closure-gap.sql";
import {
  applyStatementLimits,
  queryWithLimits,
  StatementLimits,
} from "../../common/utils/statement-limits.util";
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
   * 180, so statement time is strongly super-linear (~19x for 6x the window),
   * and the temp spill grows with it (30 days already writes ~700 MB for
   * statement 1). They run under one `Promise.all`, so a 120-day nightly run
   * spills several GB concurrently every night to recompute intervals that have
   * not changed since yesterday.
   *
   * Measured end to end against production on 2026-09-11, all parks, 24 cores
   * at load 1.3-4.2 — the staged first fill, so these are whole-job times and
   * not statement times:
   *
   * | window | 30 d  | 60 d  | 90 d  | 120 d | 180 d   | 270 d   |
   * | ------ | ----- | ----- | ----- | ----- | ------- | ------- |
   * | time   | 118 s | 207 s | 356 s | 558 s | 1 037 s | 1 713 s |
   *
   * Six times the window costs 8.8x the whole job, an apparent exponent of
   * about **1.2**. Read that as a planning figure for a staged fill and not as
   * a correction of the ~19x above: these are different quantities. The 19x is
   * statement time, while the job also does `pruneOldRows` (fixed 400-day
   * cutoff), `profiles.rebuild` (90 days) and `recovery.rebuild` — work that
   * barely moves with `windowDays` and therefore flattens the curve at the
   * short end. The two statements may well still grow at the older rate; what
   * this table establishes is the wall-clock cost of the call, which is what a
   * hand-run fill is planned against. The absolute figures age upward as the
   * table grows, so plan against the shape rather than the seconds.
   *
   * A rolling 30 days is enough for the nightly job because the table
   * accumulates: an interval written last month stays written, and the profile
   * window (90 days) reads `attraction_outages` rather than the reconstruction.
   * `OUTAGE_SCAN_START_SQL` still walks back past the window edge for any spell
   * that is open or started earlier, so a long outage is not truncated.
   *
   * The FIRST fill is a different job and is not this one: it needs the whole
   * history and belongs in 30-day stages, run by hand. It was run that way on
   * 2026-09-11 and reached `queue_data`'s own floor of 2025-12-24; see
   * `docs/analytics/ride-downtime.md`, "The first production fill, staged".
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
      const rows = await queryWithLimits<Array<{ scan_start: Date | null }>>(
        this.dataSource,
        OUTAGE_SCAN_START_SQL,
        [
          windowFrom,
          parkIds,
          // Hard floor on how far back the scan may reach, whatever it finds
          // still open. Twice the requested window: enough slack for a genuinely
          // long outage to keep its start, bounded enough that the statement
          // cannot grow without limit.
          new Date(asOf.getTime() - windowDays * 2 * 24 * 60 * 60 * 1000),
        ],
        SCAN_START_LIMITS,
      );
      if (rows[0]?.scan_start) scanStart = new Date(rows[0].scan_start);
    } catch (error) {
      // An empty or unreadable outage table is the first-run case. Falling back
      // to the requested window is correct there and merely conservative later.
      this.logger.warn(
        `Scan-start lookup failed, using the requested window: ${message(error)}`,
      );
    }

    // Every read below runs under a deadline sized from the scan it covers. A
    // statement that blows it fails loudly instead of holding the queue's only
    // slot for days; see `reconstructionReadLimits`.
    const readLimits = reconstructionReadLimits(scanStart, asOf);

    const [intervals, exposure] = await Promise.all([
      queryWithLimits<IntervalRow[]>(
        this.dataSource,
        OUTAGE_INTERVALS_SQL,
        [parkIds, scanStart, asOf, asOf],
        readLimits,
      ),
      queryWithLimits<ExposureRow[]>(
        this.dataSource,
        OUTAGE_EXPOSURE_SQL,
        [parkIds, scanStart, asOf, asOf],
        readLimits,
      ),
    ]);

    // Third statement, for the parks the first two cannot see. It restricts
    // itself to parks whose feed never emits DOWN, so it cannot double-count.
    // Its own try/catch: it is an addition for parks that would otherwise have
    // no history at all, and its failure must not cost the reconstruction.
    let closureGaps: ClosureGapRow[] = [];
    // Whether this run is in a position to rewrite a `closed_gap` row at all.
    //
    // An empty result and a failed statement look the same downstream and mean
    // opposite things: one says the window holds no closure gaps, the other
    // says this run does not know. The delete below has to tell them apart, or
    // swallowing the error here silently erases the whole signal — which is the
    // same data loss `coveredIds` exists to prevent, reached through the
    // catch block instead of through the population.
    let closureGapsRead = true;
    try {
      // With nested loops off, and that is not a tuning nicety: the planner
      // estimates one row for every CTE of this statement and stacks its final
      // aggregates as nested loops, which over the nightly 60-day scan never
      // finished (PAR-820). A timeout lands in the catch below, so the closure
      // signal is skipped for the night and the DOWN reconstruction is kept.
      closureGaps = await queryWithLimits<ClosureGapRow[]>(
        this.dataSource,
        CLOSURE_GAP_INTERVALS_SQL,
        [parkIds, scanStart, asOf],
        { ...readLimits, planner: CLOSURE_GAP_PLANNER_SETTINGS },
      );
    } catch (error) {
      closureGapsRead = false;
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
    // The direction of the remaining gap is deliberate, and it is worth being
    // precise about how wide it is. A ride that produced nothing at all this
    // run keeps what it had rather than having it deleted. For anything in
    // `tracked` that is not a gap: the ride still gets an exposure row for
    // every operating day its park published, so it is covered and its window
    // is still rewritten whole — which is what lets a ride that has newly
    // crossed `MAX_GAP_DAY_SHARE` lose its stored gaps.
    //
    // What is left is the one population that is written without being
    // tracked: a free-flow (`open_with_park`) ride in a blind park, which the
    // closure-gap statement does not exclude and the exposure statement does.
    // If such a ride stops producing gaps, its `closed_gap` rows stay until the
    // 400-day prune. The profile and coverage queries filter `signal = 'down'`
    // and never see them; the recovery curve does read them now, and for the
    // curve a row that stays is a closure that was measured when it happened —
    // a real observation that is merely no longer being repeated, not a wrong
    // one. It is still the right trade: a stale row a later run can correct,
    // against a loss no run can undo.
    //
    // The outages primary key is (`attractionId`, `started_at`), and the
    // exposure table's primary key is (`attractionId`, `op_day`), so the
    // narrower predicate is the one both keys are built for.
    const coveredIds = [
      ...new Set([
        ...exposure.map((row) => row.attractionId),
        ...intervals.map((row) => row.attractionId),
        ...closureGaps.map((row) => row.attractionId),
      ]),
    ];

    // A run that could not READ the closure gaps may not delete them.
    //
    // The two statements write one table under two signals, and only one of
    // them is allowed to fail quietly. When it does, `coveredIds` still names
    // every blind-park ride — they come through `exposure`, which succeeded —
    // so an unqualified delete strips their stored `closed_gap` rows and only
    // the `down` rows are written back. Same permanence as before: nothing else
    // writes this table and the rolling window moves on tomorrow.
    //
    // Restricting the delete to `down` is exactly as wide as what this run can
    // rewrite, which is the rule the whole predicate follows.
    const rewritableSignals = closureGapsRead
      ? ""
      : `\n            AND signal = 'down'`;

    await this.dataSource.transaction(async (manager) => {
      await applyStatementLimits(manager, WRITE_LIMITS);
      await manager.query(
        `DELETE FROM attraction_outages
          WHERE started_at >= $1
            AND ($2::uuid[] IS NULL OR "parkId" = ANY($2::uuid[]))
            AND "attractionId" = ANY($3::uuid[])${rewritableSignals}`,
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

      // A closure is written as the statement measured it. It used to be a
      // complete interval by construction -- only a ride that came back the
      // same day was returned -- so this loop stamped every row `recovered`
      // with wall minutes for operating ones. Since standing closures are kept,
      // a row may have no end (`ongoing`, `window_edge`), may span a night, and
      // may end in something other than a recovery, and each of those is a
      // fact the recovery curve reads: stamping them all `recovered` would put
      // back, one step later, exactly the survivorship the statement now
      // avoids. See CLOSURE_GAP_INTERVALS_SQL.
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
              operatingMinutes: row.operatingMinutes,
              // Every closure minute is read off an observed status change --
              // the statement admits no carried row -- so there is no
              // heartbeat share to discount.
              observedOperatingMinutes: row.operatingMinutes,
              wallMinutes: row.wallMinutes,
              operatingDays: row.operatingDays,
              endReason: row.endReason as AttractionOutage["endReason"],
              // A recovery only. `isDurationUsable` would also accept
              // `reclassified`, which for a reported DOWN is planned work
              // replacing a fault; for a closure it is the ride still not
              // running under another name, and its length is a lower bound.
              durationUsable:
                row.endReason === "recovered" && row.operatingMinutes > 0,
              // Never: a closure is cut off at LIVE_LOOKBACK_HOURS, a long way
              // inside the seven days that mark a works period.
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
      const outages = await queryWithLimits<unknown>(
        this.dataSource,
        `DELETE FROM attraction_outages WHERE started_at < $1`,
        [cutoff],
        WRITE_LIMITS,
      );
      const exposure = await queryWithLimits<unknown>(
        this.dataSource,
        `DELETE FROM attraction_exposure_days WHERE op_day < $1::date`,
        [cutoff],
        WRITE_LIMITS,
      );
      // TypeORM's Postgres driver answers a DELETE with `[rows, rowCount]`.
      const affected = (raw: unknown): number =>
        Array.isArray(raw) && typeof raw[1] === "number" ? raw[1] : 0;
      const removed = affected(outages) + affected(exposure);
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

const MINUTE_MS = 60 * 1000;
const DAY_MS = 24 * 60 * MINUTE_MS;

/**
 * Deadlines for the reconstruction's reads, from the scan they cover.
 *
 * Sized from production on 2026-10-09 (PAR-820), over the nightly scan of 60
 * days — `OUTAGE_SCAN_START_SQL` pins it to its floor of twice the 30-day
 * window, so 60 and not 30 is what the cron actually reads: statement 1 takes
 * ~30 s, statement 2 ~52 s, the closure-gap statement 12.7 s with its planner
 * settings (~110 s before PAR-157 changed it, never finishing after). The floor
 * of ten minutes is more than ten times the slowest of them.
 *
 * Above the floor it grows with the scan, ten seconds per day, so a staged
 * hand-run fill (`windowDays` up to 400, a scan of up to 800 days) is not cut
 * off by a deadline meant for the nightly run. The 270-day stage of the first
 * fill took 1713 s for the whole job; the budget there is 90 minutes per
 * statement.
 *
 * `lock_timeout` is short on purpose. These are reads: the only lock they can
 * wait on is one somebody else asked for ACCESS EXCLUSIVE on, and a read
 * queued behind that becomes the head of the queue every other reader of the
 * table then waits behind. Failing the job costs one night; queueing can cost
 * the API (PAR-563, PAR-819).
 */
export function reconstructionReadLimits(
  scanStart: Date,
  asOf: Date,
): StatementLimits {
  const scanDays = Math.max(
    1,
    Math.ceil((asOf.getTime() - scanStart.getTime()) / DAY_MS),
  );
  return {
    statementTimeoutMs: Math.max(
      READ_TIMEOUT_FLOOR_MS,
      scanDays * READ_TIMEOUT_PER_SCAN_DAY_MS,
    ),
    lockTimeoutMs: LOCK_TIMEOUT_MS,
  };
}

/** See `reconstructionReadLimits`. */
export const READ_TIMEOUT_FLOOR_MS = 10 * MINUTE_MS;
export const READ_TIMEOUT_PER_SCAN_DAY_MS = 10 * 1000;
export const LOCK_TIMEOUT_MS = 30 * 1000;

/**
 * The scan-start lookup reads `attraction_outages` only, by index — it has
 * never shown up in the slow-query log at all.
 */
const SCAN_START_LIMITS: StatementLimits = {
  statementTimeoutMs: 2 * MINUTE_MS,
  lockTimeoutMs: LOCK_TIMEOUT_MS,
};

/**
 * The write transaction and the retention prune.
 *
 * The slowest write measured is the outage DELETE at 7.7-14.7 s (slow-query
 * log, 2026-10-02..05); five minutes per statement is twenty times that. The
 * idle limit ends the session if the process stalls between two statements
 * while holding the transaction's row locks and snapshot open, which
 * `statement_timeout` cannot see.
 */
export const WRITE_LIMITS: StatementLimits = {
  statementTimeoutMs: 5 * MINUTE_MS,
  lockTimeoutMs: LOCK_TIMEOUT_MS,
  idleInTransactionTimeoutMs: 2 * MINUTE_MS,
};

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
  /** Null when the closure did not end inside its horizon. */
  endedAt: Date | null;
  startOpDay: string;
  wallMinutes: number;
  operatingMinutes: number;
  operatingDays: number;
  endReason: "recovered" | "reclassified" | "ongoing" | "window_edge";
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
