import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import {
  TRAILING_OUTAGE_LOOKBACK_DAYS,
  trailingOutageWithElapsedSql,
} from "../../common/utils/outage-rows.sql";
import { DowntimeRecoveryCurve } from "../../analytics/entities/downtime-recovery-curve.entity";
import { CURRENT_CLOSURE_GAP_SQL } from "../../common/utils/closure-gap.sql";
import { upcomingOperatingWindowsSql } from "../../common/utils/park-open-window.sql";
import type { OperatingWindow } from "../../analytics/utils/operating-clock.util";
import type { OutageSignal } from "../../analytics/entities/attraction-outage.entity";
import {
  estimateOutage,
  type OutageEstimate,
} from "../../analytics/utils/downtime-estimate.util";
import {
  CuratedOutOfServiceSource,
  isCuratedOutOfService,
} from "../utils/curated-out-of-service.util";

/**
 * One ride's currently running outage, as far as the change log can place it.
 */
export interface CurrentOutage {
  /** First `DOWN` row of the trailing run, in UTC. */
  startedAt: Date;
  /**
   * Whether the transition into `DOWN` was seen inside the lookback window.
   *
   * `false` means the run already covered the oldest row we looked at, so the
   * outage began earlier than `startedAt` and the caller must say "since the
   * 28th" rather than "since 09:05" — the window's edge is not an onset.
   */
  startObserved: boolean;
  /** `DOWN` rows behind this run. Diagnostic; never rendered. */
  rowsInRun: number;
  /**
   * Which signal placed this outage.
   *
   * `down` is the operator's own feed saying the ride is not running.
   * `closed_gap` is inferred — the ride was open earlier today, shut inside
   * opening hours, and did not shut together with the park. It only ever
   * appears for parks whose feed never emits DOWN, where the alternative is
   * silence rather than a stronger signal.
   *
   * **The wording on the page must differ.** A `closed_gap` was never reported
   * by anybody, so no sentence built on it may say „gemeldet".
   */
  signal: OutageSignal;
  /**
   * How long this outage still has to go, read off the measured curve.
   *
   * Absent whenever the curve cannot answer: too short to have a bucket, too
   * thin a sample, or a park that publishes no hours so the elapsed figure has
   * no operating clock to be counted on. Absence is never "it is about to end".
   */
  estimate?: OutageEstimate;
}

/** What the service needs to know about a candidate ride. */
export interface OutageCandidate extends CuratedOutOfServiceSource {
  id: string;
  /**
   * The ride's resolved status, and the reason the caller passes EVERY ride
   * rather than only the ones reading `DOWN`.
   *
   * The closure signal exists for parks whose feed never emits `DOWN`. Filtering
   * to `DOWN` before calling made it structurally unreachable there — the two
   * predicates are mutually exclusive, so it never ran once in production. The
   * service does its own filtering now, and it needs the status to do it.
   *
   * This used to carry a second reason — that the simultaneity filter "can only
   * work over the park", so a pre-filtered list would always count one closer.
   * It is not true and has not been since that filter was written:
   * `park_closers` counts over `$4`, the park id, and never reads `$1` at all.
   * `attraction-integration.service` passes a single ride and gets a correct
   * count. Only the reason above is load-bearing.
   */
  effectiveStatus?: string | null;
}

/** What the service needs to know about the park. */
export interface OutageParkContext {
  id: string;
  timezone: string;
  /** `parks.wiki_entity_id`. Capability, read from configuration. */
  wikiEntityId: string | null;
}

/**
 * "Since when has this ride been reported down", and nothing else.
 *
 * The one place either read path may ask. The park ride list
 * (`park-integration.service`) and the ride detail
 * (`attraction-integration.service`) resolve a ride's status through two
 * different precedence chains, in two different orders, and only one of them has
 * the optimistic free-flow fallback. If each grew its own outage query they
 * would disagree about the same sentence on two pages of the same ride, which is
 * the drift this codebase keeps writing shared utils to prevent.
 *
 * ## What it does not do
 *
 * It does not decide whether a ride is down. The caller resolves each ride's
 * status through its own chain and passes it along, and this answers when the
 * outage started. Deciding would be a third status chain.
 *
 * It does NOT take a pre-filtered DOWN list, and the wording here used to say it
 * did. Both callers pass whatever they are rendering regardless of status —
 * `park-integration.service` the page's attractions, `attraction-integration.
 * service` one ride — because the closure signal exists for parks that never
 * emit `DOWN`, and filtering to `DOWN` first makes it structurally unreachable
 * there: the two predicates are mutually exclusive, so it never ran once in
 * production. A single ride is a valid call; the statement counts simultaneous
 * closers over the park id, not over what it was handed.
 *
 * It also computes no duration, no rate and no history. A carried heartbeat row
 * is indistinguishable from an observed one apart from `lastUpdated`, so an
 * elapsed figure taken from this alone would be wrong upward exactly on the long
 * outages. The duration work needs the `is_heartbeat` column first.
 *
 * ## Two refusals, both from configuration rather than from the result
 *
 * A park with no `wiki_entity_id` can never emit `DOWN`: Queue-Times maps
 * `is_open` onto `OPERATING`/`CLOSED` and wartezeiten collapses everything that
 * is not running onto `CLOSED` or `REFURBISHMENT`. Asking is pointless there, so
 * it is not asked.
 *
 * A ride inside a curated works period reports nothing, because a rebuild seen
 * from outside looks exactly like a ride that keeps failing.
 */
@Injectable()
export class AttractionOutageService {
  private readonly logger = new Logger(AttractionOutageService.name);

  constructor(
    @InjectRepository(QueueData)
    private readonly queueDataRepository: Repository<QueueData>,
    @InjectRepository(DowntimeRecoveryCurve)
    private readonly curves: Repository<DowntimeRecoveryCurve>,
  ) {}

  /**
   * The recovery curves, cached in process.
   *
   * They are rewritten once a night by the reconstruction job and are ~400 rows
   * in total, so re-reading them per park page would be a query per request for
   * a table that changes once a day. The TTL is what makes a fresh rebuild
   * visible without a deploy; it is not a correctness boundary, because a
   * curve one hour out of date describes the same 180 days.
   */
  private curveCache: {
    at: number;
    byPark: Map<string, DowntimeRecoveryCurve[]>;
    pooled: DowntimeRecoveryCurve[];
  } | null = null;

  /**
   * The curves for one signal, park rows and pooled rows.
   *
   * Filtering here rather than in `estimateOutage` keeps the pure function
   * unable to mix the two populations by accident: a `closed_gap` outage
   * answered from `down` intervals would be read against a different end
   * definition and a different censoring regime.
   */
  private curvesFor(
    cache: {
      byPark: Map<string, DowntimeRecoveryCurve[]>;
      pooled: DowntimeRecoveryCurve[];
    },
    parkId: string,
    signal: OutageSignal,
  ): { park: DowntimeRecoveryCurve[]; pooled: DowntimeRecoveryCurve[] } {
    return {
      park: (cache.byPark.get(parkId) ?? []).filter((r) => r.signal === signal),
      pooled: cache.pooled.filter((r) => r.signal === signal),
    };
  }

  private static readonly CURVE_TTL_MS = 30 * 60 * 1000;

  /**
   * How far ahead the calendar is read to place a recovery window.
   *
   * The largest upper quartile the measured curve resolves is 460 operating
   * minutes — under eight operating hours, so one or two operating days for a
   * park that opens daily. Fourteen covers a park that only opens at weekends,
   * and stops short of pretending we can place an outage in a park that shut
   * for the season: there the field is absent, which is the same answer a park
   * with no published hours gets and for the same reason.
   */
  private static readonly WINDOW_HORIZON_DAYS = 14;

  /**
   * Upcoming operating windows per park, cached in process.
   *
   * Same reasoning as the curve cache one field up, one degree weaker: a park's
   * schedule changes rarely, and a list half an hour old still describes the
   * same days. The cached list is keyed on the park AND reused only for an
   * `asOf` at or after the one it was fetched for and within the TTL — a spec
   * pinning a fixed instant must not be answered from a list fetched for a
   * different one, and wall-clock `Date.now()` would decide that silently.
   */
  private windowCache = new Map<
    string,
    { asOf: number; windows: OperatingWindow[] }
  >();

  /**
   * The park's windows from `asOf` forward, or an empty list.
   *
   * Empty is a real answer and not a failure: 21 parks publish no hours at all,
   * and `recoveryWindowFrom` withholds the field on an empty list. A failed
   * query answers the same way on purpose — the window is a nicety on top of an
   * estimate that is itself optional, and it may not cost the outage line the
   * two statements above it just placed.
   */
  private async loadUpcomingWindows(
    parkId: string,
    asOf: Date,
  ): Promise<OperatingWindow[]> {
    const cached = this.windowCache.get(parkId);
    const age = cached ? asOf.getTime() - cached.asOf : Infinity;
    if (cached && age >= 0 && age < AttractionOutageService.CURVE_TTL_MS) {
      return cached.windows;
    }

    const until = new Date(
      asOf.getTime() +
        AttractionOutageService.WINDOW_HORIZON_DAYS * 24 * 60 * 60 * 1000,
    );
    try {
      const rows: Array<{ opensAt: Date | string; closesAt: Date | string }> =
        await this.queueDataRepository.manager.query(
          upcomingOperatingWindowsSql(),
          [[parkId], asOf, until],
        );
      const windows = rows.map((row) => ({
        opensAt: new Date(row.opensAt),
        closesAt: new Date(row.closesAt),
      }));
      this.windowCache.set(parkId, { asOf: asOf.getTime(), windows });
      return windows;
    } catch (error) {
      this.logger.warn(
        `Upcoming windows unavailable for park ${parkId}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      // Deliberately not cached: a failure is not a schedule.
      return [];
    }
  }

  private async loadCurves(): Promise<{
    byPark: Map<string, DowntimeRecoveryCurve[]>;
    pooled: DowntimeRecoveryCurve[];
  }> {
    const now = Date.now();
    if (
      this.curveCache &&
      now - this.curveCache.at < AttractionOutageService.CURVE_TTL_MS
    ) {
      return this.curveCache;
    }
    const rows = await this.curves.find({ order: { elapsedMinutes: "ASC" } });
    const byPark = new Map<string, DowntimeRecoveryCurve[]>();
    const pooled: DowntimeRecoveryCurve[] = [];
    for (const row of rows) {
      if (row.parkId === null) {
        pooled.push(row);
        continue;
      }
      const list = byPark.get(row.parkId) ?? [];
      list.push(row);
      byPark.set(row.parkId, list);
    }
    this.curveCache = { at: now, byPark, pooled };
    return this.curveCache;
  }

  /**
   * @param park - The park the candidates belong to.
   * @param candidates - **Every** ride on the page, with its resolved status and
   *   its curated works-period columns. Not a pre-filtered DOWN list: see
   *   `OutageCandidate.effectiveStatus` for the reason, which is that filtering
   *   made the closure signal structurally unreachable in production.
   * @param asOf - The instant the window is measured back from. A parameter so a
   *   spec can pin it; production passes nothing.
   * @returns attractionId → its running outage, for the rides that have one.
   */
  async getCurrentOutages(
    park: OutageParkContext,
    candidates: OutageCandidate[],
    asOf: Date = new Date(),
  ): Promise<Map<string, CurrentOutage>> {
    const out = new Map<string, CurrentOutage>();
    if (!park.wikiEntityId) return out;

    const eligible = candidates.filter(
      (c) => !isCuratedOutOfService(c, park.timezone),
    );
    if (eligible.length === 0) return out;

    // The reported-DOWN query asks only about rides that already read DOWN.
    const ids = eligible
      .filter((c) => c.effectiveStatus === "DOWN")
      .map((c) => c.id);
    // The closure query needs the whole roster — see OutageCandidate.
    const allIds = eligible.map((c) => c.id);

    // NOTE, and the three ways this has been got wrong: the closure query runs
    // for every ride the DOWN query did not place, and nothing short of the
    // statement's own population check may stop it. Not "some other ride in
    // this park reads DOWN", not "the DOWN query came back empty", and not "the
    // ride in question reads DOWN". Each of those looked equivalent to the
    // SQL's check and each silently took the closure line away from rides that
    // were standing still — on the day a blind park emits its first-ever DOWN,
    // which is the one day the two signals meet. Because the projection sends
    // `outage` on every poll, the note visibly vanishes for a ride that had not
    // recovered.

    const since = new Date(
      asOf.getTime() - TRAILING_OUTAGE_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
    );
    // The upper bound is open and sits slightly ahead of `asOf`: a row written
    // by the poll that is running right now carries that batch's timestamp, and
    // a bound of exactly `asOf` would drop the very reading the caller is
    // rendering.
    const until = new Date(asOf.getTime() + 60 * 1000);

    // Whether the DOWN query got to answer at all, which is not the same
    // question as whether it found anything. See the closure hand-off below.
    let downAnswered = false;

    try {
      const rows: Array<{
        attractionId: string;
        startedAt: Date;
        startObserved: boolean;
        rowsInRun: number;
        elapsedOperatingMinutes: number | string;
        hasWindows: boolean;
      }> =
        ids.length === 0
          ? []
          : await this.queueDataRepository.manager.query(
              trailingOutageWithElapsedSql(),
              [[park.id], since, until, ids],
            );

      // No early return on an empty result, and that is the point: it used to
      // `return out` here, which is the NOTE above reached by a different road.
      if (rows.length > 0) {
        // The curves answer "how much longer", which is optional by design —
        // `estimate` is documented as absent whenever the curve cannot answer.
        // Letting a failed read of them escape into the catch below would drop
        // the outage lines this query just placed AND, because `out` would then
        // be empty, hand every reported-DOWN ride to a statement that answers
        // with a different word. A missing estimate must not cost a sentence.
        const curves = await this.loadCurves().catch((error) => {
          this.logger.warn(
            `Recovery curves unavailable for park ${park.id}: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
          return {
            byPark: new Map<string, DowntimeRecoveryCurve[]>(),
            pooled: [],
          };
        });

        // Resolved once. curvesFor filters both the park array and the pooled
        // array on every call, and the signal is the constant "down" for every
        // row -- eight rides down meant eight identical filterings.
        const downCurves = this.curvesFor(curves, park.id, "down");

        // Estimate first, calendar second, and the order is the point: the
        // extra query is worth a round trip only where there is a quartile
        // pair to place, and "reads DOWN in a park with hours" is not that.
        // An outage under the curve's first bucket, a bucket under the sample
        // floor and a curve read that just failed all arrive here with nothing
        // to project — the last of those being exactly the moment the database
        // is already in trouble.
        //
        // A park that publishes no hours has no operating clock, so its elapsed
        // figure is zero for a reason that has nothing to do with the ride.
        // Reading a curve at that zero would answer every outage there with
        // "just started".
        const estimates = new Map(
          rows.map((row) => {
            const elapsed = Number(row.elapsedOperatingMinutes);
            return [
              row.attractionId,
              row.hasWindows && Number.isFinite(elapsed)
                ? estimateOutage(downCurves, elapsed)
                : undefined,
            ] as const;
          }),
        );
        const windows = [...estimates.values()].some((e) => e?.remaining)
          ? await this.loadUpcomingWindows(park.id, asOf)
          : [];

        for (const row of rows) {
          const estimate = estimates.get(row.attractionId);
          out.set(row.attractionId, {
            startedAt: new Date(row.startedAt),
            startObserved: row.startObserved === true,
            rowsInRun: Number(row.rowsInRun) || 0,
            signal: "down",
            // Re-derived with the calendar rather than patched onto the object,
            // so `estimateOutage` stays the one place the window comes from.
            // The call is pure and reads eleven curve rows.
            estimate:
              estimate && windows.length > 0
                ? estimateOutage(
                    downCurves,
                    Number(row.elapsedOperatingMinutes),
                    { windows, asOf },
                  )
                : estimate,
          });
        }
      }

      // Set here, after the rows have been READ, not after the query returned.
      // The flag says the DOWN query got to answer; a throw inside the loop
      // above means it did not, for the rides it had not reached yet. Setting
      // it early left those rides looking un-placed to the hand-off below, so
      // they went to a statement that answers with a different word — the third
      // road into the same wording hole this flag exists to close.
      downAnswered = true;
    } catch (error) {
      // A line under a badge is a nicety; the park page is not. The same
      // posture `downYesterday()` takes, and for the same reason: one failing
      // query here must not cost every ride on the page its status.
      this.logger.warn(
        `Current outages unavailable for park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    // Every ride the DOWN query did not place — which INCLUDES the rides that
    // read DOWN and got nothing back, and EXCLUDES them again when the query
    // never got to answer.
    //
    // The first half closes the last hole in the NOTE's scenario: the one ride
    // actually standing still had been filtered out of the closure candidates
    // on the strength of a status whose query had just answered nothing, so it
    // was the only ride in the park with no line at all.
    //
    // It is nearly free rather than free, and the difference is worth stating.
    // The closure statement only returns rides whose newest STANDBY reading is
    // CLOSED, while `effectiveStatus` comes from the caller's own precedence
    // chain over sources this statement never sees. When the two disagree, a
    // ride badged DOWN can come back `closed_gap` and read „steht still" under
    // a „Störung"-badge. That is the milder of the two errors on offer: it
    // understates what we know, where withholding the line entirely leaves the
    // one ride that IS standing still with nothing at all, and where the
    // inverse — an inferred outage worded as a reported one — is the direction
    // `CurrentOutage.signal` forbids outright.
    //
    // The second half is the difference between "no outage was found" and "we
    // could not look", and it is a wording question rather than a coverage one.
    // On a timeout `out` is empty, so without this every DOWN ride would be
    // handed to a statement that answers `closed_gap` — and `CurrentOutage.
    // signal` requires the page to say „steht still" rather than „gemeldet" for
    // that. A query timing out would silently restate what the operator told
    // us as something we merely noticed.
    const downIds = new Set(ids);
    const notDown = allIds.filter(
      (id) => !out.has(id) && (downAnswered || !downIds.has(id)),
    );
    if (notDown.length > 0) {
      await this.addClosureGaps(park, notDown, asOf, out);
    }

    return out;
  }

  /**
   * Faults inferred from a closure, for a park whose feed never says DOWN.
   *
   * Kept in its own method and behind its own try/catch: it is an addition for
   * parks that would otherwise show nothing, and it must never cost a park its
   * status row. The filters and the measurements behind them are on
   * `closure-gap.sql.ts`.
   */
  private async addClosureGaps(
    park: OutageParkContext,
    ids: string[],
    asOf: Date,
    out: Map<string, CurrentOutage>,
  ): Promise<void> {
    try {
      const rows: Array<{
        attractionId: string;
        startedAt: Date;
      }> = await this.queueDataRepository.manager.query(
        CURRENT_CLOSURE_GAP_SQL,
        [ids, park.timezone, asOf, park.id],
      );
      // No `if (rows.length === 0) return` here either. It changed nothing —
      // the loop below already does nothing on an empty array — but it is the
      // shape that put the bug in `getCurrentOutages`, waiting for the first
      // line to be added after the loop.

      for (const row of rows) {
        // Never over a reported one. The caller only asks about rides the DOWN
        // query did not place, so this should be unreachable — but the write
        // was unguarded, and an overwrite here does not lose a line, it changes
        // what the line CLAIMS: `signal` decides between „Störung gemeldet" and
        // „steht still", and downgrading a reported outage to an inferred one
        // is the one thing this feature may not do silently.
        if (out.has(row.attractionId)) continue;
        out.set(row.attractionId, {
          startedAt: new Date(row.startedAt),
          startObserved: true,
          rowsInRun: 1,
          signal: "closed_gap",
          // NO ESTIMATE, deliberately, and this is the load-bearing line.
          //
          // The curve for this signal would be fit on a population defined by
          // having recovered. `CLOSURE_GAP_INTERVALS_SQL` only emits an
          // interval once the ride is OPERATING again, and the processor stores
          // every one of them as `recovered` — so censoring in that population
          // is not the 15.6 % that licensed the estimate in the first place, it
          // is **zero**, because every closure that never came back was
          // excluded before counting. The cases thrown away are exactly the
          // visitor's worst ones: the ride that broke at 15:00 and is out for
          // the day, and the irregular early finish this signal openly cannot
          // separate from a fault. A survivorship-biased curve can only err
          // towards "it will be back soon".
          //
          // It also removes the wording problem underneath. The estimate copy
          // has no signal variant — it says „Störungen wie diese" and „waren
          // behoben" — so it undid, one line lower, the entire distinction the
          // sentence above it makes. There is no honest number to attach here
          // yet, and the line without one still carries the only thing this
          // signal knows: since when the ride has stood still.
          //
          // To turn it back on: keep a closure open when the park shuts, store
          // it censored rather than dropping it, and the Kaplan-Meier already
          // in `downtime-recovery.service.ts` does the right thing unchanged.
        });
      }
    } catch (error) {
      this.logger.warn(
        `Closure-gap lookup failed for park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
}
