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
   * It also needs the whole roster for a second reason: the simultaneity filter
   * counts how many rides shut in the same minute, and over a pre-filtered list
   * that count is always one. The filter that removes a park-wide closing can
   * only work over the park.
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
 * It does not decide whether a ride is down. The caller passes the rides that
 * already read `DOWN` through its own chain, and this answers when that started.
 * Asking it to decide would be a third status chain.
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
   * @param candidates - The rides that already read `DOWN`, with their curated
   *   works-period columns.
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

    if (ids.length === 0) {
      await this.addClosureGaps(park, allIds, asOf, out);
      return out;
    }

    const since = new Date(
      asOf.getTime() - TRAILING_OUTAGE_LOOKBACK_DAYS * 24 * 60 * 60 * 1000,
    );
    // The upper bound is open and sits slightly ahead of `asOf`: a row written
    // by the poll that is running right now carries that batch's timestamp, and
    // a bound of exactly `asOf` would drop the very reading the caller is
    // rendering.
    const until = new Date(asOf.getTime() + 60 * 1000);

    try {
      const rows: Array<{
        attractionId: string;
        startedAt: Date;
        startObserved: boolean;
        rowsInRun: number;
        elapsedOperatingMinutes: number | string;
        hasWindows: boolean;
      }> = await this.queueDataRepository.manager.query(
        trailingOutageWithElapsedSql(),
        [[park.id], since, until, ids],
      );
      if (rows.length === 0) return out;

      const curves = await this.loadCurves();

      for (const row of rows) {
        const elapsed = Number(row.elapsedOperatingMinutes);
        out.set(row.attractionId, {
          startedAt: new Date(row.startedAt),
          startObserved: row.startObserved === true,
          rowsInRun: Number(row.rowsInRun) || 0,
          signal: "down",
          // A park that publishes no hours has no operating clock, so its
          // elapsed figure is zero for a reason that has nothing to do with the
          // ride. Reading a curve at that zero would answer every outage there
          // with "just started".
          estimate:
            row.hasWindows && Number.isFinite(elapsed)
              ? estimateOutage(this.curvesFor(curves, park.id, "down"), elapsed)
              : undefined,
        });
      }
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
      if (rows.length === 0) return;

      for (const row of rows) {
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
