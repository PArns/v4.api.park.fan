import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import {
  TRAILING_OUTAGE_LOOKBACK_DAYS,
  trailingOutageWithElapsedSql,
} from "../../common/utils/outage-rows.sql";
import { DowntimeRecoveryCurve } from "../../analytics/entities/downtime-recovery-curve.entity";
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

    const ids = candidates
      .filter((c) => !isCuratedOutOfService(c, park.timezone))
      .map((c) => c.id);
    if (ids.length === 0) return out;

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
          // A park that publishes no hours has no operating clock, so its
          // elapsed figure is zero for a reason that has nothing to do with the
          // ride. Reading a curve at that zero would answer every outage there
          // with "just started".
          estimate:
            row.hasWindows && Number.isFinite(elapsed)
              ? estimateOutage(
                  {
                    park: curves.byPark.get(park.id) ?? [],
                    pooled: curves.pooled,
                  },
                  elapsed,
                )
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
}
