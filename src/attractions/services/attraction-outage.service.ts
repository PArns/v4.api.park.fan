import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import {
  TRAILING_OUTAGE_LOOKBACK_DAYS,
  TRAILING_OUTAGE_START_SQL,
} from "../../common/utils/outage-rows.sql";
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
  ) {}

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
      }> = await this.queueDataRepository.manager.query(
        TRAILING_OUTAGE_START_SQL,
        [ids, since, until],
      );

      for (const row of rows) {
        out.set(row.attractionId, {
          startedAt: new Date(row.startedAt),
          startObserved: row.startObserved === true,
          rowsInRun: Number(row.rowsInRun) || 0,
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
