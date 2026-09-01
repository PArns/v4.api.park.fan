import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../redis/redis.module";
import { RevalidationService } from "./revalidation.service";
import {
  diffParkStatuses,
  type ParkOperatingStatus,
  type TaggablePark,
} from "./park-status-transitions";

/**
 * Where the previous run's statuses live. A single JSON blob rather than a Redis hash: the
 * SKIP_REDIS stub implements `get`/`set` and not `hgetall`, so a build without Redis degrades to
 * "no previous snapshot", which this treats as the first run and stays silent.
 */
const SNAPSHOT_KEY = "revalidate:park-status";

/** The second round: `{ tag: dueAtMs }` for the transitions still inside the CDN window. */
const PENDING_KEY = "revalidate:park-status:pending";

/** Long enough to survive a quiet night and any deploy; refreshed on every write anyway. */
const SNAPSHOT_TTL_SECONDS = 7 * 24 * 60 * 60;

/**
 * How long the public copy of a park response can outlive the opening.
 *
 * Same number and same reason as `CuratedDataProcessor.CDN_SETTLE_MS`: the park endpoint is served
 * `max-age=300, s-maxage=300, stale-while-revalidate=600` and nothing in this service can purge
 * Cloudflare, so 900s is the worst case with a minute of margin.
 */
const CDN_SETTLE_MS = 16 * 60 * 1000;

/**
 * Tells the frontend which parks just opened or closed.
 *
 * The frontend caches a park's structure fetch for a day, which is right for almost all of it and
 * wrong for two blocks: a show's showtimes are dated to TODAY, and the park response reports every
 * show and restaurant as CLOSED for as long as the park itself is. So the copy the frontend holds
 * is written at whatever hour that cache entry happened to be filled — overnight, in practice —
 * and then stands until the TTL runs out. On 2026-09-01 that meant every park on the site showing
 * yesterday's showtimes under "no performances today" while this API answered OPERATING.
 *
 * A cron on the frontend could not fix it well: park opening times are per park and per day across
 * every timezone in the catalogue, and a sweep would drop 213 cache entries to catch the handful
 * that moved. This side already recomputes park status every five minutes for the cache warmup, so
 * the transition is free to observe here — and one POST carrying the parks that actually flipped
 * costs less than any schedule guessing at them.
 *
 * Both directions are reported. Opening is the one that matters most, but a park that closed is
 * still showing its shows as running until something says otherwise.
 *
 * Every transition is revalidated TWICE, for the reason the publishing order in
 * docs/architecture/caching-strategy.md gives: the edge copy of the park response cannot be purged
 * from here, so the frontend's re-fetch can land on a copy cached minutes before the gates opened —
 * and pin it for a day, which is worse than the miss it was meant to fix. The second round fires
 * after the edge window has passed and is what bounds the damage to ~16 minutes. It is scheduled in
 * Redis rather than on a queue because this method already runs every five minutes: the cycle that
 * finds an entry due sends it along with whatever else changed, in the same POST.
 */
@Injectable()
export class ParkStatusRevalidationService {
  private readonly logger = new Logger(ParkStatusRevalidationService.name);

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidationService: RevalidationService,
  ) {}

  /**
   * Compare this cycle's statuses against the previous ones and revalidate what changed.
   *
   * Best-effort throughout: this runs inside the wait-times batch and must never fail it.
   */
  async revalidateChangedParks(
    parks: TaggablePark[],
    statuses: Map<string, ParkOperatingStatus>,
  ): Promise<number> {
    if (parks.length === 0) return 0;

    try {
      const previous = await this.readSnapshot();
      const { tags, nextSnapshot } = diffParkStatuses(
        parks,
        statuses,
        previous,
      );

      await this.writeSnapshot(nextSnapshot);

      const now = Date.now();
      const pending = await this.readPending();
      const due = Object.keys(pending).filter((tag) => pending[tag] <= now);

      const stillPending: Record<string, number> = {};
      for (const [tag, dueAt] of Object.entries(pending)) {
        if (dueAt > now) stillPending[tag] = dueAt;
      }
      for (const tag of tags) stillPending[tag] = now + CDN_SETTLE_MS;
      await this.writePending(stillPending);

      const toSend = [...new Set([...tags, ...due])];
      if (toSend.length === 0) return 0;

      if (tags.length > 0) {
        this.logger.log(
          `🔁 ${tags.length} park(s) opened or closed — revalidating their frontend cache`,
        );
      }
      // `immediate`: the next request must wait for the new data rather than be served the old
      // copy one more time. Under the default profile that copy goes to the first visitor after
      // the gates open, on the park's busiest page of the day.
      await this.revalidationService.revalidateTags(toSend, {
        immediate: true,
      });
      return toSend.length;
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Park status revalidation skipped: ${msg}`);
      return 0;
    }
  }

  private async readSnapshot(): Promise<Record<string, ParkOperatingStatus>> {
    const raw = await this.redis.get(SNAPSHOT_KEY);
    if (!raw) return {};
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        return {};
      return parsed as Record<string, ParkOperatingStatus>;
    } catch {
      // A corrupt blob reads as "first run": it seeds itself on this cycle and costs one missed
      // transition, where throwing would take the whole warmup down with it.
      return {};
    }
  }

  private async writeSnapshot(
    snapshot: Record<string, ParkOperatingStatus>,
  ): Promise<void> {
    await this.redis.set(
      SNAPSHOT_KEY,
      JSON.stringify(snapshot),
      "EX",
      SNAPSHOT_TTL_SECONDS,
    );
  }

  private async readPending(): Promise<Record<string, number>> {
    const raw = await this.redis.get(PENDING_KEY);
    if (!raw) return {};
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
        return {};
      return parsed as Record<string, number>;
    } catch {
      return {};
    }
  }

  private async writePending(pending: Record<string, number>): Promise<void> {
    await this.redis.set(
      PENDING_KEY,
      JSON.stringify(pending),
      "EX",
      SNAPSHOT_TTL_SECONDS,
    );
  }
}
