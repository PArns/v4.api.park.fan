import { InjectQueue, Processor, Process } from "@nestjs/bull";
import { Inject, Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import {
  RideProfileService,
  TouchedPark,
} from "../../attractions/services/ride-profile.service";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * How long the public copy of a park response can outlive a write.
 *
 * The park endpoint is served with `max-age=300, s-maxage=300,
 * stale-while-revalidate=600` and nothing in this service can purge
 * Cloudflare — the edge copy only expires by TTL, so 900s is the worst case.
 * A minute of margin on top covers clock skew and a slow revalidation.
 */
const CDN_SETTLE_MS = 16 * 60 * 1000;

/**
 * Publishes hand-curated attraction data on its own queue.
 *
 * Deliberately separate from children-metadata: that queue is occupied for
 * hours by the detail sweep's ~7000 rate-limited wiki requests, and a job
 * queued behind it would inherit exactly the delay this exists to avoid.
 */
@Processor("manual-metadata")
export class ManualMetadataProcessor {
  private readonly logger = new Logger(ManualMetadataProcessor.name);

  constructor(
    private readonly rideProfiles: RideProfileService,
    private readonly revalidationService: RevalidationService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @InjectQueue("manual-metadata") private readonly queue: Queue,
  ) {}

  /**
   * Publish ride profiles that were curated directly in the database.
   *
   * The rows are edited by hand now — there is no seed job to write them and,
   * with it gone, nothing evicted the caches sitting in front of them either.
   * A correct row would surface only as the TTLs expired: `park:integrated`
   * up to six hours for a closed park, the Cloudflare copy 900s on top, and
   * the frontend pinning whatever it read for a day. This is the missing half
   * — the same {@link publish} the metadata seed uses, pointed at whatever was
   * curated recently.
   */
  @Process("publish-ride-profiles")
  async handlePublishRideProfiles(
    job: Job<{ sinceHours?: number }>,
  ): Promise<void> {
    const sinceHours = job.data?.sinceHours ?? 24;
    const since = new Date(Date.now() - sinceHours * 60 * 60 * 1000);
    const touchedParks = await this.rideProfiles.findCuratedSince(since);

    if (touchedParks.length === 0) {
      this.logger.log(
        `🎢 No ride profiles curated in the last ${sinceHours}h — nothing to publish`,
      );
      return;
    }

    const rides = touchedParks.reduce((n, p) => n + p.attractionIds.length, 0);
    this.logger.log(
      `🎢 Publishing ${rides} curated ride profile(s) across ${touchedParks.length} park(s)...`,
    );
    await this.publish(touchedParks);
  }

  /** The delayed half of {@link publish} — see the reasoning there. */
  @Process("revalidate-parks")
  async handleRevalidateParks(_job: Job): Promise<void> {
    this.logger.log("♻️ Re-revalidating parks after the CDN window...");
    await this.revalidationService.revalidateTags(["parks", "attractions"]);
  }

  /**
   * Make a seed write visible, in the only order that actually works.
   *
   * Telling the frontend to revalidate is the LAST step, not the first. It
   * refetches the moment it is told, and every cache between it and the
   * database still holds the pre-seed payload: `park:integrated:{parkId}` in
   * Redis (up to 6h for a closed park) and the Cloudflare edge copy. Whatever
   * comes back it then pins in its own data cache **for 24 hours** — so
   * revalidating first does not publish the write, it freezes the state from
   * before it for a day. That is how a curated ride profile could be written,
   * announced, and still be missing from the ride page the next morning.
   *
   * So: evict our own caches first, then tell the frontend — and tell it a
   * second time once the edge copy has expired, because that one we cannot
   * purge and its 900s window is wide enough to lose the race.
   */
  private async publish(touchedParks: TouchedPark[]): Promise<void> {
    for (const { parkId, attractionIds } of touchedParks) {
      try {
        await invalidateParkCaches(this.redis, parkId, attractionIds);
      } catch (error) {
        // One unreachable key must not stop the rest: the remaining parks
        // still deserve their eviction, and the frontend still needs telling.
        this.logger.warn(
          `Cache invalidation failed for park ${parkId}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
    this.logger.log(
      `🧹 Invalidated cached responses for ${touchedParks.length} park(s)`,
    );

    await this.revalidationService.revalidateTags(["parks", "attractions"]);

    await this.queue.add(
      "revalidate-parks",
      {},
      {
        delay: CDN_SETTLE_MS,
        removeOnComplete: true,
        removeOnFail: true,
        // Fixed id: re-running the seed a few times in a row (the normal way
        // to iterate on it) should leave ONE pending sweep, not a pile of them
        // all firing within a minute of each other.
        jobId: "revalidate-parks-after-cdn",
      },
    );
  }
}
