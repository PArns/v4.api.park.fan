import { InjectQueue, Processor, Process } from "@nestjs/bull";
import { Inject, Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import { RideStatsService } from "../../attractions/services/ride-stats.service";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * How long the public copy of a park response can outlive a write: the park
 * endpoint is served `max-age=300, stale-while-revalidate=600` and nothing here
 * can purge Cloudflare. Mirrors `ManualMetadataProcessor`.
 */
const CDN_SETTLE_MS = 16 * 60 * 1000;

/** Optional cap for a trial run, read off the job payload. */
interface RcdbStatsJob {
  limit?: number;
}

/**
 * Imports ride measurements from RCDB.
 *
 * Its own queue because it is the slow one: ~500 rides fetched a second and a
 * bit apart is a job measured in minutes, and parking that in front of the
 * curated-seed queue would delay a one-line seed fix by exactly the wait that
 * queue exists to avoid.
 */
@Processor("rcdb-stats")
export class RcdbStatsProcessor {
  private readonly logger = new Logger(RcdbStatsProcessor.name);

  constructor(
    private readonly rideStats: RideStatsService,
    private readonly revalidationService: RevalidationService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @InjectQueue("rcdb-stats") private readonly queue: Queue,
  ) {}

  @Process("import-stats")
  async handleImportStats(job: Job<RcdbStatsJob>): Promise<void> {
    this.logger.log("🎢 Importing ride stats from RCDB...");
    const result = await this.rideStats.import(job.data?.limit);

    if (result.written === 0) {
      this.logger.log("🎢 Nothing written — no caches to clear");
      return;
    }

    // Same order as the curated seed: our caches first, the frontend last, and
    // once more after the edge copy has expired. Revalidating first would have
    // the frontend refetch the pre-import payload and pin it for 24h.
    for (const { parkId, attractionIds } of result.touchedParks) {
      try {
        await invalidateParkCaches(this.redis, parkId, attractionIds);
      } catch (error) {
        this.logger.warn(
          `Cache invalidation failed for park ${parkId}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
    this.logger.log(
      `🧹 Invalidated cached responses for ${result.touchedParks.length} park(s)`,
    );

    await this.revalidationService.revalidateTags(["parks", "attractions"]);
    await this.queue.add(
      "revalidate-parks",
      {},
      {
        delay: CDN_SETTLE_MS,
        removeOnComplete: true,
        removeOnFail: true,
        jobId: "rcdb-revalidate-parks-after-cdn",
      },
    );
  }

  /** The delayed half of the publish, once the edge copy has expired. */
  @Process("revalidate-parks")
  async handleRevalidateParks(_job: Job): Promise<void> {
    this.logger.log("♻️ Re-revalidating parks after the CDN window...");
    await this.revalidationService.revalidateTags(["parks", "attractions"]);
  }
}
