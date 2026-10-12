import { InjectQueue, Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { CacheWarmupService } from "../services/cache-warmup.service";

export const LIVE_CACHE_WARMUP_QUEUE = "live-cache-warmup";
export const LIVE_CACHE_WARMUP_JOB = "warmup-after-sync";

/** How often to re-check whether the wait-times sync has finished. */
export const SYNC_IDLE_POLL_MS = 2_000;
/**
 * How long a park warmup waits for a running sync before giving up on this
 * round. A sync takes ~55 s; five minutes means something is stuck, and the
 * next sync enqueues a fresh warmup anyway.
 */
export const SYNC_IDLE_MAX_WAIT_MS = 5 * 60_000;

export interface LiveCacheWarmupJobData {
  /** Park ids in the sync's priority order (top parks first). */
  parkIds: string[];
  /** When the wait-times sync that asked for this warmup started (epoch ms). */
  syncStartedAt: number;
}

/**
 * The cache warmup that follows every wait-times sync (PAR-822).
 *
 * It used to run inside the `wait-times` job, after the fetch. Measured on
 * production on 2026-10-09 that put ~210 s of a ~263 s run behind the fetch
 * (park caches ~68 s, top-1000 attractions ~92 s, park occupancy ~43 s), and
 * about 125 s of that is the deliberate 1 s pause between warmup batches that
 * keeps the DB from saturating. The fetch itself took ~51 s. Because the
 * `wait-times` queue runs one job at a time, the next sync could not start
 * until the previous warmup was done, so each run that went past five minutes
 * pushed the next fetch back.
 *
 * Running it here keeps the warmup's pacing exactly as it was (same three
 * calls, still sequential) and takes it off the fetch's critical path.
 *
 * Upstream: the park warmup is not DB-only. `buildIntegratedResponse`
 * re-checks a park whose schedule reads CLOSED against its live feed
 * (queue-times, Wartezeiten or ThemeParks.wiki). Upstream access stays one
 * request stream per source, so before every park batch this waits until no
 * wait-times sync is active — the two never fetch at the same time, exactly
 * as when both lived in one job. The attraction and occupancy warmups read
 * only the DB and Redis and are not held.
 *
 * Coalescing: a warmup reads whatever is in the database when it runs, so two
 * queued warmups do the same work twice. When another request is already
 * waiting behind this one, this one returns at once and the newer one does
 * the work. Adding never drops a request — a sync that finishes while a
 * warmup is active still gets its own warmup afterwards.
 */
@Processor(LIVE_CACHE_WARMUP_QUEUE)
export class LiveCacheWarmupProcessor {
  private readonly logger = new Logger(LiveCacheWarmupProcessor.name);

  constructor(
    @InjectQueue(LIVE_CACHE_WARMUP_QUEUE) private readonly queue: Queue,
    @InjectQueue("wait-times") private readonly waitTimesQueue: Queue,
    private readonly cacheWarmupService: CacheWarmupService,
  ) {}

  /** Overridable in tests. */
  protected sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Resolves once no wait-times job is active. Throws after
   * SYNC_IDLE_MAX_WAIT_MS, which ends the park warmup for this round rather
   * than letting it fetch beside the sync.
   */
  async waitForSyncIdle(): Promise<void> {
    const startedAt = Date.now();
    let waited = false;
    while ((await this.waitTimesQueue.getActiveCount()) > 0) {
      if (Date.now() - startedAt >= SYNC_IDLE_MAX_WAIT_MS) {
        throw new Error(
          `wait-times sync still active after ${SYNC_IDLE_MAX_WAIT_MS / 1000}s — park warmup stopped for this round`,
        );
      }
      waited = true;
      await this.sleep(SYNC_IDLE_POLL_MS);
    }
    if (waited) {
      this.logger.log(
        `⏸️  Park warmup held ${Date.now() - startedAt} ms for a running wait-times sync`,
      );
    }
  }

  @Process(LIVE_CACHE_WARMUP_JOB)
  async handleWarmup(job: Job<LiveCacheWarmupJobData>): Promise<void> {
    const waiting = await this.queue.getWaitingCount();
    if (waiting > 0) {
      this.logger.log(
        `⏭️  Skipping live cache warmup — ${waiting} newer request(s) queued behind it`,
      );
      return;
    }

    const parkIds = job.data?.parkIds ?? [];
    const startedAt = Date.now();
    const phases: Record<string, number> = {};
    const timed = async (name: string, fn: () => Promise<unknown>) => {
      const t = Date.now();
      try {
        await fn();
      } catch (e) {
        // One failed warmup must not cost the other two.
        this.logger.warn(
          `Live cache warmup step ${name} failed: ${(e as Error)?.message ?? e}`,
        );
      } finally {
        phases[name] = Date.now() - t;
      }
    };

    // Sequential, not Promise.all: each warmup already fans out batched DB
    // work against queue_data, and firing all three at once recreates the
    // connection-pool/DB-saturation peak the batching is there to avoid.
    await timed("operatingParksMs", () =>
      this.cacheWarmupService.warmupOperatingParks({
        beforeBatch: () => this.waitForSyncIdle(),
      }),
    );
    await timed("topAttractionsMs", () =>
      this.cacheWarmupService.warmupTopAttractions(1000),
    );
    await timed("parkOccupancyMs", () =>
      this.cacheWarmupService.warmupParkOccupancy(parkIds),
    );

    const totalMs = Date.now() - startedAt;
    const sinceSyncStartMs = job.data?.syncStartedAt
      ? Date.now() - job.data.syncStartedAt
      : null;
    this.logger.log(
      `⏱️  live-cache-warmup phases ${JSON.stringify({ ...phases, totalMs, sinceSyncStartMs })}`,
    );
  }
}
