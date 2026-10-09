import {
  LiveCacheWarmupProcessor,
  SYNC_IDLE_MAX_WAIT_MS,
  SYNC_IDLE_POLL_MS,
} from "./live-cache-warmup.processor";
import type { CacheWarmupService } from "../services/cache-warmup.service";
import type { Job, Queue } from "bull";

/**
 * PAR-822: the post-sync cache warmup moved out of the `wait-times` job into
 * its own queue. These pin the things that move must not change — the three
 * warmups still run, in order, one after the other, and the park warmup (which
 * can hit upstream feeds) never fetches while a sync is fetching — and the one
 * thing it adds: a request with a newer one queued behind it does no work.
 */
describe("LiveCacheWarmupProcessor", () => {
  const calls: string[] = [];
  let waiting = 0;
  let inFlight = 0;
  let maxInFlight = 0;
  /** Successive answers of the wait-times queue's getActiveCount. */
  let activeAnswers: number[] = [];
  let slept: number[] = [];

  const step = (name: string) =>
    jest.fn(async () => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      calls.push(name);
      await new Promise((r) => setTimeout(r, 1));
      inFlight--;
      return 1;
    });

  const warmup = {
    // Mirrors CacheWarmupService: the hook is awaited before each park batch.
    warmupOperatingParks: jest.fn(
      async (opts?: { beforeBatch?: () => Promise<void> }) => {
        for (const batch of ["batch-1", "batch-2"]) {
          if (opts?.beforeBatch) await opts.beforeBatch();
          calls.push(`parks:${batch}`);
        }
        return 2;
      },
    ),
    warmupTopAttractions: step("attractions"),
    warmupParkOccupancy: step("occupancy"),
  };
  const queue = { getWaitingCount: jest.fn(async () => waiting) };
  const waitTimesQueue = {
    getActiveCount: jest.fn(async () => {
      const next = activeAnswers.shift() ?? 0;
      calls.push(`active?=${next}`);
      return next;
    }),
  };

  class TestProcessor extends LiveCacheWarmupProcessor {
    clock = 0;
    protected sleep(ms: number): Promise<void> {
      slept.push(ms);
      this.clock += ms;
      return Promise.resolve();
    }
  }

  let processor: TestProcessor;
  const job = (parkIds: string[]) =>
    ({ data: { parkIds, syncStartedAt: Date.now() } }) as unknown as Job;

  beforeEach(() => {
    calls.length = 0;
    waiting = 0;
    inFlight = 0;
    maxInFlight = 0;
    activeAnswers = [];
    slept = [];
    jest.clearAllMocks();
    processor = new TestProcessor(
      queue as unknown as Queue,
      waitTimesQueue as unknown as Queue,
      warmup as unknown as CacheWarmupService,
    );
  });

  it("runs the three warmups sequentially in the old order", async () => {
    await processor.handleWarmup(job(["p1", "p2"]));

    expect(calls.filter((c) => !c.startsWith("active?"))).toEqual([
      "parks:batch-1",
      "parks:batch-2",
      "attractions",
      "occupancy",
    ]);
    expect(maxInFlight).toBe(1);
    expect(warmup.warmupTopAttractions).toHaveBeenCalledWith(1000);
    expect(warmup.warmupParkOccupancy).toHaveBeenCalledWith(["p1", "p2"]);
  });

  it("holds every park batch until no wait-times sync is active", async () => {
    // Sync active for two polls before batch 1, then again once before batch 2.
    activeAnswers = [1, 1, 0, 1, 0];

    await processor.handleWarmup(job([]));

    expect(calls.slice(0, 7)).toEqual([
      "active?=1",
      "active?=1",
      "active?=0",
      "parks:batch-1",
      "active?=1",
      "active?=0",
      "parks:batch-2",
    ]);
    expect(slept).toEqual([
      SYNC_IDLE_POLL_MS,
      SYNC_IDLE_POLL_MS,
      SYNC_IDLE_POLL_MS,
    ]);
  });

  it("gives up rather than fetch beside a sync that never finishes", async () => {
    jest.spyOn(Date, "now").mockImplementation(() => processor.clock);
    waitTimesQueue.getActiveCount.mockImplementation(async () => 1);
    try {
      await expect(processor.waitForSyncIdle()).rejects.toThrow(/still active/);
      expect(processor.clock).toBeGreaterThanOrEqual(SYNC_IDLE_MAX_WAIT_MS);
    } finally {
      jest.spyOn(Date, "now").mockRestore();
      waitTimesQueue.getActiveCount.mockImplementation(async () => {
        const next = activeAnswers.shift() ?? 0;
        calls.push(`active?=${next}`);
        return next;
      });
    }
  });

  it("does not hold the DB-only warmups", async () => {
    await processor.handleWarmup(job([]));

    // Exactly one check per park batch, none for attractions/occupancy.
    expect(waitTimesQueue.getActiveCount).toHaveBeenCalledTimes(2);
  });

  it("does nothing when a newer request is already waiting", async () => {
    waiting = 1;

    await processor.handleWarmup(job(["p1"]));

    expect(calls).toEqual([]);
  });

  it("keeps going when one warmup throws", async () => {
    warmup.warmupOperatingParks.mockRejectedValueOnce(new Error("boom"));

    await expect(processor.handleWarmup(job([]))).resolves.toBeUndefined();
    expect(calls).toEqual(["attractions", "occupancy"]);
  });
});
