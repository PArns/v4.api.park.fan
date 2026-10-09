import { LiveCacheWarmupProcessor } from "./live-cache-warmup.processor";
import type { CacheWarmupService } from "../services/cache-warmup.service";
import type { Job, Queue } from "bull";

/**
 * PAR-822: the post-sync cache warmup moved out of the `wait-times` job into
 * its own queue. These pin the two things that move must not change — the
 * three warmups still run, in order, one after the other — and the one thing
 * it adds: a request with a newer one queued behind it does no work.
 */
describe("LiveCacheWarmupProcessor", () => {
  const calls: string[] = [];
  let waiting = 0;
  let inFlight = 0;
  let maxInFlight = 0;

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
    warmupOperatingParks: step("parks"),
    warmupTopAttractions: step("attractions"),
    warmupParkOccupancy: step("occupancy"),
  };
  const queue = { getWaitingCount: jest.fn(async () => waiting) };

  const processor = new LiveCacheWarmupProcessor(
    queue as unknown as Queue,
    warmup as unknown as CacheWarmupService,
  );
  const job = (parkIds: string[]) =>
    ({ data: { parkIds, syncStartedAt: Date.now() } }) as unknown as Job;

  beforeEach(() => {
    calls.length = 0;
    waiting = 0;
    inFlight = 0;
    maxInFlight = 0;
    jest.clearAllMocks();
  });

  it("runs the three warmups sequentially in the old order", async () => {
    await processor.handleWarmup(job(["p1", "p2"]));

    expect(calls).toEqual(["parks", "attractions", "occupancy"]);
    expect(maxInFlight).toBe(1);
    expect(warmup.warmupTopAttractions).toHaveBeenCalledWith(1000);
    expect(warmup.warmupParkOccupancy).toHaveBeenCalledWith(["p1", "p2"]);
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
