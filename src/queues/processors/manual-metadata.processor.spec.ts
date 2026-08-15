import { ManualMetadataProcessor } from "./manual-metadata.processor";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";

jest.mock("../../common/cache/park-cache-invalidation", () => ({
  invalidateParkCaches: jest.fn().mockResolvedValue(undefined),
}));

/**
 * A metadata write is only published when the caches in front of it are gone.
 *
 * Announcing the write to the frontend first is worse than not announcing it:
 * the frontend refetches immediately, reads the pre-seed payload from
 * `park:integrated` or the Cloudflare edge, and pins THAT for 24h. A curated
 * value was written, announced, and still missing from the ride page the next
 * morning — that is the bug these tests exist to keep fixed.
 */
describe("ManualMetadataProcessor", () => {
  const manualMetadata = { apply: jest.fn() };
  const rideProfiles = { findCuratedSince: jest.fn() };
  const revalidationService = { revalidateTags: jest.fn() };
  const redis = {};
  const queue = { add: jest.fn() };

  const processor = () =>
    new ManualMetadataProcessor(
      manualMetadata as never,
      rideProfiles as never,
      revalidationService as never,
      redis as never,
      queue as never,
    );

  beforeEach(() => {
    jest.clearAllMocks();
    (invalidateParkCaches as jest.Mock).mockResolvedValue(undefined);
    revalidationService.revalidateTags.mockResolvedValue(undefined);
    queue.add.mockResolvedValue(undefined);
  });

  const seedResult = (over: Record<string, unknown> = {}) => ({
    rcdb: 2,
    heights: 0,
    wet: 0,
    touchedParks: [{ parkId: "p1", attractionIds: ["a1", "a2"] }],
    ...over,
  });

  it("evicts the park's cached response BEFORE telling the frontend", async () => {
    const order: string[] = [];
    (invalidateParkCaches as jest.Mock).mockImplementation(() => {
      order.push("invalidate");
      return Promise.resolve();
    });
    revalidationService.revalidateTags.mockImplementation(() => {
      order.push("revalidate");
      return Promise.resolve();
    });
    manualMetadata.apply.mockResolvedValue(seedResult());

    await processor().handleApplySeed({} as never);

    expect(order).toEqual(["invalidate", "revalidate"]);
    expect(invalidateParkCaches).toHaveBeenCalledWith(redis, "p1", [
      "a1",
      "a2",
    ]);
  });

  it("queues a second revalidation past the CDN window", async () => {
    // Nothing here can purge Cloudflare, and the edge copy lives up to
    // max-age 300 + stale-while-revalidate 600. The refetch triggered above
    // can still read it, so one more sweep has to land after it expires.
    manualMetadata.apply.mockResolvedValue(seedResult());

    await processor().handleApplySeed({} as never);

    expect(queue.add).toHaveBeenCalledWith(
      "revalidate-parks",
      {},
      expect.objectContaining({ delay: 16 * 60 * 1000 }),
    );
  });

  it("collapses repeated seed runs onto one pending sweep", async () => {
    // Iterating on the seed means running it several times in a row. Each run
    // must not leave its own delayed job, or they all fire in the same minute.
    manualMetadata.apply.mockResolvedValue(seedResult());

    await processor().handleApplySeed({} as never);

    const [, , options] = queue.add.mock.calls[0] as [
      string,
      unknown,
      { jobId?: string },
    ];
    expect(options.jobId).toBe("revalidate-parks-after-cdn");
  });

  it("still tells the frontend when one park's eviction fails", async () => {
    (invalidateParkCaches as jest.Mock)
      .mockRejectedValueOnce(new Error("redis down"))
      .mockResolvedValue(undefined);
    manualMetadata.apply.mockResolvedValue(
      seedResult({
        touchedParks: [
          { parkId: "p1", attractionIds: ["a1"] },
          { parkId: "p2", attractionIds: ["a2"] },
        ],
      }),
    );

    await processor().handleApplySeed({} as never);

    expect(invalidateParkCaches).toHaveBeenCalledTimes(2);
    expect(revalidationService.revalidateTags).toHaveBeenCalledWith([
      "parks",
      "attractions",
    ]);
  });

  it("does nothing at all when the seed wrote nothing", async () => {
    manualMetadata.apply.mockResolvedValue(
      seedResult({ rcdb: 0, touchedParks: [] }),
    );

    await processor().handleApplySeed({} as never);

    expect(invalidateParkCaches).not.toHaveBeenCalled();
    expect(revalidationService.revalidateTags).not.toHaveBeenCalled();
    expect(queue.add).not.toHaveBeenCalled();
  });

  it("publishes the parks whose profiles were curated in the window", async () => {
    rideProfiles.findCuratedSince.mockResolvedValue([
      { parkId: "p1", attractionIds: ["a1", "a2"] },
    ]);

    await processor().handlePublishRideProfiles({ data: {} } as never);

    expect(invalidateParkCaches).toHaveBeenCalledWith(redis, "p1", [
      "a1",
      "a2",
    ]);
    expect(revalidationService.revalidateTags).toHaveBeenCalled();
    expect(queue.add).toHaveBeenCalled();
  });

  it("defaults to the last 24h and honours an explicit window", async () => {
    rideProfiles.findCuratedSince.mockResolvedValue([]);

    await processor().handlePublishRideProfiles({ data: {} } as never);
    const [defaultSince] = rideProfiles.findCuratedSince.mock.calls[0] as [
      Date,
    ];
    const defaultAgeHours = (Date.now() - defaultSince.getTime()) / 3_600_000;
    expect(defaultAgeHours).toBeCloseTo(24, 1);

    await processor().handlePublishRideProfiles({
      data: { sinceHours: 2 },
    } as never);
    const [explicitSince] = rideProfiles.findCuratedSince.mock.calls[1] as [
      Date,
    ];
    const explicitAgeHours = (Date.now() - explicitSince.getTime()) / 3_600_000;
    expect(explicitAgeHours).toBeCloseTo(2, 1);
  });

  it("does nothing when no profile was curated in the window", async () => {
    // A publish with nothing to publish must not evict a warm park or burn a
    // revalidation — this endpoint is meant to be safe to call speculatively.
    rideProfiles.findCuratedSince.mockResolvedValue([]);

    await processor().handlePublishRideProfiles({ data: {} } as never);

    expect(invalidateParkCaches).not.toHaveBeenCalled();
    expect(revalidationService.revalidateTags).not.toHaveBeenCalled();
    expect(queue.add).not.toHaveBeenCalled();
  });

  it("revalidates again when the delayed job fires", async () => {
    await processor().handleRevalidateParks({} as never);

    expect(revalidationService.revalidateTags).toHaveBeenCalledWith([
      "parks",
      "attractions",
    ]);
    // The delayed sweep must not schedule another one — that is a loop.
    expect(queue.add).not.toHaveBeenCalled();
  });
});
