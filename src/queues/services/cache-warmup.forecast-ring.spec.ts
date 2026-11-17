import { CacheWarmupService } from "./cache-warmup.service";
import { CDN_SETTLE_MS } from "../../common/revalidation/revalidation.service";

/**
 * After the forecast warmup the frontend hears about every rebuilt park once, best-days and the
 * calendar in the same call, and again after the CDN window. Rung alone, best-days moves to the
 * new forecast while the calendar grid and the month summary keep the old one for a day.
 */
describe("CacheWarmupService › forecast ring", () => {
  const park = {
    id: "00000000-0000-4000-8000-000000000002",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "bruehl",
  };

  function setup(snapshot: string | null) {
    const parkRepository = {
      createQueryBuilder: () => ({
        select: () => ({ getRawMany: async () => [{ id: park.id }] }),
      }),
      findOne: jest.fn(async () => park),
    };
    const redis = {
      del: jest.fn(async () => 1),
      expire: jest.fn(async () => 1),
    };
    const calendarService = {
      buildCalendarResponse: jest.fn(async () => ({ meta: {}, days: [] })),
    };
    const bestDaysService = {
      precomputeForPark: jest.fn(async () => snapshot),
    };
    const revalidationService = { revalidateTags: jest.fn(async () => true) };
    const queue = { add: jest.fn(async () => ({})) };
    const service = new CacheWarmupService(
      parkRepository as never,
      {} as never,
      redis as never,
      {} as never,
      {} as never,
      {} as never,
      calendarService as never,
      bestDaysService as never,
      revalidationService as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
      queue as never,
    );
    return { service, revalidationService, queue, redis, calendarService };
  }

  it("rings best-days and the calendar at once, and again after the CDN window", async () => {
    const { service, revalidationService, queue } = setup("phantasialand");

    await service.warmupCalendarForAllParks();

    const tags = [
      "best-days:phantasialand",
      "park-calendar:europe/germany/bruehl/phantasialand",
    ];
    expect(revalidationService.revalidateTags).toHaveBeenCalledTimes(1);
    expect(revalidationService.revalidateTags).toHaveBeenCalledWith(tags, {
      immediate: true,
    });
    expect(queue.add).toHaveBeenCalledWith(
      "revalidate-tags",
      { tags },
      expect.objectContaining({ delay: CDN_SETTLE_MS }),
    );
  });

  it("evicts the season level's inputs before it rebuilds, so the ring covers a new level", async () => {
    const { service, redis, calendarService, revalidationService } =
      setup("phantasialand");

    await service.warmupCalendarForAllParks();

    const seasonKey = `ml:season-level:v4:${park.id}`;
    const eviction = redis.del.mock.calls.findIndex((keys: unknown[]) =>
      keys.includes(seasonKey),
    );
    expect(eviction).toBeGreaterThanOrEqual(0);
    // No date in the key: the park's midnight starts no new inputs, which
    // no ring would announce.
    expect(seasonKey).not.toMatch(/\d{4}-\d{2}-\d{2}/);
    const evictedAt = redis.del.mock.invocationCallOrder[eviction];
    expect(evictedAt).toBeLessThan(
      calendarService.buildCalendarResponse.mock.invocationCallOrder[0],
    );
    expect(evictedAt).toBeLessThan(
      revalidationService.revalidateTags.mock.invocationCallOrder[0],
    );
  });

  it("rings nothing for a park whose snapshot was not rebuilt", async () => {
    const { service, revalidationService, queue } = setup(null);

    await service.warmupCalendarForAllParks();

    expect(revalidationService.revalidateTags).not.toHaveBeenCalled();
    expect(queue.add).not.toHaveBeenCalled();
  });
});
