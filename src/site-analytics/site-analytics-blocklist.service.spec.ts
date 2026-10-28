import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SITE_ANALYTICS_KEYS as KEYS } from "./site-analytics.constants";

/**
 * What is pinned: blocking a name frees its place in the day's names guard
 * and drops its combinations, so a script's names, once blocked, stop
 * crowding out the site's own; Redis away does not fail the block.
 */
describe("SiteAnalyticsBlocklist", () => {
  const NOW = new Date("2026-10-28T09:00:00Z");
  let redis: SiteAnalyticsRedisMock;
  let query: jest.Mock;
  let blocklist: SiteAnalyticsBlocklist;

  beforeEach(() => {
    redis = new SiteAnalyticsRedisMock();
    query = jest.fn().mockResolvedValue([]);
    blocklist = new SiteAnalyticsBlocklist({ query } as never, redis as never);
  });

  it("stores the block and frees the name's place in today's guard", async () => {
    redis.sets.set(
      KEYS.guard("names", "2026-10-28"),
      new Set(["junk", "real"]),
    );
    redis.sets.set(
      KEYS.guard("combinations", "2026-10-28", "junk"),
      new Set(["c1", "c2"]),
    );
    await blocklist.block("junk", "admin@park.fan", NOW);
    expect(query.mock.calls[0][1]).toEqual(["junk", "admin@park.fan"]);
    expect(redis.sets.get(KEYS.guard("names", "2026-10-28"))).toEqual(
      new Set(["real"]),
    );
    expect(
      redis.sets.has(KEYS.guard("combinations", "2026-10-28", "junk")),
    ).toBe(false);
  });

  it("blocks all the same when Redis is away", async () => {
    redis.failNext = ["down"];
    await expect(
      blocklist.block("junk", "admin@park.fan", NOW),
    ).resolves.toBeUndefined();
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("does not retry a failed load before the TTL", async () => {
    query.mockRejectedValueOnce(new Error("down"));
    await blocklist.names();
    await blocklist.names();
    expect(query).toHaveBeenCalledTimes(1);
    await blocklist.names(Date.now() + 61_000);
    expect(query).toHaveBeenCalledTimes(2);
  });
});
