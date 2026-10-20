import { invalidateParkCaches } from "./park-cache-invalidation";

/**
 * Every caller of this helper is a structural change — a park merge, repair,
 * or rename — where rows genuinely disappear. The attraction sitemap is built
 * from a flat query and cached for 24h under one global key, so a merge that
 * does not evict it leaves the sitemap advertising slugs that now 404.
 *
 * That happened for real: after the first live attraction merge the park
 * detail served only the surviving slug while /v1/sitemap/attractions still
 * listed alice-in-wonderland-2, big-dipper-2 and three more.
 */
describe("invalidateParkCaches", () => {
  const pipelineDel = jest.fn();
  const redis = {
    del: jest.fn().mockResolvedValue(1),
    keys: jest.fn(() => {
      throw new Error("KEYS blocks Redis and must not be called");
    }),
    scan: jest.fn(async (_c: string, _m: string, pattern: string) =>
      pattern.startsWith("schedule:")
        ? ["0", ["schedule:today:park-1:x"]]
        : ["0", []],
    ),
    pipeline: jest.fn(() => ({ del: pipelineDel, exec: jest.fn() })),
  };

  beforeEach(() => jest.clearAllMocks());

  const deletedKeys = () => redis.del.mock.calls.flat();

  it("evicts the glob patterns with SCAN, never KEYS", async () => {
    await invalidateParkCaches(redis as never, "park-1");

    expect(redis.scan).toHaveBeenCalled();
    expect(pipelineDel).toHaveBeenCalledWith("schedule:today:park-1:x");
  });

  it("evicts the attraction sitemap so removed slugs stop being advertised", async () => {
    await invalidateParkCaches(redis as never, "park-1");

    expect(deletedKeys()).toContain("sitemap:attractions:v4");
  });

  it("still evicts the park-scoped caches", async () => {
    await invalidateParkCaches(redis as never, "park-1");

    const keys = deletedKeys();
    expect(keys.some((k: string) => k.includes("park-1"))).toBe(true);
  });

  it("evicts the caches of attractions handed to it", async () => {
    await invalidateParkCaches(redis as never, "park-1", ["attr-9"]);

    expect(deletedKeys().some((k: string) => k.includes("attr-9"))).toBe(true);
  });
});
