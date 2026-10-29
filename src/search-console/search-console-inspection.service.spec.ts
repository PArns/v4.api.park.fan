import {
  InspectionInputError,
  SearchConsoleInspectionService,
  mapInspection,
  normalizeInspectionUrl,
  quotaDay,
} from "./search-console-inspection.service";
import { SearchConsoleApiError } from "./search-console.client";
import { INSPECTION_DAILY_LIMIT } from "./search-console.constants";

const NOW = new Date("2026-10-28T12:00:00Z");

describe("normalizeInspectionUrl", () => {
  it("makes a path an absolute park.fan URL", () => {
    expect(normalizeInspectionUrl("/de/parks/europe")).toBe(
      "https://park.fan/de/parks/europe",
    );
  });

  it("folds www and http in, and drops the query and the fragment", () => {
    expect(normalizeInspectionUrl("http://www.park.fan/de/parks?x=1#top")).toBe(
      "https://park.fan/de/parks",
    );
    expect(normalizeInspectionUrl("park.fan/en")).toBe("https://park.fan/en");
  });

  it("refuses another host and text that is not a URL", () => {
    expect(() => normalizeInspectionUrl("https://example.com/de")).toThrow(
      InspectionInputError,
    );
    expect(() => normalizeInspectionUrl("https://api.park.fan/v1")).toThrow(
      /not park.fan/,
    );
    expect(() => normalizeInspectionUrl("  ")).toThrow(InspectionInputError);
  });
});

describe("mapInspection", () => {
  const api = {
    inspectionResult: {
      inspectionResultLink: "https://search.google.com/x",
      indexStatusResult: {
        verdict: "PASS",
        coverageState: "Submitted and indexed",
        robotsTxtState: "ALLOWED",
        indexingState: "INDEXING_ALLOWED",
        lastCrawlTime: "2026-10-27T03:00:00Z",
        pageFetchState: "SUCCESSFUL",
        googleCanonical: "https://park.fan/de/parks",
        userCanonical: "https://park.fan/de/parks/",
        sitemap: ["https://park.fan/sitemap.xml"],
        referringUrls: Array.from({ length: 14 }, (_, i) => `https://r/${i}`),
        crawledAs: "MOBILE",
      },
      mobileUsabilityResult: {
        verdict: "FAIL",
        issues: [{ issueType: "TAP_TARGETS_TOO_CLOSE" }, {}],
      },
      richResultsResult: {
        verdict: "PASS",
        detectedItems: [{ richResultType: "Breadcrumb" }],
      },
    },
  };

  it("keeps the verdicts, the canonicals and a capped list of referrers", () => {
    const result = mapInspection("https://park.fan/de/parks", api);
    expect(result).toMatchObject({
      url: "https://park.fan/de/parks",
      verdict: "PASS",
      coverageState: "Submitted and indexed",
      indexingState: "INDEXING_ALLOWED",
      robotsTxtState: "ALLOWED",
      pageFetchState: "SUCCESSFUL",
      lastCrawlTime: "2026-10-27T03:00:00Z",
      crawledAs: "MOBILE",
      googleCanonical: "https://park.fan/de/parks",
      canonicalMatches: false,
      sitemap: ["https://park.fan/sitemap.xml"],
      mobileUsability: { verdict: "FAIL", issues: ["TAP_TARGETS_TOO_CLOSE"] },
      richResults: { verdict: "PASS", types: ["Breadcrumb"] },
    });
    expect(result.referringUrls).toHaveLength(10);
    expect(result).not.toHaveProperty("inspectionResultLink");
  });

  it("leaves out what Google did not report", () => {
    const result = mapInspection("https://park.fan/x", {
      inspectionResult: { indexStatusResult: { verdict: "NEUTRAL" } },
    });
    expect(result.canonicalMatches).toBeNull();
    expect(result.sitemap).toEqual([]);
    expect(result).not.toHaveProperty("mobileUsability");
    expect(result).not.toHaveProperty("richResults");
  });
});

describe("quotaDay", () => {
  it("counts Google's day, which is Pacific", () => {
    expect(quotaDay(new Date("2026-10-28T05:00:00Z"))).toBe("2026-10-27");
    expect(quotaDay(new Date("2026-10-28T09:00:00Z"))).toBe("2026-10-28");
  });
});

describe("SearchConsoleInspectionService", () => {
  const saved = { ...process.env };
  afterEach(() => {
    process.env = { ...saved };
  });

  function build(options: { count?: number; configured?: boolean } = {}) {
    const store = new Map<string, string>();
    let count = options.count ?? 0;
    const redis = {
      get: jest.fn(async (key: string) =>
        key.includes("inspections:") ? String(count) : (store.get(key) ?? null),
      ),
      set: jest.fn(async (key: string, value: string) => {
        store.set(key, value);
        return "OK";
      }),
      incr: jest.fn(async () => ++count),
      decr: jest.fn(async () => --count),
      expire: jest.fn(async () => 1),
    };
    const client = {
      setup: () =>
        options.configured === false
          ? { configured: false, reason: null, siteUrl: null }
          : { configured: true, siteUrl: "sc-domain:park.fan" },
      inspectUrl: jest.fn(async () => ({
        inspectionResult: { indexStatusResult: { verdict: "PASS" } },
      })),
    };
    return {
      service: new SearchConsoleInspectionService(
        redis as never,
        client as never,
      ),
      redis,
      client,
    };
  }

  it("inspects each URL once, normalised, and counts the day's inspections", async () => {
    const { service, client, redis } = build();
    const report = await service.inspect(
      ["/de/parks", "https://www.park.fan/de/parks", "/en/news"],
      NOW,
    );
    expect(client.inspectUrl).toHaveBeenCalledTimes(2);
    expect(report.results.map((r) => r.url)).toEqual([
      "https://park.fan/de/parks",
      "https://park.fan/en/news",
    ]);
    expect(report.quota).toEqual({
      usedToday: 2,
      dailyLimit: INSPECTION_DAILY_LIMIT,
    });
    expect(redis.incr).toHaveBeenCalledWith(
      "search-console:inspections:2026-10-28",
    );
  });

  it("answers from the cache for six hours without spending the quota", async () => {
    const { service, client, redis } = build();
    await service.inspect(["/de/parks"], NOW);
    const again = await service.inspect(["/de/parks"], NOW);
    expect(client.inspectUrl).toHaveBeenCalledTimes(1);
    expect(again.results[0]).toMatchObject({ cached: true, verdict: "PASS" });
    expect(redis.incr).toHaveBeenCalledTimes(1);
    expect(redis.set).toHaveBeenCalledWith(
      "search-console:inspection:https://park.fan/de/parks",
      expect.any(String),
      "EX",
      21600,
    );
  });

  it("refuses past the daily limit with a message, gives the count back, and still serves the cache", async () => {
    const { service, client, redis } = build({
      count: INSPECTION_DAILY_LIMIT,
    });
    const report = await service.inspect(["/de/parks"], NOW);
    expect(client.inspectUrl).not.toHaveBeenCalled();
    expect(report.results[0]).toMatchObject({
      url: "https://park.fan/de/parks",
      error: expect.stringContaining("Daily inspection limit"),
    });
    expect(redis.decr).toHaveBeenCalledTimes(1);
    expect(report.quota.usedToday).toBe(INSPECTION_DAILY_LIMIT);
  });

  it("reports a URL Google refuses and goes on, but stops for a refused account", async () => {
    const { service, client } = build();
    client.inspectUrl
      .mockRejectedValueOnce(
        new SearchConsoleApiError("Search Console answered 400", 400, false),
      )
      .mockRejectedValueOnce(
        new SearchConsoleApiError("Search Console refused", 403, true),
      );
    const report = await service
      .inspect(["/de/a", "/de/b"], NOW)
      .catch((e) => e as Error);
    expect(report).toBeInstanceOf(SearchConsoleApiError);
    const { service: second, client: secondClient } = build();
    secondClient.inspectUrl.mockRejectedValueOnce(
      new SearchConsoleApiError("Search Console answered 400", 400, false),
    );
    const partial = await second.inspect(["/de/a", "/de/b"], NOW);
    expect(partial.results[0]).toEqual({
      url: "https://park.fan/de/a",
      error: "Search Console answered 400",
    });
    expect(partial.results[1]).toMatchObject({ verdict: "PASS" });
  });

  it("asks for between one and ten URLs, and for a configured module", async () => {
    const { service } = build();
    await expect(service.inspect([], NOW)).rejects.toThrow(
      InspectionInputError,
    );
    await expect(
      service.inspect(
        Array.from({ length: 11 }, (_, i) => `/de/${i}`),
        NOW,
      ),
    ).rejects.toThrow(/between 1 and 10/);
    await expect(
      build({ configured: false }).service.inspect(["/de"], NOW),
    ).rejects.toThrow(/not configured/);
  });

  it("works without Redis: no cache, no count", async () => {
    const { service, redis } = build();
    for (const fn of Object.values(redis))
      fn.mockRejectedValue(new Error("down"));
    const report = await service.inspect(["/de/parks"], NOW);
    expect(report.results[0]).toMatchObject({ verdict: "PASS", cached: false });
    expect(report.quota.usedToday).toBeNull();
  });
});
