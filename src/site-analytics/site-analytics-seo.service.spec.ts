import { SiteAnalyticsSeoService } from "./site-analytics-seo.service";

const NOW = new Date("2026-10-28T07:12:40Z");
const GEO = {
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};

/**
 * The SEO view. What is pinned: landing pages carry their change and bounce
 * and their sources; a trend needs a minimum volume and lists rising and
 * falling apart; a page is "uncovered" when no search visit entered on its
 * frontend path in any language, retired ones not listed.
 */
describe("SiteAnalyticsSeoService", () => {
  let routes: Array<[string, unknown[]]>;
  let calls: Array<[string, unknown[]]>;
  let parks: unknown[];
  let attractions: unknown[];
  let service: SiteAnalyticsSeoService;

  beforeEach(() => {
    routes = [];
    calls = [];
    parks = [];
    attractions = [];
    const db = {
      query: async (sql: string, params: unknown[]) => {
        calls.push([sql, params]);
        return routes.find(([match]) => sql.includes(match))?.[1] ?? [];
      },
    };
    service = new SiteAnalyticsSeoService(
      db as never,
      { find: async () => parks } as never,
      { find: async () => attractions } as never,
    );
  });

  it("lists landing pages with change, bounce and their sources", async () => {
    routes = [
      [
        "GROUP BY entry",
        [
          {
            path: "/de/parks/x",
            page_type: "park",
            visits: "10",
            previous: "4",
            engaged: "6",
          },
        ],
      ],
      [
        "AS page, referrer",
        [
          { page: "/de/parks/x", referrer: "google", count: "8" },
          { page: "/de/parks/x", referrer: "bing", count: "2" },
        ],
      ],
    ];
    const result = await service.landingPages(
      { days: 28, limit: 10, channel: "ai" },
      NOW,
    );
    expect(result.channel).toBe("ai");
    expect(result.items).toEqual([
      {
        path: "/de/parks/x",
        pageType: "park",
        visits: 10,
        previous: 4,
        change: 6,
        bounceRate: 0.4,
        sources: [
          { value: "google", visits: 8 },
          { value: "bing", visits: 2 },
        ],
      },
    ]);
    expect(calls[0][1]).toContain("ai");
  });

  it("ranks rising and falling pages apart, and leaves out the ones too small to mean anything", async () => {
    routes = [
      [
        "GROUP BY entry",
        [
          {
            path: "/a",
            page_type: "park",
            visits: "30",
            previous: "10",
            engaged: "0",
          },
          {
            path: "/b",
            page_type: "park",
            visits: "2",
            previous: "20",
            engaged: "0",
          },
          {
            path: "/c",
            page_type: "news",
            visits: "6",
            previous: "1",
            engaged: "0",
          },
          {
            path: "/d",
            page_type: "news",
            visits: "4",
            previous: "3",
            engaged: "0",
          },
          {
            path: "/e",
            page_type: "news",
            visits: "5",
            previous: "5",
            engaged: "0",
          },
        ],
      ],
    ];
    const result = await service.trends({ days: 28 }, NOW);
    expect(result.channel).toBe("search");
    expect(result.rising.map((r) => [r.path, r.change])).toEqual([["/a", 20]]);
    expect(result.falling.map((r) => [r.path, r.change])).toEqual([
      ["/b", -18],
    ]);
  });

  it("finds the parks and rides no search visit entered on, in any language", async () => {
    routes = [
      [
        "SELECT DISTINCT substring(entry from 4)",
        [{ path: "/parks/europe/germany/bruehl/phantasialand" }],
      ],
    ];
    parks = [
      { id: "p1", name: "Phantasialand", slug: "phantasialand", ...GEO },
      { id: "p2", name: "Movie Park", slug: "movie-park", ...GEO },
      { id: "p3", name: "No Geo", slug: "no-geo" },
    ];
    const result = await service.uncovered(
      { days: 28, kind: "park", limit: 10 },
      NOW,
    );
    expect(result).toMatchObject({ kind: "park", total: 3, uncovered: 1 });
    expect(result.items).toEqual([
      {
        id: "p2",
        kind: "park",
        name: "Movie Park",
        url: "/parks/europe/germany/bruehl/movie-park",
      },
    ]);

    attractions = [
      {
        id: "a1",
        name: "Taron",
        slug: "taron",
        park: { name: "Phantasialand", slug: "phantasialand", ...GEO },
      },
      {
        id: "a2",
        name: "Gone Park Ride",
        slug: "gone",
        park: {
          name: "Old",
          slug: "old",
          retiredAt: new Date("2025-01-01"),
          ...GEO,
        },
      },
    ];
    const rides = await service.uncovered(
      { days: 28, kind: "attraction", limit: 10 },
      NOW,
    );
    expect(rides.items).toEqual([
      {
        id: "a1",
        kind: "attraction",
        name: "Taron",
        park: "Phantasialand",
        url: "/parks/europe/germany/bruehl/phantasialand/taron",
      },
    ]);
  });

  it("lists the not-found paths with the sites of the visits that entered on them", async () => {
    routes = [
      ["GROUP BY path", [{ path: "/de/gone", views: "5" }]],
      [
        "AS page, referrer",
        [{ page: "/de/gone", referrer: "google", count: "2" }],
      ],
    ];
    const result = await service.notFound({ days: 7, limit: 10 }, NOW);
    expect(result.items).toEqual([
      {
        path: "/de/gone",
        views: 5,
        referrers: [{ value: "google", visits: 2 }],
      },
    ]);
    expect(calls[0][1]).toContain("not_found");
    // Only a visit that entered on the dead address names a referrer: a
    // broken link inside the site leaves no visit row there.
    expect(calls[1][0]).toContain("SUM(visits)");
    expect(calls[1][0]).toContain("AND visits > 0");
    expect(calls[1][0]).toMatch(/AND entry = ANY\(\$\d+::text\[\]\)/);
    await expect(
      service.notFound(
        { days: 7, limit: 10, filter: ["referrer:google"] },
        NOW,
      ),
    ).rejects.toThrow(/can only filter visits/);
  });

  it("compares trends only where a window before exists", async () => {
    await expect(service.trends({ days: 181 }, NOW)).rejects.toThrow(
      /at most 180 days/,
    );
  });

  it("has no previous visits or change on landing pages past 180 days", async () => {
    routes = [
      [
        "GROUP BY entry",
        [
          {
            path: "/de",
            page_type: "home",
            visits: "9",
            previous: null,
            engaged: "3",
          },
        ],
      ],
    ];
    const result = await service.landingPages({ days: 365, limit: 10 }, NOW);
    expect(result.items[0]).toMatchObject({
      visits: 9,
      previous: null,
      change: null,
    });
  });
});
