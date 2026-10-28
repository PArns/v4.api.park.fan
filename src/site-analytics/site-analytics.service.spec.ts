import { SiteAnalyticsService, propsSummary } from "./site-analytics.service";

const NOW = new Date("2026-10-28T07:12:40Z");
const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const GONE_ID = "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d";

/** A grouping-set row: the columns outside its set are NULL. */
const row = (fields: Record<string, string | null>, count: number) => ({
  name: null,
  surface: null,
  result: null,
  item: null,
  position: null,
  page_type: null,
  viewport: null,
  props_key: null,
  period: null,
  ...fields,
  count: String(count),
});

/**
 * The admin's reports, assembled from what the SQL returns. What is pinned:
 * every period of the interval is listed with zeros, a cut or unfinished one
 * marked partial; visitors are null under a filter rather than guessed, and
 * for an hour before today; time on page and visit divide by their own
 * counts; a visit dimension counts visits and carries a bounce rate and the
 * average visit, dim=exit the visits ended; events are what was seen plus
 * what is blocked; the
 * grouping-set rows are told apart by their NULLs; a property keeps fifty
 * values and counts the rest; a crosstab folds what is past thirty into
 * `__rest__`; the search report lists the surfaces seen with their rates.
 */
describe("SiteAnalyticsService", () => {
  let service: SiteAnalyticsService;
  let routes: Array<[string, unknown[]]>;
  let calls: Array<[string, unknown[]]>;
  let blocked: Set<string>;
  let resolve: jest.Mock;
  let pfcounts: Map<string, number>;

  const db = {
    query: jest.fn(async (sql: string, params: unknown[]) => {
      calls.push([sql, params]);
      return routes.find(([match]) => sql.includes(match))?.[1] ?? [];
    }),
  };
  const redis = {
    pipeline: () => {
      const keys: string[] = [];
      const chain = {
        pfcount: (key: string) => (keys.push(key), chain),
        exec: async () => keys.map((key) => [null, pfcounts.get(key) ?? 0]),
      };
      return chain;
    },
    pfcount: jest.fn(async () => 4),
    mget: jest.fn(async (...keys: string[]) =>
      keys.map((key) => (key.includes("202610280712") ? "9" : null)),
    ),
  };
  const sqlOf = (match: string) =>
    calls.find(([sql]) => sql.includes(match)) ?? ["", []];

  beforeEach(() => {
    routes = [];
    calls = [];
    blocked = new Set();
    pfcounts = new Map();
    resolve = jest.fn(async () => new Map());
    db.query.mockClear();
    service = new SiteAnalyticsService(
      db as never,
      redis as never,
      { names: async () => blocked } as never,
      { resolve } as never,
    );
  });

  describe("overview", () => {
    it("sums both windows, fills every day, takes today's visitors from Redis, and adds the realtime minute", async () => {
      routes = [
        [
          "SUM(engaged) AS engaged",
          [
            {
              period: "2026-10-27",
              views: "30",
              seconds: "600",
              visits: "10",
              engaged: "4",
              visit_seconds: "1200",
            },
            {
              period: "2026-10-28",
              views: "12",
              seconds: "120",
              visits: "4",
              engaged: "1",
              visit_seconds: "200",
            },
            {
              period: "2026-10-25",
              views: "5",
              seconds: "50",
              visits: "5",
              engaged: "0",
              visit_seconds: "25",
            },
          ],
        ],
        [
          "SUM(count) AS events",
          [
            { period: "2026-10-28", events: "7" },
            { period: "2026-10-25", events: "1" },
          ],
        ],
        ["FROM site_visitor_days", [{ day: "2026-10-27", visitors: 8 }]],
      ];
      pfcounts.set("site-analytics:uv:2026-10-28:all", 3);
      pfcounts.set("site-analytics:uv:2026-10-27:all", 99);

      const overview = await service.overview({ days: 2 }, NOW);

      expect(overview).toMatchObject({
        from: "2026-10-27",
        to: "2026-10-28",
        days: 2,
        locale: null,
        filters: [],
        interval: "day",
        totals: {
          pageviews: 42,
          visits: 14,
          visitors: 11,
          events: 7,
          viewsPerVisit: 3,
          bounceRate: 0.6429,
          avgVisitSeconds: 100,
          avgTimeOnPageSeconds: 17,
        },
        previous: {
          pageviews: 5,
          visits: 5,
          visitors: 0,
          events: 1,
          viewsPerVisit: 1,
          bounceRate: 1,
          avgVisitSeconds: 5,
          avgTimeOnPageSeconds: 10,
        },
      });
      expect(overview.series).toEqual([
        {
          day: "2026-10-27",
          pageviews: 30,
          visits: 10,
          visitors: 8,
          events: 0,
          viewsPerVisit: 3,
          bounceRate: 0.6,
          avgVisitSeconds: 120,
          avgTimeOnPageSeconds: 20,
        },
        // Today is under way.
        {
          day: "2026-10-28",
          partial: true,
          pageviews: 12,
          visits: 4,
          visitors: 3,
          events: 7,
          viewsPerVisit: 3,
          bounceRate: 0.75,
          avgVisitSeconds: 50,
          avgTimeOnPageSeconds: 10,
        },
      ]);
      expect(overview.realtime.activeVisitors).toBe(4);
      expect(overview.realtime.perMinute).toHaveLength(30);
      expect(overview.realtime.perMinute[29]).toEqual({
        minute: "2026-10-28T07:12:00Z",
        pageviews: 9,
      });
    });

    it("gives no visitor count under a filter, and passes the filter to both tables", async () => {
      const overview = await service.overview(
        { days: 7, filter: ["country:nl", "browser:safari"] },
        NOW,
      );
      expect(overview.totals.visitors).toBeNull();
      expect(overview.series.every((p) => p.visitors === null)).toBe(true);
      expect(overview.filters).toEqual(["country:nl", "browser:safari"]);
      for (const table of ["FROM site_pageviews", "FROM site_events"]) {
        const [sql, params] = sqlOf(table);
        expect(sql).toContain("country = $5 AND browser = $6");
        expect(params.slice(4)).toEqual(["nl", "safari"]);
      }
      expect(sqlOf("site_visitor_days")[0]).toBe("");
    });

    it("follows a visit filter with visits and bounce only: page views and events are null", async () => {
      routes = [
        [
          "SUM(engaged) AS engaged",
          [
            {
              period: "2026-10-28",
              views: "0",
              visits: "4",
              engaged: "1",
              visit_seconds: "80",
            },
          ],
        ],
      ];
      const overview = await service.overview(
        { days: 7, filter: ["channel:search"] },
        NOW,
      );
      expect(overview.totals).toEqual({
        pageviews: null,
        visits: 4,
        visitors: null,
        events: null,
        viewsPerVisit: null,
        bounceRate: 0.75,
        avgVisitSeconds: 20,
        avgTimeOnPageSeconds: null,
      });
      expect(overview.series[overview.series.length - 1]).toMatchObject({
        pageviews: null,
        visits: 4,
        events: null,
      });
      // The events table carries no visit dimension, so it is not asked.
      expect(sqlOf("FROM site_events")[0]).toBe("");
    });

    it("ends the window before at the current hour, and has none past 180 days", async () => {
      await service.overview({ days: 7 }, NOW);
      const [sql, params] = sqlOf("FROM site_pageviews");
      expect(sql).toContain(
        "NOT (bucket >= $3::timestamptz AND bucket < $4::timestamptz)",
      );
      expect(params.slice(0, 4)).toEqual([
        "2026-10-15T00:00:00.000Z",
        "2026-10-29T00:00:00.000Z",
        "2026-10-21T08:00:00.000Z",
        "2026-10-22T00:00:00.000Z",
      ]);
      calls = [];
      const long = await service.overview({ days: 365 }, NOW);
      expect(long.previous).toBeNull();
      expect(sqlOf("FROM site_pageviews")[1][0]).toBe(
        "2025-10-29T00:00:00.000Z",
      );
    });

    it("draws today by the hour for a one-day window", async () => {
      routes = [
        [
          "SUM(engaged) AS engaged",
          [
            {
              period: "2026-10-28T06:00:00Z",
              views: "3",
              seconds: "30",
              visits: "2",
              engaged: "1",
              visit_seconds: "90",
            },
          ],
        ],
      ];
      pfcounts.set("site-analytics:uvh:2026102806:all", 2);
      const overview = await service.overview({ days: 1 }, NOW);
      expect(overview.series).toHaveLength(8);
      expect(overview.series[6]).toEqual({
        day: "2026-10-28T06:00:00Z",
        pageviews: 3,
        visits: 2,
        visitors: 2,
        events: 0,
        viewsPerVisit: 1.5,
        bounceRate: 0.5,
        avgVisitSeconds: 45,
        avgTimeOnPageSeconds: 10,
      });
      expect(overview.series[7]).toMatchObject({
        day: "2026-10-28T07:00:00Z",
        partial: true,
        pageviews: 0,
        avgVisitSeconds: null,
        avgTimeOnPageSeconds: null,
      });
      expect(sqlOf("SUM(engaged)")[0]).toContain("time_bucket('1 hour'");
    });

    it("draws two days by the hour, yesterday's hours without visitors", async () => {
      pfcounts.set("site-analytics:uvh:2026102807:all", 5);
      const overview = await service.overview(
        { days: 2, interval: "hour" },
        NOW,
      );
      expect(overview.interval).toBe("hour");
      expect(overview.series).toHaveLength(24 + 8);
      expect(overview.series[0]).toMatchObject({
        day: "2026-10-27T00:00:00Z",
        visitors: null,
      });
      expect(overview.series[31]).toMatchObject({
        day: "2026-10-28T07:00:00Z",
        visitors: 5,
        partial: true,
      });
    });

    it("folds the days of a from–to window into ISO weeks, the cut ones partial", async () => {
      routes = [
        [
          "SUM(engaged) AS engaged",
          [
            { period: "2026-10-01", views: "4", visits: "2", engaged: "1" },
            { period: "2026-10-02", views: "6", visits: "2", engaged: "2" },
            { period: "2026-10-27", views: "1", visits: "1", engaged: "0" },
            // The window before, which no point may count.
            { period: "2026-09-20", views: "50", visits: "9", engaged: "0" },
          ],
        ],
        [
          "FROM site_visitor_days",
          [
            { day: "2026-10-01", visitors: 2 },
            { day: "2026-10-02", visitors: 3 },
            { day: "2026-09-20", visitors: 40 },
          ],
        ],
      ];
      const overview = await service.overview(
        { from: "2026-10-01", to: "2026-10-28", interval: "week" },
        NOW,
      );
      expect(overview).toMatchObject({
        from: "2026-10-01",
        to: "2026-10-28",
        days: 28,
        interval: "week",
      });
      expect(
        overview.series.map((p) => [p.day, p.partial, p.pageviews, p.visitors]),
      ).toEqual([
        ["2026-09-28", true, 10, 5],
        ["2026-10-05", undefined, 0, 0],
        ["2026-10-12", undefined, 0, 0],
        ["2026-10-19", undefined, 0, 0],
        ["2026-10-26", true, 1, 0],
      ]);
      expect(overview.totals.pageviews).toBe(11);
    });
  });

  describe("breakdown", () => {
    it("counts a visit dimension in visits, with a bounce rate and the share of all visits", async () => {
      routes = [
        [
          "WITH per_value",
          [
            {
              value: "search",
              views: "40",
              visits: "6",
              engaged: "3",
              visit_seconds: "600",
              total: "10",
              total_visits: "10",
            },
            {
              value: "direct",
              views: "20",
              visits: "4",
              engaged: "0",
              visit_seconds: "40",
              total: "10",
              total_visits: "10",
            },
          ],
        ],
      ];
      const result = await service.breakdown(
        { dim: "channel", days: 30, limit: 2 },
        NOW,
      );
      expect(result.measure).toBe("visits");
      expect(result.total).toEqual({ views: 10, visits: 10 });
      expect(result.items).toEqual([
        {
          value: "search",
          views: 6,
          visits: 6,
          share: 0.6,
          bounceRate: 0.5,
          avgVisitSeconds: 100,
        },
        {
          value: "direct",
          views: 4,
          visits: 4,
          share: 0.4,
          bounceRate: 1,
          avgVisitSeconds: 10,
        },
      ]);
      const [sql] = sqlOf("WITH per_value");
      expect(sql).toContain("SELECT channel AS value");
      expect(sql).toContain("HAVING SUM(visits) > 0");
      expect(sql).toContain("ORDER BY visits DESC");
    });

    it("counts a page dimension in views, leaves direct out of the referrers", async () => {
      routes = [
        [
          "WITH per_value",
          [
            {
              value: "/de",
              views: "8",
              seconds: "80",
              visits: "2",
              engaged: "1",
              exits: "2",
              total: "10",
              total_visits: "3",
            },
          ],
        ],
      ];
      const pages = await service.breakdown(
        { dim: "path", days: 7, limit: 50, locale: "de" },
        NOW,
      );
      expect(pages.measure).toBe("views");
      expect(pages.items).toEqual([
        {
          value: "/de",
          views: 8,
          visits: 2,
          share: 0.8,
          avgTimeOnPageSeconds: 10,
          exits: 2,
          exitRate: 0.25,
        },
      ]);
      await service.breakdown({ dim: "referrer", days: 7, limit: 50 }, NOW);
      expect(calls[1][0]).toContain("AND referrer <> ''");
    });

    it("switches a page dimension to visits started under a visit filter: the landing pages", async () => {
      routes = [
        [
          "WITH per_value",
          [
            {
              value: "/de",
              views: "0",
              visits: "5",
              engaged: "2",
              visit_seconds: "250",
              total: "5",
              total_visits: "5",
            },
          ],
        ],
      ];
      const result = await service.breakdown(
        { dim: "path", days: 7, limit: 50, filter: ["channel:search"] },
        NOW,
      );
      expect(result.measure).toBe("visits");
      expect(result.items).toEqual([
        {
          value: "/de",
          views: 5,
          visits: 5,
          share: 1,
          bounceRate: 0.6,
          avgVisitSeconds: 50,
        },
      ]);
      expect(sqlOf("WITH per_value")[0]).toContain("HAVING SUM(visits) > 0");
    });

    it("lists the pages visits ended on for dim=exit, and groups by the browser language", async () => {
      routes = [
        [
          "WITH per_value",
          [
            {
              value: "/de/news",
              views: "0",
              visits: "0",
              exits: "3",
              total: "4",
              total_visits: "0",
            },
          ],
        ],
      ];
      const result = await service.breakdown(
        { dim: "exit", days: 7, limit: 50 },
        NOW,
      );
      expect(result.measure).toBe("exits");
      expect(result.total).toEqual({ views: 4, visits: 4 });
      expect(result.items).toEqual([
        { value: "/de/news", views: 3, visits: 3, share: 0.75 },
      ]);
      const [sql] = sqlOf("WITH per_value");
      expect(sql).toContain("SELECT path AS value");
      expect(sql).toContain("HAVING SUM(exits) > 0");
      calls = [];
      await service.breakdown(
        { dim: "browser_language", days: 7, limit: 50 },
        NOW,
      );
      expect(sqlOf("WITH per_value")[0]).toContain(
        "SELECT browser_language AS value",
      );
    });

    it("answers an empty window with a zero total", async () => {
      const result = await service.breakdown(
        { dim: "country", days: 1, limit: 50 },
        NOW,
      );
      expect(result.total).toEqual({ views: 0, visits: 0 });
      expect(result.items).toEqual([]);
    });
  });

  describe("events", () => {
    it("lists every name seen and every blocked one, most counted first, with every day", async () => {
      blocked = new Set(["noisy_event"]);
      routes = [
        [
          "SELECT name,",
          [
            { name: "search_opened", period: "2026-10-28", count: "5" },
            { name: "search_opened", period: "2026-10-26", count: "2" },
            { name: "tab_changed", period: "2026-10-27", count: "9" },
            { name: "faded_event", period: "2026-10-25", count: "4" },
          ],
        ],
      ];
      const result = await service.events({ days: 2 }, NOW);
      expect(result.items.map((i) => [i.name, i.count, i.previous])).toEqual([
        ["tab_changed", 9, 0],
        ["search_opened", 5, 2],
        ["faded_event", 0, 4],
        ["noisy_event", 0, 0],
      ]);
      expect(result.items[0]).toMatchObject({
        blocked: false,
        series: [
          { day: "2026-10-27", count: 9 },
          { day: "2026-10-28", count: 0, partial: true },
        ],
      });
      expect(result.items[3].blocked).toBe(true);
    });

    it("has no previous counts past 180 days, and refuses a visit filter", async () => {
      routes = [
        [
          "SELECT name,",
          [{ name: "search_opened", period: "2026-10-28", count: "5" }],
        ],
      ];
      const long = await service.events({ days: 365 }, NOW);
      expect(long.items[0]).toMatchObject({ count: 5, previous: null });
      for (const report of [
        () => service.events({ days: 7, filter: ["channel:search"] }, NOW),
        () =>
          service.event("tab_changed", { days: 7, filter: ["entry:/de"] }, NOW),
        () =>
          service.crosstab(
            "tab_changed",
            {
              days: 7,
              filter: ["campaign:x"],
              row: "prop:tab",
              col: "viewport",
            },
            NOW,
          ),
        () => service.search({ days: 7, filter: ["referrer:google"] }, NOW),
      ]) {
        await expect(report()).rejects.toThrow(/can only filter visits/);
      }
      // Events carry no browser language either.
      await expect(
        service.events({ days: 7, filter: ["browser_language:de"] }, NOW),
      ).rejects.toThrow(/do not carry browser_language/);
    });
  });

  describe("event", () => {
    it("splits the grouping sets, names its entities and keeps an unknown one unnamed", async () => {
      routes = [
        [
          "GROUPING SETS ((page_type)",
          [
            row({}, 10),
            row({ page_type: "home" }, 8),
            row({ page_type: "park" }, 2),
            row({ viewport: "mobile" }, 10),
            row({ props_key: "result=park&surface=hero" }, 7),
            row({ props_key: "result=show&surface=header" }, 3),
            row({ period: "2026-10-28" }, 10),
          ],
        ],
        [
          "jsonb_each_text",
          [
            { key: "surface", value: "hero", count: "7" },
            { key: "surface", value: "header", count: "3" },
            { key: "result", value: "park", count: "7" },
            { key: "result", value: "show", count: "3" },
          ],
        ],
        [
          "site_entity_merges",
          [
            { entity: PARK_ID, count: "4" },
            { entity: GONE_ID, count: "1" },
          ],
        ],
      ];
      resolve.mockResolvedValueOnce(
        new Map([
          [PARK_ID, { kind: "park", name: "Phantasialand", url: "/parks/x" }],
        ]),
      );
      const result = await service.event(
        "search_result_clicked",
        { days: 2 },
        NOW,
      );
      expect(result).toMatchObject({
        name: "search_result_clicked",
        blocked: false,
        total: 10,
        combinations: [
          { props: { result: "park", surface: "hero" }, count: 7 },
          { props: { result: "show", surface: "header" }, count: 3 },
        ],
        byPageType: [
          { value: "home", count: 8 },
          { value: "park", count: 2 },
        ],
        byViewport: [
          { value: "mobile", count: 10 },
          { value: "tablet", count: 0 },
          { value: "desktop", count: 0 },
        ],
        entities: [
          {
            id: PARK_ID,
            kind: "park",
            name: "Phantasialand",
            url: "/parks/x",
            count: 4,
          },
          { id: GONE_ID, kind: null, name: null, url: null, count: 1 },
        ],
        series: [
          { day: "2026-10-27", count: 0 },
          { day: "2026-10-28", count: 10, partial: true },
        ],
      });
      expect(result.props.map((p) => p.prop)).toEqual(["result", "surface"]);
      expect(resolve).toHaveBeenCalledWith([PARK_ID, GONE_ID]);
    });
  });

  describe("propsSummary", () => {
    it("keeps fifty values per key and counts the rest", () => {
      const rows = Array.from({ length: 60 }, (_, i) => ({
        key: "term",
        value: `term-${String(i).padStart(2, "0")}`,
        count: String(i + 1),
      }));
      const [summary] = propsSummary(rows);
      expect(summary.values).toHaveLength(50);
      expect(summary.values[0]).toEqual({ value: "term-59", count: 60 });
      // The ten smallest: 1 + 2 + … + 10.
      expect(summary.otherCount).toBe(55);
    });
  });

  describe("crosstab", () => {
    it("folds what is past thirty into __rest__, apart from a real value other, labels the empty value, and keeps every day", async () => {
      routes = [
        [
          "AS r,",
          [
            ...Array.from({ length: 31 }, (_, i) => ({
              r: `v${String(i).padStart(2, "0")}`,
              c: "home",
              count: String(100 - i),
            })),
            // A real value "other" (a page type of that name) ranks too.
            { r: "other", c: "home", count: "69" },
            { r: "", c: "park", count: "1" },
          ],
        ],
      ];
      const result = await service.crosstab(
        "search_opened",
        { days: 7, row: "prop:surface", col: "page_type" },
        NOW,
      );
      expect(result.rows).toHaveLength(31);
      expect(result.rows[30]).toBe("__rest__");
      expect(result.rows).not.toContain("other");
      expect(result.cols).toEqual(["home", "park"]);
      expect(result.cells).toContainEqual({
        row: "__rest__",
        col: "home",
        count: 69 + 70,
      });
      expect(result.cells).toContainEqual({
        row: "__rest__",
        col: "park",
        count: 1,
      });
      expect(result.total).toBe(
        Array.from({ length: 32 }, (_, i) => 100 - i).reduce((a, b) => a + b) +
          1,
      );
      const [sql, params] = calls[0];
      expect(sql).toContain("COALESCE(props->>$2, '')");
      expect(params[1]).toBe("surface");

      routes = [
        [
          "AS r,",
          Array.from({ length: 40 }, (_, i) => ({
            r: `2026-09-${String((i % 30) + 1).padStart(2, "0")}`,
            c: "mobile",
            count: "1",
          })),
        ],
      ];
      const byDay = await service.crosstab(
        "search_opened",
        { days: 30, row: "day", col: "viewport" },
        NOW,
      );
      expect(byDay.rows).toHaveLength(30);
      expect(byDay.rows[0]).toBe("2026-09-01");
    });
  });

  describe("trafficCrosstab", () => {
    it("counts page views by two page dimensions, and folds days into weeks", async () => {
      routes = [
        [
          "AS r,",
          [
            { r: "ios", c: "mobile", count: "5" },
            { r: "windows", c: "desktop", count: "3" },
          ],
        ],
      ];
      const result = await service.trafficCrosstab(
        { days: 7, row: "os", col: "viewport", metric: "pageviews" },
        NOW,
      );
      expect(result).toMatchObject({
        metric: "pageviews",
        row: "os",
        col: "viewport",
        rows: ["ios", "windows"],
        cols: ["mobile", "desktop"],
        total: 8,
      });
      const [sql] = calls[0];
      expect(sql).toContain("SELECT os AS r, viewport AS c");
      expect(sql).toContain("SUM(views) AS count");
      expect(sql).toContain("AND views > 0");

      routes = [
        [
          "AS r,",
          [
            { r: new Date("2026-10-01T00:00:00Z"), c: "de", count: "2" },
            { r: new Date("2026-10-02T00:00:00Z"), c: "de", count: "3" },
            { r: new Date("2026-10-27T00:00:00Z"), c: "nl", count: "1" },
          ],
        ],
      ];
      const weeks = await service.trafficCrosstab(
        {
          from: "2026-10-01",
          to: "2026-10-28",
          interval: "week",
          row: "day",
          col: "locale",
          metric: "pageviews",
        },
        NOW,
      );
      expect(weeks.rows).toEqual(["2026-09-28", "2026-10-26"]);
      expect(weeks.cells).toContainEqual({
        row: "2026-09-28",
        col: "de",
        count: 5,
      });
    });

    it("counts visits by a visit side, and ended visits on their exit rows", async () => {
      await service.trafficCrosstab(
        { days: 7, row: "channel", col: "country", metric: "visits" },
        NOW,
      );
      expect(calls[0][0]).toContain("SUM(visits) AS count");
      calls = [];
      await service.trafficCrosstab(
        { days: 7, row: "channel", col: "exit", metric: "visits" },
        NOW,
      );
      expect(calls[0][0]).toContain("SELECT channel AS r, path AS c");
      expect(calls[0][0]).toContain("SUM(exits) AS count");
    });

    it("refuses a visit side or a visit filter for page views", async () => {
      await expect(
        service.trafficCrosstab(
          { days: 7, row: "channel", col: "os", metric: "pageviews" },
          NOW,
        ),
      ).rejects.toThrow(/metric=pageviews takes/);
      await expect(
        service.trafficCrosstab(
          {
            days: 7,
            row: "os",
            col: "viewport",
            metric: "pageviews",
            filter: ["channel:search"],
          },
          NOW,
        ),
      ).rejects.toThrow(/can only filter visits/);
      expect(calls).toEqual([]);
    });
  });

  describe("search", () => {
    it("lists the surfaces seen with their rates, fills viewports, keeps the position order, draws every day", async () => {
      routes = [
        [
          "SELECT name, surface, SUM(count)",
          [
            { name: "search_opened", surface: "header", count: "10" },
            { name: "search_opened", surface: "hero", count: "40" },
            // Clicks past the openings: the search page counts one opening
            // per visit to it, its results are clicked many times.
            { name: "search_opened", surface: "search_page", count: "2" },
            {
              name: "search_result_clicked",
              surface: "search_page",
              count: "5",
            },
            { name: "search_typed", surface: "hero", count: "20" },
            { name: "search_result_clicked", surface: "hero", count: "9" },
            { name: "search_browse_clicked", surface: "hero", count: "1" },
            { name: "search_no_results", surface: "hero", count: "5" },
            { name: "search_no_results", surface: "header", count: "1" },
            { name: "search_view_all", surface: "hero", count: "2" },
          ],
        ],
        [
          "GROUPING SETS ((name, surface, page_type)",
          [
            row(
              { name: "search_opened", surface: "header", page_type: "park" },
              6,
            ),
            row(
              { name: "search_opened", surface: "hero", page_type: "home" },
              40,
            ),
            row(
              { name: "search_opened", surface: "hero", viewport: "mobile" },
              30,
            ),
            row(
              { name: "search_opened", surface: "hero", period: "2026-10-28" },
              40,
            ),
          ],
        ],
        [
          "GROUPING SETS ((name, surface, result)",
          [
            row(
              {
                name: "search_result_clicked",
                surface: "hero",
                result: "park",
              },
              9,
            ),
            row(
              {
                name: "search_browse_clicked",
                surface: "hero",
                item: "nearby_park",
              },
              1,
            ),
            row({ name: "search_result_clicked", position: "1" }, 7),
            row({ name: "search_result_clicked", position: "99" }, 2),
            row({ name: "search_browse_clicked", position: "1" }, 1),
          ],
        ],
      ];
      const result = await service.search({ days: 2 }, NOW);

      expect(result.surfaces.map((s) => s.surface)).toEqual([
        "hero",
        "header",
        "search_page",
      ]);
      expect(result.surfaces[2].clickRate).toBe(1);
      expect(result.surfaces[0]).toEqual({
        surface: "hero",
        opened: 40,
        typed: 20,
        resultClicks: 9,
        browseClicks: 1,
        noResults: 5,
        viewAll: 2,
        clickRate: 0.25,
        noResultRate: 0.25,
      });
      // Typed nothing: the no-result rate divides by zero to zero.
      expect(result.surfaces[1]).toMatchObject({
        surface: "header",
        noResults: 1,
        noResultRate: 0,
      });
      expect(result.bySurfaceAndPageType).toEqual([
        { surface: "hero", pageType: "home", opened: 40 },
        { surface: "header", pageType: "park", opened: 6 },
      ]);
      expect(result.bySurfaceAndViewport).toHaveLength(9);
      expect(result.bySurfaceAndViewport[0]).toEqual({
        surface: "hero",
        viewport: "mobile",
        opened: 30,
      });
      expect(result.resultTypes).toEqual([
        { surface: "hero", result: "park", clicks: 9 },
      ]);
      expect(result.browseItems).toEqual([
        { surface: "hero", item: "nearby_park", clicks: 1 },
      ]);
      expect(result.positions.map((p) => [p.position, p.clicks])).toEqual([
        ["1", 7],
        ["2", 0],
        ["3", 0],
        ["4-5", 0],
        ["6-10", 0],
        ["11+", 0],
        ["99", 2],
      ]);
      expect(result.series).toHaveLength(6);
      expect(result.series).toContainEqual({
        day: "2026-10-28",
        partial: true,
        surface: "hero",
        opened: 40,
      });
      expect(calls.map(([, params]) => params[0])).toEqual([
        [
          "search_opened",
          "search_typed",
          "search_result_clicked",
          "search_browse_clicked",
          "search_no_results",
          "search_view_all",
        ],
        ["search_opened"],
        ["search_result_clicked", "search_browse_clicked"],
      ]);
    });
  });
});
