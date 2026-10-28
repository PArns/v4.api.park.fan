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
  device: null,
  props_key: null,
  period: null,
  ...fields,
  count: String(count),
});

/**
 * The admin's reports, assembled from what the SQL returns. What is pinned:
 * every period of the interval is listed with zeros, a cut or unfinished one
 * marked partial; visitors are null under a filter that has no count rather
 * than guessed (two filters, a page, a visit's entry or referrer), and given
 * under one value of country, device, browser, OS or channel, per value in
 * the breakdowns of those, never by the hour under a filter, and null for an
 * hour before today; time on page and visit divide by their own counts; a visit dimension counts visits and carries a bounce rate and the
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
  let rolled: Set<string>;

  const db = {
    query: jest.fn(async (sql: string, params: unknown[]) => {
      calls.push([sql, params]);
      return routes.find(([match]) => sql.includes(match))?.[1] ?? [];
    }),
  };
  /** The runner without a gate or a timeout; `rolled` is what the nightly job has done. */
  const runner = {
    run: <T>(report: () => Promise<T>) => report(),
    query: db.query,
    rolledDays: async () => rolled,
  };
  const redis = {
    pipeline: () => {
      const keys: string[] = [];
      const chain = {
        pfcount: (key: string) => (keys.push(key), chain),
        exists: (key: string) => (keys.push(key), chain),
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
    rolled = new Set();
    resolve = jest.fn(async () => new Map());
    db.query.mockClear();
    service = new SiteAnalyticsService(
      runner as never,
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
          visitors: null,
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

    it("counts the visitors of the window before only once the window is longer than a week", async () => {
      const week = await service.overview({ days: 7 }, NOW);
      expect(week.previous?.visitors).toBeNull();
      const longer = await service.overview({ days: 8 }, NOW);
      expect(longer.previous?.visitors).toBe(0);
    });

    it("builds no events statement under a filter the events do not carry", async () => {
      const overview = await service.overview(
        { days: 7, filter: ["browser_language:de"] },
        NOW,
      );
      expect(sqlOf("FROM site_events")[0]).toBe("");
      expect(overview.totals.events).toBeNull();
      expect(overview.series.every((p) => p.events === null)).toBe(true);
      expect(sqlOf("FROM site_pageviews")[0]).toContain("browser_language");
    });

    describe("visitors under one filter", () => {
      /** The snapshot's rows of one value, as the SQL returns them. */
      const dimRows = (value: string) => [
        { day: "2026-10-27", value, visitors: 8 },
        { day: "2026-10-22", value, visitors: 5 },
      ];

      it("counts the days of the window and the one before under one value, today from Redis", async () => {
        routes = [["FROM site_visitor_dim_days", dimRows("de")]];
        pfcounts.set("site-analytics:uvd:2026-10-28:all:country:de", 3);
        pfcounts.set("site-analytics:uv:2026-10-28:all", 99);
        const overview = await service.overview(
          { days: 2, filter: ["country:de"] },
          NOW,
        );
        expect(sqlOf("FROM site_visitor_dim_days")[1]).toEqual([
          "all",
          "country",
          ["de"],
          "2026-10-25",
          "2026-10-28",
        ]);
        expect(sqlOf("FROM site_visitor_days")[0]).toBe("");
        expect(overview.totals.visitors).toBe(11);
        // A short window ending today has no comparable window before.
        expect(overview.previous?.visitors).toBeNull();
        expect(overview.series.map((point) => point.visitors)).toEqual([8, 3]);
      });

      it("counts per language under a language and a filter, and under a visit dimension", async () => {
        routes = [["FROM site_visitor_dim_days", dimRows("search")]];
        pfcounts.set("site-analytics:uvd:2026-10-28:de:channel:search", 2);
        const overview = await service.overview(
          { days: 2, locale: "de", filter: ["channel:search"] },
          NOW,
        );
        expect(sqlOf("FROM site_visitor_dim_days")[1]).toEqual([
          "de",
          "channel",
          ["search"],
          "2026-10-25",
          "2026-10-28",
        ]);
        expect(overview.totals.visitors).toBe(10);
        // Visits follow the filter as before.
        expect(overview.totals.pageviews).toBeNull();
      });

      it("leaves an hourly series without visitors, the totals with them", async () => {
        routes = [["FROM site_visitor_dim_days", dimRows("mobile")]];
        pfcounts.set("site-analytics:uvd:2026-10-28:all:device:mobile", 3);
        const overview = await service.overview(
          { days: 1, filter: ["device:mobile"] },
          NOW,
        );
        expect(overview.interval).toBe("hour");
        expect(overview.totals.visitors).toBe(3);
        expect(overview.series).toHaveLength(8);
        expect(overview.series.every((p) => p.visitors === null)).toBe(true);
        expect(sqlOf("FROM site_visitor_hours")[0]).toBe("");
      });

      it.each([
        ["two dimensions", ["country:de", "device:mobile"]],
        ["two values of one dimension", ["country:de", "country:nl"]],
        ["a page", ["path:/de"]],
        ["a page type", ["page_type:park"]],
        ["a visit dimension without visitors", ["entry:/de"]],
        ["the language as a filter", ["locale:de"]],
      ])("gives none for %s", async (_name, filter) => {
        const overview = await service.overview({ days: 7, filter }, NOW);
        expect(overview.totals.visitors).toBeNull();
        expect(overview.previous?.visitors).toBeNull();
        expect(overview.series.every((p) => p.visitors === null)).toBe(true);
        expect(sqlOf("FROM site_visitor_dim_days")[0]).toBe("");
      });

      it("sums a week's and a month's days, as the totals count them", async () => {
        routes = [
          [
            "FROM site_visitor_dim_days",
            [
              { day: "2026-10-05", value: "ios", visitors: 2 },
              { day: "2026-10-06", value: "ios", visitors: 3 },
              { day: "2026-10-12", value: "ios", visitors: 4 },
            ],
          ],
        ];
        const overview = await service.overview(
          {
            from: "2026-10-05",
            to: "2026-10-14",
            interval: "week",
            filter: ["os:ios"],
          },
          NOW,
        );
        expect(overview.series.map((p) => [p.day, p.visitors])).toEqual([
          ["2026-10-05", 5],
          ["2026-10-12", 4],
        ]);
        expect(overview.totals.visitors).toBe(9);
      });
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
        { days: 7, filter: ["referrer:google"] },
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
        [
          "FROM site_visitor_dim_days",
          [
            { day: "2026-10-27", value: "search", visitors: 5 },
            { day: "2026-10-26", value: "search", visitors: 1 },
            { day: "2026-10-27", value: "direct", visitors: 2 },
          ],
        ],
      ];
      pfcounts.set("site-analytics:uvd:2026-10-28:all:channel:search", 3);
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
          // The snapshot's two days and today's HyperLogLog.
          visitors: 9,
        },
        {
          value: "direct",
          views: 4,
          visits: 4,
          share: 0.4,
          bounceRate: 1,
          avgVisitSeconds: 10,
          visitors: 2,
        },
      ]);
      const [sql] = sqlOf("WITH per_value");
      expect(sql).toContain("SELECT channel AS value");
      expect(sql).toContain("HAVING SUM(visits) > 0");
      expect(sql).toContain("ORDER BY visits DESC");
    });

    describe("visitors per value", () => {
      const countries = [
        {
          value: "de",
          views: "8",
          seconds: "80",
          visits: "2",
          engaged: "1",
          total: "10",
          total_visits: "3",
        },
        {
          value: "nl",
          views: "2",
          seconds: "20",
          visits: "1",
          engaged: "0",
          total: "10",
          total_visits: "3",
        },
      ];

      it("lists each value's visitors of a visitor dimension, for the listed values only", async () => {
        routes = [
          ["WITH per_value", countries],
          [
            "FROM site_visitor_dim_days",
            [
              { day: "2026-10-27", value: "de", visitors: 6 },
              { day: "2026-10-26", value: "de", visitors: 4 },
            ],
          ],
        ];
        pfcounts.set("site-analytics:uvd:2026-10-28:de:country:nl", 2);
        const result = await service.breakdown(
          { dim: "country", days: 7, limit: 2, locale: "de" },
          NOW,
        );
        expect(sqlOf("FROM site_visitor_dim_days")[1]).toEqual([
          "de",
          "country",
          ["de", "nl"],
          "2026-10-22",
          "2026-10-28",
        ]);
        expect(result.items.map((item) => [item.value, item.visitors])).toEqual(
          [
            ["de", 10],
            ["nl", 2],
          ],
        );
      });

      it("gives the one value of a filter on the same dimension, and none under another filter or for another dimension", async () => {
        routes = [["WITH per_value", [countries[0]]]];
        const same = await service.breakdown(
          { dim: "country", days: 7, limit: 5, filter: ["country:de"] },
          NOW,
        );
        expect(same.items.map((item) => item.visitors)).toEqual([0]);
        for (const query of [
          { dim: "country" as const, filter: ["device:mobile"] },
          { dim: "country" as const, filter: ["country:de", "device:mobile"] },
          { dim: "path" as const },
          { dim: "exit" as const },
          { dim: "browser_language" as const },
        ]) {
          calls = [];
          const result = await service.breakdown(
            { days: 7, limit: 5, ...query },
            NOW,
          );
          expect(
            result.items.every((item) => item.visitors === undefined),
          ).toBe(true);
          expect(sqlOf("FROM site_visitor_dim_days")[0]).toBe("");
        }
      });
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
              filter: ["channel:direct"],
              row: "prop:tab",
              col: "device",
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
            row({ device: "mobile" }, 10),
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
        byDevice: [
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

  describe("the daily level", () => {
    const week = ["2026-10-22", "2026-10-23", "2026-10-24", "2026-10-25"];

    it("reads rolled days from the daily rows and today from the hourly ones, with the window before", async () => {
      rolled = new Set([...week, "2026-10-26", "2026-10-27", "2026-10-15"]);
      await service.overview({ days: 7 }, NOW);
      const [sql, params] = sqlOf("SUM(engaged) AS engaged");
      expect(sql).toContain("FROM site_pageviews_daily WHERE cuboid = $1");
      expect(sql).toMatch(/UNION ALL SELECT .* FROM site_pageviews WHERE/s);
      expect(sql).toContain(") AS site_pageviews");
      // The cuboid of the day alone, then the bounds of the rolled and the hourly ranges.
      expect(params[0]).toBe(1);
      expect(sqlOf("SUM(count) AS events")[0]).toContain(
        "FROM site_events_daily WHERE cuboid = $1",
      );
    });

    it("picks the cuboid of the dimension, of the dimension and the filter, and none when no cuboid has both", async () => {
      rolled = new Set(week);
      await service.breakdown({ days: 7, dim: "country", limit: 5 }, NOW);
      expect(sqlOf("per_value")[1][0]).toBe(7);

      calls = [];
      await service.breakdown(
        { days: 7, dim: "country", limit: 5, filter: ["os:ios"] },
        NOW,
      );
      expect(sqlOf("per_value")[0]).toContain("site_pageviews_daily");
      expect(sqlOf("per_value")[1].slice(0, 2)).toEqual(["ios", 31]);

      // `exit` reads the path's cuboid and its exits; a path filter is none.
      calls = [];
      await service.breakdown({ days: 7, dim: "exit", limit: 5 }, NOW);
      expect(sqlOf("per_value")[1][0]).toBe(2);
      calls = [];
      await service.breakdown(
        { days: 7, dim: "country", limit: 5, filter: ["path:/de"] },
        NOW,
      );
      expect(sqlOf("per_value")[0]).not.toContain("_daily");
      expect(sqlOf("per_value")[0]).toContain("FROM site_pageviews\n");
    });

    it("reads the hourly rows when nothing of the window is rolled up, and for an hourly series", async () => {
      await service.breakdown({ days: 7, dim: "country", limit: 5 }, NOW);
      expect(sqlOf("per_value")[0]).not.toContain("_daily");
      rolled = new Set(["2026-10-27"]);
      calls = [];
      await service.overview({ days: 2, interval: "hour" }, NOW);
      expect(sqlOf("SUM(engaged)")[0]).not.toContain("_daily");
    });

    it("reads each statement of an event from the cuboid it needs", async () => {
      rolled = new Set(week);
      await service.event("tab_changed", { days: 7 }, NOW);
      expect(sqlOf("GROUPING SETS")[1].slice(1, 2)).toEqual([3]);
      expect(sqlOf("jsonb_each_text")[1]).toContain(2);
      expect(sqlOf("LEFT JOIN site_entity_merges")[1]).toContain(4);
    });

    it("leaves a filter on a dimension no events cuboid has to the hourly rows", async () => {
      rolled = new Set(week);
      await service.events({ days: 7, filter: ["country:de"] }, NOW);
      expect(sqlOf("FROM site_events")[0]).not.toContain("_daily");
      calls = [];
      await service.events({ days: 7, filter: ["device:mobile"] }, NOW);
      expect(sqlOf("_daily")[0]).toContain("site_events_daily");
    });

    it("gives the search report's statements the columns they read and no more", async () => {
      rolled = new Set(week);
      await service.search({ days: 7 }, NOW);
      const [all, opened, clicks] = calls
        .map(([sql]) => sql)
        .filter((sql) => sql.includes("FROM site_events_daily"));
      expect(opened).toContain("page_type, device");
      expect(all).not.toContain("device");
      expect(clicks).not.toContain("device");
    });
  });

  describe("hourlyFrom", () => {
    /** Every day of the 200 before today is rolled up. */
    const rolledBack = () =>
      new Set(
        Array.from({ length: 200 }, (_, i) =>
          new Date(Date.parse("2026-10-27T00:00:00Z") - i * 86_400_000)
            .toISOString()
            .slice(0, 10),
        ),
      );

    it("is null for a report every statement of which a cuboid serves, however long the window", async () => {
      rolled = rolledBack();
      expect(
        await service.overview({ days: 180, interval: "month" }, NOW),
      ).toMatchObject({ hourlyFrom: null });
      expect(
        await service.breakdown({ days: 180, dim: "country", limit: 5 }, NOW),
      ).toMatchObject({ hourlyFrom: null });
    });

    it("names the horizon when a statement no cuboid serves read hourly rows for a window that starts before it", async () => {
      rolled = rolledBack();
      const filtered = {
        days: 120,
        dim: "country" as const,
        limit: 5,
        filter: ["path:/de/news"],
      };
      expect(await service.breakdown(filtered, NOW)).toMatchObject({
        days: 120,
        hourlyFrom: "2026-07-30",
      });
      expect(
        await service.breakdown({ ...filtered, days: 30 }, NOW),
      ).toMatchObject({ hourlyFrom: null });
    });

    it("is named for the window before alone, which a report without one never reads", async () => {
      rolled = rolledBack();
      // 60 days: the window starts 30 August, after the horizon; the window
      // before starts 30 June, before it.
      const events = await service.events(
        { days: 60, filter: ["country:de"] },
        NOW,
      );
      expect(events.from).toBe("2026-08-30");
      expect(events.hourlyFrom).toBe("2026-07-30");
      const breakdown = await service.breakdown(
        { days: 60, dim: "country", limit: 5, filter: ["path:/de/news"] },
        NOW,
      );
      expect(breakdown.hourlyFrom).toBeNull();
    });

    it("is named by an hourly series over a day before the horizon", async () => {
      rolled = rolledBack();
      const overview = await service.overview(
        { from: "2026-06-01", to: "2026-06-02", interval: "hour" },
        NOW,
      );
      expect(overview.hourlyFrom).toBe("2026-07-30");
    });

    it("is null while no day before the horizon is rolled up", async () => {
      rolled = new Set(["2026-10-01", "2026-10-02"]);
      const overview = await service.overview(
        { days: 120, filter: ["country:de", "os:ios", "device:mobile"] },
        NOW,
      );
      expect(overview.hourlyFrom).toBeNull();
    });
  });

  describe("an hourly series' visitors", () => {
    it("reads past hours from the snapshot's rows, and the hours it has not written from Redis", async () => {
      routes = [
        [
          "FROM site_visitor_hours",
          [
            { hour: "2026-10-27T00:00:00Z", visitors: 4 },
            { hour: "2026-10-27T01:00:00Z", visitors: 0 },
          ],
        ],
      ];
      pfcounts.set("site-analytics:uvh:2026102802:all", 3);
      const overview = await service.overview(
        { days: 2, interval: "hour" },
        NOW,
      );
      expect(sqlOf("FROM site_visitor_hours")[1]).toEqual([
        "all",
        "2026-10-27T00:00:00.000Z",
        "2026-10-29T00:00:00.000Z",
      ]);
      const visitors = overview.series.map((point) => point.visitors);
      expect(visitors.slice(0, 3)).toEqual([4, 0, null]);
      expect(visitors[24 + 2]).toBe(3);
      // An hour of today nobody visited is zero.
      expect(visitors[24 + 3]).toBe(0);
    });

    it("reads yesterday's hours from Redis until the snapshot has run, zero only where the day was counted", async () => {
      pfcounts.set("site-analytics:uvh:2026102709:all", 6);
      let overview = await service.overview({ days: 2, interval: "hour" }, NOW);
      expect(overview.series[9].visitors).toBe(6);
      // No key for the day: an hour without one is unknown, not zero.
      expect(overview.series[10].visitors).toBeNull();

      pfcounts.set("site-analytics:uv:2026-10-27:all", 1);
      overview = await service.overview({ days: 2, interval: "hour" }, NOW);
      expect(overview.series[9].visitors).toBe(6);
      expect(overview.series[10].visitors).toBe(0);
    });

    it("leaves an hour of a day before the snapshot kept hours unknown", async () => {
      const overview = await service.overview(
        { from: "2026-10-20", to: "2026-10-21", interval: "hour" },
        NOW,
      );
      expect(overview.series.every((point) => point.visitors === null)).toBe(
        true,
      );
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
        { days: 30, row: "day", col: "device" },
        NOW,
      );
      expect(byDay.rows).toHaveLength(30);
      expect(byDay.rows[0]).toBe("2026-09-01");
    });
  });

  describe("trafficCrosstab", () => {
    it("refuses a crosstab with two wide sides before fetching any cell", async () => {
      await expect(
        service.trafficCrosstab(
          { days: 7, row: "path", col: "referrer", metric: "visits" },
          NOW,
        ),
      ).rejects.toThrow(/not two of them/);
      await expect(
        service.trafficCrosstab(
          { days: 7, row: "entry", col: "exit", metric: "visits" },
          NOW,
        ),
      ).rejects.toThrow(/not two of them/);
      await expect(
        service.crosstab(
          "tab_changed",
          { days: 7, row: "path", col: "path" },
          NOW,
        ),
      ).rejects.toThrow(/not two of them/);
      expect(calls.filter(([sql]) => sql.includes("GROUP BY 1, 2"))).toEqual(
        [],
      );
    });

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
        { days: 7, row: "os", col: "device", metric: "pageviews" },
        NOW,
      );
      expect(result).toMatchObject({
        metric: "pageviews",
        row: "os",
        col: "device",
        rows: ["ios", "windows"],
        cols: ["mobile", "desktop"],
        total: 8,
      });
      const [sql] = calls[0];
      expect(sql).toContain("SELECT os AS r, device AS c");
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
            col: "device",
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
    it("lists the surfaces seen with their rates, fills devices, keeps the position order, draws every day", async () => {
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
              { name: "search_opened", surface: "hero", device: "mobile" },
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
      expect(result.bySurfaceAndDevice).toHaveLength(9);
      expect(result.bySurfaceAndDevice[0]).toEqual({
        surface: "hero",
        device: "mobile",
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
