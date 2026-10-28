import { BadRequestException } from "@nestjs/common";
import {
  SearchConsoleService,
  levelPlan,
  pageFilterOf,
} from "./search-console.service";

const NOW = new Date("2026-10-28T12:00:00Z");

interface Batch {
  id: number;
  day: string;
  rows: number;
  format?: number;
}

/** A service over a fake database that answers by the shape of the statement. */
function build(options: {
  batches?: Batch[];
  items?: Array<Record<string, unknown>>;
  total?: Record<string, unknown>;
  perDay?: Array<Record<string, unknown>>;
  site?: Array<Record<string, unknown>>;
  coverage?: Record<string, unknown>;
  success?: Date | null;
  latest?: { ok: boolean; error: string | null };
}) {
  const calls: Array<{ sql: string; params: unknown[] }> = [];
  const db = {
    query: jest.fn(async (sql: string, params: unknown[] = []) => {
      calls.push({ sql, params });
      if (
        sql.includes("FROM search_console_batches") &&
        sql.includes("ORDER BY day, id DESC") &&
        sql.includes("DISTINCT ON (day) id")
      ) {
        return (options.batches ?? []).map((b) => ({
          id: b.id,
          format: b.format ?? 2,
          day: b.day,
          rows: b.rows,
          clicks: 0,
          impressions: 0,
          position_sum: 0,
        }));
      }
      if (sql.includes("MIN(day)::text AS first"))
        return [options.coverage ?? { first: null, last: null, rows: 0 }];
      if (sql.includes("MAX(finished_at)"))
        return [{ at: options.success ?? null }];
      if (sql.includes("FROM search_console_runs"))
        return options.latest ? [options.latest] : [];
      if (sql.includes("FROM site_pageviews")) return options.site ?? [];
      if (sql.includes("GROUP BY r.day")) return options.perDay ?? [];
      if (sql.includes("GROUP BY r.page") || sql.includes("GROUP BY r.query"))
        return options.items ?? [];
      if (sql.includes("FROM search_console_rows"))
        return [options.total ?? {}];
      return [];
    }),
  };
  return { service: new SearchConsoleService(db as never), calls, db };
}

const sums = (
  prefix: string,
  clicks: number,
  impressions: number,
  positionSum: number,
) => ({
  [`${prefix}_clicks`]: String(clicks),
  [`${prefix}_impressions`]: String(impressions),
  [`${prefix}_position_sum`]: positionSum,
});

const days = (from: number, to: number) =>
  Array.from(
    { length: to - from + 1 },
    (_, i) => `2026-10-${String(from + i).padStart(2, "0")}`,
  );

/** Days 22–25 imported with data, 26 and 27 asked and empty, as a night's run leaves them. */
const WEEK: Batch[] = [
  ...days(15, 18).map((day, i) => ({ id: 10 + i, day, rows: 5 })),
  ...days(22, 25).map((day, i) => ({ id: 20 + i, day, rows: 5 })),
  ...days(26, 27).map((day, i) => ({ id: 30 + i, day, rows: 0 })),
];

describe("pageFilterOf", () => {
  it("is the path, lower case and without a trailing slash, from a path or a URL", () => {
    expect(pageFilterOf("/DE/Parks/")).toBe("/de/parks");
    expect(pageFilterOf("de/parks")).toBe("/de/parks");
    expect(pageFilterOf("https://www.park.fan/de/parks/?x=1#y")).toBe(
      "/de/parks",
    );
    expect(pageFilterOf("/de/parks?x=1")).toBe("/de/parks");
    expect(pageFilterOf("/")).toBe("/");
  });
});

describe("SearchConsoleService.status", () => {
  const savedEnv = { ...process.env };
  afterEach(() => {
    process.env = { ...savedEnv };
  });

  it("says it is off, and why the credentials do not count", async () => {
    process.env.GSC_SERVICE_ACCOUNT_JSON = "{oops";
    process.env.GSC_SITE_URL = "sc-domain:park.fan";
    const { service } = build({});
    expect(await service.status()).toEqual({
      configured: false,
      siteUrl: "sc-domain:park.fan",
      lastImportAt: null,
      lastError: expect.stringContaining("JSON"),
      coverage: null,
      rows: 0,
    });
  });

  it("is plainly off without any setting", async () => {
    delete process.env.GSC_SERVICE_ACCOUNT_JSON;
    delete process.env.GSC_SITE_URL;
    const { service } = build({});
    expect(await service.status()).toMatchObject({
      configured: false,
      siteUrl: null,
      lastError: null,
    });
  });

  it("carries the last success, the coverage and a failed last run's error", async () => {
    const { generateKeyPairSync } = await import("crypto");
    process.env.GSC_SERVICE_ACCOUNT_JSON = JSON.stringify({
      client_email: "gsc@x.iam.gserviceaccount.com",
      private_key: generateKeyPairSync("rsa", {
        modulusLength: 2048,
        publicKeyEncoding: { type: "spki", format: "pem" },
        privateKeyEncoding: { type: "pkcs8", format: "pem" },
      }).privateKey,
    });
    process.env.GSC_SITE_URL = "sc-domain:park.fan";
    const { service } = build({
      coverage: { first: "2025-07-05", last: "2026-10-25", rows: "81234" },
      success: new Date("2026-10-27T09:25:00Z"),
      latest: { ok: false, error: "Search Console refused the account" },
    });
    expect(await service.status()).toEqual({
      configured: true,
      siteUrl: "sc-domain:park.fan",
      lastImportAt: "2026-10-27T09:25:00.000Z",
      lastError: "Search Console refused the account",
      coverage: { from: "2025-07-05", to: "2026-10-25" },
      rows: 81234,
    });
  });
});

describe("SearchConsoleService.queries", () => {
  it("reads up to the last day with data and compares with as many days at the start of the window before", async () => {
    const { service, calls } = build({
      batches: WEEK,
      items: [
        {
          item_key: "phantasialand",
          ...sums("c", 8, 100, 300),
          ...sums("p", 5, 80, 400),
        },
        {
          item_key: "new query",
          ...sums("c", 2, 20, 100),
          ...sums("p", 0, 0, 0),
        },
      ],
      total: { ...sums("c", 30, 400, 1600), ...sums("p", 20, 300, 1500) },
    });
    const report = await service.queries({ days: 7 }, NOW);

    expect(report).toMatchObject({
      from: "2026-10-22",
      to: "2026-10-28",
      days: 7,
      locale: null,
      totals: { clicks: 30, impressions: 400, ctr: 0.075, position: 4 },
      previous: { clicks: 20, impressions: 300, ctr: 0.0667, position: 5 },
    });
    const grouped = calls.find((c) => c.sql.includes("GROUP BY r.query"))!;
    // current 22–25, previous 15–18: four days each.
    expect(grouped.params).toEqual(
      expect.arrayContaining([
        "2026-10-22",
        "2026-10-25",
        "2026-10-15",
        "2026-10-18",
      ]),
    );
    expect(grouped.params).not.toContain("2026-10-28");
    // Only the batches of days with rows are read.
    expect(grouped.params[0]).toEqual([10, 11, 12, 13, 20, 21, 22, 23]);
    expect(grouped.sql).toContain("r.query IS NOT NULL");
  });

  it("computes ctr, position and the change per item", async () => {
    const { service } = build({
      batches: WEEK,
      items: [
        {
          item_key: "phantasialand",
          ...sums("c", 8, 100, 300),
          ...sums("p", 5, 80, 400),
        },
        {
          item_key: "new query",
          ...sums("c", 2, 20, 100),
          ...sums("p", 0, 0, 0),
        },
      ],
    });
    const { items } = await service.queries({ days: 7 }, NOW);
    expect(items[0]).toEqual({
      query: "phantasialand",
      clicks: 8,
      impressions: 100,
      ctr: 0.08,
      position: 3,
      // 3.0 against 5.0: two places better.
      change: { clicks: 3, position: -2 },
    });
    // A query the window before did not have: clicks up by all of them, no position to compare.
    expect(items[1].change).toEqual({ clicks: 2, position: null });
  });

  it("has no window before, and no change, over 180 days", async () => {
    const { service } = build({
      batches: WEEK,
      items: [
        { item_key: "q", ...sums("c", 1, 10, 20), ...sums("p", 0, 0, 0) },
      ],
    });
    const report = await service.queries({ days: 200 }, NOW);
    expect(report.previous).toBeNull();
    expect(report.items[0].change).toBeNull();
  });

  it("has no previous when the window before has no data", async () => {
    const { service } = build({
      batches: WEEK.filter((b) => b.day >= "2026-10-22"),
      items: [
        { item_key: "q", ...sums("c", 1, 10, 20), ...sums("p", 0, 0, 0) },
      ],
    });
    const report = await service.queries({ days: 7 }, NOW);
    expect(report.previous).toBeNull();
    expect(report.items[0].change).toBeNull();
  });

  it("answers an empty window as zeros, without a query for rows", async () => {
    const { service, calls } = build({ batches: [] });
    const report = await service.queries({ days: 7 }, NOW);
    expect(report).toMatchObject({
      totals: { clicks: 0, impressions: 0, ctr: 0, position: null },
      previous: null,
      items: [],
    });
    expect(calls.some((c) => c.sql.includes("FROM search_console_rows"))).toBe(
      false,
    );
  });

  it("binds the filters as parameters", async () => {
    const { service, calls } = build({ batches: WEEK, items: [] });
    await service.queries(
      {
        days: 7,
        locale: "de",
        page: "https://park.fan/DE/Parks/",
        country: "DEU",
        device: "mobile",
        limit: 10,
      },
      NOW,
    );
    const grouped = calls.find((c) => c.sql.includes("GROUP BY r.query"))!;
    expect(grouped.sql).toContain("r.locale =");
    expect(grouped.sql).toContain("r.page =");
    expect(grouped.sql).toContain("r.country =");
    expect(grouped.sql).toContain("r.device =");
    expect(grouped.params).toEqual(
      expect.arrayContaining(["de", "/de/parks", "deu", "mobile", 10]),
    );
    expect(grouped.sql).not.toContain("/de/parks");
  });

  it("refuses a window the site statistic refuses", async () => {
    const { service } = build({});
    await expect(
      service.queries({ days: 7, from: "2026-10-01", to: "2026-10-07" }, NOW),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      service.queries({ from: "2026-10-01" }, NOW),
    ).rejects.toBeInstanceOf(BadRequestException);
  });
});

describe("SearchConsoleService.pages", () => {
  it("reads the rows without a query, and joins the site's own numbers", async () => {
    const { service, calls } = build({
      batches: WEEK,
      items: [
        {
          item_key: "/de/parks",
          ...sums("c", 8, 100, 300),
          ...sums("p", 8, 100, 300),
        },
        {
          item_key: "/en/news",
          ...sums("c", 1, 10, 80),
          ...sums("p", 0, 0, 0),
        },
      ],
      site: [
        {
          path: "/de/parks",
          visits: "40",
          engaged: "10",
          visit_seconds: "2000",
        },
      ],
    });
    const report = await service.pages({ days: 7 }, NOW);

    const grouped = calls.find((c) => c.sql.includes("GROUP BY r.page"))!;
    expect(grouped.sql).toContain("r.query IS NULL AND r.page <> ''");
    expect(report.items[0]).toMatchObject({
      path: "/de/parks",
      clicks: 8,
      change: { clicks: 0, position: 0 },
      site: { visits: 40, bounceRate: 0.75, avgVisitSeconds: 50 },
    });
    expect(report.items[1].site).toBeNull();

    const site = calls.find((c) => c.sql.includes("FROM site_pageviews"))!;
    expect(site.sql).toContain("channel = 'search'");
    expect(site.params).toEqual(
      expect.arrayContaining([["/de/parks", "/en/news"]]),
    );
    // The site's own window is the whole requested one, not cut at the data.
    expect(site.params).toEqual(
      expect.arrayContaining([
        "2026-10-22T00:00:00.000Z",
        "2026-10-29T00:00:00.000Z",
      ]),
    );
  });

  it("reads the rows of one query when asked for it", async () => {
    const { service, calls } = build({ batches: WEEK, items: [] });
    await service.pages({ days: 7, query: " Phantasialand " }, NOW);
    const grouped = calls.find((c) => c.sql.includes("GROUP BY r.page"))!;
    expect(grouped.sql).toContain("r.query IS NOT NULL");
    expect(grouped.sql).toContain("r.query =");
    expect(grouped.params).toContain("phantasialand");
  });

  it("does not ask the site statistic when there are no pages", async () => {
    const { service, calls } = build({ batches: WEEK, items: [] });
    await service.pages({ days: 7 }, NOW);
    expect(calls.some((c) => c.sql.includes("FROM site_pageviews"))).toBe(
      false,
    );
  });
});

describe("SearchConsoleService.series", () => {
  const perDay = [
    { day: "2026-10-22", clicks: "4", impressions: "100", position_sum: 400 },
    { day: "2026-10-24", clicks: "6", impressions: "100", position_sum: 200 },
  ];

  it("has a point per day to the last day with data, a quiet day inside as zeros", async () => {
    const { service } = build({ batches: WEEK, perDay });
    const report = await service.series({ days: 7 }, NOW);
    expect(report).toMatchObject({
      from: "2026-10-22",
      to: "2026-10-28",
      days: 7,
      interval: "day",
    });
    expect(report.series.map((p) => p.day)).toEqual([
      "2026-10-22",
      "2026-10-23",
      "2026-10-24",
      "2026-10-25",
    ]);
    expect(report.series[0]).toEqual({
      day: "2026-10-22",
      clicks: 4,
      impressions: 100,
      ctr: 0.04,
      position: 4,
    });
    expect(report.series[1]).toEqual({
      day: "2026-10-23",
      clicks: 0,
      impressions: 0,
      ctr: 0,
      position: null,
    });
  });

  it("folds days into ISO weeks, the periods the window or the data cut being partial", async () => {
    const { service } = build({ batches: WEEK, perDay });
    const report = await service.series({ days: 7, interval: "week" }, NOW);
    // 22–25 October 2026 is Thursday to Sunday of the week of Monday the 19th.
    expect(report.series).toEqual([
      {
        day: "2026-10-19",
        clicks: 10,
        impressions: 200,
        ctr: 0.05,
        position: 3,
        partial: true,
      },
    ]);
  });

  it("marks the periods the window and the data cut partial, and no other", async () => {
    const { service } = build({
      batches: [...days(1, 23).map((day, i) => ({ id: i + 1, day, rows: 5 }))],
      perDay: [],
    });
    const report = await service.series(
      { from: "2026-10-01", to: "2026-10-28", interval: "week" },
      NOW,
    );
    const flagged = report.series.filter((p) => p.partial).map((p) => p.day);
    // The week the window starts in, and the one the data ends in (Friday the 23rd); the whole weeks between are not.
    expect(flagged).toEqual(["2026-09-28", "2026-10-19"]);
  });

  it("uses the month for a long window and refuses nothing the contract allows", async () => {
    const { service } = build({ batches: WEEK, perDay: [] });
    const report = await service.series({ days: 365 }, NOW);
    expect(report.interval).toBe("month");
  });

  it("reads the rows of a query when it is filtered by one", async () => {
    const { service, calls } = build({ batches: WEEK, perDay: [] });
    await service.series({ days: 7, query: "phantasialand" }, NOW);
    const grouped = calls.find((c) => c.sql.includes("GROUP BY r.day"))!;
    expect(grouped.sql).toContain("r.query IS NOT NULL");
  });

  it("answers an empty window as an empty series", async () => {
    const { service } = build({ batches: [] });
    expect((await service.series({ days: 7 }, NOW)).series).toEqual([]);
  });
});

describe("levelPlan", () => {
  it("answers everything from the property and page rows without a country or device", () => {
    expect(levelPlan({}, "series")).toEqual({
      list: "page",
      totals: "property",
      partialBreakdown: false,
    });
    expect(levelPlan({ page: "/de" }, "pages").totals).toBe("page");
    expect(levelPlan({ locale: "de" }, "series").totals).toBe("page");
    expect(levelPlan({}, "queries")).toMatchObject({
      list: "query",
      totals: "property",
    });
  });

  it("takes complete totals from the country or device rows and the list from the queries", () => {
    expect(levelPlan({ country: "deu" }, "pages")).toEqual({
      list: "query",
      totals: "country",
      partialBreakdown: true,
    });
    expect(levelPlan({ device: "mobile" }, "series").totals).toBe("device");
    expect(levelPlan({ device: "mobile" }, "queries").partialBreakdown).toBe(
      false,
    );
  });

  it("falls back to the query rows when nothing complete exists", () => {
    for (const query of [
      { country: "deu", device: "mobile" },
      { country: "deu", page: "/de" },
      { device: "mobile", locale: "de" },
    ]) {
      expect(levelPlan(query, "series")).toMatchObject({
        totals: "query",
        partialBreakdown: true,
      });
    }
    expect(levelPlan({ query: "x", country: "deu" }, "pages")).toEqual({
      list: "query",
      totals: "query",
      partialBreakdown: false,
    });
  });
});

describe("SearchConsoleService levels", () => {
  const totalsSql = (calls: Array<{ sql: string }>) =>
    calls
      .filter(
        (c) =>
          c.sql.includes("FROM search_console_rows") &&
          !c.sql.includes("GROUP BY"),
      )
      .pop()!.sql;

  it("reads the property rows for the totals and the page rows for the items", async () => {
    const { service, calls } = build({ batches: WEEK });
    const report = await service.pages({ days: 7 }, NOW);
    expect(report).not.toHaveProperty("partialBreakdown");
    expect(totalsSql(calls)).toContain("r.page = '' AND r.country = ''");
    expect(calls.find((c) => c.sql.includes("GROUP BY r.page"))!.sql).toContain(
      "r.page <> ''",
    );
  });

  it("reads the country rows for totals under a country filter, the query rows for the pages, and says so", async () => {
    const { service, calls } = build({ batches: WEEK });
    const report = await service.pages({ days: 7, country: "deu" }, NOW);
    expect(report.partialBreakdown).toBe(true);
    expect(totalsSql(calls)).toContain("r.country <> ''");
    expect(calls.find((c) => c.sql.includes("GROUP BY r.page"))!.sql).toContain(
      "r.query IS NOT NULL",
    );
  });

  it("reads the device rows for a series under a device filter", async () => {
    const { service, calls } = build({ batches: WEEK });
    const report = await service.series({ days: 7, device: "mobile" }, NOW);
    expect(report.partialBreakdown).toBe(true);
    expect(calls.find((c) => c.sql.includes("GROUP BY r.day"))!.sql).toContain(
      "r.device <> ''",
    );
  });

  it("reads the query rows when both filters are given", async () => {
    const { service, calls } = build({ batches: WEEK });
    const report = await service.series(
      { days: 7, country: "deu", device: "mobile" },
      NOW,
    );
    expect(report.partialBreakdown).toBe(true);
    expect(calls.find((c) => c.sql.includes("GROUP BY r.day"))!.sql).toContain(
      "r.query IS NOT NULL",
    );
  });

  it("ignores a day whose batch is in an older layout", async () => {
    const { service } = build({
      batches: WEEK.map((b) => ({ ...b, format: 1 })),
    });
    const report = await service.series({ days: 7 }, NOW);
    expect(report.series).toEqual([]);
  });
});
