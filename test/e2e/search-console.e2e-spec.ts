import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { generateKeyPairSync } from "crypto";
import { AppModule } from "../../src/app.module";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import type { AdminRole } from "../../src/admin/auth/entities/admin-user.entity";
import { addIsoDays } from "../../src/common/utils/date.util";
import { utcDay } from "../../src/common/utils/reader-hash.util";
import {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
} from "../../src/common/swagger/swagger-config";
import type Redis from "ioredis";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { SearchConsoleImportService } from "../../src/search-console/search-console-import.service";
import { SearchConsoleClient } from "../../src/search-console/search-console.client";
import { BACKFILL_DAYS } from "../../src/search-console/search-console.constants";

const PASSWORD = "a-long-enough-password";
const PARK_PATH = "/de/parks/europe/germany/bruehl/phantasialand";
const PARK_URL = `https://park.fan${PARK_PATH}/`;

const today = utcDay(new Date());
/** The day `n` days before today. */
const d = (n: number) => addIsoDays(today, -n);

interface ApiRow {
  keys: string[];
  clicks: number;
  impressions: number;
  ctr: number;
  position: number;
}

const apiRow = (
  keys: string[],
  clicks: number,
  impressions: number,
  position: number,
): ApiRow => ({ keys, clicks, impressions, ctr: 0, position });

/**
 * What the mocked Search Console answers for a day, by the dimensions asked
 * for (joined with a comma). The property, country and device rows are
 * complete and agree (16 clicks at scale 1); the page rows add up to a little
 * more (17, Google aggregates them by page); the query rows leave out the
 * anonymised queries (10 clicks). `scale` multiplies the figures, so a day
 * can come back different.
 */
function googleDay(scale: number): Record<string, ApiRow[]> {
  const n = (value: number) => value * scale;
  return {
    // Without dimensions Google sends no keys.
    "": [{ clicks: n(16), impressions: n(340), ctr: 0, position: 3 } as ApiRow],
    page: [
      apiRow([PARK_URL], n(10), n(200), 3),
      apiRow(["https://www.park.fan" + PARK_PATH], n(5), n(100), 4),
      apiRow(["https://park.fan/en/news"], n(2), n(50), 8),
      // A subdomain of the domain property: not a page of the site.
      apiRow(["https://api.park.fan/v1/parks"], 99, 99, 1),
    ],
    country: [
      apiRow(["deu"], n(15), n(300), 3),
      apiRow(["usa"], n(1), n(40), 8),
    ],
    device: [
      apiRow(["MOBILE"], n(10), n(200), 3),
      apiRow(["DESKTOP"], n(6), n(140), 4),
    ],
    "page,query,country,device": [
      apiRow(
        [PARK_URL, "phantasialand wartezeiten", "deu", "MOBILE"],
        n(6),
        n(80),
        2,
      ),
      apiRow([PARK_URL, "Phantasialand", "deu", "DESKTOP"], n(3), n(60), 1.5),
      apiRow(
        ["https://park.fan/en/news", "park news", "usa", "DESKTOP"],
        n(1),
        n(20),
        7,
      ),
    ],
  };
}

/**
 * Search Console against a real TimescaleDB and Redis, with Google mocked at
 * `fetch`: the import's batches and rows, the backfill, a day imported again
 * as a newer batch with nothing updated or deleted, the reports' SQL with
 * filters, the window before and the join with the site statistic, the chunk
 * drop, the admin routes and the MCP tools. No real credentials are used, and
 * nothing here reaches Google.
 */
describe("Search Console (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let importService: SearchConsoleImportService;
  /** The day a mocked Google has rows for, and their scale. */
  let google: Map<string, number>;
  let apiCalls: Array<{ day: string; dimensions: string[]; type: string }>;
  /** Days a mocked Google has rows for in another type, `type:day`, and their scale. */
  let googleTypes: Map<string, number>;
  let inspectionCalls: string[];
  let fetchSpy: jest.SpyInstance;
  const savedEnv = { ...process.env };

  beforeAll(async () => {
    process.env.GSC_SERVICE_ACCOUNT_JSON = JSON.stringify({
      client_email: "gsc-e2e@park-fan.iam.gserviceaccount.com",
      private_key: generateKeyPairSync("rsa", {
        modulusLength: 2048,
        publicKeyEncoding: { type: "spki", format: "pem" },
        privateKeyEncoding: { type: "pkcs8", format: "pem" },
      }).privateKey,
    });
    process.env.GSC_SITE_URL = "sc-domain:park.fan";

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix(GLOBAL_PREFIX, GLOBAL_PREFIX_OPTIONS);
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: true },
      }),
    );
    await app.init();
    dataSource = app.get(DataSource);
    importService = app.get(SearchConsoleImportService);
    // The mocked Google needs no spacing, and a backfill spaced like the
    // real client's requests outlasts the test timeout.
    app.get(SearchConsoleClient).wait = async () => undefined;
  });

  afterAll(async () => {
    process.env = { ...savedEnv };
    await app.close();
  });

  beforeEach(() => {
    google = new Map();
    googleTypes = new Map();
    apiCalls = [];
    inspectionCalls = [];
    const real = globalThis.fetch;
    fetchSpy = jest
      .spyOn(globalThis, "fetch")
      .mockImplementation(
        async (input: string | URL | Request, init?: RequestInit) => {
          const url = String(input);
          if (url.startsWith("https://oauth2.googleapis.com/")) {
            return Response.json({
              access_token: "ya29.e2e",
              expires_in: 3600,
            });
          }
          if (url.startsWith("https://www.googleapis.com/webmasters/")) {
            const body = JSON.parse(String(init?.body));
            expect(url).toContain("/sites/sc-domain%3Apark.fan/");
            const type = body.type as string;
            apiCalls.push({
              day: body.startDate,
              dimensions: body.dimensions,
              type,
            });
            const dimensions = (body.dimensions as string[]).join(",");
            // Discover has neither queries, devices nor the property aggregation.
            if (
              type === "discover" &&
              (dimensions === "" ||
                dimensions.includes("query") ||
                dimensions.includes("device"))
            ) {
              return Response.json(
                {
                  error: {
                    message: dimensions.includes("device")
                      ? "Requests for Discover cannot be grouped by device"
                      : "Unsupported for discover",
                  },
                },
                { status: 400 },
              );
            }
            const scale =
              type === "web"
                ? google.get(body.startDate)
                : googleTypes.get(`${type}:${body.startDate}`);
            if (scale === undefined) return Response.json({});
            const rows = googleDay(scale)[dimensions] ?? [];
            return Response.json({
              // Discover lists the site's own pages only.
              rows:
                type === "discover"
                  ? rows.filter((row) => !row.keys?.[0]?.includes("api."))
                  : rows,
            });
          }
          if (
            url ===
            "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"
          ) {
            const body = JSON.parse(String(init?.body));
            expect(body.siteUrl).toBe("sc-domain:park.fan");
            inspectionCalls.push(body.inspectionUrl);
            return Response.json({
              inspectionResult: {
                indexStatusResult: {
                  verdict: "PASS",
                  coverageState: "Submitted and indexed",
                  googleCanonical: body.inspectionUrl,
                  userCanonical: body.inspectionUrl,
                  crawledAs: "MOBILE",
                },
              },
            });
          }
          return real(input, init);
        },
      );
  });

  afterEach(() => fetchSpy.mockRestore());

  async function account(email: string, role: AdminRole) {
    const created = await app.get(AdminAuthService).create({
      email,
      role,
      password: PASSWORD,
    });
    await dataSource.query(
      `UPDATE admin_users SET must_change_password = false WHERE id = $1::uuid`,
      [created.id],
    );
    const { token } = await app.get(AdminSessionStore).create({
      userId: created.id,
      email: created.email,
      displayName: created.displayName,
      role,
      ip: null,
      userAgent: "e2e",
      mustChangePassword: false,
      mustEnrolTotp: false,
    });
    return { id: created.id, session: token };
  }

  const get = (path: string, session: string) =>
    request(app.getHttpServer())
      .get(path)
      .set("Authorization", `Bearer ${session}`);

  /** Data on the days the reports below read: two before the window, three in it. */
  function seedGoogle() {
    google.set(d(9), 1);
    google.set(d(4), 1);
    google.set(d(3), 2);
    google.set(d(2), 1);
  }

  it("is off without credentials, and says so", async () => {
    const viewer = await account("gsc-off@example.invalid", "viewer");
    const key = process.env.GSC_SERVICE_ACCOUNT_JSON;
    delete process.env.GSC_SERVICE_ACCOUNT_JSON;
    try {
      const status = await get(
        "/v1/admin/search-console/status",
        viewer.session,
      ).expect(200);
      expect(status.body).toEqual({
        configured: false,
        siteUrl: "sc-domain:park.fan",
        lastImportAt: null,
        lastError: null,
        runningSince: null,
        coverage: null,
        rows: 0,
        types: {
          web: { coverage: null, rows: 0 },
          image: { coverage: null, rows: 0 },
          news: { coverage: null, rows: 0 },
          discover: { coverage: null, rows: 0 },
        },
      });
      expect(await importService.run("manual")).toMatchObject({
        skipped: "not-configured",
      });
      expect(apiCalls).toEqual([]);
      await request(app.getHttpServer())
        .post("/v1/admin/search-console/import")
        .set(
          "Authorization",
          `Bearer ${(await account("gsc-off-ed@example.invalid", "editor")).session}`,
        )
        .expect(409);
    } finally {
      process.env.GSC_SERVICE_ACCOUNT_JSON = key;
    }
  });

  it("backfills sixteen months on the first run, a batch per day, and writes only what changed after", async () => {
    seedGoogle();
    const first = await importService.run("cron");
    // Every type has its own backfill: a batch per day and type, empty or not.
    expect(first).toMatchObject({
      daysChecked: BACKFILL_DAYS * 4,
      batchesWritten: BACKFILL_DAYS * 4,
      failedDays: [],
    });
    // Page rows merged by path, rows off the site skipped, both kinds stored.
    expect(first.skippedRows).toBeGreaterThan(0);
    const batches = await dataSource.query(
      `SELECT count(*)::int AS days, count(*) FILTER (WHERE rows > 0)::int AS with_rows,
              min(day)::text AS first, max(day)::text AS last
         FROM search_console_batches WHERE search_type = 'web'`,
    );
    expect(batches[0]).toEqual({
      days: BACKFILL_DAYS,
      with_rows: 4,
      first: d(BACKFILL_DAYS),
      last: d(1),
    });
    const stored = await dataSource.query(
      `SELECT page, query, locale, country, device, clicks, impressions, position_sum
         FROM search_console_rows WHERE day = $1::date
        ORDER BY query NULLS FIRST, page, device`,
      [d(2)],
    );
    // Property 1, page 2, country 2, device 2 and the query rows 3.
    expect(stored).toHaveLength(10);
    expect(
      stored.filter((row: { query: string | null }) => row.query === null),
    ).toHaveLength(7);
    expect(
      stored.find(
        (row: { page: string; query: string | null }) =>
          row.page === PARK_PATH && row.query === null,
      ),
    ).toEqual({
      page: PARK_PATH,
      query: null,
      locale: "de",
      country: "",
      device: "",
      clicks: 15,
      impressions: 300,
      position_sum: 1000,
    });
    expect(
      stored.find(
        (row: { page: string; country: string; query: string | null }) =>
          row.page === "" && row.country === "usa" && row.query === null,
      ),
    ).toMatchObject({ device: "", clicks: 1, impressions: 40 });
    expect(
      stored.map((row: { query: string | null }) => row.query).filter(Boolean),
    ).toEqual(["park news", "phantasialand", "phantasialand wartezeiten"]);

    // A second run asks Google for the recent days again and nothing else.
    apiCalls.length = 0;
    const second = await importService.run("cron");
    expect(second).toMatchObject({
      daysChecked: 12,
      batchesWritten: 0,
      unchanged: 12,
    });
    // Five requests a day for web, image and news: property, page, country,
    // device, query; two for discover: page, country.
    const requests = (type: string) =>
      apiCalls.filter((c) => c.type === type).map((c) => c.day);
    for (const type of ["web", "image", "news"]) {
      expect(requests(type).sort()).toEqual(
        [d(1), d(2), d(3)].flatMap((day) => Array(5).fill(day)).sort(),
      );
    }
    expect(requests("discover").sort()).toEqual(
      [d(1), d(2), d(3)].flatMap((day) => Array(2).fill(day)).sort(),
    );

    // A day Google revised is a newer batch; the older batch's rows stay.
    google.set(d(2), 3);
    const third = await importService.run("cron");
    expect(third).toMatchObject({ batchesWritten: 1, unchanged: 11 });
    const perBatch = await dataSource.query(
      `SELECT batch_id, count(*)::int AS rows, sum(clicks) FILTER (WHERE query IS NULL AND page = '' AND country = '' AND device = '')::int AS clicks
         FROM search_console_rows WHERE day = $1::date GROUP BY batch_id ORDER BY batch_id`,
      [d(2)],
    );
    expect(perBatch).toHaveLength(2);
    expect(perBatch.map((b: { clicks: number }) => b.clicks)).toEqual([16, 48]);
  });

  it("reports the newest batch of each day: queries, pages, series, with filters and the window before", async () => {
    seedGoogle();
    await importService.run("cron");
    google.set(d(2), 3); // revised: the first batch of this day must not count
    await importService.run("cron");
    const viewer = await account("gsc-reports@example.invalid", "viewer");

    // The window is the 7 days up to the last day with data, d(8)…d(2), against
    // the 7 days right before, d(15)…d(9).
    const queries = await get(
      "/v1/admin/search-console/queries?days=7",
      viewer.session,
    ).expect(200);
    expect(queries.headers["cache-control"]).toContain("no-store");
    expect(queries.body).toMatchObject({
      from: d(8),
      to: d(2),
      days: 7,
      locale: null,
      // Query rows only: 10 clicks on d(4), 20 on d(3), 30 on d(2) (the
      // revised day), against the property's own 16 + 32 + 48.
      totals: { clicks: 60, impressions: 960 },
      previous: { clicks: 10, impressions: 160 },
    });
    expect(queries.body.items.map((i: { query: string }) => i.query)).toEqual([
      "phantasialand wartezeiten",
      "phantasialand",
      "park news",
    ]);
    expect(queries.body.items[0]).toEqual({
      query: "phantasialand wartezeiten",
      clicks: 36,
      impressions: 480,
      ctr: 0.075,
      position: 2,
      change: { clicks: 30, position: 0 },
    });
    const parkNews = queries.body.items[2];
    expect(parkNews).toMatchObject({ query: "park news", clicks: 6 });
    expect(parkNews.change).toEqual({ clicks: 5, position: 0 });

    const pages = await get(
      "/v1/admin/search-console/pages?days=7",
      viewer.session,
    ).expect(200);
    // Totals are the property's; the items are by page and add up to more.
    expect(pages.body.totals).toMatchObject({ clicks: 16 + 32 + 48 });
    expect(pages.body.previous).toMatchObject({ clicks: 16 });
    expect(pages.body).not.toHaveProperty("partialBreakdown");
    expect(pages.body.items[0]).toMatchObject({
      path: PARK_PATH,
      clicks: 15 + 30 + 45,
      change: { clicks: 75 },
    });
    expect(pages.body.items[1].path).toBe("/en/news");

    const byQuery = await get(
      "/v1/admin/search-console/pages?days=7&query=Phantasialand",
      viewer.session,
    ).expect(200);
    expect(byQuery.body.items).toHaveLength(1);
    expect(byQuery.body.items[0]).toMatchObject({ path: PARK_PATH });

    const byPage = await get(
      `/v1/admin/search-console/queries?days=7&page=${encodeURIComponent(PARK_URL)}&device=desktop&country=DEU`,
      viewer.session,
    ).expect(200);
    expect(byPage.body.items.map((i: { query: string }) => i.query)).toEqual([
      "phantasialand",
    ]);

    const english = await get(
      "/v1/admin/search-console/pages?days=7&locale=en",
      viewer.session,
    ).expect(200);
    expect(english.body.items.map((i: { path: string }) => i.path)).toEqual([
      "/en/news",
    ]);
    expect(english.body.locale).toBe("en");

    const series = await get(
      "/v1/admin/search-console/series?days=7&interval=day",
      viewer.session,
    ).expect(200);
    expect(series.body.interval).toBe("day");
    expect(series.body.series.map((p: { day: string }) => p.day)).toEqual([
      d(8),
      d(7),
      d(6),
      d(5),
      d(4),
      d(3),
      d(2),
    ]);
    expect(series.body.series[0]).toEqual({
      day: d(8),
      clicks: 0,
      impressions: 0,
      ctr: 0,
      position: null,
    });
    expect(series.body.series[6]).toMatchObject({ day: d(2), clicks: 48 });

    const long = await get(
      "/v1/admin/search-console/series?days=365",
      viewer.session,
    ).expect(200);
    expect(long.body.interval).toBe("month");
    const total = long.body.series.reduce(
      (sum: number, p: { clicks: number }) => sum + p.clicks,
      0,
    );
    expect(total).toBe(16 + 16 + 32 + 48);
  });

  it("answers a country or device filter from the complete country and device rows, and marks the page list", async () => {
    seedGoogle();
    await importService.run("cron");
    const viewer = await account("gsc-slice@example.invalid", "viewer");

    const series = await get(
      "/v1/admin/search-console/series?days=7&interval=day&country=deu",
      viewer.session,
    ).expect(200);
    expect(series.body.partialBreakdown).toBe(true);
    expect(series.body.series.slice(-1)[0]).toMatchObject({
      day: d(2),
      clicks: 15,
    });

    const pages = await get(
      "/v1/admin/search-console/pages?days=7&device=mobile",
      viewer.session,
    ).expect(200);
    expect(pages.body.partialBreakdown).toBe(true);
    expect(pages.body.totals).toMatchObject({ clicks: 10 + 20 + 10 });
    // The list is the visible queries' pages: 6 clicks a scale unit.
    expect(pages.body.items).toEqual([
      expect.objectContaining({ path: PARK_PATH, clicks: 6 + 12 + 6 }),
    ]);

    const both = await get(
      "/v1/admin/search-console/series?days=7&interval=day&country=deu&device=mobile",
      viewer.session,
    ).expect(200);
    expect(both.body.partialBreakdown).toBe(true);
    expect(both.body.series.slice(-1)[0]).toMatchObject({
      day: d(2),
      clicks: 6,
    });

    const queries = await get(
      "/v1/admin/search-console/queries?days=7&country=deu",
      viewer.session,
    ).expect(200);
    expect(queries.body).not.toHaveProperty("partialBreakdown");
  });

  it("fetches every day again once when its batch is in the older layout", async () => {
    seedGoogle();
    await importService.run("cron");
    await dataSource.query(
      `UPDATE search_console_batches SET format = 1 WHERE search_type = 'web'`,
    );
    const viewer = await account("gsc-legacy@example.invalid", "viewer");
    const empty = await get(
      "/v1/admin/search-console/series?days=7",
      viewer.session,
    ).expect(200);
    expect(empty.body.series).toEqual([]);

    apiCalls.length = 0;
    const rerun = await importService.run("cron");
    // The web days again, and the recent days of the three other types.
    expect(rerun).toMatchObject({
      daysChecked: BACKFILL_DAYS + 9,
      batchesWritten: BACKFILL_DAYS,
    });
    const formats = await dataSource.query(
      `SELECT DISTINCT ON (day) format FROM search_console_batches
        WHERE search_type = 'web' ORDER BY day, id DESC`,
    );
    expect(formats.every((row: { format: number }) => row.format === 2)).toBe(
      true,
    );
    const next = await importService.run("cron");
    expect(next).toMatchObject({ daysChecked: 12, batchesWritten: 0 });
  });

  it("joins the pages with the site statistic's own visits from search", async () => {
    seedGoogle();
    await importService.run("cron");
    const bucket = new Date(`${d(3)}T10:00:00Z`);
    await dataSource.query(
      `INSERT INTO site_pageviews (bucket, locale, path, page_type, device, browser, os,
                                   country, entry, referrer, channel, views, visits, engaged, visit_seconds)
       VALUES ($1, 'de', $2, 'park', 'mobile', 'safari', 'ios', 'de', $2, 'google', 'search', 0, 20, 5, 1200),
              ($1, 'de', $2, 'park', 'mobile', 'safari', 'ios', 'de', $2, '', 'direct', 0, 50, 10, 500),
              ($1, 'en', '/en/news', 'news', 'desktop', 'chrome', 'windows', 'gb', '/en/news', 'bing', 'search', 0, 0, 0, 0)`,
      [bucket, PARK_PATH],
    );
    const viewer = await account("gsc-site@example.invalid", "viewer");
    const pages = await get(
      "/v1/admin/search-console/pages?days=7",
      viewer.session,
    ).expect(200);
    const park = pages.body.items.find(
      (i: { path: string }) => i.path === PARK_PATH,
    );
    // Search visits only: the direct ones are not Google's.
    expect(park.site).toEqual({
      visits: 20,
      bounceRate: 0.75,
      avgVisitSeconds: 60,
    });
    const news = pages.body.items.find(
      (i: { path: string }) => i.path === "/en/news",
    );
    expect(news.site).toBeNull();
  });

  it("answers an empty window as zeros and refuses a window the site statistic refuses", async () => {
    const viewer = await account("gsc-empty@example.invalid", "viewer");
    const queries = await get(
      "/v1/admin/search-console/queries",
      viewer.session,
    ).expect(200);
    expect(queries.body).toMatchObject({
      days: 30,
      totals: { clicks: 0, impressions: 0, ctr: 0, position: null },
      previous: null,
      items: [],
    });
    await get(
      "/v1/admin/search-console/series?days=7&interval=hour",
      viewer.session,
    ).expect(400);
    await get(
      "/v1/admin/search-console/queries?days=7&from=2026-01-01&to=2026-01-31",
      viewer.session,
    ).expect(400);
    await get(
      "/v1/admin/search-console/queries?limit=500",
      viewer.session,
    ).expect(400);
    await get(
      "/v1/admin/search-console/queries?device=watch",
      viewer.session,
    ).expect(400);
  });

  it("keeps the table as a hypertable with its compression, and synchronize finds nothing to change", async () => {
    const hypertable = await dataSource.query(
      `SELECT compression_enabled FROM timescaledb_information.hypertables
        WHERE hypertable_name = 'search_console_rows'`,
    );
    expect(hypertable).toEqual([{ compression_enabled: true }]);
    const settings = await dataSource.query(
      `SELECT attname, segmentby_column_index, orderby_column_index
         FROM timescaledb_information.compression_settings
        WHERE hypertable_name = 'search_console_rows'
          AND (segmentby_column_index IS NOT NULL OR orderby_column_index IS NOT NULL)`,
    );
    expect(
      settings.find((s: { attname: string }) => s.attname === "batch_id")
        .segmentby_column_index,
    ).toBe(1);

    // Added to a compressed hypertable: a row from before reads as web.
    const column = await dataSource.query(
      `SELECT is_nullable, column_default FROM information_schema.columns
        WHERE table_name = 'search_console_rows' AND column_name = 'search_type'`,
    );
    expect(column).toEqual([
      { is_nullable: "YES", column_default: "'web'::character varying" },
    ]);

    const pending = await dataSource.driver.createSchemaBuilder().log();
    expect(
      pending.upQueries
        .map((q) => q.query)
        .filter((sql) => /search_console_/.test(sql)),
    ).toEqual([]);
  });

  it("drops chunks past 500 days whole and the bookkeeping that names nothing", async () => {
    seedGoogle();
    await importService.run("cron");
    const old = d(620);
    await dataSource.query(
      `INSERT INTO search_console_batches (day, rows, clicks, impressions, position_sum)
       VALUES ($1::date, 1, 1, 1, 1)`,
      [old],
    );
    await dataSource.query(
      `INSERT INTO search_console_rows (day, batch_id, page, query, country, device, clicks, impressions)
       SELECT $1::date, max(id), '/de', NULL, 'deu', 'mobile', 1, 1 FROM search_console_batches`,
      [old],
    );
    await dataSource.query(
      `INSERT INTO search_console_runs (triggered_by, started_at, finished_at, ok)
       VALUES ('cron', now() - interval '120 days', now() - interval '120 days', true)`,
    );
    const chunks = () =>
      dataSource.query(
        `SELECT count(*)::int AS n FROM timescaledb_information.chunks
          WHERE hypertable_name = 'search_console_rows'`,
      );
    const before = (await chunks())[0].n;

    expect(await importService.dropOldChunks()).toBe(1);
    expect((await chunks())[0].n).toBe(before - 1);
    const left = await dataSource.query(
      `SELECT (SELECT count(*)::int FROM search_console_rows WHERE day = $1::date) AS old_rows,
              (SELECT count(*)::int FROM search_console_batches WHERE day = $1::date) AS old_batches,
              (SELECT count(*)::int FROM search_console_runs WHERE ok AND started_at < now() - interval '90 days') AS old_runs,
              (SELECT count(*)::int FROM search_console_rows WHERE day = $2::date) AS recent_rows`,
      [old, d(2)],
    );
    expect(left[0]).toEqual({
      old_rows: 0,
      old_batches: 0,
      old_runs: 0,
      recent_rows: 10,
    });
  });

  it("lets a viewer read and only an editor import, and audits the import", async () => {
    const viewer = await account("gsc-viewer@example.invalid", "viewer");
    const editor = await account("gsc-editor@example.invalid", "editor");
    await request(app.getHttpServer())
      .get("/v1/admin/search-console/status")
      .expect(401);
    await get("/v1/admin/search-console/status", viewer.session).expect(200);
    await request(app.getHttpServer())
      .post("/v1/admin/search-console/import")
      .set("Authorization", `Bearer ${viewer.session}`)
      .expect(403);

    seedGoogle();
    const queued = await request(app.getHttpServer())
      .post("/v1/admin/search-console/import")
      .set("Authorization", `Bearer ${editor.session}`)
      .expect(202);
    expect(queued.body).toEqual({ queued: true });

    // The processor picks the job up: wait for the run it records.
    const deadline = Date.now() + 40_000;
    let run: Array<{
      ok: boolean;
      triggered_by: string;
      batches_written: number;
    }> = [];
    while (Date.now() < deadline) {
      run = await dataSource.query(
        `SELECT ok, triggered_by, batches_written FROM search_console_runs
          WHERE finished_at IS NOT NULL`,
      );
      if (run.length > 0) break;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    expect(run).toEqual([
      { ok: true, triggered_by: "manual", batches_written: BACKFILL_DAYS },
    ]);
    // Let the job close before the next test empties Redis under it.
    await new Promise((resolve) => setTimeout(resolve, 500));

    const status = await get(
      "/v1/admin/search-console/status",
      viewer.session,
    ).expect(200);
    expect(status.body).toMatchObject({
      configured: true,
      lastError: null,
      coverage: { from: d(9), to: d(2) },
    });
    expect(Date.parse(status.body.lastImportAt)).toBeGreaterThan(
      Date.now() - 60_000,
    );
    expect(status.body.rows).toBeGreaterThan(0);

    const audit = await dataSource.query(
      `SELECT action FROM admin_audit_log WHERE action LIKE 'job.search-console%'`,
    );
    expect(audit).toEqual([{ action: "job.search-console/import" }]);
  });

  it("says since when an import runs, and not for a run that finished or stopped showing signs of life", async () => {
    const viewer = await account("gsc-running@example.invalid", "viewer");
    const runningSince = async () =>
      (await get("/v1/admin/search-console/status", viewer.session).expect(200))
        .body.runningSince as string | null;
    const insertRun = async (
      startedAgo: string,
      heartbeatAgo: string | null,
    ) => {
      const [{ id, started_at }]: Array<{ id: number; started_at: Date }> =
        await dataSource.query(
          `INSERT INTO search_console_runs (triggered_by, started_at, heartbeat_at)
           VALUES ('cron', now() - $1::interval, now() - $2::interval)
           RETURNING id, started_at`,
          [startedAgo, heartbeatAgo],
        );
      return { id, since: new Date(started_at).toISOString() };
    };

    // Lost with its process hours ago, never stamped: not running.
    await insertRun("4 hours", null);
    expect(await runningSince()).toBeNull();

    // A long run that stamped half a minute ago is running since its start.
    const long = await insertRun("2 hours", "30 seconds");
    expect(await runningSince()).toBe(long.since);

    // The same run, silent for ten minutes: its process is gone.
    await dataSource.query(
      `UPDATE search_console_runs SET heartbeat_at = now() - interval '10 minutes' WHERE id = $1`,
      [long.id],
    );
    expect(await runningSince()).toBeNull();

    // A run a minute old, before its first stamp, is running; finished, it is not.
    const fresh = await insertRun("1 minute", null);
    expect(await runningSince()).toBe(fresh.since);
    await dataSource.query(
      `UPDATE search_console_runs SET finished_at = now(), ok = true WHERE id = $1`,
      [fresh.id],
    );
    expect(await runningSince()).toBeNull();
  });

  it("records why a run failed when Google refuses the account, and clears it with the next success", async () => {
    const viewer = await account("gsc-failure@example.invalid", "viewer");
    fetchSpy.mockImplementation(async (input: string | URL | Request) => {
      if (String(input).startsWith("https://oauth2.googleapis.com/")) {
        return Response.json({ access_token: "ya29.e2e", expires_in: 3600 });
      }
      return Response.json(
        { error: { message: "User does not have sufficient permission" } },
        { status: 403 },
      );
    });
    await expect(importService.run("cron")).rejects.toThrow(/add the account/);
    const failed = await get(
      "/v1/admin/search-console/status",
      viewer.session,
    ).expect(200);
    expect(failed.body.lastImportAt).toBeNull();
    expect(failed.body.lastError).toContain(
      "gsc-e2e@park-fan.iam.gserviceaccount.com",
    );
    expect(failed.body.lastError).toContain("sc-domain:park.fan");
  });

  it("imports image, news and discover next to web, each type with its own batches", async () => {
    seedGoogle();
    googleTypes.set(`image:${d(2)}`, 1);
    googleTypes.set(`news:${d(2)}`, 2);
    googleTypes.set(`discover:${d(3)}`, 1);
    const run = await importService.run("cron");
    expect(run.failedDays).toEqual([]);
    const perType = await dataSource.query(
      `SELECT search_type, count(*)::int AS batches, count(*) FILTER (WHERE rows > 0)::int AS with_rows
         FROM search_console_batches GROUP BY search_type ORDER BY search_type`,
    );
    expect(perType).toEqual([
      { search_type: "discover", batches: BACKFILL_DAYS, with_rows: 1 },
      { search_type: "image", batches: BACKFILL_DAYS, with_rows: 1 },
      { search_type: "news", batches: BACKFILL_DAYS, with_rows: 1 },
      { search_type: "web", batches: BACKFILL_DAYS, with_rows: 4 },
    ]);
    // Discover: property row summed from the pages, and no query or device rows.
    const discover = await dataSource.query(
      `SELECT count(*)::int AS rows,
              count(*) FILTER (WHERE query IS NOT NULL)::int AS with_query,
              count(*) FILTER (WHERE device <> '')::int AS with_device,
              max(clicks) FILTER (WHERE page = '' AND country = '' AND device = '')::int AS property_clicks
         FROM search_console_rows WHERE day = $1::date AND search_type = 'discover'`,
      [d(3)],
    );
    expect(discover[0]).toEqual({
      rows: 5,
      with_query: 0,
      with_device: 0,
      // 10 + 5 + 2 clicks of the three pages, summed into the property row.
      property_clicks: 17,
    });
    // Discover is never asked for what Google refuses it.
    expect(
      apiCalls.filter(
        (c) =>
          c.type === "discover" &&
          (c.dimensions.length === 0 ||
            c.dimensions.includes("query") ||
            c.dimensions.includes("device")),
      ),
    ).toEqual([]);
  });

  it("answers every report per type, web by default, and refuses queries and devices for discover", async () => {
    seedGoogle();
    googleTypes.set(`image:${d(2)}`, 1);
    googleTypes.set(`discover:${d(2)}`, 1);
    await importService.run("cron");
    const viewer = await account("gsc-types@example.invalid", "viewer");
    const read = (path: string) => get(path, viewer.session);

    const web = await read("/v1/admin/search-console/pages?days=7").expect(200);
    const explicit = await read(
      "/v1/admin/search-console/pages?days=7&type=web",
    ).expect(200);
    expect(explicit.body).toEqual(web.body);

    const image = await read(
      "/v1/admin/search-console/pages?days=7&type=image",
    ).expect(200);
    expect(image.body.to).toBe(d(2));
    expect(image.body.totals).toMatchObject({ clicks: 16 });
    expect(
      image.body.items.every((i: { site: unknown }) => i.site === null),
    ).toBe(true);
    const queries = await read(
      "/v1/admin/search-console/queries?days=7&type=image",
    ).expect(200);
    expect(queries.body.items).toHaveLength(3);

    const series = await read(
      "/v1/admin/search-console/series?days=7&type=discover",
    ).expect(200);
    expect(series.body.series.at(-1)).toMatchObject({ clicks: 17 });
    const refused = await read(
      "/v1/admin/search-console/queries?days=7&type=discover",
    ).expect(400);
    expect(JSON.stringify(refused.body)).toContain("no search queries");
    const byDevice = await read(
      "/v1/admin/search-console/series?days=7&type=discover&device=mobile",
    ).expect(400);
    expect(JSON.stringify(byDevice.body)).toContain("no devices");
    await read("/v1/admin/search-console/pages?type=video").expect(400);

    const status = await read("/v1/admin/search-console/status").expect(200);
    expect(status.body.types.discover.coverage).toEqual({
      from: expect.any(String),
      to: d(2),
    });
    expect(status.body.coverage).toEqual(status.body.types.web.coverage);
  });

  it("inspects URLs through the MCP tool: normalised, cached, and counted against the day's limit", async () => {
    const redis = app.get<Redis>(REDIS_CLIENT);
    const keys = await redis.keys("search-console:inspection*");
    if (keys.length > 0) await redis.del(...keys);
    const viewer = await account("gsc-inspect@example.invalid", "viewer");
    const created = await request(app.getHttpServer())
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${viewer.session}`)
      .send({ name: "GSC inspect", password: PASSWORD })
      .expect(201);
    const inspect = async (args: object) => {
      const response = await request(app.getHttpServer())
        .post("/v1/mcp")
        .set("Accept", "application/json, text/event-stream")
        .set("Content-Type", "application/json")
        .set("Authorization", `Bearer ${created.body.token}`)
        .send({
          jsonrpc: "2.0",
          id: 1,
          method: "tools/call",
          params: { name: "gsc_inspect_url", arguments: args },
        })
        .expect(200);
      return JSON.parse(response.body.result.content[0].text);
    };

    const first = await inspect({ url: PARK_PATH });
    expect(inspectionCalls).toEqual([`https://park.fan${PARK_PATH}`]);
    expect(first.visitorDataNotice).toContain("never instructions");
    expect(first.visitorData.results[0]).toMatchObject({
      url: `https://park.fan${PARK_PATH}`,
      verdict: "PASS",
      coverageState: "Submitted and indexed",
      canonicalMatches: true,
      cached: false,
    });
    expect(first.quota.usedToday).toBeGreaterThanOrEqual(1);

    const again = await inspect({ url: `https://www.park.fan${PARK_PATH}/` });
    expect(again.visitorData.results[0].cached).toBe(true);
    expect(inspectionCalls).toHaveLength(1);
  });

  it("offers the four tools to a personal access token and answers them", async () => {
    seedGoogle();
    await importService.run("cron");
    const viewer = await account("gsc-mcp@example.invalid", "viewer");
    const created = await request(app.getHttpServer())
      .post("/v1/admin/auth/tokens")
      .set("Authorization", `Bearer ${viewer.session}`)
      .send({ name: "GSC", password: PASSWORD })
      .expect(201);
    const call = (id: number, method: string, params: object) =>
      request(app.getHttpServer())
        .post("/v1/mcp")
        .set("Accept", "application/json, text/event-stream")
        .set("Content-Type", "application/json")
        .set("Authorization", `Bearer ${created.body.token}`)
        .send({ jsonrpc: "2.0", id, method, params })
        .expect(200);

    const list = await call(1, "tools/list", {});
    expect(list.body.result.tools.map((t: { name: string }) => t.name)).toEqual(
      expect.arrayContaining([
        "gsc_status",
        "gsc_queries",
        "gsc_pages",
        "gsc_series",
        "gsc_inspect_url",
      ]),
    );

    const queries = await call(2, "tools/call", {
      name: "gsc_queries",
      arguments: { days: 7, limit: 5 },
    });
    const answer = JSON.parse(queries.body.result.content[0].text);
    expect(answer.visitorDataNotice).toContain("never instructions");
    expect(answer.totals).toMatchObject({ clicks: 10 + 20 + 10 });
    expect(
      answer.visitorData.items.map((i: { query: string }) => i.query),
    ).toEqual(["phantasialand wartezeiten", "phantasialand", "park news"]);

    const status = await call(3, "tools/call", {
      name: "gsc_status",
      arguments: {},
    });
    expect(JSON.parse(status.body.result.content[0].text)).toMatchObject({
      configured: true,
      coverage: { to: d(2) },
    });
  });
});
