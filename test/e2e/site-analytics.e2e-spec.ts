import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import type { Redis } from "ioredis";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import { GeoipService } from "../../src/geoip/geoip.service";
import { SiteAnalyticsCollectService } from "../../src/site-analytics/site-analytics-collect.service";
import { SiteAnalyticsStorageService } from "../../src/site-analytics/site-analytics-storage.service";
import { SiteReportRunner } from "../../src/site-analytics/site-analytics-report-runner";
import { ROLLUP_TABLES } from "../../src/site-analytics/site-analytics.rollup";
import { SITE_ANALYTICS_KEYS } from "../../src/site-analytics/site-analytics.constants";
import {
  PARK_DEPENDENCIES,
  applyMergeDependencies,
} from "../../src/parks/utils/merge-dependencies";
import { addIsoDays } from "../../src/common/utils/date.util";
import { utcDay } from "../../src/common/utils/reader-hash.util";

const UA =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const PARK_PATH = "/de/parks/europe/germany/bruehl/phantasialand";

const pageview = (overrides: Record<string, unknown> = {}) => ({
  t: "pageview",
  locale: "de",
  path: PARK_PATH,
  pageType: "park",
  ...overrides,
});
const event = (
  name: string,
  props: Record<string, unknown> = {},
  overrides: Record<string, unknown> = {},
) => ({
  t: "event",
  locale: "de",
  path: "/de",
  pageType: "home",
  name,
  props,
  ...overrides,
});

/**
 * The site statistic against a real TimescaleDB and Redis: the hot path into
 * Redis (the guard script included), the flush into hourly rows and its
 * idempotency, rows that never tie a later page to a visit's referrer, the
 * reports' SQL with filters, empty days and the window before, the most-read
 * rankings served from `page_read`, the one-off import of `page_view_days`,
 * merges resolved when read, the guards, the snapshot, the chunk drop, and a
 * schema `synchronize` leaves alone. A unit test with a mocked repository
 * proves a statement is issued; only a database proves it runs.
 */
describe("Site analytics (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;
  let token: string;
  const today = utcDay(new Date());
  /** A moment past every window written so far, so a flush takes them all. */
  const later = () => new Date(Date.now() + 15 * 60 * 1000);

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix("v1");
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
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    const account = await app.get(AdminAuthService).create({
      email: "site-analytics-e2e@example.invalid",
      role: "editor",
      password: "a-long-enough-password",
    });
    await dataSource.query(
      `UPDATE admin_users SET must_change_password = false WHERE id = $1`,
      [account.id],
    );
    const issued = await app.get(AdminSessionStore).create({
      userId: account.id,
      email: account.email,
      displayName: account.displayName,
      role: "editor",
      ip: null,
      userAgent: "e2e",
      mustChangePassword: false,
      mustEnrolTotp: false,
    });
    token = issued.token;
  });

  const beacon = (hits: unknown[], ip = "203.0.113.7") =>
    request(app.getHttpServer())
      .post("/v1/analytics/collect")
      .set("Content-Type", "text/plain;charset=UTF-8")
      .set("User-Agent", UA)
      .set("CF-Connecting-IP", ip)
      .send(JSON.stringify({ v: 1, hits }));

  const admin = (url: string) =>
    request(app.getHttpServer())
      .get(url)
      .set("Authorization", `Bearer ${token}`);

  const flush = () => app.get(SiteAnalyticsStorageService).flush(later());

  const savePark = () =>
    dataSource.getRepository(Park).save({
      externalId: "e2e-site-analytics",
      name: "Phantasialand",
      slug: "phantasialand",
      latitude: 50.8,
      longitude: 6.88,
      timezone: "Europe/Berlin",
      continent: "Europe",
      continentSlug: "europe",
      country: "Germany",
      countrySlug: "germany",
      city: "Brühl",
      citySlug: "bruehl",
    });

  it("buffers hits in Redis, writes nothing to Postgres until the flush, and flushes each window once", async () => {
    await beacon([pageview({ referrer: "www.google.de" })]).expect(204);
    await beacon([
      pageview({ path: "/de/news", pageType: "news_index" }),
      event("search_opened", { surface: "hero" }),
    ]).expect(204);
    await beacon([pageview()], "198.51.100.4").expect(204);

    expect(await dataSource.query(`SELECT 1 FROM site_pageviews`)).toEqual([]);
    expect(await redis.smembers(SITE_ANALYTICS_KEYS.windows)).toHaveLength(1);

    await expect(flush()).resolves.toMatchObject({ windows: 1, failed: 0 });
    await expect(flush()).resolves.toMatchObject({ windows: 0 });
    expect(await redis.smembers(SITE_ANALYTICS_KEYS.windows)).toEqual([]);

    const reader = {
      device: "mobile",
      browser: "safari",
      os: "ios",
      country: "zz",
    };
    const page = { ...reader, entry: "", referrer: "", channel: "" };
    expect(
      await dataSource.query(
        `SELECT path, page_type, device, browser, os, country, entry,
                referrer, channel, views, visits, engaged
           FROM site_pageviews ORDER BY path, entry, channel`,
      ),
    ).toEqual([
      // A page view's row knows nothing of the visit it belongs to.
      {
        ...page,
        path: "/de/news",
        page_type: "news_index",
        views: 1,
        visits: 0,
        engaged: 0,
      },
      {
        ...page,
        path: PARK_PATH,
        page_type: "park",
        views: 2,
        visits: 0,
        engaged: 0,
      },
      // A visit's row: its first page, where it came from, its engaged.
      {
        ...reader,
        path: PARK_PATH,
        page_type: "park",
        entry: PARK_PATH,
        referrer: "",
        channel: "direct",
        views: 0,
        visits: 1,
        engaged: 0,
      },
      {
        ...reader,
        path: PARK_PATH,
        page_type: "park",
        entry: PARK_PATH,
        referrer: "google",
        channel: "search",
        views: 0,
        visits: 1,
        engaged: 1,
      },
    ]);
    // Every row is one UTC hour.
    expect(
      await dataSource.query(
        `SELECT count(*)::int AS off FROM site_pageviews
          WHERE bucket <> date_trunc('hour', bucket)`,
      ),
    ).toEqual([{ off: 0 }]);
    expect(
      await dataSource.query(
        `SELECT name, path, props_key, props, entity, count FROM site_events`,
      ),
    ).toEqual([
      {
        name: "search_opened",
        path: "/de",
        props_key: "surface=hero",
        props: { surface: "hero" },
        entity: null,
        count: 1,
      },
    ]);
    // An event's row has no column for the visit at all.
    const columns = await dataSource.query(
      `SELECT column_name FROM information_schema.columns
        WHERE table_name = 'site_events'`,
    );
    expect(
      columns.map((c: { column_name: string }) => c.column_name),
    ).not.toEqual(expect.arrayContaining(["entry"]));
    expect(
      await dataSource.query(
        `SELECT pageview_rows, event_rows, claim IS NOT NULL AS claimed
           FROM site_analytics_flushes`,
      ),
    ).toEqual([{ pageview_rows: 4, event_rows: 1, claimed: true }]);
  });

  it("takes the device from the User-Agent and counts a batch whose hits still carry `viewport` and `campaign`", async () => {
    const stale = { viewport: "desktop", campaign: "summer-2026" };
    const send = (userAgent: string, ip: string, hits: unknown[]) =>
      request(app.getHttpServer())
        .post("/v1/analytics/collect")
        .set("Content-Type", "text/plain;charset=UTF-8")
        .set("User-Agent", userAgent)
        .set("CF-Connecting-IP", ip)
        .send(JSON.stringify({ v: 1, hits }));
    // The global pipe rejects unknown fields on DTOs; the collect body is none.
    await send(UA, "203.0.113.21", [
      pageview({ ...stale, path: "/de/stale-a", referrer: "www.google.de" }),
      event("search_opened", { surface: "hero" }, stale),
    ]).expect(204);
    await send(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "203.0.113.22",
      [pageview({ path: "/de/stale-b" })],
    ).expect(204);
    await send(
      "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "203.0.113.23",
      [pageview({ path: "/de/stale-c", ...stale })],
    ).expect(204);
    await flush();
    expect(
      await dataSource.query(
        `SELECT path, device, channel, views, visits FROM site_pageviews
          ORDER BY path, channel`,
      ),
    ).toEqual([
      {
        path: "/de/stale-a",
        device: "mobile",
        channel: "",
        views: 1,
        visits: 0,
      },
      {
        path: "/de/stale-a",
        device: "mobile",
        channel: "search",
        views: 0,
        visits: 1,
      },
      {
        path: "/de/stale-b",
        device: "desktop",
        channel: "",
        views: 1,
        visits: 0,
      },
      {
        path: "/de/stale-b",
        device: "desktop",
        channel: "direct",
        views: 0,
        visits: 1,
      },
      {
        path: "/de/stale-c",
        device: "tablet",
        channel: "",
        views: 1,
        visits: 0,
      },
      {
        path: "/de/stale-c",
        device: "tablet",
        channel: "direct",
        views: 0,
        visits: 1,
      },
    ]);
    expect(
      await dataSource.query(`SELECT device, count FROM site_events`),
    ).toEqual([{ device: "mobile", count: 1 }]);
    const columns = await dataSource.query(
      `SELECT table_name, column_name FROM information_schema.columns
        WHERE table_name LIKE 'site\\_%'
          AND column_name IN ('viewport', 'campaign')`,
    );
    expect(columns).toEqual([]);
  });

  it("leaves the statistic's tables as synchronize finds them after the hypertables are made", async () => {
    // Every start runs synchronize; a difference here would be an ALTER or a
    // dropped index on a large hypertable at every deploy.
    const pending = await dataSource.driver.createSchemaBuilder().log();
    expect(
      pending.upQueries
        .map((q) => q.query)
        .filter((sql) =>
          /site_(pageviews|events|analytics_flushes|rollup_days|visitor_hours|visitor_dim_days)/.test(
            sql,
          ),
        ),
    ).toEqual([]);
    // And the bucket indexes create_hypertable made are still there, on the
    // daily tables too.
    const indexes = await dataSource.query(
      `SELECT indexname FROM pg_indexes
        WHERE tablename IN ('site_pageviews', 'site_events',
                            'site_pageviews_daily', 'site_events_daily')`,
    );
    expect(indexes.map((i: { indexname: string }) => i.indexname)).toEqual(
      expect.arrayContaining([
        "site_pageviews_bucket_idx",
        "site_events_bucket_idx",
        "site_pageviews_daily_bucket_idx",
        "site_events_daily_bucket_idx",
        "idx_site_pageviews_daily_cuboid_bucket",
        "idx_site_events_daily_cuboid_bucket",
      ]),
    );
    // Hypertables with the compression the daily tables are meant to have.
    const settings = await dataSource.query(
      `SELECT hypertable_name, compression_enabled
         FROM timescaledb_information.hypertables
        WHERE hypertable_name IN ('site_pageviews_daily', 'site_events_daily')
        ORDER BY 1`,
    );
    expect(settings).toEqual([
      { hypertable_name: "site_events_daily", compression_enabled: true },
      { hypertable_name: "site_pageviews_daily", compression_enabled: true },
    ]);
  });

  it("answers 400 for a malformed body and records nothing for a bot", async () => {
    await request(app.getHttpServer())
      .post("/v1/analytics/collect")
      .set("Content-Type", "text/plain")
      .send("{")
      .expect(400);
    await request(app.getHttpServer())
      .post("/v1/analytics/collect")
      .set("Content-Type", "text/plain")
      .set("User-Agent", "Googlebot/2.1")
      .send(JSON.stringify({ v: 1, hits: [pageview()] }))
      .expect(204);
    expect(await redis.smembers(SITE_ANALYTICS_KEYS.windows)).toEqual([]);
  });

  it("reports traffic with filters, bounce, empty days and the window before", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    // Just after midnight, so the window before a one-day window, which ends
    // at the current hour, holds these hits whenever the test runs.
    const yesterday = new Date(`${addIsoDays(today, -1)}T00:10:00Z`);
    const lastWeek = new Date(`${addIsoDays(today, -9)}T00:10:00Z`);
    const visit = async (ip: string, hits: unknown[][], at: Date) => {
      for (const [i, batch] of hits.entries()) {
        await collector.collect(
          { hits: batch, ip, userAgent: UA },
          new Date(at.getTime() + i * 60_000),
        );
      }
    };
    // Yesterday: a search visit of two pages, a bounced one from Bing, a
    // direct visit in Dutch that met a 404.
    await visit(
      "203.0.113.1",
      [
        [pageview({ referrer: "google.com" })],
        [pageview({ path: "/de/news", pageType: "news_index" })],
      ],
      yesterday,
    );
    await visit(
      "203.0.113.2",
      [[pageview({ referrer: "bing.com" })]],
      yesterday,
    );
    await visit(
      "203.0.113.3",
      [
        [
          pageview({
            locale: "nl",
            path: "/nl/gone",
            pageType: "not_found",
          }),
        ],
      ],
      yesterday,
    );
    // Nine days ago: one search visit.
    await visit(
      "203.0.113.4",
      [[pageview({ referrer: "google.com" })]],
      lastWeek,
    );
    await flush();

    const overview = await admin("/v1/admin/analytics/overview?days=7").expect(
      200,
    );
    expect(overview.body).toMatchObject({
      to: today,
      days: 7,
      locale: null,
      filters: [],
      totals: { pageviews: 4, visits: 3, events: 0, bounceRate: 0.6667 },
      previous: { pageviews: 1, visits: 1, bounceRate: 1 },
    });
    expect(overview.body.totals.viewsPerVisit).toBeCloseTo(1.33, 2);
    expect(overview.body.series).toHaveLength(7);
    expect(overview.body.series[5]).toMatchObject({
      day: addIsoDays(today, -1),
      pageviews: 4,
      visits: 3,
    });
    expect(overview.body.series[6]).toMatchObject({
      day: today,
      pageviews: 0,
      visits: 0,
    });

    // A visit filter: visits and bounce follow it, page views cannot. The two
    // readers whose visits began on search are counted from the HyperLogLog
    // of yesterday's channel.
    const search = await admin(
      "/v1/admin/analytics/overview?days=7&filter=channel:search",
    ).expect(200);
    expect(search.body.totals).toEqual({
      pageviews: null,
      visits: 2,
      visitors: 2,
      events: null,
      viewsPerVisit: null,
      bounceRate: 0.5,
      // No engagement was sent: the visits were timed at nothing.
      avgVisitSeconds: 0,
      avgTimeOnPageSeconds: null,
    });
    expect(search.body.filters).toEqual(["channel:search"]);

    const channels = await admin(
      "/v1/admin/analytics/breakdown?dim=channel&days=7",
    ).expect(200);
    expect(channels.body).toMatchObject({
      dim: "channel",
      measure: "visits",
      total: { views: 3, visits: 3 },
      items: [
        {
          value: "search",
          views: 2,
          visits: 2,
          share: 0.6667,
          bounceRate: 0.5,
        },
        {
          value: "direct",
          views: 1,
          visits: 1,
          share: 0.3333,
          bounceRate: 1,
        },
      ],
    });
    const referrers = await admin(
      "/v1/admin/analytics/breakdown?dim=referrer&days=7",
    ).expect(200);
    expect(referrers.body.items.map((i: { value: string }) => i.value)).toEqual(
      ["bing", "google"],
    );

    const pages = await admin(
      "/v1/admin/analytics/breakdown?dim=path&days=7",
    ).expect(200);
    expect(pages.body).toMatchObject({
      measure: "views",
      items: [
        { value: PARK_PATH, views: 2, visits: 2, share: 0.5 },
        { value: "/de/news", views: 1, visits: 0, share: 0.25 },
        { value: "/nl/gone", views: 1, visits: 1, share: 0.25 },
      ],
    });
    // Under a visit filter the pages are where those visits entered: the
    // news page the search visit read next is not tied to Google.
    const entries = await admin(
      "/v1/admin/analytics/breakdown?dim=path&days=7&filter=channel:search",
    ).expect(200);
    expect(entries.body).toMatchObject({
      measure: "visits",
      items: [
        { value: PARK_PATH, views: 2, visits: 2, share: 1, bounceRate: 0.5 },
      ],
    });

    const landing = await admin(
      "/v1/admin/analytics/seo/landing-pages?days=7",
    ).expect(200);
    expect(landing.body.items).toEqual([
      {
        path: PARK_PATH,
        pageType: "park",
        visits: 2,
        previous: 1,
        change: 1,
        bounceRate: 0.5,
        avgVisitSeconds: 0,
        sources: [
          { value: "bing", visits: 1 },
          { value: "google", visits: 1 },
        ],
      },
    ]);

    const missing = await admin(
      "/v1/admin/analytics/seo/not-found?days=7",
    ).expect(200);
    expect(missing.body.items).toEqual([
      { path: "/nl/gone", views: 1, referrers: [] },
    ]);
    await admin(
      "/v1/admin/analytics/seo/not-found?days=7&filter=channel:search",
    ).expect(400);
    await admin("/v1/admin/analytics/events?days=7&filter=entry:/de").expect(
      400,
    );

    const hourly = await admin("/v1/admin/analytics/overview?days=1").expect(
      200,
    );
    expect(hourly.body.series[0].day).toBe(`${today}T00:00:00Z`);
    expect(hourly.body.previous).toMatchObject({ pageviews: 4, visits: 3 });

    // A year has no window before: most of it would lie past the retention.
    const year = await admin("/v1/admin/analytics/overview?days=365").expect(
      200,
    );
    expect(year.body.previous).toBeNull();
    expect(year.body.totals).toMatchObject({ pageviews: 5, visits: 4 });
    const yearLanding = await admin(
      "/v1/admin/analytics/seo/landing-pages?days=365",
    ).expect(200);
    expect(yearLanding.body.items[0]).toMatchObject({
      visits: 3,
      previous: null,
      change: null,
    });
    await admin("/v1/admin/analytics/seo/trends?days=181").expect(400);
  });

  it("names a referrer for a 404 only when a visit entered on it, never for a broken link inside the site", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    const at = new Date(`${addIsoDays(today, -1)}T00:20:00Z`);
    const gone = (path: string) => pageview({ path, pageType: "not_found" });
    // A visit from a newsletter's site that then follows a broken link inside the site.
    await collector.collect(
      {
        hits: [pageview({ referrer: "newsletter.example.org" })],
        ip: "203.0.113.50",
        userAgent: UA,
      },
      at,
    );
    await collector.collect(
      { hits: [gone("/de/broken-inside")], ip: "203.0.113.50", userAgent: UA },
      new Date(at.getTime() + 60_000),
    );
    // A Google result that points at a page that is gone.
    await collector.collect(
      {
        hits: [gone("/de/gone-from-google")],
        ip: "203.0.113.51",
        userAgent: UA,
      },
      at,
    );
    await flush();
    const missing = await admin(
      "/v1/admin/analytics/seo/not-found?days=7",
    ).expect(200);
    expect(missing.body.items).toEqual([
      { path: "/de/broken-inside", views: 1, referrers: [] },
      { path: "/de/gone-from-google", views: 1, referrers: [] },
    ]);
    await collector.collect(
      {
        hits: [{ ...gone("/de/gone-from-google"), referrer: "www.google.de" }],
        ip: "203.0.113.52",
        userAgent: UA,
      },
      at,
    );
    await flush();
    const again = await admin(
      "/v1/admin/analytics/seo/not-found?days=7",
    ).expect(200);
    expect(again.body.items).toEqual([
      {
        path: "/de/gone-from-google",
        views: 2,
        referrers: [{ value: "google", visits: 1 }],
      },
      { path: "/de/broken-inside", views: 1, referrers: [] },
    ]);
  });

  it("reports events generically: names seen, props discovered, crosstab, search, block list", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    const at = new Date(`${addIsoDays(today, -1)}T08:00:00Z`);
    const park = await savePark();
    await collector.collect(
      {
        hits: [
          pageview(),
          event("search_opened", { surface: "hero" }),
          event("search_opened", { surface: "hero" }),
          event("search_opened", { surface: "header" }, { pageType: "park" }),
          event("search_typed", { surface: "hero" }),
          event("search_result_clicked", {
            surface: "hero",
            result: "park",
            position: "1",
          }),
          event("tab_changed", { tab: "map" }, { entity: park.id }),
          event("brand_new_event", { flag: true, size: 3 }),
        ],
        ip: "203.0.113.9",
        userAgent: UA,
      },
      at,
    );
    await flush();

    const events = await admin("/v1/admin/analytics/events?days=7").expect(200);
    expect(
      events.body.items.map((i: { name: string; count: number }) => [
        i.name,
        i.count,
      ]),
    ).toEqual([
      ["search_opened", 3],
      ["brand_new_event", 1],
      ["search_result_clicked", 1],
      ["search_typed", 1],
      ["tab_changed", 1],
    ]);

    const detail = await admin(
      "/v1/admin/analytics/events/search_opened?days=7",
    ).expect(200);
    expect(detail.body).toMatchObject({
      name: "search_opened",
      total: 3,
      blocked: false,
      props: [
        {
          prop: "surface",
          values: [
            { value: "hero", count: 2 },
            { value: "header", count: 1 },
          ],
          otherCount: 0,
        },
      ],
      combinations: [
        { props: { surface: "hero" }, count: 2 },
        { props: { surface: "header" }, count: 1 },
      ],
      byPageType: [
        { value: "home", count: 2 },
        { value: "park", count: 1 },
      ],
    });
    expect(detail.body.series).toHaveLength(7);

    const custom = await admin(
      "/v1/admin/analytics/events/brand_new_event?days=7",
    ).expect(200);
    expect(custom.body.combinations).toEqual([
      { props: { flag: "true", size: "3" }, count: 1 },
    ]);

    const tabs = await admin(
      "/v1/admin/analytics/events/tab_changed?days=7",
    ).expect(200);
    expect(tabs.body.entities).toEqual([
      {
        id: park.id,
        kind: "park",
        name: "Phantasialand",
        url: "/parks/europe/germany/bruehl/phantasialand",
        count: 1,
      },
    ]);

    const crosstab = await admin(
      "/v1/admin/analytics/events/search_opened/crosstab?days=7&row=prop:surface&col=page_type",
    ).expect(200);
    expect(crosstab.body).toMatchObject({
      rows: ["hero", "header"],
      cols: ["home", "park"],
      total: 3,
    });
    expect(crosstab.body.cells).toEqual(
      expect.arrayContaining([
        { row: "hero", col: "home", count: 2 },
        { row: "header", col: "park", count: 1 },
      ]),
    );

    const search = await admin("/v1/admin/analytics/search?days=7").expect(200);
    expect(search.body.surfaces[0]).toEqual({
      surface: "hero",
      opened: 2,
      typed: 1,
      resultClicks: 1,
      browseClicks: 0,
      noResults: 0,
      viewAll: 0,
      clickRate: 0.5,
      noResultRate: 0,
    });
    expect(search.body.positions[0]).toEqual({ position: "1", clicks: 1 });
    expect(search.body.series).toHaveLength(7 * 2);

    await request(app.getHttpServer())
      .put("/v1/admin/analytics/blocked-events/brand_new_event")
      .set("Authorization", `Bearer ${token}`)
      .expect(204);
    const blocked = await admin("/v1/admin/analytics/blocked-events").expect(
      200,
    );
    expect(blocked.body.items).toEqual([
      expect.objectContaining({
        name: "brand_new_event",
        blockedBy: "site-analytics-e2e@example.invalid",
      }),
    ]);
    await expect(
      collector.collect({
        hits: [event("brand_new_event")],
        ip: "203.0.113.9",
        userAgent: UA,
      }),
    ).resolves.toBe(0);
    const listed = await admin("/v1/admin/analytics/events?days=7").expect(200);
    expect(
      listed.body.items.find(
        (i: { name: string }) => i.name === "brand_new_event",
      ),
    ).toMatchObject({ blocked: true, count: 1 });
    await request(app.getHttpServer())
      .delete("/v1/admin/analytics/blocked-events/brand_new_event")
      .set("Authorization", `Bearer ${token}`)
      .expect(204);
  });

  it("serves the most-read rankings from page_read, with the old table copied once and merges resolved", async () => {
    const park = await savePark();
    const ghost = "22222222-2222-4222-8222-222222222222";
    const gone = "33333333-3333-4333-8333-333333333333";
    const day = addIsoDays(today, -2);
    await dataSource.query(
      `INSERT INTO page_view_days (kind, key, locale, day, views) VALUES
         ('news', 'old-news', 'de', $1, 7),
         ('park', $2, 'de', $1, 3),
         ('park', $3, 'de', $1, 50),
         ('park', 'junk', 'de', $1, 99)`,
      [day, ghost, gone],
    );
    const storage = app.get(SiteAnalyticsStorageService);
    await expect(storage.importPageViewDays()).resolves.toBe(3);
    await expect(storage.importPageViewDays()).resolves.toBe(0);

    // A read through the old endpoint and two through the beacon.
    await request(app.getHttpServer())
      .post("/v1/page-views")
      .set("User-Agent", UA)
      .set("CF-Connecting-IP", "203.0.113.20")
      .send({ kind: "news", key: "old-news", locale: "de" })
      .expect(204);
    await beacon([
      event("page_read", { kind: "park" }, { entity: park.id }),
      event("page_read", { kind: "news", key: "fresh-news" }),
      // A park that does not exist is not counted at all.
      event("page_read", { kind: "park" }, { entity: gone }),
    ]).expect(204);
    // Three reads per reader, page and day, as before.
    for (let i = 0; i < 4; i++) {
      await beacon(
        [event("page_read", { kind: "news", key: "capped" })],
        "198.51.100.30",
      ).expect(204);
    }
    await flush();
    expect(
      await dataSource.query(
        `SELECT SUM(count)::int AS reads FROM site_events WHERE props->>'key' = 'capped'`,
      ),
    ).toEqual([{ reads: 3 }]);

    // The ghost park was merged into the real one: its reads count there.
    await dataSource.transaction((manager) =>
      applyMergeDependencies(
        manager,
        PARK_DEPENDENCIES.filter((d) => d.table === "site_entity_merges"),
        park.id,
        ghost,
      ),
    );

    const news = await request(app.getHttpServer())
      .get("/v1/page-views/top?kind=news&locale=de&days=7")
      .expect(200);
    expect(news.body.items).toEqual([
      { key: "old-news", views: 8 },
      { key: "capped", views: 3 },
      { key: "fresh-news", views: 1 },
    ]);
    const parks = await request(app.getHttpServer())
      .get("/v1/page-views/top?kind=park&locale=de&days=7")
      .expect(200);
    // The deleted park's copied reads stay off the public list.
    expect(parks.body.items).toEqual([{ key: park.id, views: 4 }]);
    expect(
      await dataSource.query(
        `SELECT SUM(count)::int AS reads FROM site_events WHERE entity = $1::uuid`,
        [gone],
      ),
    ).toEqual([{ reads: 50 }]);

    const overview = await admin("/v1/admin/page-views?days=7").expect(200);
    const parkKind = overview.body.kinds.find(
      (k: { kind: string }) => k.kind === "park",
    );
    // The admin sees every counted read, the deleted park's unnamed.
    expect(parkKind).toMatchObject({ total: 54, byLocale: { de: 54 } });
    expect(parkKind.top[0]).toMatchObject({ key: gone, total: 50 });
    expect(parkKind.top[0].name).toBeUndefined();
    expect(parkKind.top[1]).toMatchObject({
      key: park.id,
      name: "Phantasialand",
    });
  });

  it("caps distinct names per day, admits yesterday's, and folds a flood of property combinations into _overflow", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    const day = utcDay(new Date());
    await redis.sadd(
      SITE_ANALYTICS_KEYS.guard("names", day),
      ...Array.from({ length: 300 }, (_, i) => `name_${i}`),
    );
    await redis.sadd(
      SITE_ANALYTICS_KEYS.guard("names", addIsoDays(day, -1)),
      "known_event",
    );
    await redis.sadd(
      SITE_ANALYTICS_KEYS.guard("combinations", day, "name_1"),
      ...Array.from({ length: 2000 }, (_, i) => `combo-${i}`),
    );
    await expect(
      collector.collect({
        hits: [
          event("one_too_many"),
          event("name_1", { value: "new" }),
          event("known_event"),
        ],
        ip: "203.0.113.40",
        userAgent: UA,
      }),
    ).resolves.toBe(2);
    expect(
      await redis.sismember(
        SITE_ANALYTICS_KEYS.guard("names", day),
        "one_too_many",
      ),
    ).toBe(0);
    expect(await redis.hgetall(SITE_ANALYTICS_KEYS.drops(day))).toMatchObject({
      "dropped:name": "1",
      "overflow:combination": "1",
    });
    await flush();
    expect(
      await dataSource.query(
        `SELECT name, props_key, props FROM site_events ORDER BY name`,
      ),
    ).toEqual([
      { name: "known_event", props_key: "", props: {} },
      { name: "name_1", props_key: "_overflow=1", props: { _overflow: "1" } },
    ]);
  });

  it("times pages and visits, ends idle visits with an exit, and serves ranges, intervals, the traffic crosstab and the live view", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    const yesterday = addIsoDays(today, -1);
    const at = new Date(`${yesterday}T10:00:00Z`);
    const minutes = (n: number) => new Date(at.getTime() + n * 60_000);
    const engagement = (seconds: number, path = PARK_PATH) => ({
      t: "engagement",
      locale: "de",
      path,
      pageType: path === PARK_PATH ? "park" : "news_index",
      seconds,
    });
    const reader = {
      ip: "203.0.113.70",
      userAgent: UA,
      acceptLanguage: "nl-BE,nl;q=0.9,en;q=0.5",
    };
    // A search visit of two pages, 30 and 20 seconds in the foreground.
    await collector.collect(
      { ...reader, hits: [pageview({ referrer: "www.google.de" })] },
      at,
    );
    await collector.collect({ ...reader, hits: [engagement(30)] }, minutes(1));
    await collector.collect(
      {
        ...reader,
        hits: [pageview({ path: "/de/news", pageType: "news_index" })],
      },
      minutes(2),
    );
    await collector.collect(
      { ...reader, hits: [engagement(20, "/de/news")] },
      minutes(3),
    );
    // A direct bounce, 10 seconds, from a browser that sent no language.
    await collector.collect(
      { ip: "203.0.113.71", userAgent: UA, hits: [pageview()] },
      at,
    );
    await collector.collect(
      { ip: "203.0.113.71", userAgent: UA, hits: [engagement(10)] },
      minutes(1),
    );
    expect(await redis.zcard(SITE_ANALYTICS_KEYS.openVisits)).toBe(2);

    // Both visits are idle by now: the flush ends them first, then writes.
    await expect(flush()).resolves.toMatchObject({
      visitsClosed: 2,
      failed: 0,
    });
    expect(await redis.zcard(SITE_ANALYTICS_KEYS.openVisits)).toBe(0);
    expect(await redis.keys("site-analytics:visit:*")).toEqual([]);
    // A page view's row has its seconds, a visit's row the visit's, an exit
    // row the visit's span from first to last hit. A visit that ended where
    // it began counts its exit on its own row.
    expect(
      await dataSource.query(
        `SELECT path, entry, channel, browser_language,
                to_char(MIN(bucket), 'HH24') AS hour,
                SUM(views)::int AS views, SUM(seconds)::int AS seconds,
                SUM(visits)::int AS visits,
                SUM(visit_seconds)::int AS visit_seconds,
                SUM(exits)::int AS exits,
                SUM(visit_span_seconds)::int AS span
           FROM site_pageviews
          GROUP BY 1, 2, 3, 4 ORDER BY 1, 3`,
      ),
    ).toEqual([
      {
        path: "/de/news",
        entry: "",
        channel: "",
        browser_language: "nl",
        hour: "10",
        views: 1,
        seconds: 20,
        visits: 0,
        visit_seconds: 0,
        exits: 0,
        span: 0,
      },
      {
        path: "/de/news",
        entry: PARK_PATH,
        channel: "search",
        browser_language: "nl",
        hour: "10",
        views: 0,
        seconds: 0,
        visits: 0,
        visit_seconds: 0,
        exits: 1,
        span: 180,
      },
      {
        path: PARK_PATH,
        entry: "",
        channel: "",
        browser_language: "nl",
        hour: "10",
        views: 1,
        seconds: 30,
        visits: 0,
        visit_seconds: 0,
        exits: 0,
        span: 0,
      },
      {
        path: PARK_PATH,
        entry: "",
        channel: "",
        browser_language: "zz",
        hour: "10",
        views: 1,
        seconds: 10,
        visits: 0,
        visit_seconds: 0,
        exits: 0,
        span: 0,
      },
      {
        path: PARK_PATH,
        entry: PARK_PATH,
        channel: "direct",
        browser_language: "zz",
        hour: "10",
        views: 0,
        seconds: 0,
        visits: 1,
        visit_seconds: 10,
        exits: 1,
        span: 60,
      },
      {
        path: PARK_PATH,
        entry: PARK_PATH,
        channel: "search",
        browser_language: "nl",
        hour: "10",
        views: 0,
        seconds: 0,
        visits: 1,
        visit_seconds: 50,
        exits: 0,
        span: 0,
      },
    ]);

    const window = `from=${yesterday}&to=${yesterday}`;
    const overview = await admin(
      `/v1/admin/analytics/overview?${window}`,
    ).expect(200);
    expect(overview.body).toMatchObject({
      from: yesterday,
      to: yesterday,
      days: 1,
      interval: "hour",
      totals: {
        pageviews: 3,
        visits: 2,
        avgVisitSeconds: 30,
        avgTimeOnPageSeconds: 20,
      },
    });
    expect(overview.body.series).toHaveLength(24);
    expect(overview.body.series[10]).toMatchObject({
      day: `${yesterday}T10:00:00Z`,
      pageviews: 3,
      visits: 2,
      // Two readers, from the hour's HyperLogLog, which lives until the
      // snapshot has written yesterday's hours.
      visitors: 2,
      avgVisitSeconds: 30,
    });
    expect(overview.body.series[10]).not.toHaveProperty("partial");
    await admin(`/v1/admin/analytics/overview?${window}&days=7`).expect(400);
    await admin(`/v1/admin/analytics/overview?days=7&interval=hour`).expect(
      400,
    );
    const weeks = await admin(
      "/v1/admin/analytics/overview?days=28&interval=week",
    ).expect(200);
    expect(weeks.body.interval).toBe("week");
    expect(weeks.body.series[weeks.body.series.length - 1].partial).toBe(true);

    const pages = await admin(
      `/v1/admin/analytics/breakdown?dim=path&${window}`,
    ).expect(200);
    expect(pages.body.items).toEqual([
      {
        value: PARK_PATH,
        views: 2,
        visits: 2,
        share: 0.6667,
        avgTimeOnPageSeconds: 20,
        exits: 1,
        exitRate: 0.5,
      },
      {
        value: "/de/news",
        views: 1,
        visits: 0,
        share: 0.3333,
        avgTimeOnPageSeconds: 20,
        exits: 1,
        exitRate: 1,
      },
    ]);
    const exits = await admin(
      `/v1/admin/analytics/breakdown?dim=exit&${window}&filter=channel:search`,
    ).expect(200);
    expect(exits.body).toMatchObject({
      measure: "exits",
      total: { views: 1, visits: 1 },
      items: [{ value: "/de/news", views: 1, visits: 1, share: 1 }],
    });
    const languages = await admin(
      `/v1/admin/analytics/breakdown?dim=browser_language&${window}`,
    ).expect(200);
    expect(
      languages.body.items.map((i: { value: string; views: number }) => [
        i.value,
        i.views,
      ]),
    ).toEqual([
      ["nl", 2],
      ["zz", 1],
    ]);
    const channels = await admin(
      `/v1/admin/analytics/breakdown?dim=channel&${window}`,
    ).expect(200);
    expect(channels.body.items).toEqual([
      expect.objectContaining({ value: "direct", avgVisitSeconds: 10 }),
      expect.objectContaining({ value: "search", avgVisitSeconds: 50 }),
    ]);

    const ended = await admin(
      `/v1/admin/analytics/crosstab?${window}&metric=visits&row=channel&col=exit`,
    ).expect(200);
    expect(ended.body).toMatchObject({
      metric: "visits",
      row: "channel",
      col: "exit",
      total: 2,
    });
    expect(ended.body.cells).toEqual(
      expect.arrayContaining([
        { row: "search", col: "/de/news", count: 1 },
        { row: "direct", col: PARK_PATH, count: 1 },
      ]),
    );
    const devices = await admin(
      `/v1/admin/analytics/crosstab?${window}&row=browser_language&col=device`,
    ).expect(200);
    expect(devices.body.cells).toEqual(
      expect.arrayContaining([
        { row: "nl", col: "mobile", count: 2 },
        { row: "zz", col: "mobile", count: 1 },
      ]),
    );
    await admin(
      `/v1/admin/analytics/crosstab?${window}&row=os&col=device&filter=channel:search`,
    ).expect(400);

    // The live view reads what came in just now, from Redis only.
    await request(app.getHttpServer())
      .post("/v1/analytics/collect")
      .set("Content-Type", "text/plain;charset=UTF-8")
      .set("User-Agent", UA)
      .set("CF-Connecting-IP", "203.0.113.72")
      .set("Accept-Language", "fr-FR")
      .send(
        JSON.stringify({
          v: 1,
          hits: [
            pageview({ referrer: "www.bing.com" }),
            event("search_opened", { surface: "hero" }),
            engagement(5),
          ],
        }),
      )
      .expect(204);
    const live = await admin("/v1/admin/analytics/live").expect(200);
    expect(live.headers["cache-control"]).toContain("no-store");
    expect(live.body).toMatchObject({
      activeVisitors: 1,
      pages: [{ path: PARK_PATH, pageType: "park", views: 1 }],
      countries: [{ country: expect.any(String), views: 1 }],
      channels: [{ channel: "search", visits: 1 }],
      referrers: [{ referrer: "bing", visits: 1 }],
      devices: [{ device: "mobile", views: 1 }],
      events: [{ name: "search_opened", count: 1 }],
    });
    expect(live.body.perMinute).toHaveLength(31);
    // The beacon's minute is the current one, or the one before if the
    // minute turned in between.
    const recent = live.body.perMinute.slice(-2) as Array<
      Record<string, number>
    >;
    for (const field of ["pageviews", "events"]) {
      expect(recent.reduce((sum, m) => sum + m[field], 0)).toBe(1);
    }
    const nl = await admin("/v1/admin/analytics/live?locale=nl").expect(200);
    expect(nl.body).toMatchObject({ activeVisitors: 0, pages: [] });
  });

  it("shows the flush backlog and the day's drops in the health report", async () => {
    await beacon([pageview()]).expect(204);
    const backlog = await app.get(SiteAnalyticsStorageService).backlog();
    expect(backlog).toMatchObject({
      pendingWindows: 1,
      readyWindows: 0,
      parkedWindows: 0,
      healthy: true,
    });
  });

  it("writes yesterday's visitors from Redis once, and reads today's live", async () => {
    const collector = app.get(SiteAnalyticsCollectService);
    const yesterdayNoon = new Date(`${addIsoDays(today, -1)}T12:00:00Z`);
    for (const ip of ["203.0.113.1", "203.0.113.2", "203.0.113.2"]) {
      await collector.collect(
        { hits: [pageview()], ip, userAgent: UA },
        yesterdayNoon,
      );
    }
    await collector.collect({
      hits: [pageview({ locale: "en", path: "/en" })],
      ip: "203.0.113.3",
      userAgent: UA,
    });
    const storage = app.get(SiteAnalyticsStorageService);
    await expect(storage.snapshotVisitors()).resolves.toEqual({
      found: 2,
      written: 2,
      hoursWritten: 168,
    });
    await expect(storage.snapshotVisitors()).resolves.toEqual({
      found: 2,
      written: 0,
      hoursWritten: 0,
    });
    const overview = await admin("/v1/admin/analytics/overview?days=2").expect(
      200,
    );
    expect(
      overview.body.series.map((d: { visitors: number }) => d.visitors),
    ).toEqual([2, 1]);
    expect(overview.body.realtime.activeVisitors).toBe(1);

    // The hours of yesterday, which has no HyperLogLog any more once the
    // snapshot has run: noon had two readers, the rest of the day none.
    await redis.del(...(await redis.keys("site-analytics:uv*")));
    const yesterday = addIsoDays(today, -1);
    const hourly = await admin(
      `/v1/admin/analytics/overview?from=${yesterday}&to=${yesterday}&interval=hour`,
    ).expect(200);
    expect(hourly.body.series).toHaveLength(24);
    expect(
      hourly.body.series.map((h: { visitors: number | null }) => h.visitors),
    ).toEqual(Array.from({ length: 24 }, (_, h) => (h === 12 ? 2 : 0)));
    // Another language has its own hours.
    const english = await admin(
      `/v1/admin/analytics/overview?from=${yesterday}&to=${yesterday}&interval=hour&locale=en`,
    ).expect(200);
    expect(
      english.body.series.every((h: { visitors: number }) => h.visitors === 0),
    ).toBe(true);
    // A day from before the snapshot kept hours stays unknown.
    const before = addIsoDays(today, -5);
    const unknown = await admin(
      `/v1/admin/analytics/overview?from=${before}&to=${before}&interval=hour`,
    ).expect(200);
    expect(
      unknown.body.series.every(
        (h: { visitors: number | null }) => h.visitors === null,
      ),
    ).toBe(true);
  });

  it("gives visitors under a country filter for a finished day, from the snapshot, and for today, from Redis", async () => {
    // Two readers from Germany and one from France on each day; one German
    // reader is the same on both days, and counts on both.
    const countries = new Map([
      ["203.0.113.1", "de"],
      ["203.0.113.2", "de"],
      ["203.0.113.3", "de"],
      ["203.0.113.50", "fr"],
    ]);
    const lookup = jest
      .spyOn(app.get(GeoipService, { strict: false }), "lookupCountry")
      .mockImplementation((ip) => countries.get(ip) ?? null);
    try {
      const collector = app.get(SiteAnalyticsCollectService);
      const yesterday = addIsoDays(today, -1);
      const yesterdayNoon = new Date(`${yesterday}T12:00:00Z`);
      for (const ip of ["203.0.113.1", "203.0.113.2", "203.0.113.2"]) {
        await collector.collect(
          { hits: [pageview()], ip, userAgent: UA },
          yesterdayNoon,
        );
      }
      await collector.collect(
        { hits: [pageview()], ip: "203.0.113.50", userAgent: UA },
        yesterdayNoon,
      );
      for (const ip of ["203.0.113.1", "203.0.113.3", "203.0.113.50"]) {
        await collector.collect({ hits: [pageview()], ip, userAgent: UA });
      }

      const storage = app.get(SiteAnalyticsStorageService);
      await storage.snapshotVisitors();
      // Country, device, browser, OS and channel, each for all languages and German.
      await expect(storage.snapshotVisitorDims()).resolves.toEqual({
        found: 12,
        written: 12,
      });
      await expect(storage.snapshotVisitorDims()).resolves.toEqual({
        found: 12,
        written: 0,
      });
      expect(
        await dataSource.query(
          `SELECT scope, dim, value, visitors FROM site_visitor_dim_days
            WHERE dim = 'country' ORDER BY scope, value`,
        ),
      ).toEqual([
        { scope: "all", dim: "country", value: "de", visitors: 2 },
        { scope: "all", dim: "country", value: "fr", visitors: 1 },
        { scope: "de", dim: "country", value: "de", visitors: 2 },
        { scope: "de", dim: "country", value: "fr", visitors: 1 },
      ]);
      // Yesterday now comes from the table alone; today's keys stay.
      await redis.del(
        ...(await redis.keys(`site-analytics:uvd*${yesterday}*`)),
      );

      const visitorsOf = async (query: string) => {
        const { body } = await admin(
          `/v1/admin/analytics/overview?days=2&${query}`,
        ).expect(200);
        return [
          body.totals.visitors,
          body.previous?.visitors,
          body.series.map((p: { visitors: number | null }) => p.visitors),
        ];
      };
      expect(await visitorsOf("filter=country:de")).toEqual([4, 0, [2, 2]]);
      expect(await visitorsOf("filter=country:fr")).toEqual([2, 0, [1, 1]]);
      expect(await visitorsOf("filter=country:nl")).toEqual([0, 0, [0, 0]]);
      expect(await visitorsOf("filter=device:mobile&locale=de")).toEqual([
        6,
        0,
        [3, 3],
      ]);
      expect(await visitorsOf("filter=channel:direct")).toEqual([6, 0, [3, 3]]);
      expect(await visitorsOf("")).toEqual([6, 0, [3, 3]]);
      // No count for two filters, or for two values of one.
      expect(
        await visitorsOf("filter=country:de&filter=device:mobile"),
      ).toEqual([null, null, [null, null]]);
      expect(await visitorsOf("filter=country:de&filter=country:fr")).toEqual([
        null,
        null,
        [null, null],
      ]);

      // The hours have no count under a filter, the day's total does.
      const hourly = await admin(
        `/v1/admin/analytics/overview?days=1&interval=hour&filter=country:de`,
      ).expect(200);
      expect(hourly.body.totals.visitors).toBe(2);
      expect(
        hourly.body.series.every(
          (h: { visitors: number | null }) => h.visitors === null,
        ),
      ).toBe(true);

      // A breakdown by country lists each country's visitors.
      await flush();
      const byCountry = await admin(
        "/v1/admin/analytics/breakdown?dim=country&days=2",
      ).expect(200);
      expect(
        byCountry.body.items.map((i: { value: string; visitors: number }) => [
          i.value,
          i.visitors,
        ]),
      ).toEqual(
        expect.arrayContaining([
          ["de", 4],
          ["fr", 2],
        ]),
      );
    } finally {
      lookup.mockRestore();
    }
  });

  describe("the daily level", () => {
    const ANDROID =
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36";
    const WINDOWS =
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0";
    const IPAD =
      "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1";
    const ENTITY = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
    const BYPASS = "daily-level-e2e";

    // 1,700 requests from one address would meet the origin's rate limit.
    beforeAll(() => {
      process.env.THROTTLE_BYPASS_KEYS = BYPASS;
    });
    afterAll(() => {
      delete process.env.THROTTLE_BYPASS_KEYS;
    });

    /** Twelve past days of varied traffic, one of them empty, flushed. */
    const seedDays = async () => {
      const collector = app.get(SiteAnalyticsCollectService);
      const readers = [
        { ip: "203.0.113.11", ua: UA },
        { ip: "203.0.113.12", ua: ANDROID },
        { ip: "203.0.113.13", ua: WINDOWS },
        { ip: "203.0.113.14", ua: IPAD },
      ];
      for (let back = 12; back >= 1; back--) {
        if (back === 7) continue;
        const day = addIsoDays(today, -back);
        for (const [i, reader] of readers.entries()) {
          const at = (minute: number) =>
            new Date(
              `${day}T${String(8 + i).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00Z`,
            );
          const entry = [
            pageview({
              referrer: ["google.com", "bing.com", "chatgpt.com", undefined][i],
            }),
            pageview({
              locale: "en",
              path: `/en/parks/europe/germany/bruehl/park-${back}`,
              pageType: "park",
              referrer: "www.google.com",
            }),
            pageview({
              locale: "nl",
              path: `/nl/gone-${i}`,
              pageType: "not_found",
            }),
            pageview({ path: "/de", pageType: "home" }),
          ][i];
          await collector.collect(
            { hits: [entry], ip: reader.ip, userAgent: reader.ua },
            at(0),
          );
          // A second page, its seconds, and events.
          await collector.collect(
            {
              hits: [
                pageview({ path: "/de/news", pageType: "news_index" }),
                {
                  t: "engagement",
                  locale: "de",
                  path: "/de/news",
                  pageType: "news_index",
                  seconds: 20 + i,
                },
                event(
                  "tab_changed",
                  { tab: ["map", "shows"][i % 2] },
                  { entity: ENTITY },
                ),
                event("search_opened", { surface: ["hero", "header"][i % 2] }),
                event("search_result_clicked", {
                  surface: "hero",
                  result: "park",
                  position: "1",
                }),
              ],
              ip: reader.ip,
              userAgent: reader.ua,
            },
            at(5),
          );
        }
      }
      await flush();
    };

    /** Every report the admin and its MCP tools ask, with the filters its panels apply. */
    const urls = () => {
      const from = addIsoDays(today, -11);
      const windows = [
        "days=14",
        "days=14&interval=week",
        `from=${from}&to=${addIsoDays(today, -2)}`,
        `from=${from}&to=${addIsoDays(today, -2)}&locale=de`,
        "days=1",
      ];
      const filters = [
        "",
        "&filter=os:ios",
        "&filter=channel:search",
        "&filter=device:mobile&filter=page_type:park",
        "&filter=path:/de/news",
        "&filter=entry:/de",
      ];
      const dims =
        "channel referrer path page_type entry exit country browser_language locale device browser os".split(
          " ",
        );
      const base = "/v1/admin/analytics";
      const list: string[] = [];
      for (const window of windows) {
        for (const filter of filters) {
          const q = `${window}${filter}`;
          list.push(`${base}/overview?${q}`);
          for (const dim of dims)
            list.push(`${base}/breakdown?dim=${dim}&${q}`);
          list.push(`${base}/crosstab?row=os&col=device&metric=visits&${q}`);
          list.push(`${base}/crosstab?row=channel&col=exit&metric=visits&${q}`);
          list.push(`${base}/crosstab?row=path&col=day&metric=pageviews&${q}`);
          list.push(`${base}/seo/landing-pages?${q}`);
          list.push(`${base}/seo/not-found?${q}`);
          list.push(`${base}/seo/uncovered?kind=park&${q}`);
          list.push(`${base}/seo/trends?${q}`);
          list.push(`${base}/events?${q}`);
          list.push(`${base}/events/tab_changed?${q}`);
          list.push(`${base}/events/page_read?${q}`);
          list.push(
            `${base}/events/search_opened/crosstab?row=prop:surface&col=page_type&${q}`,
          );
          list.push(
            `${base}/events/search_opened/crosstab?row=prop:surface&col=day&${q}`,
          );
          list.push(
            `${base}/events/search_opened/crosstab?row=prop:surface&col=browser&${q}`,
          );
          list.push(`${base}/search?${q}`);
        }
      }
      return list;
    };

    /**
     * What a report answers, without the realtime minute that moves with the
     * clock. Sixteen at a time, as the Traffic page asks: the runner refuses
     * what does not fit its four places and 32 more.
     */
    const answers = async () => {
      const all = urls();
      const out: Array<readonly [string, number, Record<string, unknown>]> = [];
      for (let at = 0; at < all.length; at += 16) {
        out.push(
          ...(await Promise.all(
            all.slice(at, at + 16).map(async (url) => {
              const response = await admin(url).set("x-auth-key", BYPASS);
              const { realtime: _realtime, ...body } = response.body;
              return [url, response.status, body] as const;
            }),
          )),
        );
      }
      return out;
    };

    it("reads whole rolled days from the daily rows and answers exactly as the hourly rows do", async () => {
      await seedDays();
      await savePark();
      const runner = app.get(SiteReportRunner);
      const storage = app.get(SiteAnalyticsStorageService);
      const sql: string[] = [];
      const query = runner.query.bind(runner);
      jest.spyOn(runner, "query").mockImplementation((text, params) => {
        sql.push(text);
        return query(text, params);
      });
      const forgetRolledDays = () =>
        (runner as unknown as { rolled: Map<string, unknown> }).rolled.clear();

      const hourly = await answers();
      expect(sql.some((text) => text.includes("_daily"))).toBe(false);
      expect(
        hourly.filter(([, status]) => status === 200).length,
      ).toBeGreaterThan(hourly.length / 2);

      const rolled = await storage.rollupDays(new Date());
      expect(rolled.days).toBeGreaterThanOrEqual(10);
      expect(rolled.rows).toBeGreaterThan(0);
      forgetRolledDays();
      sql.length = 0;
      const daily = await answers();
      expect(sql.some((text) => text.includes("site_pageviews_daily"))).toBe(
        true,
      );
      expect(sql.some((text) => text.includes("site_events_daily"))).toBe(true);
      expect(daily).toEqual(hourly);
    });

    it("rolls each cuboid of each day once, adds up to the hourly rows, and never rewrites a row", async () => {
      await seedDays();
      const storage = app.get(SiteAnalyticsStorageService);
      const first = await storage.rollupDays(new Date());
      const cuboids =
        ROLLUP_TABLES.pageviews.cuboids.length +
        ROLLUP_TABLES.events.cuboids.length;
      expect(first.cuboids).toBe(first.days * cuboids);
      const markers = await dataSource.query(
        `SELECT source, count(*)::int AS n, count(DISTINCT day)::int AS days,
                to_char(max(day), 'YYYY-MM-DD') AS last
           FROM site_rollup_days GROUP BY source ORDER BY source`,
      );
      expect(markers).toEqual([
        {
          source: "events",
          n: first.days * 4,
          days: first.days,
          last: expect.any(String),
        },
        {
          source: "pageviews",
          n: first.days * ROLLUP_TABLES.pageviews.cuboids.length,
          days: first.days,
          last: expect.any(String),
        },
      ]);
      // The empty day has its markers and no rows.
      const empty = addIsoDays(today, -7);
      expect(
        await dataSource.query(
          `SELECT sum(rows)::int AS rows FROM site_rollup_days WHERE day = $1::date`,
          [empty],
        ),
      ).toEqual([{ rows: 0 }]);

      // Every cuboid adds up to the hourly rows of the days rolled.
      const through = markers[0].last as string;
      const [hourly] = await dataSource.query(
        `SELECT sum(views)::int AS views, sum(visits)::int AS visits,
                sum(seconds)::int AS seconds, sum(exits)::int AS exits
           FROM site_pageviews WHERE bucket < ($1::date + 1)::timestamptz`,
        [through],
      );
      const perCuboid = await dataSource.query(
        `SELECT cuboid, sum(views)::int AS views, sum(visits)::int AS visits,
                sum(seconds)::int AS seconds, sum(exits)::int AS exits
           FROM site_pageviews_daily GROUP BY cuboid ORDER BY cuboid`,
      );
      expect(perCuboid).toHaveLength(ROLLUP_TABLES.pageviews.cuboids.length);
      for (const row of perCuboid) {
        const { cuboid: _cuboid, ...sums } = row;
        expect(sums).toEqual(hourly);
      }

      // A second run finds nothing to do and writes nothing.
      const before = await dataSource.query(
        `SELECT (SELECT count(*) FROM site_pageviews_daily)::int AS pv,
                (SELECT count(*) FROM site_events_daily)::int AS ev,
                (SELECT count(*) FROM site_rollup_days)::int AS markers`,
      );
      await expect(storage.rollupDays(new Date())).resolves.toEqual({
        days: 0,
        cuboids: 0,
        rows: 0,
      });
      expect(
        await dataSource.query(
          `SELECT (SELECT count(*) FROM site_pageviews_daily)::int AS pv,
                  (SELECT count(*) FROM site_events_daily)::int AS ev,
                  (SELECT count(*) FROM site_rollup_days)::int AS markers`,
        ),
      ).toEqual(before);
    });

    it("waits for a day to settle, and for the windows of the day to be flushed", async () => {
      const collector = app.get(SiteAnalyticsCollectService);
      const storage = app.get(SiteAnalyticsStorageService);
      const yesterday = addIsoDays(today, -1);
      const last = async () =>
        (
          await dataSource.query(
            `SELECT to_char(max(day), 'YYYY-MM-DD') AS day FROM site_rollup_days`,
          )
        )[0].day;
      await collector.collect(
        { hits: [pageview()], ip: "203.0.113.21", userAgent: UA },
        new Date(`${addIsoDays(today, -3)}T10:00:00Z`),
      );
      // A hit of yesterday's last minutes whose window is still in Redis.
      await collector.collect(
        { hits: [pageview()], ip: "203.0.113.22", userAgent: UA },
        new Date(`${yesterday}T23:58:00Z`),
      );
      await flush();
      await collector.collect(
        { hits: [pageview()], ip: "203.0.113.23", userAgent: UA },
        new Date(`${yesterday}T23:59:00Z`),
      );
      // Not yet two hours past midnight: yesterday waits, the day before is done.
      await storage.rollupDays(new Date(`${today}T01:30:00Z`));
      expect(await last()).toBe(addIsoDays(today, -2));
      // Two hours on, but a window of yesterday is not written yet.
      await storage.rollupDays(new Date(`${today}T02:30:00Z`));
      expect(await last()).toBe(addIsoDays(today, -2));
      await flush();
      await storage.rollupDays(new Date(`${today}T02:30:00Z`));
      expect(await last()).toBe(yesterday);
      expect(
        await dataSource.query(
          `SELECT sum(views)::int AS views FROM site_pageviews_daily
            WHERE cuboid = 1 AND bucket = $1::timestamptz`,
          [`${yesterday}T00:00:00Z`],
        ),
      ).toEqual([{ views: 2 }]);
    });

    it("fills in a cuboid added to the lists after the days were rolled", async () => {
      await seedDays();
      const storage = app.get(SiteAnalyticsStorageService);
      await storage.rollupDays(new Date());
      // As if the lists had one cuboid fewer when these days were rolled.
      await dataSource.query(
        `DELETE FROM site_pageviews_daily WHERE cuboid = 7`,
      );
      await dataSource.query(
        `DELETE FROM site_rollup_days WHERE source = 'pageviews' AND cuboid = 7`,
      );
      const filled = await storage.rollupDays(new Date());
      expect(filled.cuboids).toBeGreaterThan(0);
      // Every day has its marker again; the empty day has no rows.
      expect(
        await dataSource.query(
          `SELECT count(*)::int AS markers FROM site_rollup_days
            WHERE source = 'pageviews' AND cuboid = 7`,
        ),
      ).toEqual([{ markers: filled.days }]);
      expect(
        await dataSource.query(
          `SELECT count(DISTINCT bucket)::int AS days FROM site_pageviews_daily WHERE cuboid = 7`,
        ),
      ).toEqual([{ days: filled.days - 1 }]);
    });

    it("refuses with 503 and a message when a statement runs past its timeout", async () => {
      await app
        .get(SiteAnalyticsCollectService)
        .collect({ hits: [pageview()], ip: "203.0.113.31", userAgent: UA });
      await flush();
      // A statement that cannot finish within the limit.
      const runner = app.get(SiteReportRunner) as unknown as {
        timeoutMs: number;
      };
      const was = runner.timeoutMs;
      runner.timeoutMs = 1;
      try {
        const response = await admin("/v1/admin/analytics/overview?days=30");
        // One millisecond may or may not be enough on a warm database; when it
        // is not, the answer is the 503, never a 500.
        expect([200, 503]).toContain(response.status);
        if (response.status === 503) {
          expect(response.body).toMatchObject({
            statusCode: 503,
            reason: "timeout",
          });
          expect(response.body.message).toContain("was stopped");
        }
        const slow = await app
          .get(SiteReportRunner)
          .query("SELECT pg_sleep(0.5)")
          .catch((error: { response?: { reason?: string } }) => error);
        expect(
          (slow as { response?: { reason?: string } }).response?.reason,
        ).toBe("timeout");
      } finally {
        runner.timeoutMs = was;
      }
    });
  });

  it("drops chunks past 400 days and keeps the rest", async () => {
    const old = new Date(`${addIsoDays(today, -430)}T00:00:00Z`);
    const kept = new Date(`${addIsoDays(today, -20)}T00:00:00Z`);
    for (const bucket of [old, kept]) {
      await dataSource.query(
        `INSERT INTO site_pageviews (bucket, locale, path, page_type, device,
                                     browser, os, country, views)
         VALUES ($1, 'de', '/de', 'home', 'mobile', 'safari', 'ios', 'de', 1)`,
        [bucket],
      );
    }
    await dataSource.query(
      `INSERT INTO site_pageviews_daily
         (bucket, cuboid, locale, path, page_type, device, browser, os,
          country, browser_language, entry, referrer, channel,
          views, seconds, visits, engaged, visit_seconds, exits,
          visit_span_seconds)
       SELECT b, 1, 'de', '', '', '', '', '', '', '', '', '', '', 1, 0, 0,
              0, 0, 0, 0
         FROM unnest($1::timestamptz[]) b`,
      [[old, kept]],
    );
    await dataSource.query(
      `INSERT INTO site_visitor_dim_days (day, scope, dim, value, visitors)
       SELECT d::date, 'all', 'country', 'de', 1
         FROM unnest($1::timestamptz[]) d`,
      [[old, kept]],
    );
    const dropped = await app.get(SiteAnalyticsStorageService).dropOldChunks();
    expect(dropped.site_pageviews).toBe(1);
    expect(dropped.site_pageviews_daily).toBe(1);
    expect(dropped.site_visitor_dim_days).toBe(1);
    expect(
      await dataSource.query(
        `SELECT count(*)::int AS n FROM site_visitor_dim_days`,
      ),
    ).toEqual([{ n: 1 }]);
    expect(
      await dataSource.query(`SELECT count(*)::int AS n FROM site_pageviews`),
    ).toEqual([{ n: 1 }]);
    expect(
      await dataSource.query(
        `SELECT count(*)::int AS n FROM site_pageviews_daily`,
      ),
    ).toEqual([{ n: 1 }]);
  });

  it("refuses the reports without a session", async () => {
    await request(app.getHttpServer())
      .get("/v1/admin/analytics/overview")
      .expect(401);
  });
});
