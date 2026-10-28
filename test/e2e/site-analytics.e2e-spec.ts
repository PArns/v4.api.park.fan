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
import { SiteAnalyticsCollectService } from "../../src/site-analytics/site-analytics-collect.service";
import { SiteAnalyticsStorageService } from "../../src/site-analytics/site-analytics-storage.service";
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
  viewport: "mobile",
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
  viewport: "desktop",
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

    const device = {
      viewport: "mobile",
      browser: "safari",
      os: "ios",
      country: "zz",
      campaign: "",
    };
    const page = { ...device, entry: "", referrer: "", channel: "" };
    expect(
      await dataSource.query(
        `SELECT path, page_type, viewport, browser, os, country, entry,
                referrer, channel, campaign, views, visits, engaged
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
        ...device,
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
        ...device,
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

  it("leaves the statistic's tables as synchronize finds them after the hypertables are made", async () => {
    // Every start runs synchronize; a difference here would be an ALTER or a
    // dropped index on a large hypertable at every deploy.
    const pending = await dataSource.driver.createSchemaBuilder().log();
    expect(
      pending.upQueries
        .map((q) => q.query)
        .filter((sql) => /site_(pageviews|events|analytics_flushes)/.test(sql)),
    ).toEqual([]);
    // And the bucket indexes create_hypertable made are still there.
    const indexes = await dataSource.query(
      `SELECT indexname FROM pg_indexes
        WHERE tablename IN ('site_pageviews', 'site_events')`,
    );
    expect(indexes.map((i: { indexname: string }) => i.indexname)).toEqual(
      expect.arrayContaining([
        "site_pageviews_bucket_idx",
        "site_events_bucket_idx",
      ]),
    );
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
            viewport: "desktop",
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

    // A visit filter: visits and bounce follow it, page views cannot.
    const search = await admin(
      "/v1/admin/analytics/overview?days=7&filter=channel:search",
    ).expect(200);
    expect(search.body.totals).toEqual({
      pageviews: null,
      visits: 2,
      visitors: null,
      events: null,
      viewsPerVisit: null,
      bounceRate: 0.5,
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
    // A newsletter visit that then follows a broken link inside the site.
    await collector.collect(
      {
        hits: [pageview({ referrer: "newsletter", campaign: "october" })],
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
    });
    await expect(storage.snapshotVisitors()).resolves.toEqual({
      found: 2,
      written: 0,
    });
    const overview = await admin("/v1/admin/analytics/overview?days=2").expect(
      200,
    );
    expect(
      overview.body.series.map((d: { visitors: number }) => d.visitors),
    ).toEqual([2, 1]);
    expect(overview.body.realtime.activeVisitors).toBe(1);
  });

  it("drops chunks past 400 days and keeps the rest", async () => {
    const old = new Date(`${addIsoDays(today, -430)}T00:00:00Z`);
    const kept = new Date(`${addIsoDays(today, -20)}T00:00:00Z`);
    for (const bucket of [old, kept]) {
      await dataSource.query(
        `INSERT INTO site_pageviews (bucket, locale, path, page_type, viewport,
                                     browser, os, country, views)
         VALUES ($1, 'de', '/de', 'home', 'mobile', 'safari', 'ios', 'de', 1)`,
        [bucket],
      );
    }
    const dropped = await app.get(SiteAnalyticsStorageService).dropOldChunks();
    expect(dropped.site_pageviews).toBe(1);
    expect(
      await dataSource.query(`SELECT count(*)::int AS n FROM site_pageviews`),
    ).toEqual([{ n: 1 }]);
  });

  it("refuses the reports without a session", async () => {
    await request(app.getHttpServer())
      .get("/v1/admin/analytics/overview")
      .expect(401);
  });
});
