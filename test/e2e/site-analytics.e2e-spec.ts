import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { AdminAuthService } from "../../src/admin/auth/admin-auth.service";
import { AdminSessionStore } from "../../src/admin/auth/admin-session.store";
import { SiteAnalyticsCollectService } from "../../src/site-analytics/site-analytics-collect.service";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  SHOW_DEPENDENCIES,
  applyMergeDependencies,
} from "../../src/parks/utils/merge-dependencies";
import { addIsoDays } from "../../src/common/utils/date.util";
import { utcDay } from "../../src/common/utils/reader-hash.util";

const UA =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1";
const PATH = "/de/parks/europe/germany/bruehl/phantasialand";
const WINNER = "11111111-1111-4111-8111-111111111111";
const LOSER = "22222222-2222-4222-8222-222222222222";

const pageview = (overrides: Record<string, unknown> = {}) => ({
  t: "pageview",
  locale: "de",
  path: PATH,
  pageType: "park",
  viewport: "mobile",
  ...overrides,
});
const searchOpened = (surface: string, pageType = "home") => ({
  t: "event",
  locale: "de",
  path: "/de",
  pageType,
  viewport: "desktop",
  name: "search_opened",
  props: { surface },
});

/**
 * The site statistic against a real Postgres and Redis: the one-statement
 * upsert and its conflict keys, the merge move, the nightly snapshot, the
 * prune, and the admin reports' SQL with its empty days and previous window.
 * A unit test with a mocked repository proves a statement is issued; only a
 * database proves it runs.
 */
describe("Site analytics (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let token: string;
  const today = utcDay(new Date());

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
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    const account = await app.get(AdminAuthService).create({
      email: "site-analytics-e2e@example.invalid",
      role: "viewer",
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
      role: "viewer",
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

  const traffic = async () =>
    Object.fromEntries(
      (
        await dataSource.query(
          `SELECT dim, value, views, visits FROM site_traffic_days`,
        )
      ).map(
        (r: { dim: string; value: string; views: number; visits: number }) => [
          `${r.dim}:${r.value}`,
          [r.views, r.visits],
        ],
      ),
    );

  it("upserts a beacon's hits, adding up a second request and counting the visit once", async () => {
    const park = await dataSource.getRepository(Park).save({
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
    const tab = {
      ...pageview(),
      t: "event",
      name: "tab_changed",
      props: { tab: "map" },
      entity: park.id,
    };
    await beacon([pageview({ referrer: "google.com" }), tab]).expect(204);
    await beacon([pageview(), tab, searchOpened("hero")]).expect(204);
    await beacon([pageview()], "198.51.100.4").expect(204);

    expect(await traffic()).toMatchObject({
      [`path:${PATH}`]: [3, 2],
      [`entry:${PATH}`]: [2, 2],
      "referrer:google.com": [1, 1],
      "browser:safari": [3, 2],
      "os:ios": [3, 2],
      "country:zz": [3, 2],
      "locale:de": [3, 2],
    });
    expect(
      await dataSource.query(
        `SELECT name, page_type, viewport, p1, p2, p3, count
           FROM site_event_days ORDER BY name`,
      ),
    ).toEqual([
      {
        name: "search_opened",
        page_type: "home",
        viewport: "desktop",
        p1: "hero",
        p2: "",
        p3: "",
        count: 1,
      },
      {
        name: "tab_changed",
        page_type: "park",
        viewport: "mobile",
        p1: "map",
        p2: "",
        p3: "",
        count: 2,
      },
    ]);
    expect(
      await dataSource.query(
        `SELECT entity::text AS entity, kind, count FROM site_event_entity_days`,
      ),
    ).toEqual([{ entity: park.id, kind: "park", count: 2 }]);

    const detail = await admin(
      "/v1/admin/analytics/events/tab_changed?days=7",
    ).expect(200);
    expect(detail.body).toMatchObject({
      name: "tab_changed",
      total: 2,
      locale: null,
      entities: [
        {
          id: park.id,
          kind: "park",
          name: "Phantasialand",
          url: "/parks/europe/germany/bruehl/phantasialand",
          count: 2,
        },
      ],
    });
    expect(detail.body.props[0].values[0]).toEqual({ value: "map", count: 2 });
    expect(detail.body.series).toHaveLength(7);
  });

  it("answers 400 for a malformed body and writes nothing for a bot", async () => {
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
    expect(await traffic()).toEqual({});
  });

  it("fills every day of the reports, compares with the window before and splits by language", async () => {
    const yesterday = addIsoDays(today, -1);
    const lastWeek = addIsoDays(today, -9);
    await dataSource.query(
      `INSERT INTO site_traffic_days (dim, day, locale, value, views, visits) VALUES
         ('locale', $1, 'de', 'de', 30, 10), ('locale', $1, 'nl', 'nl', 6, 2),
         ('entry', $1, 'de', '/de', 9, 9), ('entry', $1, 'nl', '/nl', 2, 2),
         ('locale', $2, 'de', 'de', 8, 4), ('entry', $2, 'de', '/de', 4, 4),
         ('referrer', $1, 'de', 'google.com', 6, 6), ('referrer', $1, 'nl', 'bing.com', 2, 2)`,
      [yesterday, lastWeek],
    );
    await dataSource.query(
      `INSERT INTO site_event_days (name, day, locale, page_type, viewport, p1, p2, p3, count) VALUES
         ('search_opened', $1, 'de', 'home', 'desktop', 'hero', '', '', 8),
         ('search_opened', $1, 'de', 'park', 'mobile', 'header', '', '', 4),
         ('search_typed', $1, 'de', 'home', 'desktop', 'hero', '', '', 4),
         ('search_no_results', $1, 'de', 'home', 'desktop', 'hero', '', '', 1),
         ('search_result_clicked', $1, 'de', 'home', 'desktop', 'hero', 'park', '1', 2),
         ('search_opened', $2, 'nl', 'home', 'desktop', 'hero', '', '', 3)`,
      [yesterday, lastWeek],
    );
    await dataSource.query(
      `INSERT INTO site_visitor_days (day, scope, visitors) VALUES
         ($1, 'all', 9), ($1, 'de', 8), ($2, 'all', 3)`,
      [yesterday, lastWeek],
    );

    const overview = await admin("/v1/admin/analytics/overview?days=7").expect(
      200,
    );
    expect(overview.body).toMatchObject({
      to: today,
      days: 7,
      locale: null,
      totals: { pageviews: 36, visits: 11, visitors: 9, events: 19 },
      previous: { pageviews: 8, visits: 4, visitors: 3, events: 3 },
    });
    expect(overview.body.totals.viewsPerVisit).toBeCloseTo(3.27, 2);
    expect(overview.body.series).toHaveLength(7);
    expect(overview.body.series[5]).toEqual({
      day: yesterday,
      pageviews: 36,
      visits: 11,
      visitors: 9,
    });
    expect(overview.body.series[6]).toEqual({
      day: today,
      pageviews: 0,
      visits: 0,
      visitors: 0,
    });
    expect(overview.body.realtime.perMinute).toHaveLength(30);

    const dutch = await admin(
      "/v1/admin/analytics/overview?days=7&locale=nl",
    ).expect(200);
    expect(dutch.body.totals).toMatchObject({
      pageviews: 6,
      visits: 2,
      events: 0,
    });
    expect(dutch.body.previous.events).toBe(3);

    const referrers = await admin(
      "/v1/admin/analytics/breakdown?dim=referrer&days=7&limit=1",
    ).expect(200);
    expect(referrers.body).toMatchObject({
      dim: "referrer",
      total: { views: 8, visits: 8 },
      items: [{ value: "google.com", views: 6, visits: 6, share: 0.75 }],
    });

    const events = await admin("/v1/admin/analytics/events?days=7").expect(200);
    expect(events.body.items).toHaveLength(36);
    expect(events.body.items[0]).toMatchObject({
      name: "search_opened",
      count: 12,
      previous: 3,
    });

    const search = await admin("/v1/admin/analytics/search?days=7").expect(200);
    expect(search.body.surfaces[0]).toEqual({
      surface: "hero",
      opened: 8,
      typed: 4,
      resultClicks: 2,
      browseClicks: 0,
      noResults: 1,
      viewAll: 0,
      clickRate: 0.25,
      noResultRate: 0.25,
    });
    expect(search.body.bySurfaceAndPageType).toEqual([
      { surface: "hero", pageType: "home", opened: 8 },
      { surface: "header", pageType: "park", opened: 4 },
    ]);
    expect(search.body.positions[0]).toEqual({ position: "1", clicks: 2 });
    expect(search.body.series).toHaveLength(7 * 7);
  });

  it("writes yesterday's visitors from Redis, and reads today's live", async () => {
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

    await expect(collector.snapshotVisitors()).resolves.toBe(2);
    expect(
      await dataSource.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day, scope, visitors
           FROM site_visitor_days ORDER BY scope`,
      ),
    ).toEqual([
      { day: addIsoDays(today, -1), scope: "all", visitors: 2 },
      { day: addIsoDays(today, -1), scope: "de", visitors: 2 },
    ]);

    const overview = await admin("/v1/admin/analytics/overview?days=2").expect(
      200,
    );
    expect(
      overview.body.series.map((d: { visitors: number }) => d.visitors),
    ).toEqual([2, 1]);
    expect(overview.body.realtime.activeVisitors).toBe(1);
  });

  it("moves a merged entity's event counts onto the survivor, adding up shared days", async () => {
    const yesterday = addIsoDays(today, -1);
    await dataSource.query(
      `INSERT INTO site_event_entity_days (name, day, locale, entity, kind, count) VALUES
         ('tab_changed', $1, 'de', $2, 'park', 5),
         ('tab_changed', $1, 'de', $3, 'park', 2),
         ('tab_changed', $1, 'nl', $3, 'park', 1),
         ('favorite_add', $1, 'de', $3, 'park', 4)`,
      [yesterday, WINNER, LOSER],
    );
    for (const list of [
      PARK_DEPENDENCIES,
      ATTRACTION_DEPENDENCIES,
      SHOW_DEPENDENCIES,
    ]) {
      const entry = list.filter((d) => d.table === "site_event_entity_days");
      expect(entry).toHaveLength(1);
    }
    await dataSource.transaction((manager) =>
      applyMergeDependencies(
        manager,
        PARK_DEPENDENCIES.filter((d) => d.table === "site_event_entity_days"),
        WINNER,
        LOSER,
      ),
    );
    expect(
      await dataSource.query(
        `SELECT name, locale, entity::text AS entity, count
           FROM site_event_entity_days ORDER BY name, locale`,
      ),
    ).toEqual([
      { name: "favorite_add", locale: "de", entity: WINNER, count: 4 },
      { name: "tab_changed", locale: "de", entity: WINNER, count: 7 },
      { name: "tab_changed", locale: "nl", entity: WINNER, count: 1 },
    ]);
  });

  it("prunes what is older than 400 days and keeps the rest", async () => {
    const old = addIsoDays(today, -401);
    const kept = addIsoDays(today, -400);
    for (const day of [old, kept]) {
      await dataSource.query(
        `INSERT INTO site_traffic_days (dim, day, locale, value, views, visits)
         VALUES ('locale', $1, 'de', 'de', 1, 1)`,
        [day],
      );
      await dataSource.query(
        `INSERT INTO site_event_days (name, day, locale, page_type, viewport, count)
         VALUES ('pwa_installed', $1, 'de', 'home', 'mobile', 1)`,
        [day],
      );
      await dataSource.query(
        `INSERT INTO site_visitor_days (day, scope, visitors) VALUES ($1, 'all', 1)`,
        [day],
      );
    }
    const deleted = await app.get(SiteAnalyticsCollectService).prune();
    expect(deleted).toEqual({
      site_traffic_days: 1,
      site_event_days: 1,
      site_event_entity_days: 0,
      site_visitor_days: 1,
    });
    expect(
      await dataSource.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day FROM site_traffic_days`,
      ),
    ).toEqual([{ day: kept }]);
  });

  it("refuses the reports without a session", async () => {
    await request(app.getHttpServer())
      .get("/v1/admin/analytics/overview")
      .expect(401);
  });
});
