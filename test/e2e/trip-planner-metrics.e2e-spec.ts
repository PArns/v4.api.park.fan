import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { ApiKeyService } from "../../src/api-keys/api-key.service";
import { addIsoDays } from "../../src/common/utils/date.util";
import { utcDay } from "../../src/common/utils/reader-hash.util";
import { TripRequestMetricsService } from "../../src/trip-regions/metrics/trip-request-metrics.service";
import {
  metricRowOf,
  type TripAnswerFacts,
} from "../../src/trip-regions/metrics/trip-request-facts";
import type { TripRequestTrace } from "../../src/trip-regions/metrics/trip-request-trace";
import { SiteAnalyticsTripService } from "../../src/site-analytics/site-analytics-trip.service";

const facts = (over: Partial<TripAnswerFacts> = {}): TripAnswerFacts => ({
  exact: true,
  relaxed: [],
  unplacedReasons: [],
  bases: null,
  moves: null,
  windows: null,
  windowsPlanned: null,
  ...over,
});

/**
 * The trip planner's metrics against a real PostgreSQL: the table as
 * `synchronize` makes it (and leaves it), the writer's batches with their
 * arrays and nulls, every statement of the ops report (ordered-set
 * aggregates under FILTER, GROUPING SETS, unnest), the nightly prune, two
 * refusals through the real routes, and the trip report's SQL over the
 * statistic's event rows. A unit test with a mocked repository proves a
 * statement is issued; only a database proves it runs.
 */
describe("Trip planner metrics (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let metrics: TripRequestMetricsService;
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
    metrics = app.get(TripRequestMetricsService);
  });

  afterAll(async () => {
    await app.close();
  });

  /** One answer's row, recorded `daysAgo` UTC days back at 10:20. */
  const answer = (input: {
    route?: "plan" | "when" | "best_days";
    region?: string;
    daysAgo?: number;
    latencyMs: number;
    status?: number;
    errorCode?: Parameters<typeof metricRowOf>[0]["errorCode"];
    trace?: TripRequestTrace;
    facts?: TripAnswerFacts;
  }) =>
    metrics.record(
      metricRowOf({
        route: input.route ?? "plan",
        region: input.region ?? "orlando",
        at: new Date(`${addIsoDays(today, -(input.daysAgo ?? 0))}T10:20:00Z`),
        latencyMs: input.latencyMs,
        trace: input.trace ?? {},
        status: input.status ?? 200,
        errorCode: input.errorCode,
        facts: input.facts,
      }),
    );

  it("leaves the table as synchronize finds it", async () => {
    const pending = await dataSource.driver.createSchemaBuilder().log();
    expect(
      pending.upQueries
        .map((q) => q.query)
        .filter((sql) => sql.includes("trip_request_metrics")),
    ).toEqual([]);
    const columns: Array<{ column_name: string; data_type: string }> =
      await dataSource.query(
        `SELECT column_name, data_type FROM information_schema.columns
          WHERE table_name = 'trip_request_metrics'
            AND column_name IN ('relaxed', 'unplaced_reasons', 'ticket_kinds', 'bucket')
          ORDER BY column_name`,
      );
    expect(columns).toEqual([
      { column_name: "bucket", data_type: "timestamp with time zone" },
      { column_name: "relaxed", data_type: "ARRAY" },
      { column_name: "ticket_kinds", data_type: "ARRAY" },
      { column_name: "unplaced_reasons", data_type: "ARRAY" },
    ]);
  });

  it("writes the buffered rows with their arrays and nulls, at the hour", async () => {
    answer({
      latencyMs: 812.4,
      trace: {
        cache: "miss",
        calendarCache: "hit",
        shape: {
          tripDays: 9,
          parks: 4,
          parksNamed: true,
          visits: 5,
          locks: 1,
          ticketKinds: ["multi_day", "pass"],
          hopper: true,
        },
      },
      facts: facts({
        exact: false,
        relaxed: ["blocks_relaxed"],
        unplacedReasons: ["no_day_left", "no_open_day"],
      }),
    });
    expect(await metrics.flush()).toBe(1);
    const rows = await dataSource.query(
      `SELECT bucket, region, route, status, error_code, latency_ms, cache, calendar_cache,
              exact, relaxed, unplaced_reasons, trip_days, parks, parks_named, visits, locks,
              ticket_kinds, hopper, bases, moves, range_days, windows_planned, windows, within
         FROM trip_request_metrics`,
    );
    expect(rows).toEqual([
      {
        bucket: new Date(`${today}T10:00:00Z`),
        region: "orlando",
        route: "plan",
        status: 200,
        error_code: null,
        latency_ms: 812,
        cache: "miss",
        calendar_cache: "hit",
        exact: false,
        relaxed: ["blocks_relaxed"],
        unplaced_reasons: ["no_day_left", "no_open_day"],
        trip_days: 9,
        parks: 4,
        parks_named: true,
        visits: 5,
        locks: 1,
        ticket_kinds: ["multi_day", "pass"],
        hopper: true,
        bases: null,
        moves: null,
        range_days: null,
        windows_planned: null,
        windows: null,
        within: null,
      },
    ]);
  });

  it("aggregates a window per region and route, its codes, shapes and series", async () => {
    const built = (latencyMs: number, tripDays: number, parks: number) =>
      answer({
        latencyMs,
        trace: {
          cache: "miss",
          calendarCache: "miss",
          shape: { tripDays, parks, ticketKinds: [], hopper: false },
        },
        facts: facts(),
      });
    built(400, 5, 3);
    built(600, 5, 3);
    built(2400, 14, 8);
    answer({
      latencyMs: 2000,
      trace: {
        cache: "miss",
        calendarCache: "hit",
        shape: { tripDays: 14, parks: 8, ticketKinds: ["pass"], hopper: true },
      },
      facts: facts({
        exact: false,
        relaxed: ["streak_relaxed"],
        unplacedReasons: ["no_open_day", "no_open_day"],
      }),
    });
    for (let i = 0; i < 4; i++) {
      answer({
        latencyMs: 5 + i,
        trace: { cache: "hit", shape: { tripDays: 5, parks: 3 } },
        facts: facts(),
      });
    }
    answer({ latencyMs: 30, status: 400, errorCode: "unknown_park" });
    answer({
      latencyMs: 3,
      status: 404,
      errorCode: "unknown_region",
      region: "(unknown)",
    });
    answer({
      route: "when",
      latencyMs: 1500,
      trace: {
        cache: "miss",
        calendarCache: "miss",
        shape: { tripDays: 7, parks: 2, rangeDays: 365 },
      },
      facts: facts({
        windows: 3,
        windowsPlanned: 40,
        unplacedReasons: ["unknown"],
      }),
    });
    // Outside the window: eight days back.
    answer({
      latencyMs: 99999,
      daysAgo: 8,
      trace: { cache: "miss" },
      facts: facts(),
    });
    await metrics.flush();

    const report = await metrics.ops({
      from: addIsoDays(today, -6),
      to: today,
      limit: 2,
      series: true,
    });
    expect(report.totals).toEqual({ requests: 11, answered: 9, errors: 2 });
    const plan = report.routes.find(
      (row) => row.region === "orlando" && row.route === "plan",
    );
    expect(plan).toMatchObject({
      requests: 9,
      answered: 8,
      errors: 1,
      cache: { hit: 4, joined: 0, miss: 4, hitRate: 0.5 },
      calendarCache: { hit: 1, miss: 3, joined: 0 },
      exactShare: 0.875,
      relaxedShare: 0.125,
      unplacedShare: 0.125,
      hopperShare: 0.125,
      ownedTicketsShare: 0.125,
      builtLatencyMs: { count: 4, p50: 1300, max: 2400 },
      latencyMs: { count: 8, max: 2400 },
    });
    expect(plan?.builtLatencyMs.p90).toBe(2280);
    expect(report.routes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          region: "(unknown)",
          route: "plan",
          errors: 1,
        }),
        expect.objectContaining({
          region: "orlando",
          route: "when",
          exactShare: 1,
        }),
      ]),
    );
    expect(report.relaxed).toEqual([
      { route: "plan", code: "streak_relaxed", requests: 1, share: 0.125 },
    ]);
    expect(report.unplaced).toEqual([
      { route: "plan", reason: "no_open_day", visits: 2, requests: 1 },
      { route: "when", reason: "unknown", visits: 1, requests: 1 },
    ]);
    expect(report.errors).toEqual([
      { route: "plan", status: 400, code: "unknown_park", count: 1 },
      { route: "plan", status: 404, code: "unknown_region", count: 1 },
    ]);
    expect(report.ticketKinds).toEqual([
      { route: "plan", kind: "pass", tickets: 1, requests: 1 },
    ]);
    // The long trip is the slow one; the limit keeps two shapes.
    expect(report.slowest).toEqual([
      {
        route: "plan",
        tripDays: 14,
        parks: 8,
        bases: null,
        requests: 2,
        p50: 2200,
        p90: 2360,
        max: 2400,
      },
      {
        route: "when",
        tripDays: 7,
        parks: 2,
        bases: null,
        requests: 1,
        p50: 1500,
        p90: 1500,
        max: 1500,
      },
    ]);
    expect(
      report.byLength.map((row) => [row.route, row.tripDays, row.requests]),
    ).toEqual([
      ["plan", 5, 2],
      ["plan", 14, 2],
      ["when", 7, 1],
    ]);
    expect(report.byBases).toEqual([
      expect.objectContaining({ route: "plan", bases: null, requests: 4 }),
      expect.objectContaining({ route: "when", bases: null, requests: 1 }),
    ]);
    expect(report.series).toEqual([
      // The refusal of an unknown region is a plan request too.
      {
        day: today,
        route: "plan",
        requests: 10,
        errors: 2,
        hitRate: 0.5,
        builtP90: 2280,
      },
      {
        day: today,
        route: "when",
        requests: 1,
        errors: 0,
        hitRate: 0,
        builtP90: 1500,
      },
    ]);

    const narrowed = await metrics.ops({
      from: addIsoDays(today, -8),
      to: addIsoDays(today, -8),
      region: "orlando",
      route: "plan",
      limit: 10,
      series: false,
    });
    expect(narrowed.totals.requests).toBe(1);
    expect(narrowed.routes[0].latencyMs.max).toBe(99999);
  });

  it("deletes the rows past 90 days and keeps the rest", async () => {
    answer({ latencyMs: 1, daysAgo: 91, trace: {}, facts: facts() });
    answer({ latencyMs: 2, daysAgo: 89, trace: {}, facts: facts() });
    answer({ latencyMs: 3, trace: {}, facts: facts() });
    await metrics.flush();
    expect(await metrics.prune()).toBe(1);
    const left: Array<{ latency_ms: number }> = await dataSource.query(
      `SELECT latency_ms FROM trip_request_metrics ORDER BY latency_ms`,
    );
    expect(left.map((row) => row.latency_ms)).toEqual([2, 3]);
    expect(await metrics.prune()).toBe(0);
  });

  it("records a refusal of the real routes, and nothing the key guard turns away", async () => {
    const { key } = await app
      .get(ApiKeyService)
      .create({ name: "trip-metrics-e2e", usage: "internal" }, null);
    const from = addIsoDays(today, 10);
    const to = addIsoDays(today, 14);
    await request(app.getHttpServer())
      .get(`/v1/trip-regions/atlantis/plan?from=${from}&to=${to}`)
      .set("X-API-Key", key)
      .expect(404);
    await request(app.getHttpServer())
      .get(
        `/v1/trip-regions/orlando/plan?from=${from}&to=${to}&parks=europa-park`,
      )
      .set("X-API-Key", key)
      .expect(400);
    await request(app.getHttpServer())
      .get(`/v1/trip-regions/orlando/when?days=2`)
      .set("X-API-Key", key)
      .expect(400);
    await request(app.getHttpServer())
      .get(`/v1/trip-regions/orlando/plan?from=${from}&to=${to}`)
      .expect(401);
    await metrics.flush();
    const rows = await dataSource.query(
      `SELECT region, route, status, error_code FROM trip_request_metrics ORDER BY id`,
    );
    expect(rows).toEqual([
      {
        region: "(unknown)",
        route: "plan",
        status: 404,
        error_code: "unknown_region",
      },
      {
        region: "orlando",
        route: "plan",
        status: 400,
        error_code: "unknown_park",
      },
      {
        region: "orlando",
        route: "when",
        status: 400,
        error_code: "invalid_query",
      },
    ]);
  });

  /** A trip event as the flusher writes it: the UTC hour `daysAgo` back, a page type. */
  const insertEvents = async (
    events: Array<{
      name: string;
      props: Record<string, string>;
      count: number;
      daysAgo?: number;
      pageType?: string;
      path?: string;
    }>,
  ) => {
    for (const e of events) {
      const bucket = new Date();
      bucket.setUTCMinutes(0, 0, 0);
      bucket.setUTCDate(bucket.getUTCDate() - (e.daysAgo ?? 0));
      await dataSource.query(
        `INSERT INTO site_events (bucket, name, locale, path, page_type, device, browser, os,
                                  country, network, props_key, props, count)
         VALUES ($1, $2, 'en', $7, $3, 'desktop', 'chrome', 'windows', 'us', 'other',
                 $4, $5::jsonb, $6)`,
        [
          bucket,
          e.name,
          e.pageType ?? "trip_hub",
          Object.keys(e.props)
            .sort()
            .map((k) => `${k}=${e.props[k]}`)
            .join("&"),
          JSON.stringify(e.props),
          e.count,
          e.path ?? "/en/orlando",
        ],
      );
    }
  };
  const lastDays = (days: number) => ({
    days,
    from: null,
    to: null,
    interval: null,
    locale: null,
    filter: [],
    datacenter: null,
  });

  it("splits the trip funnel at the first day of the dates-first planner's events, and reads page types and the tile", async () => {
    const MK_PAGE =
      "/de/parks/north-america/united-states/orlando/magic-kingdom-park";
    const orlando = { region: "orlando" };
    await insertEvents([
      // The old planner, four days back.
      {
        name: "trip_planner_started",
        props: { ...orlando, source: "hero" },
        count: 6,
        daysAgo: 4,
        pageType: "other",
      },
      {
        name: "trip_tickets_answered",
        props: { ...orlando, answer: "yes" },
        count: 2,
        daysAgo: 4,
      },
      {
        name: "trip_plan_shown",
        props: { ...orlando, mode: "dates", outcome: "ok" },
        count: 5,
        daysAgo: 4,
      },
      // The dates-first planner, today.
      {
        name: "trip_planner_started",
        props: { ...orlando, source: "dates" },
        count: 7,
      },
      {
        name: "trip_plan_requested",
        props: { ...orlando, mode: "when", parks: "0" },
        count: 4,
        pageType: "trip_best_time",
      },
      {
        name: "trip_plan_shown",
        props: { ...orlando, mode: "when", outcome: "ok", windows: "3" },
        count: 4,
      },
      {
        name: "trip_edit_opened",
        props: { ...orlando, panel: "parks", from: "answer" },
        count: 3,
      },
      {
        name: "trip_tickets_opened",
        props: { ...orlando, from: "card", first: "true" },
        count: 2,
      },
      {
        name: "trip_tickets_answered",
        props: { ...orlando, answer: "yes" },
        count: 1,
      },
      {
        name: "trip_planner_started",
        props: { region: "japan", source: "hero" },
        count: 2,
      },
      {
        name: "ticket_link_clicked",
        props: { product: "wdw-date-based", kind: "official", position: "1" },
        count: 3,
      },
      // The tile on a park page: its views by the page, and another section of it.
      {
        name: "section_seen",
        props: { section: "trip_tile" },
        count: 9,
        pageType: "park",
        path: MK_PAGE,
      },
      {
        name: "section_seen",
        props: { section: "main_end" },
        count: 30,
        pageType: "park",
        path: MK_PAGE,
      },
      {
        name: "trip_tile_clicked",
        props: { ...orlando, link: "plan" },
        count: 3,
        pageType: "park",
        path: MK_PAGE,
      },
    ]);
    const options = {
      region: "orlando",
      knownRegions: ["orlando"],
      productRegions: new Map([["wdw-date-based", "orlando"]]),
      parkRegions: new Map([
        ["north-america/united-states/orlando/magic-kingdom-park", "orlando"],
      ]),
      series: true,
    };
    const report = await app
      .get(SiteAnalyticsTripService)
      .report(lastDays(7), options);
    expect(report.newFunnelFrom).toBe(today);
    const step = (name: string) => report.funnel.find((s) => s.step === name);
    expect(step("datesPicked")).toEqual({
      step: "datesPicked",
      before: 6,
      since: 7,
    });
    expect(step("planShown")).toMatchObject({ before: 5, since: 4 });
    expect(step("changed")).toMatchObject({ before: null, since: 3 });
    // A yes before the cut-over, the ticket step's openings since; today's yes is neither.
    expect(step("ticketsEntered")).toMatchObject({ before: 2, since: 2 });
    expect(report.totals).toMatchObject({
      planRequested: 4,
      datesSearched: 4,
      parksHeadliners: 4,
      findDatesShown: 4,
      ticketLinks: 3,
    });
    expect(report.lists.windowsFound).toEqual([{ windows: "3", count: 4 }]);
    // The tile's views come from the hourly rows by page; its other sections are no view of it.
    expect(report.tile).toEqual([
      { region: "orlando", shown: 9, clicked: 3, plan: 3, calendar: 0 },
    ]);
    expect(report.totals).toMatchObject({ tileShown: 9, tilePlan: 3 });
    expect(report.lists.linksByProduct).toEqual([
      { product: "wdw-date-based", count: 3 },
    ]);
    expect(report.pageTypes).toEqual([
      expect.objectContaining({
        pageType: "trip_hub",
        datesPicked: 7,
        planShown: 9,
      }),
      expect.objectContaining({ pageType: "other", datesPicked: 6 }),
      expect.objectContaining({ pageType: "trip_best_time", planRequested: 4 }),
    ]);
    // Japan is no region of this list: its starts are (other).
    expect(report.regions.map((row) => [row.region, row.datesPicked])).toEqual([
      ["orlando", 13],
      ["(other)", 2],
    ]);
    expect(
      report.series?.reduce((sum, point) => sum + point.datesPicked, 0),
    ).toBe(13);

    // One day, hourly: today's rows by their hour, all of it the new planner's.
    const hourly = await app
      .get(SiteAnalyticsTripService)
      .report(lastDays(1), options);
    expect(hourly.interval).toBe("hour");
    expect(hourly.funnel.find((s) => s.step === "datesPicked")).toMatchObject({
      before: null,
      since: 7,
    });
    expect(
      hourly.series?.reduce((sum, point) => sum + point.datesPicked, 0),
    ).toBe(7);
  });

  it("records Parky's calls with the caller, and the stays and parks left out of every row", async () => {
    const answer = {
      meta: {
        exact: true,
        stays: [{ base: "tokyo" }, { base: "osaka" }],
        staySuggestion: { base: "nagoya" },
      },
      days: [],
      parks: [],
      totals: { moves: 1 },
      unplaced: [],
      parksLeftOut: [{ park: "legoland-japan", reason: "no_day_left" }],
    };
    await metrics.measure(
      { route: "plan", region: "japan", caller: "parky" },
      async () => answer,
    );
    metrics.record(
      metricRowOf({
        route: "plan",
        region: "japan",
        at: new Date(),
        latencyMs: 40,
        trace: { cache: "hit", shape: { moveBuffer: 60, gateways: "both" } },
        status: 200,
        facts: facts({ bases: 2, leftOut: 0, staySuggested: false }),
      }),
    );
    await metrics.flush();
    const rows = await dataSource.query(
      `SELECT caller, left_out, move_buffer, gateways, stay_suggested, bases, moves
         FROM trip_request_metrics ORDER BY id`,
    );
    expect(rows).toEqual([
      {
        caller: "parky",
        left_out: 1,
        move_buffer: null,
        gateways: null,
        stay_suggested: true,
        bases: 2,
        moves: 1,
      },
      {
        caller: "site",
        left_out: 0,
        move_buffer: 60,
        gateways: "both",
        stay_suggested: false,
        bases: 2,
        moves: null,
      },
    ]);

    const report = await metrics.ops({
      from: today,
      to: today,
      limit: 5,
      series: false,
    });
    expect(
      report.routes.map((row) => [
        row.caller,
        row.leftOutShare,
        row.staySuggestedShare,
      ]),
    ).toEqual(
      expect.arrayContaining([
        ["parky", 1, 1],
        ["site", 0, 0],
      ]),
    );
    expect(report.stays).toEqual([
      { route: "plan", dim: "gateways", value: "both", requests: 1 },
      { route: "plan", dim: "move_buffer", value: "60", requests: 1 },
    ]);
    const parky = await metrics.ops({
      from: today,
      to: today,
      caller: "parky",
      limit: 5,
      series: false,
    });
    expect(parky.totals.requests).toBe(1);
  });
});
