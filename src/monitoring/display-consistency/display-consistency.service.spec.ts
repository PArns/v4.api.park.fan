import type { ModuleRef } from "@nestjs/core";
import type { Redis } from "ioredis";
import type { DataSource } from "typeorm";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import type { Park } from "../../parks/entities/park.entity";
import { ParksService } from "../../parks/parks.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { BestDaysService } from "../../parks/services/best-days.service";
import { PlanDayService } from "../../parks/services/plan-day.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { TripRegionService } from "../../trip-regions/trip-region.service";
import { regionOfPark } from "../../trip-regions/regions";
import { PageViewsService } from "../../page-views/page-views.service";
import { parksWithRunningSeason } from "../../ml/utils/season-level.query";
import { DisplayConsistencyService } from "./display-consistency.service";

/** Parky's three tools as the service runs them; their answers are set per test. */
const parkyAnswers: Record<
  string,
  (input: Record<string, unknown>) => unknown
> = {};
jest.mock("../../assistant/tools/assistant-tools", () => ({
  ASSISTANT_TOOLS: ["get_park_live", "get_days", "get_park_day"].map(
    (name) => ({
      name,
      input: { parse: (input: unknown) => input },
      run: (input: Record<string, unknown>) =>
        Promise.resolve(parkyAnswers[name](input)),
    }),
  ),
}));
jest.mock("../../ml/utils/season-level.query", () => ({
  parksWithRunningSeason: jest.fn(),
}));
jest.mock("../../trip-regions/regions", () => ({
  ...jest.requireActual("../../trip-regions/regions"),
  regionOfPark: jest.fn(),
}));

const park = {
  id: "11111111-1111-4111-8111-111111111111",
  name: "Phantasialand",
  slug: "phantasialand",
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruhl",
  timezone: "Europe/Berlin",
} as Park;
const RELAY = "https://park.fan/api/parks/europe/germany/bruhl/phantasialand";
const PARK_A = "22222222-2222-4222-8222-222222222222";
const PARK_B = "33333333-3333-4333-8333-333333333333";
const today = getCurrentDateInTimezone(park.timezone);
const tomorrow = addIsoDays(today, 1);

/** The calendar day every surface agrees with unless a test says otherwise. */
const calendarDay = (date: string, crowdLevel: string, avgWait: number) => ({
  date,
  status: "OPERATING",
  crowdLevel,
  predictedCrowdLevel: crowdLevel,
  headlinerForecast: {
    avgWait,
    exactWait: avgWait,
    rides: [{ attractionId: "taron", name: "Taron", waitTime: avgWait }],
  },
});

describe("DisplayConsistencyService", () => {
  let store: Map<string, string>;
  let redis: Redis;
  let services: Map<unknown, unknown>;
  let calendar: { buildCalendarResponse: jest.Mock };
  let bestDays: { getBestDays: jest.Mock };
  let planDays: { buildPlanDay: jest.Mock };
  let integration: { buildIntegratedResponse: jest.Mock };
  let parks: { findById: jest.Mock; loadParkRelations: jest.Mock };
  let service: DisplayConsistencyService;

  beforeEach(() => {
    store = new Map();
    redis = {
      get: jest.fn((key: string) => Promise.resolve(store.get(key) ?? null)),
      set: jest.fn((key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve("OK");
      }),
    } as unknown as Redis;

    const days = [
      calendarDay(today, "very_low", 20),
      calendarDay(tomorrow, "high", 45),
    ];
    // A day past the two above, as the calendar's third: the daily run reads nine.
    const dayOf = (date: string) =>
      days.find((d) => d.date === date) ?? calendarDay(date, "low", 25);
    calendar = {
      buildCalendarResponse: jest.fn().mockResolvedValue({
        meta: {},
        days: [...days, calendarDay(addIsoDays(today, 2), "low", 25)],
      }),
    };
    bestDays = {
      getBestDays: jest.fn().mockResolvedValue({
        meta: { computedAt: "2026-11-17T08:05:00.000Z" },
        days: days.map((d) => ({ date: d.date, crowdLevel: d.crowdLevel })),
      }),
    };
    planDays = {
      buildPlanDay: jest.fn((_park: Park, date: string) => {
        const day = dayOf(date);
        return Promise.resolve({
          context: { crowdLevel: day.crowdLevel },
          rides: [
            {
              attractionId: "taron",
              isHeadliner: true,
              dayPeak: day.headlinerForecast.avgWait,
            },
            { attractionId: "kiddie", dayPeak: 5 },
          ],
        });
      }),
    };
    integration = {
      buildIntegratedResponse: jest.fn().mockResolvedValue({
        status: "OPERATING",
        analytics: {
          statistics: {
            crowdLevel: "moderate",
            timestamp: "2026-11-17T09:40:00.000Z",
          },
        },
      }),
    };
    parks = {
      findById: jest.fn().mockResolvedValue(park),
      loadParkRelations: jest.fn((p: Park) => {
        p.attractions = [];
        return Promise.resolve(p);
      }),
    };
    services = new Map<unknown, unknown>([
      [CalendarService, calendar],
      [BestDaysService, bestDays],
      [PlanDayService, planDays],
      [ParkIntegrationService, integration],
      [
        AnalyticsService,
        {
          getBatchParkOccupancy: jest.fn().mockResolvedValue(
            new Map([
              [
                park.id,
                {
                  crowdLevel: "moderate",
                  updatedAt: "2026-11-17T09:38:00.000Z",
                },
              ],
            ]),
          ),
        },
      ],
      [ParksService, parks],
      [TripRegionService, { calendar: jest.fn() }],
      [
        PageViewsService,
        {
          adminOverview: jest.fn().mockResolvedValue({
            kinds: [
              { kind: "article", top: [{ key: "a-post" }] },
              {
                kind: "park",
                top: [{ key: PARK_B }, { key: "not-a-uuid" }, { key: PARK_A }],
              },
            ],
          }),
        },
      ],
    ]);
    const moduleRef = {
      get: (token: unknown) => services.get(token),
    } as unknown as ModuleRef;

    parkyAnswers.get_park_live = () => ({
      status: "OPERATING",
      crowd: { level: "moderate" },
      todayExpected: {
        crowdLevel: dayOf(today).crowdLevel,
        expectedPeakWaitMinutes: dayOf(today).headlinerForecast.avgWait,
      },
    });
    parkyAnswers.get_days = () => ({
      days: days.map((d) => ({
        date: d.date,
        crowdLevel: d.crowdLevel,
        expectedPeakWaitMinutes: d.headlinerForecast.avgWait,
      })),
    });
    parkyAnswers.get_park_day = (input) => ({
      crowdLevel: dayOf(input.date as string).crowdLevel,
    });
    (regionOfPark as jest.Mock).mockReturnValue(null);
    (parksWithRunningSeason as jest.Mock).mockResolvedValue([PARK_A]);

    service = new DisplayConsistencyService(moduleRef, {} as DataSource, redis);
  });

  it("reads every surface through its own method and finds no mismatch where they agree", async () => {
    const result = await service.check(park, 2);

    expect(result.failures).toEqual([]);
    expect(result.mismatches).toEqual([]);
    expect(result.days.map((d) => d.date)).toEqual([today, tomorrow]);
    expect(result.days[0].crowdLevel).toEqual({
      calendar: "very_low",
      best_days: "very_low",
      plan_day: "very_low",
      parky_get_days: "very_low",
      parky_get_park_day: "very_low",
      parky_today_expected: "very_low",
      park_live: "moderate",
      park_lists_live: "moderate",
      parky_live: "moderate",
    });
    expect(result.days[0].headlinerWait).toEqual({
      calendar: 20,
      plan_day: 20,
      parky_get_days: 20,
      parky_today_expected: 20,
    });
    // The calendar grid's variant, and /plan/day built once per day through its cache.
    expect(calendar.buildCalendarResponse.mock.calls[0][3]).toBe("none");
    expect(planDays.buildPlanDay).toHaveBeenCalledTimes(2);
    expect(result.compared).toBeGreaterThan(20);
    // Live against the day's forecast is two statistics, read side by side.
    expect(result.differences).toContainEqual(
      expect.objectContaining({
        date: today,
        a: { surface: "calendar", value: "very_low" },
        b: expect.objectContaining({ surface: "park_live", value: "moderate" }),
      }),
    );
  });

  it("names the surfaces of a park-day that disagree, with the statistic and when each was computed", async () => {
    bestDays.getBestDays.mockResolvedValue({
      meta: { computedAt: "2026-11-16T20:05:00.000Z" },
      days: [
        { date: today, crowdLevel: "very_low" },
        { date: tomorrow, crowdLevel: "low" },
      ],
    });

    const { mismatches } = await service.check(park, 2);

    expect(mismatches).toEqual([
      expect.objectContaining({
        date: tomorrow,
        measure: "crowd_level",
        a: { surface: "calendar", value: "high" },
        b: {
          surface: "best_days",
          value: "low",
          asOf: "2026-11-16T20:05:00.000Z",
        },
        statistic: expect.stringContaining("projectDay"),
      }),
    ]);
  });

  it("leaves a surface that cannot be read out of the comparison and names it", async () => {
    bestDays.getBestDays.mockRejectedValue(new Error("redis down"));

    const result = await service.check(park, 2);

    expect(result.failures).toEqual([
      { surface: "best_days", error: "redis down" },
    ]);
    expect(result.mismatches).toEqual([]);
    expect(result.days[0].crowdLevel.calendar).toBe("very_low");
  });

  it("reads no live level for a park that is not open", async () => {
    integration.buildIntegratedResponse.mockResolvedValue({
      status: "CLOSED",
      analytics: { statistics: { crowdLevel: "very_low" } },
    });
    parkyAnswers.get_park_live = () => ({
      status: "CLOSED",
      crowd: { level: null },
    });

    const result = await service.check(park, 1);

    expect(result.days[0].crowdLevel).not.toHaveProperty("park_live");
    expect(result.days[0].crowdLevel).not.toHaveProperty("parky_live");
    expect(result.mismatches).toEqual([]);
  });

  it("reads the region calendar's cell of a park in a trip region, by its basis", async () => {
    (regionOfPark as jest.Mock).mockReturnValue({ slug: "rhineland" });
    (services.get(TripRegionService) as { calendar: jest.Mock }).calendar = jest
      .fn()
      .mockResolvedValue({
        meta: { computedAt: "2026-11-17T09:00:00.000Z" },
        days: [
          {
            date: today,
            parks: {
              phantasialand: {
                crowdLevel: "very_low",
                basis: "forecast",
                forecastWait: 25,
              },
            },
          },
          {
            date: tomorrow,
            parks: {
              phantasialand: { crowdLevel: "moderate", basis: "last_year" },
            },
          },
        ],
      });

    const result = await service.check(park, 2);

    expect(result.mismatches).toEqual([
      expect.objectContaining({
        date: today,
        measure: "headliner_wait",
        a: { surface: "calendar", value: 20 },
        b: expect.objectContaining({ surface: "trip_region", value: 25 }),
      }),
    ]);
    expect(result.differences).toContainEqual(
      expect.objectContaining({
        date: tomorrow,
        b: expect.objectContaining({
          surface: "trip_region_last_year",
          value: "moderate",
        }),
      }),
    );
  });

  describe("park.fan's relays", () => {
    const json = (body: unknown, headers: Record<string, string> = {}) =>
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "content-type": "application/json", ...headers },
      });
    /** What the relays answer; by default the copy of what the API serves now. */
    let calendarRelay: () => Promise<Response>;
    let bestDaysRelay: () => Promise<Response>;
    let fetchMock: jest.SpyInstance;
    const months = new Set([today, tomorrow].map((date) => date.slice(0, 7)));

    beforeEach(() => {
      process.env.REVALIDATE_SECRET = "test";
      calendarRelay = () =>
        Promise.resolve(
          json({
            meta: {},
            days: [
              calendarDay(today, "very_low", 20),
              calendarDay(tomorrow, "high", 45),
            ],
          }),
        );
      bestDaysRelay = () =>
        Promise.resolve(
          json({
            meta: { computedAt: "2026-11-17T08:05:00.000Z" },
            days: [
              { date: today, crowdLevel: "very_low" },
              { date: tomorrow, crowdLevel: "high" },
            ],
          }),
        );
      fetchMock = jest
        .spyOn(globalThis, "fetch")
        .mockImplementation((input) =>
          String(input).endsWith("/best-days")
            ? bestDaysRelay()
            : calendarRelay(),
        );
    });

    afterEach(() => {
      delete process.env.REVALIDATE_SECRET;
      fetchMock.mockRestore();
    });

    it("reads each month as the grid asks for it and the best-days snapshot, as the park page does", async () => {
      const result = await service.check(park, 2);

      const urls = fetchMock.mock.calls.map(([url]) => String(url));
      expect(urls).toHaveLength(months.size + 1);
      expect(urls[0]).toMatch(
        new RegExp(
          `^${RELAY}/calendar\\?from=${today.slice(0, 7)}-01&to=${today.slice(0, 7)}-(28|29|30|31)$`,
        ),
      );
      expect(urls).toContain(`${RELAY}/best-days`);
      const init = fetchMock.mock.calls[0][1] as RequestInit;
      expect(init.signal).toBeInstanceOf(AbortSignal);
      expect(init.headers).toMatchObject({
        Referer: "https://park.fan/parks/europe/germany/bruhl/phantasialand",
        "User-Agent": "api.park.fan-display-consistency",
      });
      expect(result.failures).toEqual([]);
      expect(result.mismatches).toEqual([]);
      expect(result.days[0].crowdLevel).toMatchObject({
        calendar_relay: "very_low",
        best_days_relay: "very_low",
      });
      expect(result.days[1].headlinerWait.calendar_relay).toBe(45);
    });

    it("names a stale edge copy against the API and against its neighbour on the page, with its age", async () => {
      calendarRelay = () =>
        Promise.resolve(
          json(
            { meta: {}, days: [calendarDay(today, "high", 45)] },
            {
              age: "3300",
              date: "Tue, 17 Nov 2026 09:40:00 GMT",
              "cf-cache-status": "HIT",
              "x-vercel-cache": "HIT",
            },
          ),
        );

      const { mismatches } = await service.check(park, 1);

      const copy = {
        age: 3300,
        date: "Tue, 17 Nov 2026 09:40:00 GMT",
        cloudflare: "HIT",
        vercel: "HIT",
      };
      expect(mismatches).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            measure: "crowd_level",
            a: { surface: "calendar", value: "very_low" },
            b: { surface: "calendar_relay", value: "high", copy },
          }),
          expect.objectContaining({
            measure: "headliner_wait",
            a: { surface: "calendar", value: 20 },
            b: { surface: "calendar_relay", value: 45, copy },
          }),
          expect.objectContaining({
            measure: "crowd_level",
            a: { surface: "calendar_relay", value: "high", copy },
            b: {
              surface: "best_days_relay",
              value: "very_low",
              asOf: "2026-11-17T08:05:00.000Z",
              copy: {},
            },
          }),
        ]),
      );
      expect(mismatches).toHaveLength(3);
    });

    it.each([
      [
        "no answer in time",
        () =>
          Promise.reject(new Error("The operation was aborted due to timeout")),
        /calendar\?from=.*: The operation was aborted due to timeout/,
      ],
      [
        "a Cloudflare challenge",
        () =>
          Promise.resolve(
            new Response("<html>Just a moment…</html>", {
              status: 403,
              headers: {
                "content-type": "text/html",
                "cf-mitigated": "challenge",
              },
            }),
          ),
        /Cloudflare challenge \(403\)/,
      ],
      [
        "an error status",
        () =>
          Promise.resolve(
            new Response(JSON.stringify({ error: "Failed" }), {
              status: 500,
              headers: { "content-type": "application/json" },
            }),
          ),
        /answered 500/,
      ],
      [
        "a page instead of JSON",
        () =>
          Promise.resolve(
            new Response("<html></html>", {
              status: 200,
              headers: { "content-type": "text/html" },
            }),
          ),
        /answered text\/html, not JSON/,
      ],
    ])(
      "lists a relay it cannot read as a failure, never a mismatch: %s",
      async (_case, answer, error) => {
        calendarRelay = answer;
        bestDaysRelay = answer;

        const result = await service.check(park, 2);

        expect(result.failures).toHaveLength(2);
        expect(result.failures).toEqual(
          expect.arrayContaining([
            { surface: "calendar_relay", error: expect.stringMatching(error) },
            {
              surface: "best_days_relay",
              error: expect.stringContaining("/best-days"),
            },
          ]),
        );
        expect(result.mismatches).toEqual([]);
        expect(result.days[0].crowdLevel).not.toHaveProperty("calendar_relay");
      },
    );

    it("reads no relay where this API does not ring the frontend, nor for a park without a page", async () => {
      delete process.env.REVALIDATE_SECRET;
      const offline = await service.check(park, 1);
      process.env.REVALIDATE_SECRET = "test";
      const pageless = await service.check(
        { ...park, citySlug: null } as unknown as Park,
        1,
      );

      expect(fetchMock).not.toHaveBeenCalled();
      expect(offline.failures).toEqual([]);
      expect(pageless.failures).toEqual([]);
    });
  });

  it("checks the parks in a season and the most read ones once each, ids only, and keeps the run for two days", async () => {
    parks.findById.mockImplementation((id: string) =>
      Promise.resolve({ ...park, id, name: id }),
    );

    const summary = await service.runDaily();

    expect(summary.parks.map((p) => p.id)).toEqual([PARK_A, PARK_B]);
    expect(summary.mismatches).toBe(0);
    expect(summary.failures).toBe(0);
    expect(redis.set).toHaveBeenCalledWith(
      `display-consistency:v1:park:${PARK_A}`,
      expect.any(String),
      "EX",
      2 * 24 * 60 * 60,
    );
    expect(await service.latestSummary()).toEqual(summary);
    expect((await service.latestForPark(PARK_B))?.park.id).toBe(PARK_B);
  });

  it("counts the surfaces it could not read into the run, and a park whose check threw", async () => {
    parks.findById.mockImplementation((id: string) =>
      Promise.resolve(
        id === PARK_B
          ? { ...park, id, name: "Efteling", timezone: "Not/AZone" }
          : { ...park, id, name: "Phantasialand" },
      ),
    );
    bestDays.getBestDays.mockRejectedValue(new Error("redis down"));

    const summary = await service.runDaily();

    expect(summary.mismatches).toBe(0);
    expect(summary.failures).toBe(2);
    expect(summary.parks).toEqual([
      { id: PARK_A, name: "Phantasialand", mismatches: 0, failures: 1 },
      {
        id: PARK_B,
        name: "Efteling",
        mismatches: 0,
        failures: 1,
        error: expect.any(String),
      },
    ]);
  });

  it("answers the last run with the result of each park it flagged, as the admin and MCP show it", async () => {
    expect(await service.latestRun()).toBeNull();
    parks.findById.mockImplementation((id: string) =>
      Promise.resolve({ ...park, id, name: id }),
    );
    bestDays.getBestDays.mockImplementation(() =>
      Promise.resolve({
        meta: {},
        days: [{ date: today, crowdLevel: "high" }],
      }),
    );
    (parksWithRunningSeason as jest.Mock).mockResolvedValue([]);
    (
      services.get(PageViewsService) as { adminOverview: jest.Mock }
    ).adminOverview.mockResolvedValue({
      kinds: [{ kind: "park", top: [{ key: PARK_A }] }],
    });

    const summary = await service.runDaily();
    const run = await service.latestRun();

    expect(run).toEqual({
      summary,
      parks: [
        expect.objectContaining({
          park: expect.objectContaining({ id: PARK_A }),
          mismatches: [
            expect.objectContaining({
              a: { surface: "calendar", value: "very_low" },
              b: { surface: "best_days", value: "high" },
            }),
          ],
          failures: [],
        }),
      ],
    });
    expect(Object.keys(run!.parks[0].days[0])).toEqual([
      "date",
      "crowdLevel",
      "headlinerWait",
    ]);
  });
});
