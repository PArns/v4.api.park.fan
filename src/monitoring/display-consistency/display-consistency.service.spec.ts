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
import { ParkyTipService } from "../../assistant/parky-tip.service";
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
  timezone: "Europe/Berlin",
} as Park;
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
  let tips: { readDay: jest.Mock; servedVerdicts: jest.Mock };
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
        const day = days.find((d) => d.date === date)!;
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
    tips = {
      readDay: jest.fn((_park: Park, date: string) =>
        Promise.resolve({
          days,
          verdict:
            days.find((d) => d.date === date)?.crowdLevel === "high"
              ? "bad"
              : "good",
        }),
      ),
      servedVerdicts: jest.fn().mockResolvedValue({}),
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
      [ParkyTipService, tips],
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

    const dayOf = (date: string) => days.find((d) => d.date === date);
    parkyAnswers.get_park_live = () => ({
      status: "OPERATING",
      crowd: { level: "moderate" },
      todayExpected: {
        crowdLevel: dayOf(today)!.crowdLevel,
        expectedPeakWaitMinutes: dayOf(today)!.headlinerForecast.avgWait,
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
      crowdLevel: dayOf(input.date as string)!.crowdLevel,
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
      calendar_predicted: "very_low",
      best_days: "very_low",
      plan_day: "very_low",
      parky_get_days: "very_low",
      parky_get_park_day: "very_low",
      parky_today_expected: "very_low",
      park_live: "moderate",
      park_lists_live: "moderate",
      parky_live: "moderate",
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

  it("names the surfaces of a park-day that disagree, as on Phantasialand's tip of 2026-11-17", async () => {
    tips.servedVerdicts.mockImplementation((_park: Park, date: string) =>
      Promise.resolve(date === today ? { de: "bad", en: "good" } : {}),
    );
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
        date: today,
        measure: "verdict",
        key: "de",
        a: { surface: "parky_tip", value: "good" },
        b: { surface: "parky_tip_served", value: "bad" },
      }),
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

  it("checks the parks in a season and the most read ones once each, ids only, and keeps the run for two days", async () => {
    parks.findById.mockImplementation((id: string) =>
      Promise.resolve({ ...park, id, name: id }),
    );

    const summary = await service.runDaily();

    expect(summary.parks.map((p) => p.id)).toEqual([PARK_A, PARK_B]);
    expect(summary.mismatches).toBe(0);
    expect(redis.set).toHaveBeenCalledWith(
      `display-consistency:v1:park:${PARK_A}`,
      expect.any(String),
      "EX",
      2 * 24 * 60 * 60,
    );
    expect(await service.latestSummary()).toEqual(summary);
    expect((await service.latestForPark(PARK_B))?.park.id).toBe(PARK_B);
  });
});
