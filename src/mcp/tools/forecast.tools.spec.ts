import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import { ParksService } from "../../parks/parks.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { MlOverviewService } from "../../admin/ml-overview.service";
import { ForecastErrorBreakdownService } from "../../ml/services/forecast-error-breakdown.service";
import { DataSource } from "typeorm";
import { parkForecastTool } from "./park-forecast.tool";
import { mlAccuracyTool } from "./ml-accuracy.tool";

const PARK_ID = "11111111-1111-4111-8111-111111111111";
const park = {
  id: PARK_ID,
  name: "Phantasialand",
  slug: "phantasialand",
  city: "Bruehl",
  country: "Germany",
  timezone: "Europe/Berlin",
  continent: "europe",
  citySlug: "bruehl",
  countrySlug: "germany",
  continentSlug: "europe",
};

function ctxWith(providers: Map<unknown, unknown>): McpToolContext {
  return {
    admin: {},
    get: (token: unknown) => providers.get(token),
  } as unknown as McpToolContext;
}

const parksService = { findById: jest.fn(async () => park) };
const dataSource = {
  getRepository: () => ({ find: jest.fn(async () => [park]) }),
};

describe("park_forecast", () => {
  const day = (date: string, extra = {}) => ({
    date,
    status: "OPERATING",
    isToday: false,
    isHoliday: false,
    isBridgeDay: false,
    isSchoolVacation: false,
    crowdLevel: "moderate",
    hours: {
      openingTime: `${date}T08:00:00.000Z`,
      closingTime: `${date}T17:00:00.000Z`,
      type: "OPERATING",
      isInferred: false,
    },
    ...extra,
  });
  const calendar = {
    buildCalendarResponse: jest.fn(async () => ({
      meta: { scheduleCoverage: { from: "2026-03-01", to: "2026-11-01" } },
      days: [
        day("2026-10-29", {
          isToday: true,
          headlinerForecast: {
            avgWait: 40,
            rides: [
              { attractionId: "a", name: "Taron", waitTime: 55 },
              { attractionId: "b", name: "Black Mamba", waitTime: 30 },
            ],
          },
          hourly: [{ hour: 10, crowdLevel: "high", predictedWaitTime: 35 }],
        }),
        day("2026-10-30", {
          isHoliday: true,
          events: [{ name: "Reformation Day", type: "holiday" }],
          weather: {
            basis: "forecast",
            condition: "Rain",
            tempMin: 4,
            tempMax: 9,
            rainChance: 3,
          },
        }),
      ],
    })),
  };
  const overview = {
    getOverview: jest.fn(async () => ({
      components: [
        {
          id: "pcn",
          name: "PCN",
          delivers: "live",
          role: "served",
          status: "ok",
        },
        {
          id: "chronos",
          name: "Chronos",
          delivers: "x",
          role: "shadow",
          status: "ok",
        },
      ],
    })),
  };
  const ctx = ctxWith(
    new Map<unknown, unknown>([
      [ParksService, parksService],
      [DataSource, dataSource],
      [CalendarService, calendar],
      [MlOverviewService, overview],
    ]),
  );

  it("compacts the calendar and names the served models", async () => {
    const input = z.object(parkForecastTool.input).parse({ park: PARK_ID });
    expect(input.days).toBe(14);
    const out = (await parkForecastTool.handler(input as never, ctx)) as any;

    expect(calendar.buildCalendarResponse).toHaveBeenCalledWith(
      park,
      expect.any(Date),
      expect.any(Date),
      "today",
    );
    expect(out.days[0]).toMatchObject({
      date: "2026-10-29",
      opens: "2026-10-29T09:00+01:00",
      expectedPeakWaitMinutes: 40,
    });
    expect(out.days[1]).toMatchObject({
      holiday: true,
      events: ["Reformation Day (holiday)"],
      weather: { precipitationMm: 3 },
    });
    expect(out.todayHourly).toEqual([
      { hour: 10, crowdLevel: "high", waitMinutes: 35 },
    ]);
    expect(out.todayHeadliners[0]).toMatchObject({ name: "Taron" });
    expect(out.servingModels.map((m: { id: string }) => m.id)).toEqual(["pcn"]);
  });

  it("refuses more than 60 days", () => {
    expect(() =>
      z.object(parkForecastTool.input).parse({ park: PARK_ID, days: 61 }),
    ).toThrow();
  });
});

describe("ml_accuracy", () => {
  const window = (mae: number | null, n = 100) => ({
    n,
    mae,
    bias: 1,
    baselineMae: 9,
    skill: mae === null ? null : mae / 9,
  });
  const row = (id: string, recent: number, previous: number, n = 100) => ({
    id,
    name: id,
    model: "catboost",
    recent: window(recent, n),
    previous: window(previous),
    maeChange: Math.round((recent - previous) * 100) / 100,
  });
  const breakdown = jest.fn(async () => ({
    grain: "park",
    days: 7,
    window: {},
    totals: [],
    rows: [
      row("calm", 5, 5),
      row("bad", 12, 7),
      row("thin", 30, 1, 3),
      row("worse", 9, 2),
    ],
  }));
  const ctx = ctxWith(
    new Map<unknown, unknown>([
      [ForecastErrorBreakdownService, { breakdown }],
      [ParksService, parksService],
      [DataSource, dataSource],
    ]),
  );
  const run = async (raw: Record<string, unknown>) =>
    (await mlAccuracyTool.handler(
      z.object(mlAccuracyTool.input).parse(raw) as never,
      ctx,
    )) as any;

  it("ranks by error and drops thin rows", async () => {
    const out = await run({});
    expect(out.parks.items.map((r: { id: string }) => r.id)).toEqual([
      "bad",
      "worse",
      "calm",
    ]);
    expect(breakdown).toHaveBeenCalledWith({
      days: 7,
      parkId: undefined,
      model: undefined,
    });
  });

  it("ranks by the rise against the window before", async () => {
    const out = await run({ rankBy: "change", limit: 1 });
    expect(out.parks.items.map((r: { id: string }) => r.id)).toEqual(["worse"]);
    expect(out.parks.total).toBe(3);
  });

  it("goes to ride level for one park", async () => {
    breakdown.mockResolvedValueOnce({
      grain: "ride",
      days: 7,
      window: {},
      totals: [],
      rows: [row("taron", 11, 8)],
    } as never);
    const out = await run({ park: PARK_ID, model: "pcn" });
    expect(breakdown).toHaveBeenLastCalledWith({
      days: 7,
      parkId: PARK_ID,
      model: "pcn",
    });
    expect(out.rides.items[0].id).toBe("taron");
    expect(out.park).toEqual({ id: PARK_ID, name: "Phantasialand" });
  });

  it("turns a statement timeout into a message for the model", async () => {
    breakdown.mockRejectedValueOnce(
      new Error("canceling statement due to statement timeout"),
    );
    await expect(run({})).rejects.toThrow(/narrow|smaller/);
  });

  it("caps days at 28", () => {
    expect(() => z.object(mlAccuracyTool.input).parse({ days: 29 })).toThrow();
  });
});
