import { ForecastArchiveService } from "./forecast-archive.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { PlanDayDto, PlanDayShadowCurve } from "../dto/plan-day.dto";
import { Park } from "../entities/park.entity";
import {
  CHRONOS_LAST_RUN_KEY,
  ChronosResponse,
} from "./chronos-shadow.service";

const TZ = "Europe/Berlin";

function pred(
  attractionId: string,
  iso: string,
  wait: number,
  modelVersion = "v20261008",
  uncertaintyMinutes: number | null = 12,
): PredictionDto {
  return {
    attractionId,
    predictedTime: iso,
    predictedWaitTime: wait,
    predictionType: "hourly",
    confidence: 80,
    uncertaintyMinutes,
    modelVersion,
  } as PredictionDto;
}

describe("ForecastArchiveService.originKindAt", () => {
  it("reads the park's own clock", () => {
    // 04:05 UTC = 06:05 in Berlin (CEST) → daily; 00:05 in New York.
    const at = new Date("2026-10-08T04:05:00Z");
    expect(ForecastArchiveService.originKindAt(at, TZ)).toBe("daily");
    expect(
      ForecastArchiveService.originKindAt(at, "America/New_York"),
    ).toBeNull();
    expect(
      ForecastArchiveService.originKindAt(new Date("2026-10-08T05:05:00Z"), TZ),
    ).toBe("long");
    expect(
      ForecastArchiveService.originKindAt(new Date("2026-10-08T10:05:00Z"), TZ),
    ).toBe("intraday");
    expect(
      ForecastArchiveService.originKindAt(new Date("2026-10-08T09:05:00Z"), TZ),
    ).toBeNull();
  });
});

describe("ForecastArchiveService.curvesFromServed", () => {
  it("splits the served curve per ride and operating day, keeps PCN per slot", () => {
    const preds = [
      pred("r1", "2026-10-08T08:00:00Z", 20),
      pred("r1", "2026-10-08T08:15:00Z", 25, "v20261008+pcn"),
      // 08:30 missing — a gap the row must keep, not close up.
      pred("r1", "2026-10-08T08:45:00Z", 30, "v20261008", null),
      // 06:00 on the 9th in Berlin: the next operating day.
      pred("r1", "2026-10-09T04:00:00Z", 5),
      pred("r2", "2026-10-08T08:00:00Z", 40),
      {
        ...pred("r2", "2026-10-08T09:00:00Z", 40),
        predictionType: "daily",
      } as PredictionDto,
    ];
    const rows = ForecastArchiveService.curvesFromServed(
      preds,
      TZ,
      "2026-10-08",
      Date.parse("2026-10-08T08:00:00Z"),
      Number.POSITIVE_INFINITY,
    );
    const r1today = rows.find(
      (r) => r.attractionId === "r1" && r.targetDate === "2026-10-08",
    )!;
    expect(r1today.waits).toEqual([20, 25, null, 30]);
    expect(r1today.sources).toBe("cp-c");
    expect(r1today.bands).toEqual([12, 12, null, null]);
    expect(r1today.slotMinutes).toBe(15);
    expect(r1today.leadDays).toBe(0);
    expect(r1today.modelVersion).toBe("v20261008");
    expect(r1today.slotStart.toISOString()).toBe("2026-10-08T08:00:00.000Z");

    const r1tomorrow = rows.find(
      (r) => r.attractionId === "r1" && r.targetDate === "2026-10-09",
    )!;
    expect(r1tomorrow.leadDays).toBe(1);
    expect(r1tomorrow.waits).toEqual([5]);

    // The daily row is not part of the intraday curve.
    expect(rows.find((r) => r.attractionId === "r2")!.waits).toEqual([40]);
  });

  it("keeps a midnight-crossing evening on its operating day", () => {
    const rows = ForecastArchiveService.curvesFromServed(
      [
        pred("r1", "2026-10-08T21:45:00Z", 20), // 23:45 local
        pred("r1", "2026-10-08T22:30:00Z", 10), // 00:30 local on the 9th
      ],
      TZ,
      "2026-10-08",
      0,
      Number.POSITIVE_INFINITY,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].targetDate).toBe("2026-10-08");
    expect(rows[0].waits).toEqual([20, null, null, 10]);
  });

  it("an intraday origin keeps only its window", () => {
    const preds = [0, 1, 2, 3, 4, 5].map((i) =>
      pred(
        "r1",
        new Date(
          Date.parse("2026-10-08T08:00:00Z") + i * 3_600_000,
        ).toISOString(),
        10,
      ),
    );
    const rows = ForecastArchiveService.curvesFromServed(
      preds,
      TZ,
      "2026-10-08",
      Date.parse("2026-10-08T08:00:00Z"),
      Date.parse("2026-10-08T12:00:00Z"),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].waits.filter((w) => w !== null)).toHaveLength(4);
  });
});

describe("ForecastArchiveService.curvesFromServed — Chronos served (PAR-911)", () => {
  it("codes a Chronos-served slot `s` and strips both served suffixes", () => {
    const rows = ForecastArchiveService.curvesFromServed(
      [
        pred("r1", "2026-10-08T08:00:00Z", 20, "v1+pcn"),
        pred("r1", "2026-10-08T08:15:00Z", 25, "v1+chronos"),
        pred("r1", "2026-10-08T08:30:00Z", 30, "v1"),
      ],
      TZ,
      "2026-10-08",
      0,
      Number.POSITIVE_INFINITY,
    );
    expect(rows[0].sources).toBe("psc");
    expect(rows[0].modelVersion).toBe("v1");
    expect(rows[0].surface).toBe(1);
  });

  it("writes the incumbent read under the surface it is given", () => {
    const rows = ForecastArchiveService.curvesFromServed(
      [pred("r1", "2026-10-08T08:00:00Z", 20)],
      TZ,
      "2026-10-08",
      0,
      Number.POSITIVE_INFINITY,
      8,
    );
    expect(rows[0].surface).toBe(8);
  });
});

describe("ForecastArchiveService.isChronosRefreshHour (PAR-911)", () => {
  it("is the odd hours between the intraday origins, on the park's clock", () => {
    const at = (iso: string) =>
      ForecastArchiveService.isChronosRefreshHour(new Date(iso), TZ);
    // Berlin is UTC+2 in October.
    expect(at("2026-10-08T09:05:00Z")).toBe(true); // 11
    expect(at("2026-10-08T15:05:00Z")).toBe(true); // 17
    expect(at("2026-10-08T10:05:00Z")).toBe(false); // 12 — an origin
    expect(at("2026-10-08T07:05:00Z")).toBe(false); // 09 — outside 10–18
    expect(at("2026-10-08T17:05:00Z")).toBe(false); // 19 — outside 10–18
  });
});

describe("ForecastArchiveService.curvesFromPlanDay", () => {
  const plan = (tier: PlanDayDto["tier"], rides: PlanDayDto["rides"]) =>
    ({ tier, rides }) as unknown as PlanDayDto;

  it("stores the planner's hours with source per hour and the dayPeak band", () => {
    const rows = ForecastArchiveService.curvesFromPlanDay(
      plan("measured", [
        {
          attractionSlug: "taron",
          hours: [
            { hour: 10, wait: 30 },
            { hour: 11, wait: 45, source: "composed" },
          ],
          dayPeak: 55,
          uncertaintyMinutes: 9,
          expectedError: 14,
        },
        {
          attractionSlug: "unknown-ride",
          hours: [{ hour: 10, wait: 5 }],
          dayPeak: 5,
        },
      ] as unknown as PlanDayDto["rides"]),
      new Map([["taron", "id-taron"]]),
      TZ,
      "2026-10-08",
      2,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      attractionId: "id-taron",
      targetDate: "2026-10-08",
      leadDays: 2,
      slotMinutes: 60,
      waits: [30, 45],
      sources: "mk",
      // The band is around dayPeak, not around each hour.
      bands: null,
      peakBand: 9,
      dayPeak: 55,
      expectedError: 14,
    });
    // 10:00 CEST = 08:00 UTC.
    expect(rows[0].slotStart.toISOString()).toBe("2026-10-08T08:00:00.000Z");
  });

  it("resolves a first hour past midnight to the next calendar day", () => {
    const rows = ForecastArchiveService.curvesFromPlanDay(
      plan("composed", [
        {
          attractionSlug: "late",
          hours: [
            { hour: 24, wait: 10 },
            { hour: 25, wait: 5 },
          ],
          dayPeak: 10,
        },
      ] as unknown as PlanDayDto["rides"]),
      new Map([["late", "id-late"]]),
      "America/Toronto",
      "2026-10-08",
      0,
    );
    // Midnight of the 9th in Toronto (EDT, UTC-4) = 04:00 UTC.
    expect(rows[0].slotStart.toISOString()).toBe("2026-10-09T04:00:00.000Z");
    expect(rows[0].sources).toBe("kk");
    expect(rows[0].peakBand).toBeNull();
  });

  // PAR-834: the H5 composer serves quarter-hours, and the board has to tell
  // it from the old composer to compare the two.
  it("archives a ride with slots at 15 minutes, coded by its composer", () => {
    const rows = ForecastArchiveService.curvesFromPlanDay(
      plan("measured", [
        {
          attractionSlug: "taron",
          hours: [
            { hour: 10, wait: 30 },
            { hour: 11, wait: 40, source: "composed" },
          ],
          slots: [
            { minute: 600, start: "2026-10-08T08:00:00.000Z", wait: 28 },
            { minute: 615, start: "2026-10-08T08:15:00.000Z", wait: 31 },
            {
              minute: 660,
              start: "2026-10-08T09:00:00.000Z",
              wait: 38,
              source: "composed",
            },
            {
              minute: 675,
              start: "2026-10-08T09:15:00.000Z",
              wait: 42,
              source: "composed",
            },
          ],
          composer: "h5_tft",
          dayPeak: 55,
        },
      ] as unknown as PlanDayDto["rides"]),
      new Map([["taron", "id-taron"]]),
      TZ,
      "2026-10-08",
      1,
    );
    expect(rows[0]).toMatchObject({
      slotMinutes: 15,
      // 10:30 and 10:45 have no slot, but hour 10 HAS slots, so they are not
      // padded with the hour's value.
      waits: [28, 31, null, null, 38, 42],
      sources: "mm--tt",
    });
    expect(rows[0].slotStart.toISOString()).toBe("2026-10-08T08:00:00.000Z");
  });

  it("fills an hour without slots from the hour, and codes the plain profile", () => {
    const rows = ForecastArchiveService.curvesFromPlanDay(
      plan("composed", [
        {
          attractionSlug: "taron",
          hours: [
            { hour: 10, wait: 30 },
            { hour: 11, wait: 40 },
          ],
          slots: [{ minute: 660, start: "2026-10-08T09:00:00.000Z", wait: 38 }],
          composer: "h5",
          dayPeak: 55,
        },
      ] as unknown as PlanDayDto["rides"]),
      new Map([["taron", "id-taron"]]),
      TZ,
      "2026-10-08",
      5,
    );
    expect(rows[0].waits).toEqual([30, 30, 30, 30, 38]);
    expect(rows[0].sources).toBe("hhhhh");
    // 10:00 CEST, resolved from the hour because no slot starts there.
    expect(rows[0].slotStart.toISOString()).toBe("2026-10-08T08:00:00.000Z");
  });
});

describe("ForecastArchiveService.nextRideRows (D1)", () => {
  it("applies the frontend rule on hour starts and keeps the look-ahead hours", () => {
    const plan = {
      tier: "measured",
      context: { openHour: 10, closeHour: 18 },
      rides: [
        {
          attractionSlug: "taron",
          hours: [10, 11, 12, 13, 14].map((hour) => ({
            hour,
            wait: [20, 30, 50, 40, 60][hour - 10],
          })),
          dayPeak: 60,
        },
      ],
    } as unknown as PlanDayDto;
    // 10:10 CEST: hours starting 11:00 and 12:00 qualify (13:00 is 170 min out).
    const rows = ForecastArchiveService.nextRideRows(
      plan,
      new Map([["taron", "id-taron"]]),
      TZ,
      "2026-10-08",
      new Date("2026-10-08T08:10:00Z"),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].laterWait).toBe(50);
    expect(rows[0].laterAt!.toISOString()).toBe("2026-10-08T10:00:00.000Z");
    // 10:00 (begun) through 12:00 kept for audit.
    expect(rows[0].waits).toEqual([20, 30, 50]);
    expect(rows[0].slotStart.toISOString()).toBe("2026-10-08T08:00:00.000Z");
  });
});

describe("ForecastArchiveService.capturePark", () => {
  function build(
    served: PredictionDto[],
    opts: {
      planThrows?: boolean;
      shadow?: (date: string) => PlanDayShadowCurve[];
      chronos?: ChronosResponse | null;
      catboostDaily?: PredictionDto[];
    } = {},
  ) {
    const inserted: unknown[][] = [];
    const qb = {
      insert: () => qb,
      into: () => qb,
      values: (v: unknown[]) => {
        inserted.push(v);
        return qb;
      },
      orIgnore: () => qb,
      execute: async () => ({}),
    };
    const sql: string[] = [];
    const em = {
      query: async (q: string) => {
        sql.push(q);
        return q.includes("DISTINCT ON")
          ? [
              {
                id: "id-taron",
                status: "OPERATING",
                wait: 35,
                ts: new Date(Date.now() - 50 * 60_000),
              },
            ]
          : [];
      },
    };
    const repo = {
      create: (x: unknown) => x,
      createQueryBuilder: () => qb,
      manager: {
        connection: {
          transaction: async (fn: (m: unknown) => unknown) => fn(em),
        },
        // Curves and park-days are written in one transaction.
        transaction: async (fn: (m: unknown) => unknown) =>
          fn({ createQueryBuilder: () => qb }),
      },
    };
    const ml = {
      getParkPredictions: jest
        .fn()
        .mockImplementation(async (_id: string, type: string) => ({
          predictions: type === "daily" ? (opts.catboostDaily ?? []) : served,
        })),
      getRawParkPredictions: jest.fn(),
      getServingDailyPredictions: jest.fn().mockResolvedValue({
        predictions: [
          {
            attractionId: "id-taron",
            predictedTime: "2026-10-08T00:00:00.000Z",
            modelVersion: "tft",
          },
          {
            attractionId: "id-taron",
            predictedTime: "2026-10-18T00:00:00.000Z",
            modelVersion: "v20261008",
          },
        ],
      }),
    };
    const planDay = {
      buildPlanDay: jest
        .fn()
        .mockImplementation(async (_p: Park, date: string) => {
          if (opts.planThrows && date === "2026-10-09") {
            throw new Error("no tlist entry for key 6");
          }
          const has = date === "2026-10-08" || date === "2026-10-18";
          return {
            tier: "measured",
            context: {
              status: "OPERATING",
              crowdLevel: "high",
              openHour: 10,
              closeHour: 18,
            },
            accuracy: { basis: "measured", typicalError: 9 },
            leadTimeMae: 12,
            rides: has
              ? [
                  {
                    attractionSlug: "taron",
                    hours: [10, 11, 12, 13].map((hour) => ({
                      hour,
                      wait: 30,
                    })),
                    dayPeak: 40,
                  },
                ]
              : [],
            ridesUnavailable: has ? undefined : { reason: "no_forecast" },
          };
        }),
      // The capture path: the same plan, plus the composer A/B.
      buildPlanDayWithShadow: jest.fn(),
    };
    planDay.buildPlanDayWithShadow.mockImplementation(
      async (p: Park, date: string) => ({
        plan: await planDay.buildPlanDay(p, date),
        shadow: opts.shadow?.(date) ?? [],
      }),
    );
    const calendar = {
      buildCalendarResponse: jest.fn().mockResolvedValue({
        days: [{ date: "2026-10-08", predictedCrowdLevel: "moderate" }],
      }),
    };
    const analytics = {
      getHeadlinerAttractions: jest
        .fn()
        .mockResolvedValue([{ attractionId: "id-taron" }]),
      getTypicalDayPeakFromCache: jest.fn().mockResolvedValue(42),
    };
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    const attractions = {
      find: jest.fn().mockResolvedValue([{ id: "id-taron", slug: "taron" }]),
    };
    const chronos = {
      forecast: jest.fn().mockResolvedValue(opts.chronos ?? null),
    };
    const service = new ForecastArchiveService(
      {} as never,
      attractions as never,
      repo as never,
      repo as never,
      ml as never,
      planDay as never,
      calendar as never,
      analytics as never,
      redis as never,
      chronos as never,
    );
    return { service, ml, planDay, inserted, redis, sql, chronos };
  }
  const park = { id: "p1", slug: "phl", timezone: TZ } as Park;

  afterEach(() => jest.useRealTimers());

  it("archives the SERVED curve (never the raw one) and d0…d7 of the planner", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    const { service, ml, planDay, inserted, redis, sql } = build([
      pred("id-taron", "2026-10-08T08:00:00Z", 20, "v1+pcn"),
    ]);
    const res = await service.capturePark(park, "daily", new Date());

    expect(ml.getParkPredictions).toHaveBeenCalledWith("p1", "hourly");
    expect(ml.getRawParkPredictions).not.toHaveBeenCalled();
    expect(ml.getServingDailyPredictions).toHaveBeenCalledWith("p1");
    expect(planDay.buildPlanDay).toHaveBeenCalledTimes(8);
    expect(res).toEqual({ curves: 2, parkDays: 8 });
    // Every read carries its own limits (statement-limits.util).
    expect(sql.some((q) => q.startsWith("SET LOCAL statement_timeout"))).toBe(
      true,
    );
    expect(sql.some((q) => q.startsWith("SET LOCAL idle_in_transaction"))).toBe(
      true,
    );

    const curves = inserted[0] as Array<Record<string, unknown>>;
    const served = curves.find((c) => c.surface === 1)!;
    const planned = curves.find((c) => c.surface === 2)!;
    expect(served).toMatchObject({
      parkId: "p1",
      isHeadliner: true,
      originKind: 1,
      liveWait: 35,
      liveAgeMin: 50,
      levelSource: null,
    });
    expect(planned).toMatchObject({ liveWait: null, levelSource: "tft" });

    const parkDays = inserted[1] as Array<Record<string, unknown>>;
    expect(parkDays[0]).toMatchObject({
      targetDate: "2026-10-08",
      predictedCrowdLevel: "moderate",
      crowdLevelFallback: false,
      crowdLevel: "high",
      typicalDayPeak: 42,
      ridesOffered: 1,
      ridesUnavailable: null,
      typicalError: 9,
      levelSource: "tft",
    });
    expect(parkDays[1]).toMatchObject({
      ridesOffered: 0,
      ridesUnavailable: "no_forecast",
      crowdLevelFallback: true,
      levelSource: "none",
    });
    expect(redis.set).toHaveBeenCalledWith(
      "forecast-archive:active:p1:2026-10-08",
      "1",
      "EX",
      26 * 3600,
    );
  });

  // PAR-1042: CatBoost's level beside TFT's, only on the ride-days TFT levelled.
  it("stores the CatBoost day level as a shadow on TFT-levelled planner rows only", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    const { service, ml, inserted } = build(
      [pred("id-taron", "2026-10-08T08:00:00Z", 20, "v1+pcn")],
      {
        catboostDaily: [
          pred("id-taron", "2026-10-08T00:00:00.000Z", 55.4),
          pred("id-taron", "2026-10-18T00:00:00.000Z", 61),
        ],
      },
    );
    await service.capturePark(park, "daily", new Date());
    // One extra read per park capture: the cached CatBoost daily answer.
    expect(
      ml.getParkPredictions.mock.calls.filter((c) => c[1] === "daily"),
    ).toHaveLength(1);
    expect(ml.getServingDailyPredictions).toHaveBeenCalledTimes(1);
    const curves = inserted[0] as Array<Record<string, unknown>>;
    const planned = curves.find((c) => c.surface === 2)!;
    expect(planned).toMatchObject({
      targetDate: "2026-10-08",
      levelSource: "tft",
      dayPeak: 40,
      catboostDayPeak: 55,
    });
    expect(curves.find((c) => c.surface === 1)!.catboostDayPeak).toBeNull();
  });

  it("leaves the shadow empty where CatBoost served the level itself", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T05:05:00Z"));
    const { service, inserted } = build([], {
      catboostDaily: [pred("id-taron", "2026-10-18T00:00:00.000Z", 61)],
    });
    await service.capturePark(park, "long", new Date());
    const planned = (inserted[0] as Array<Record<string, unknown>>).find(
      (c) => c.surface === 2,
    )!;
    expect(planned).toMatchObject({
      targetDate: "2026-10-18",
      levelSource: "catboost",
      catboostDayPeak: null,
    });
  });

  it("records a plan that threw as an empty plan (D9), not a crash", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    const { service, inserted } = build([], { planThrows: true });
    const res = await service.capturePark(park, "daily", new Date());
    expect(res.parkDays).toBe(8);
    const parkDays = inserted[1] as Array<Record<string, unknown>>;
    expect(parkDays.find((d) => d.targetDate === "2026-10-09")).toMatchObject({
      ridesOffered: 0,
      ridesUnavailable: ForecastArchiveService.CAPTURE_ERROR_REASON,
      tier: null,
    });
  });

  it("writes the composer A/B under its own surfaces, one row per composer", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T05:05:00Z"));
    const slot = (minute: number, wait: number, code: "k" | "h" | "t") => ({
      minute,
      start: new Date(Date.UTC(2026, 9, 18, 0, minute - 120)).toISOString(),
      wait,
      code,
    });
    const { service, inserted } = build([], {
      shadow: (date) =>
        date === "2026-10-18"
          ? [
              {
                attractionId: "id-taron",
                composer: "peak_scaled",
                slots: [slot(600, 30, "k"), slot(630, 30, "k")],
              },
              {
                attractionId: "id-taron",
                composer: "h5",
                slots: [slot(600, 22, "h"), slot(630, 27, "h")],
              },
              {
                attractionId: "id-taron",
                composer: "h5_routed",
                slots: [slot(600, 22, "h"), slot(630, 27, "h")],
              },
            ]
          : [],
    });
    const res = await service.capturePark(park, "long", new Date());
    // The served planner row plus three shadow rows.
    expect(res.curves).toBe(4);
    const rows = inserted[0] as Array<Record<string, unknown>>;
    const shadow = rows.filter((r) => (r.surface as number) >= 3);
    expect(shadow.map((r) => r.surface)).toEqual([3, 4, 5]);
    expect(shadow[1]).toMatchObject({
      slotMinutes: 15,
      waits: [22, null, 27],
      sources: "h-h",
      leadDays: 10,
      dayPeak: null,
      // Level source and the live anchor belong to the served planner row.
      levelSource: null,
      liveWait: null,
    });
    expect((shadow[0].slotStart as Date).toISOString()).toBe(
      "2026-10-18T08:00:00.000Z",
    );
  });

  it("the long-lead origin builds only d10 … d90 and no served curve", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T05:05:00Z"));
    const { service, ml, planDay, inserted } = build([]);
    const res = await service.capturePark(park, "long", new Date());
    // Only the cached CatBoost daily read (PAR-1042), never a served curve.
    expect(ml.getParkPredictions.mock.calls).toEqual([["p1", "daily"]]);
    expect(planDay.buildPlanDay.mock.calls.map((c: unknown[]) => c[1])).toEqual(
      [
        "2026-10-18",
        "2026-10-22",
        "2026-10-29",
        "2026-11-07",
        "2026-11-22",
        "2026-12-07",
        "2027-01-06",
      ],
    );
    expect(res).toEqual({ curves: 1, parkDays: 7 });
    const curve = (inserted[0] as Array<Record<string, unknown>>)[0];
    expect(curve).toMatchObject({
      leadDays: 10,
      originKind: 3,
      levelSource: "catboost",
    });
  });

  it("an intraday origin archives the served window and the D1 decision", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T08:10:00Z"));
    const { service, planDay, inserted } = build([
      pred("id-taron", "2026-10-08T08:15:00Z", 20),
    ]);
    const res = await service.capturePark(park, "intraday", new Date());
    expect(planDay.buildPlanDay).toHaveBeenCalledTimes(1);
    expect(planDay.buildPlanDay).toHaveBeenCalledWith(park, "2026-10-08");
    expect(res).toEqual({ curves: 2, parkDays: 0 });
    const nextRide = (inserted[0] as Array<Record<string, unknown>>).find(
      (c) => c.surface === 2,
    )!;
    expect(nextRide).toMatchObject({
      originKind: 2,
      liveWait: 35,
      laterWait: 30,
      levelSource: null,
    });
  });
});

describe("ForecastArchiveService — Chronos-2 shadow (PAR-852)", () => {
  const park = { id: "p1", slug: "phl", timezone: TZ } as Park;
  const response = (date: string, start: string): ChronosResponse => ({
    status: "ok",
    revision: "29ec3766d36d6f73f0696f85560a422f50e8498c",
    curves: [
      {
        attractionId: "id-taron",
        date,
        slotStart: start,
        slotMinutes: 15,
        covariates: { holidays: "short", schedule: "published" },
        q50: [10.4, null, 30, 40, 50],
        q80: [12, null, 33, 44, 55],
        q95: [20, null, 25, null, 70],
      },
      // A ride the park does not know (merged away) is dropped.
      {
        attractionId: "ghost",
        date,
        slotStart: start,
        slotMinutes: 15,
        covariates: { holidays: "full", schedule: "published" },
        q50: [1],
        q80: [1],
        q95: [1],
      },
    ],
  });

  function build(chronos: ChronosResponse | null) {
    const inserted: unknown[][] = [];
    const qb = {
      insert: () => qb,
      into: () => qb,
      values: (v: unknown[]) => {
        inserted.push(v);
        return qb;
      },
      orIgnore: () => qb,
      execute: async () => ({}),
    };
    const em = {
      query: async (q: string) =>
        q.includes("DISTINCT ON")
          ? [
              {
                id: "id-taron",
                status: "OPERATING",
                wait: 35,
                ts: new Date(Date.now() - 10 * 60_000),
              },
            ]
          : [],
    };
    const repo = {
      create: (x: unknown) => x,
      manager: {
        connection: {
          transaction: async (fn: (m: unknown) => unknown) => fn(em),
        },
        transaction: async (fn: (m: unknown) => unknown) =>
          fn({ createQueryBuilder: () => qb }),
      },
    };
    const plan = {
      tier: "measured",
      context: { openHour: 10, closeHour: 18 },
      rides: [],
    };
    const shadow = {
      forecast: jest.fn().mockResolvedValue(chronos),
    };
    const ml = {
      getParkPredictions: jest.fn().mockResolvedValue({ predictions: [] }),
      getServingDailyPredictions: jest
        .fn()
        .mockResolvedValue({ predictions: [] }),
    };
    const cacheSet = jest.fn();
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
      pipeline: () => ({
        set: cacheSet,
        exec: jest.fn().mockResolvedValue([]),
      }),
    };
    const service = new ForecastArchiveService(
      {} as never,
      {
        find: jest.fn().mockResolvedValue([{ id: "id-taron", slug: "taron" }]),
      } as never,
      repo as never,
      repo as never,
      ml as never,
      {
        buildPlanDay: jest.fn().mockResolvedValue(plan),
        buildPlanDayWithShadow: jest
          .fn()
          .mockResolvedValue({ plan, shadow: [] }),
      } as never,
      {
        buildCalendarResponse: jest.fn().mockResolvedValue({ days: [] }),
      } as never,
      {
        getHeadlinerAttractions: jest.fn().mockResolvedValue([]),
        getTypicalDayPeakFromCache: jest.fn().mockResolvedValue(0),
      } as never,
      redis as never,
      shadow as never,
    );
    return { service, shadow, inserted, ml, cacheSet };
  }

  const prevServe = process.env.SERVE_CHRONOS_INTRADAY;
  afterEach(() => {
    jest.useRealTimers();
    if (prevServe === undefined) delete process.env.SERVE_CHRONOS_INTRADAY;
    else process.env.SERVE_CHRONOS_INTRADAY = prevServe;
  });

  // PAR-911: the live curves feed the serving cache, and with the flag on the
  // archive also keeps the incumbent so the live A/B keeps a reference.
  it("writes the live curves to the serving cache, flag on or off", async () => {
    delete process.env.SERVE_CHRONOS_INTRADAY;
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T10:05:00Z"));
    const { service, ml, cacheSet, inserted } = build(
      response("2026-10-08", "2026-10-08T10:15:00Z"),
    );
    const origin = new Date();
    await service.capturePark(park, "intraday", origin);
    expect(cacheSet).toHaveBeenCalledTimes(2); // id-taron and the ghost ride
    expect(cacheSet.mock.calls[0][0]).toBe("ml:chronos-serve:id-taron");
    expect(JSON.parse(cacheSet.mock.calls[0][1])).toMatchObject({
      originAt: origin.getTime(),
      revision: "29ec3766d36d6f73f0696f85560a422f50e8498c",
    });
    // Flag off: one served read, no incumbent surface.
    expect(ml.getParkPredictions).toHaveBeenCalledTimes(1);
    expect(
      (inserted[0] as Array<Record<string, unknown>>).some(
        (r) => r.surface === 8,
      ),
    ).toBe(false);
  });

  it("with the flag on, caches before the served read and archives the incumbent as surface 8", async () => {
    process.env.SERVE_CHRONOS_INTRADAY = "true";
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T10:05:00Z"));
    const { service, ml, cacheSet, inserted } = build(
      response("2026-10-08", "2026-10-08T10:15:00Z"),
    );
    const order: string[] = [];
    cacheSet.mockImplementation(() => order.push("cache"));
    ml.getParkPredictions.mockImplementation(
      async (_id: string, _t: string, _m: unknown, _l: unknown, o?: object) => {
        order.push(o ? "incumbent" : "served");
        return {
          predictions: [
            pred(
              "id-taron",
              "2026-10-08T11:00:00Z",
              o ? 50 : 40,
              o ? "v1+pcn" : "v1+chronos",
            ),
          ],
        };
      },
    );
    await service.capturePark(park, "intraday", new Date());
    expect(order.indexOf("cache")).toBeLessThan(order.indexOf("served"));
    expect(ml.getParkPredictions).toHaveBeenLastCalledWith(
      "p1",
      "hourly",
      undefined,
      undefined,
      { chronos: false },
    );
    const rows = inserted[0] as Array<Record<string, unknown>>;
    expect(rows.find((r) => r.surface === 1)).toMatchObject({
      waits: [40],
      sources: "s",
      modelVersion: "v1",
    });
    expect(rows.find((r) => r.surface === 8)).toMatchObject({
      waits: [50],
      sources: "p",
    });
  });

  it("captures the grid variant at the 06:00 origin as surface 7, d0 … d7", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    const { service, shadow, inserted } = build(
      response("2026-10-10", "2026-10-10T08:00:00Z"),
    );
    const origin = new Date();
    await service.capturePark(park, "daily", origin);
    expect(shadow.forecast).toHaveBeenCalledWith("p1", origin, "grid");
    const rows = (inserted[0] as Array<Record<string, unknown>>).filter(
      (r) => r.surface === 7,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      originAt: origin,
      leadDays: 2,
      slotMinutes: 15,
      waits: [10, null, 30, 40, 50],
      sources: "g-ggg",
      // q95 − q50, one-sided; never below 0 (30 → 25 is clipped to 0).
      bands: [10, null, 0, null, 20],
      dayPeak: 47, // P90 of 10, 30, 40, 50
      modelVersion: "chronos-2@29ec376;hol=short",
      liveWait: null,
    });
  });

  it("captures the live variant at an intraday origin as surface 6, with the persistence anchor", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T10:05:00Z"));
    const { service, shadow, inserted } = build(
      response("2026-10-08", "2026-10-08T10:15:00Z"),
    );
    const origin = new Date();
    await service.capturePark(park, "intraday", origin);
    expect(shadow.forecast).toHaveBeenCalledWith("p1", origin, "live", 240);
    const row = (inserted[0] as Array<Record<string, unknown>>).find(
      (r) => r.surface === 6,
    )!;
    expect(row).toMatchObject({
      sources: "x-xxx",
      leadDays: 0,
      dayPeak: null,
      liveWait: 35,
      liveAgeMin: 10,
    });
  });

  it("a missing shadow costs the capture nothing, and the long-lead origin never asks", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    const off = build(null);
    const res = await off.service.capturePark(park, "daily", new Date());
    expect(res.parkDays).toBe(8);
    expect(
      (off.inserted[0] ?? []).some(
        (r) => (r as Record<string, unknown>).surface === 7,
      ),
    ).toBe(false);

    jest.setSystemTime(new Date("2026-10-08T05:05:00Z"));
    const long = build(null);
    await long.service.capturePark(park, "long", new Date());
    expect(long.shadow.forecast).not.toHaveBeenCalled();
  });

  it("drops days outside the variant's leads", () => {
    const rows = ForecastArchiveService.curvesFromChronos(
      response("2026-10-11", "2026-10-11T08:00:00Z"),
      6,
      "2026-10-08",
      new Set(["id-taron"]),
      1,
    );
    expect(rows).toEqual([]);
  });
});

describe("ForecastArchiveService.captureDue", () => {
  it("takes each park's origin from the clock right before its capture, and claims the hour after", async () => {
    const service = Object.create(
      ForecastArchiveService.prototype,
    ) as ForecastArchiveService;
    const parks = [
      { id: "a", slug: "a", timezone: TZ },
      { id: "b", slug: "b", timezone: TZ },
    ];
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    const chronos = {
      beginRun: jest.fn(),
      endRun: jest.fn().mockReturnValue({
        calls: 2,
        ok: 1,
        failures: 0,
        warming: 1,
        skipped: 0,
      }),
    };
    Object.assign(service, {
      parkRepository: { find: jest.fn().mockResolvedValue(parks) },
      redis,
      logger: { warn: jest.fn(), debug: jest.fn() },
      chronos,
    });
    const origins: number[] = [];
    const capture = jest
      .spyOn(service, "capturePark")
      .mockImplementation(async (p, _k, at) => {
        origins.push(at.getTime());
        if (p.id === "b") throw new Error("boom");
        return { curves: 1, parkDays: 0 };
      });
    let t = Date.parse("2026-10-08T04:05:00Z");
    const res = await service.captureDue(() => new Date((t += 60_000)));
    expect(capture).toHaveBeenCalledTimes(2);
    expect(new Set(origins).size).toBe(2);
    expect(res).toMatchObject({ parks: 1, failed: 1 });
    // Only the park that succeeded is marked done for this origin hour; the
    // second write is the run's Chronos summary for the admin overview.
    expect(redis.set).toHaveBeenCalledTimes(2);
    expect(redis.set.mock.calls[0][0]).toBe(
      "forecast-archive:origin:a:2026-10-08:06",
    );
    expect(redis.set.mock.calls[1][0]).toBe(CHRONOS_LAST_RUN_KEY);
    expect(JSON.parse(redis.set.mock.calls[1][1])).toMatchObject({
      calls: 2,
      ok: 1,
      warming: 1,
    });
    // One breaker / one log line per run for the Chronos shadow.
    expect(chronos.beginRun).toHaveBeenCalledTimes(1);
    expect(chronos.endRun).toHaveBeenCalledTimes(1);
  });

  describe("Chronos serving refresh between the origins (PAR-911)", () => {
    const prev = process.env.SERVE_CHRONOS_INTRADAY;
    afterEach(() => {
      if (prev === undefined) delete process.env.SERVE_CHRONOS_INTRADAY;
      else process.env.SERVE_CHRONOS_INTRADAY = prev;
    });

    function setup() {
      const service = Object.create(
        ForecastArchiveService.prototype,
      ) as ForecastArchiveService;
      const parks = [
        { id: "open-park", slug: "a", timezone: TZ },
        { id: "closed", slug: "c", timezone: TZ },
      ];
      const cacheSet = jest.fn();
      const redis = {
        // Only the first park's daily capture found a curve for today.
        get: jest.fn((k: string) =>
          Promise.resolve(k.includes(":open-park:") ? "1" : null),
        ),
        set: jest.fn().mockResolvedValue("OK"),
        pipeline: () => ({ set: cacheSet, exec: jest.fn() }),
      };
      const chronos = {
        beginRun: jest.fn(),
        endRun: jest.fn().mockReturnValue({
          calls: 1,
          ok: 1,
          failures: 0,
          warming: 0,
          skipped: 0,
        }),
        forecast: jest.fn().mockResolvedValue({
          status: "ok",
          revision: "r",
          curves: [
            {
              attractionId: "ride",
              date: "2026-10-08",
              slotStart: "2026-10-08T09:15:00Z",
              slotMinutes: 15,
              covariates: { holidays: "full", schedule: "published" },
              q50: [30],
              q80: [40],
              q95: [50],
            },
          ],
        }),
      };
      Object.assign(service, {
        parkRepository: { find: jest.fn().mockResolvedValue(parks) },
        redis,
        logger: { warn: jest.fn(), debug: jest.fn() },
        chronos,
      });
      const capture = jest.spyOn(service, "capturePark");
      return { service, chronos, cacheSet, capture, redis };
    }

    // 09:05 UTC = 11:05 in Berlin: no archive origin, a refresh hour.
    const clock = () => new Date("2026-10-08T09:05:00Z");

    it("refreshes active parks only, writes the cache and no archive rows", async () => {
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      const { service, chronos, cacheSet, capture, redis } = setup();
      const res = await service.captureDue(clock);
      expect(capture).not.toHaveBeenCalled();
      expect(chronos.forecast).toHaveBeenCalledTimes(1);
      expect(chronos.forecast).toHaveBeenCalledWith(
        "open-park",
        expect.any(Date),
        "live",
        240,
      );
      expect(cacheSet.mock.calls[0][0]).toBe("ml:chronos-serve:ride");
      expect(res.chronosRefreshed).toBe(1);
      // Claimed once per park and hour (NX), so a re-run asks no second time.
      expect(redis.set).toHaveBeenCalledWith(
        "chronos-serve:refresh:open-park:2026-10-08:11",
        "1",
        "EX",
        3 * 3600,
        "NX",
      );
    });

    it("costs the GPU nothing while the flag is off", async () => {
      delete process.env.SERVE_CHRONOS_INTRADAY;
      const { service, chronos } = setup();
      const res = await service.captureDue(clock);
      expect(chronos.forecast).not.toHaveBeenCalled();
      expect(res.chronosRefreshed).toBeUndefined();
    });
  });
});
