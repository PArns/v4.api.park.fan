import { ForecastArchiveService } from "./forecast-archive.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { PlanDayDto } from "../dto/plan-day.dto";
import { Park } from "../entities/park.entity";

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
  function build(served: PredictionDto[], opts: { planThrows?: boolean } = {}) {
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
      },
    };
    const ml = {
      getParkPredictions: jest.fn().mockResolvedValue({ predictions: served }),
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
    };
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
    );
    return { service, ml, planDay, inserted, redis, sql };
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

  it("the long-lead origin builds only d10 … d90 and no served curve", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T05:05:00Z"));
    const { service, ml, planDay, inserted } = build([]);
    const res = await service.capturePark(park, "long", new Date());
    expect(ml.getParkPredictions).not.toHaveBeenCalled();
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
    Object.assign(service, {
      parkRepository: { find: jest.fn().mockResolvedValue(parks) },
      redis,
      logger: { warn: jest.fn(), debug: jest.fn() },
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
    // Only the park that succeeded is marked done for this origin hour.
    expect(redis.set).toHaveBeenCalledTimes(1);
    expect(redis.set.mock.calls[0][0]).toBe(
      "forecast-archive:origin:a:2026-10-08:06",
    );
  });
});
