import { ForecastArchiveService } from "./forecast-archive.service";
import { ForecastArchiveScoringService } from "./forecast-archive-scoring.service";
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
    // 04:05 UTC = 06:05 in Berlin (CEST) → daily; 22:05 New York the day before.
    const at = new Date("2026-10-08T04:05:00Z");
    expect(ForecastArchiveService.originKindAt(at, TZ)).toBe("daily");
    expect(
      ForecastArchiveService.originKindAt(at, "America/New_York"),
    ).toBeNull();
    expect(
      ForecastArchiveService.originKindAt(new Date("2026-10-08T10:05:00Z"), TZ),
    ).toBe("intraday");
    expect(
      ForecastArchiveService.originKindAt(new Date("2026-10-08T09:05:00Z"), TZ),
    ).toBeNull();
  });
});

describe("ForecastArchiveService.curvesFromServed", () => {
  it("splits the served curve per ride and park-local date, keeps PCN per slot", () => {
    const preds = [
      pred("r1", "2026-10-08T08:00:00Z", 20),
      pred("r1", "2026-10-08T08:15:00Z", 25, "v20261008+pcn"),
      // 08:30 missing — a gap the row must keep, not close up.
      pred("r1", "2026-10-08T08:45:00Z", 30, "v20261008", null),
      // 22:00 UTC is already the 9th in Berlin.
      pred("r1", "2026-10-08T22:00:00Z", 5),
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
    // 08:00 … 11:00 hourly = 13 quarter-hour slots, four of them filled.
    expect(rows[0].waits.filter((w) => w !== null)).toHaveLength(4);
  });
});

describe("ForecastArchiveService.curvesFromPlanDay", () => {
  const plan = (tier: PlanDayDto["tier"], rides: PlanDayDto["rides"]) =>
    ({ tier, rides }) as unknown as PlanDayDto;

  it("stores the planner's hours with source per hour and the ride's band", () => {
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
      bands: [9, 9],
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
    expect(rows[0].bands).toBeNull();
  });
});

describe("ForecastArchiveService.capturePark", () => {
  function build(served: PredictionDto[]) {
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
    const repo = {
      create: (x: unknown) => x,
      createQueryBuilder: () => qb,
      manager: {
        transaction: async (fn: (em: unknown) => unknown) =>
          fn({ query: async () => [] }),
      },
    };
    const ml = {
      getParkPredictions: jest.fn().mockResolvedValue({ predictions: served }),
      getRawParkPredictions: jest.fn(),
    };
    const planDay = {
      buildPlanDay: jest
        .fn()
        .mockImplementation(async (_p: Park, date: string) => ({
          tier: "measured",
          context: {
            status: "OPERATING",
            crowdLevel: "high",
            openHour: 10,
            closeHour: 18,
          },
          accuracy: { basis: "measured", typicalError: 9 },
          leadTimeMae: 12,
          rides:
            date === "2026-10-08"
              ? [
                  {
                    attractionSlug: "taron",
                    hours: [{ hour: 10, wait: 30 }],
                    dayPeak: 40,
                  },
                ]
              : [],
          ridesUnavailable:
            date === "2026-10-08" ? undefined : { reason: "no_forecast" },
        })),
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
    return { service, ml, planDay, inserted, redis };
  }

  it("archives the SERVED curve (never the raw one) and d0…d7 of the planner", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T04:05:00Z"));
    try {
      const { service, ml, planDay, inserted, redis } = build([
        pred("id-taron", "2026-10-08T08:00:00Z", 20, "v1+pcn"),
      ]);
      const park = { id: "p1", slug: "phl", timezone: TZ } as Park;
      const res = await service.capturePark(park, "daily", new Date());

      expect(ml.getParkPredictions).toHaveBeenCalledWith("p1", "hourly");
      expect(ml.getRawParkPredictions).not.toHaveBeenCalled();
      expect(planDay.buildPlanDay).toHaveBeenCalledTimes(8);
      expect(res).toEqual({ curves: 2, parkDays: 8 });

      const curves = inserted[0] as Array<Record<string, unknown>>;
      expect(curves.map((c) => c.surface).sort()).toEqual([1, 2]);
      expect(curves[0]).toMatchObject({
        parkId: "p1",
        isHeadliner: true,
        originKind: 1,
      });

      const parkDays = inserted[1] as Array<Record<string, unknown>>;
      expect(parkDays[0]).toMatchObject({
        targetDate: "2026-10-08",
        predictedCrowdLevel: "moderate",
        crowdLevel: "high",
        ridesOffered: 1,
        ridesUnavailable: null,
        typicalError: 9,
      });
      expect(parkDays[1]).toMatchObject({
        ridesOffered: 0,
        ridesUnavailable: "no_forecast",
      });
      // Today had a served curve → the intraday origins are armed.
      expect(redis.set).toHaveBeenCalledWith(
        "forecast-archive:active:p1:2026-10-08",
        "1",
        "EX",
        26 * 3600,
      );
    } finally {
      jest.useRealTimers();
    }
  });

  it("an intraday origin captures no planner days", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-08T08:05:00Z"));
    try {
      const { service, planDay } = build([
        pred("id-taron", "2026-10-08T08:15:00Z", 20),
      ]);
      const park = { id: "p1", slug: "phl", timezone: TZ } as Park;
      const res = await service.capturePark(park, "intraday", new Date());
      expect(planDay.buildPlanDay).not.toHaveBeenCalled();
      expect(res).toEqual({ curves: 1, parkDays: 0 });
    } finally {
      jest.useRealTimers();
    }
  });
});

describe("ForecastArchiveScoringService board helpers", () => {
  it("derives means from pooled counters and leaves foreign counters null", () => {
    const m = ForecastArchiveScoringService.deriveMetrics({
      n: 4,
      sae: 30,
      se: -10,
      rd: 2,
      hitTop2: 1,
      hit30: 2,
      regret: 10,
    });
    expect(m.mae).toBe(7.5);
    expect(m.bias).toBe(-2.5);
    expect(m.bestTimeHitTop2).toBe(0.5);
    expect(m.bestTimeRegret).toBe(5);
    // `n` is shared with D6 rows; without `exact` it is not a crowd row.
    expect(m.crowdExact).toBeNull();
    expect(m.bandCoverage).toBeNull();
  });

  it("orders slot leads before day leads", () => {
    const leads = ["d1", "h24-48", "d0", "h0-1", "h2-6"];
    expect(
      leads.sort(
        (a, b) =>
          ForecastArchiveScoringService.leadOrder(a) -
          ForecastArchiveScoringService.leadOrder(b),
      ),
    ).toEqual(["h0-1", "h2-6", "h24-48", "d0", "d1"]);
  });
});
