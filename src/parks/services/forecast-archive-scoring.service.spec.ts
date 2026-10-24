import { PAIRED_DEFINITION } from "../../ml/entities/forecast-archive-paired.entity";
import { ForecastArchiveScoringService } from "./forecast-archive-scoring.service";
import { SCORE_KEY_WIDTHS } from "../../ml/entities/forecast-archive-score.entity";

const TZ = "Europe/Berlin";
const T = (iso: string) => new Date(iso);

/**
 * One park, one ride, one window (10:00–12:00 CEST = 08:00–10:00 UTC on
 * 2026-10-08), one d1 plan curve archived at 06:00 the day before.
 */
function build(
  opts: {
    alreadyScored?: string[];
    extraCurves?: object[];
    staleDates?: string[];
    failCurves?: boolean;
  } = {},
) {
  const sql: string[] = [];
  const connection = {
    transaction: async (fn: (em: unknown) => unknown) =>
      fn({
        query: async (q: string) => {
          sql.push(q);
          if (q.includes("schedule_entries")) {
            return [
              {
                open: T("2026-10-08T08:00:00Z"),
                close: T("2026-10-08T10:00:00Z"),
              },
            ];
          }
          if (q.includes("FROM queue_data")) {
            return [
              {
                id: "r1",
                ts: T("2026-10-08T07:55:00Z"),
                status: "OPERATING",
                wait: 20,
              },
              {
                id: "r1",
                ts: T("2026-10-08T08:40:00Z"),
                status: "OPERATING",
                wait: 40,
              },
              {
                id: "r1",
                ts: T("2026-10-08T09:20:00Z"),
                status: "OPERATING",
                wait: 30,
              },
            ];
          }
          return [];
        },
      }),
  };
  const deleted: unknown[] = [];
  const insertedScores: Array<Record<string, unknown>> = [];
  const insertedPaired: Array<Record<string, unknown>> = [];
  const scoreRepository = {
    create: (x: unknown) => x,
    manager: {
      query: jest
        .fn()
        .mockImplementation(async (q: string) =>
          q.includes("SELECT DISTINCT target_date")
            ? (opts.alreadyScored ?? []).map((d) => ({ d }))
            : [],
        ),
      transaction: async (fn: (em: unknown) => unknown) =>
        fn({
          delete: async (e: { name: string }, where: object) =>
            deleted.push({ entity: e.name, ...where }),
          insert: async (
            e: { name: string },
            rows: Array<Record<string, unknown>>,
          ) =>
            (e.name === "ForecastArchivePaired"
              ? insertedPaired
              : insertedScores
            ).push(...rows),
        }),
    },
    createQueryBuilder: jest.fn(),
  };
  const pairedRepository = {
    create: (x: unknown) => x,
    manager: {
      query: jest
        .fn()
        .mockImplementation(async (q: string) =>
          q.includes("HAVING max(definition)")
            ? (opts.staleDates ?? []).map((d) => ({ d }))
            : [],
        ),
    },
    createQueryBuilder: jest.fn().mockImplementation(() => {
      const qb = {
        where: () => qb,
        andWhere: () => qb,
        getMany: async () => [],
      };
      return qb;
    }),
  };
  const curve = {
    originAt: T("2026-10-07T04:00:00Z"),
    attractionId: "r1",
    surface: 2,
    targetDate: "2026-10-08",
    parkId: "p1",
    originKind: 1,
    leadDays: 1,
    slotStart: T("2026-10-08T08:00:00Z"),
    slotMinutes: 60,
    waits: [25, 35],
    sources: "kk",
    bands: null,
    dayPeak: 45,
    expectedError: 10,
    peakBand: 12,
    modelVersion: null,
    rideQ90: 50,
    isHeadliner: true,
    liveWait: null,
    liveAgeMin: null,
    laterWait: null,
    laterAt: null,
    levelSource: "climatology",
  };
  const parkDay = {
    originAt: T("2026-10-07T04:00:00Z"),
    parkId: "p1",
    targetDate: "2026-10-08",
    leadDays: 1,
    tier: "climatology",
    crowdLevel: "high",
    predictedCrowdLevel: "high",
    crowdLevelFallback: false,
    typicalDayPeak: 30,
    levelSource: "climatology",
  };
  const redis = {
    get: jest.fn().mockResolvedValue("3"),
    set: jest.fn().mockResolvedValue("OK"),
    incr: jest.fn().mockResolvedValue(4),
  };
  const service = new ForecastArchiveScoringService(
    {
      find: jest
        .fn()
        .mockResolvedValue([
          { id: "p1", slug: "phl", timezone: TZ, continentSlug: "europe" },
          ...(opts.failCurves
            ? [{ id: "p2", slug: "ep", timezone: TZ, continentSlug: "europe" }]
            : []),
        ]),
      manager: { connection },
    } as never,
    { find: jest.fn().mockResolvedValue([{ id: "r1" }]) } as never,
    {
      // failCurves: the first park scores, the second one times out.
      find: opts.failCurves
        ? jest
            .fn()
            .mockResolvedValueOnce([curve])
            .mockRejectedValue(new Error("statement timeout"))
        : jest.fn().mockResolvedValue([curve, ...(opts.extraCurves ?? [])]),
      manager: { connection },
    } as never,
    { find: jest.fn().mockResolvedValue([parkDay]) } as never,
    scoreRepository as never,
    pairedRepository as never,
    {
      getHeadlinerAttractions: jest
        .fn()
        .mockResolvedValue([{ attractionId: "r1" }]),
    } as never,
    redis as never,
  );
  return {
    service,
    sql,
    deleted,
    insertedScores,
    insertedPaired,
    scoreRepository,
    pairedRepository,
    redis,
  };
}

describe("ForecastArchiveScoringService.scoreDue", () => {
  it("keeps a replayed date's old rows when a park fails (PAR-1024)", async () => {
    const { service, deleted } = build({
      alreadyScored: ["2026-10-06", "2026-10-07", "2026-10-08"],
      staleDates: ["2026-10-01"],
      failCurves: true,
    });
    const res = await service.scoreDue(T("2026-10-09T12:00:00Z"));
    expect(res.dates).toEqual([]);
    expect(deleted).toEqual([]);
  });

  it("replays dates scored under an older definition, and truth skips heartbeats (PAR-1024)", async () => {
    const { service, deleted, pairedRepository, sql } = build({
      alreadyScored: ["2026-10-06", "2026-10-07", "2026-10-08"],
      staleDates: ["2026-10-08"],
    });
    const res = await service.scoreDue(T("2026-10-09T12:00:00Z"));
    // Nothing new to catch up, but the stale date is rescored.
    expect(res.dates).toEqual(["2026-10-08"]);
    expect(deleted).toContainEqual({
      entity: "ForecastArchivePaired",
      targetDate: "2026-10-08",
    });
    const [, params] = (
      pairedRepository.manager.query as jest.Mock
    ).mock.calls.find(([q]: [string]) => q.includes("HAVING max(definition)"));
    // Window: the 14 days curves are kept, ending yesterday.
    expect(params.slice(0, 3)).toEqual([
      "2026-09-25",
      "2026-10-08",
      PAIRED_DEFINITION,
    ]);
    const truth = sql.find((q) => q.includes("FROM queue_data"))!;
    expect(truth).toContain("NOT COALESCE(is_heartbeat, false)");
  });

  it("catches up the unscored dates only, replaces each date's rows, and retires the board cache", async () => {
    const { service, deleted, insertedScores, redis, sql } = build({
      alreadyScored: ["2026-10-06", "2026-10-07"],
    });
    const res = await service.scoreDue(T("2026-10-09T12:00:00Z"));
    expect(res.dates).toEqual(["2026-10-08"]);
    // Delete-then-insert inside one transaction, for that date only — the
    // scores and the shadow's paired rows alike.
    expect(deleted).toEqual([
      { entity: "ForecastArchiveScore", targetDate: "2026-10-08" },
      { entity: "ForecastArchivePaired", targetDate: "2026-10-08" },
    ]);
    expect(insertedScores.length).toBeGreaterThan(0);
    expect(redis.incr).toHaveBeenCalledWith("forecast-archive:board:version");
    // Reads carry their limits.
    expect(
      sql.filter((q) => q.startsWith("SET LOCAL lock_timeout")).length,
    ).toBeGreaterThan(0);

    const byKey = new Map(
      insertedScores.map((r) => [
        [r.region, r.useCase, r.lead, r.source, r.segment].join("|"),
        r.sums as Record<string, number>,
      ]),
    );
    // 08:00–08:45 truth 20/20/40/40 vs 25; 09:00–09:45 40/30/30/30 vs 35.
    const uc3 = byKey.get("EU|UC3|d1|composed|all")!;
    expect(uc3.n).toBe(8);
    // The 17-character label that broke the old varchar(16) is written.
    expect(byKey.has("EU|UC3|d1|level_climatology|all")).toBe(true);
    // dayPeak 45 vs truth P90 40: an over-forecast, inside the one-sided band.
    expect(uc3).toMatchObject({ nPeak: 1, nPeakBand: 1, nPeakBandCov: 1 });
    // D6: calendar day value P90(20,40,30 ≥ 10) = 38 ÷ 30 = 127 % → high.
    expect(byKey.get("EU|D6|d1|predicted|all")).toMatchObject({
      n: 1,
      exact: 1,
    });
    for (const r of insertedScores) {
      expect(String(r.source).length).toBeLessThanOrEqual(
        SCORE_KEY_WIDTHS.source,
      );
    }
  });

  it("scores the Chronos grid shadow under its own source and pairs it with the served plan and H5", async () => {
    // Same origin as the served d1 plan (06:00 the day before). Truth over the
    // eight quarter-hours is 20,20,20,40,40,30,30,30.
    const shadowRow = (surface: number, waits: number[], code: string) => ({
      originAt: T("2026-10-07T04:00:00Z"),
      attractionId: "r1",
      surface,
      targetDate: "2026-10-08",
      parkId: "p1",
      originKind: 1,
      leadDays: 1,
      slotStart: T("2026-10-08T08:00:00Z"),
      slotMinutes: 15,
      waits,
      sources: code.repeat(waits.length),
      bands: surface === 7 ? waits.map(() => 5) : null,
      dayPeak: surface === 7 ? 40 : null,
      expectedError: null,
      peakBand: null,
      modelVersion: surface === 7 ? "chronos-2@29ec376;hol=full" : null,
      rideQ90: 50,
      isHeadliner: true,
      liveWait: null,
      liveAgeMin: null,
      laterWait: null,
      laterAt: null,
      levelSource: null,
    });
    const { service, insertedScores, insertedPaired } = build({
      alreadyScored: ["2026-10-06", "2026-10-07"],
      extraCurves: [
        shadowRow(7, [20, 20, 20, 40, 40, 30, 30, 30], "g"),
        shadowRow(4, [25, 25, 25, 25, 25, 25, 25, 25], "h"),
      ],
    });
    await service.scoreDue(T("2026-10-09T12:00:00Z"));
    const byKey = new Map(
      insertedScores.map((r) => [
        [r.region, r.useCase, r.lead, r.source, r.segment].join("|"),
        r.sums as Record<string, number>,
      ]),
    );
    expect(byKey.get("EU|UC3|d1|chronos2_grid|all")).toMatchObject({
      n: 8,
      sae: 0,
      nBand: 8,
      nPeak: 1,
    });
    // Never pooled into what the served planner is judged by.
    expect(byKey.get("EU|UC3|d1|all|all")!.n).toBe(8);

    const paired = (reference: string) =>
      insertedPaired.find(
        (r) =>
          r.reference === reference &&
          r.segment === "all" &&
          r.model === "chronos2_grid",
      );
    // Served plan: 25 for the first hour, 35 for the second → Σ|e| = 50.
    expect(paired("served")).toMatchObject({
      parkId: "p1",
      region: "EU",
      useCase: "UC3",
      lead: "d1",
      n: 8,
      saeModel: 0,
      saeRef: 50,
    });
    expect(paired("h5")).toMatchObject({ n: 8, saeRef: 60 });
    expect(
      insertedPaired
        .filter((r) => r.model === "chronos2_grid" && r.useCase === "UC3")
        .map((r) => r.segment)
        .sort(),
    ).toEqual(["all", "all", "busy", "busy", "headliner", "headliner"]);
    // PAR-1035: the matrix pairs a question's candidates only where ALL of
    // them have a value. This park-day has no old-composer curve and no
    // weekday history, so no matrix row is written — and PAR-852's own rows
    // above are the only paired rows.
    expect(
      insertedPaired.filter((r) => r.reference === "served_joint"),
    ).toEqual([]);
    expect(
      insertedPaired.every((r) =>
        ["served", "h5"].includes(String(r.reference)),
      ),
    ).toBe(true);
  });

  it("does nothing when every date is scored", async () => {
    const { service, deleted, redis } = build({
      alreadyScored: ["2026-10-06", "2026-10-07", "2026-10-08"],
    });
    expect(await service.scoreDue(T("2026-10-09T12:00:00Z"))).toEqual({
      dates: [],
    });
    expect(deleted).toEqual([]);
    expect(redis.incr).not.toHaveBeenCalled();
  });
});

describe("ForecastArchiveScoringService.getBoard", () => {
  it("pools counters across days before dividing, keyed on the scoring version", async () => {
    const { service, scoreRepository, redis } = build();
    redis.get.mockImplementation(async (k: string) =>
      k === "forecast-archive:board:version" ? "3" : null,
    );
    const rows = [
      {
        targetDate: "2026-10-07",
        region: "ALL",
        useCase: "UC3",
        lead: "d1",
        source: "all",
        segment: "all",
        sums: { n: 2, sae: 10, se: 2 },
      },
      {
        targetDate: "2026-10-08",
        region: "ALL",
        useCase: "UC3",
        lead: "d1",
        source: "all",
        segment: "all",
        sums: { n: 8, sae: 10, se: -8 },
      },
      {
        targetDate: "2026-10-08",
        region: "ALL",
        useCase: "UC1",
        lead: "h0-1",
        source: "all",
        segment: "all",
        sums: { n: 1, sae: 3, se: 3 },
      },
    ];
    const qb = {
      where: () => qb,
      andWhere: () => qb,
      getMany: async () => rows,
    };
    scoreRepository.createQueryBuilder.mockReturnValue(qb);
    const board = await service.getBoard(14, "ALL");
    expect(board.targetDates).toEqual({
      first: "2026-10-07",
      last: "2026-10-08",
      count: 2,
    });
    expect(board.rows.map((r) => `${r.useCase}|${r.lead}`)).toEqual([
      "UC1|h0-1",
      "UC3|d1",
    ]);
    const uc3 = board.rows[1];
    expect(uc3.days).toBe(2);
    // (10 + 10) / (2 + 8) — not the mean of 5 and 1.25.
    expect(uc3.metrics.mae).toBe(2);
    expect(uc3.metrics.bias).toBe(-0.6);
    expect(redis.set.mock.calls[0][0]).toBe("forecast-archive:board:v3:ALL:14");
  });

  it("returns the Chronos section: own MAE plus paired differences with a park-cluster CI", async () => {
    const { service, scoreRepository, pairedRepository, redis } = build();
    redis.get.mockResolvedValue(null);
    const scoreRows = [
      {
        targetDate: "2026-10-08",
        region: "ALL",
        useCase: "UC1",
        lead: "h0-1",
        source: "chronos2",
        segment: "all",
        sums: { n: 10, sae: 50, se: -10, nBand: 10, nBandCov: 9 },
      },
    ];
    const sqb = {
      where: () => sqb,
      andWhere: () => sqb,
      getMany: async () => scoreRows,
    };
    scoreRepository.createQueryBuilder.mockReturnValue(sqb);
    const pairedRows = ["p1", "p2", "p3"].map((parkId, i) => ({
      targetDate: "2026-10-08",
      parkId,
      region: "EU",
      useCase: "UC1",
      lead: "h0-1",
      model: "chronos2",
      reference: "served",
      segment: "all",
      n: 10,
      saeModel: 40 + i,
      saeRef: 60 + i,
      seModel: 0,
      seRef: 0,
    }));
    const pqb = {
      where: () => pqb,
      andWhere: () => pqb,
      getMany: async () => pairedRows,
    };
    pairedRepository.createQueryBuilder.mockReturnValue(pqb);
    const board = await service.getBoard(14, "ALL");
    expect(board.chronos.method).toMatchObject({ minDays: 14, minParks: 10 });
    const row = board.chronos.rows[0];
    expect(row).toMatchObject({
      useCase: "UC1",
      lead: "h0-1",
      model: "chronos2",
      surface: "chronos_live",
      mae: 5,
      bandCoverage: 0.9,
    });
    expect(row.comparisons[0]).toMatchObject({
      reference: "served",
      n: 30,
      parks: 3,
      days: 1,
      modelMae: 4.1,
      referenceMae: 6.1,
      diff: -2,
      enoughData: false,
      verdict: "insufficient_data",
    });
    // Every park is 2 min better → the bootstrap cannot move off −2.
    expect(row.comparisons[0].ciLow).toBe(-2);
    expect(row.comparisons[0].ciHigh).toBe(-2);
  });
});
