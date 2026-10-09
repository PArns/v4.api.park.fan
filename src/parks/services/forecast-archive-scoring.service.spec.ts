import { ForecastArchiveScoringService } from "./forecast-archive-scoring.service";
import { SCORE_KEY_WIDTHS } from "../../ml/entities/forecast-archive-score.entity";

const TZ = "Europe/Berlin";
const T = (iso: string) => new Date(iso);

/**
 * One park, one ride, one window (10:00–12:00 CEST = 08:00–10:00 UTC on
 * 2026-10-08), one d1 plan curve archived at 06:00 the day before.
 */
function build(opts: { alreadyScored?: string[] } = {}) {
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
          delete: async (_e: unknown, where: unknown) => deleted.push(where),
          insert: async (_e: unknown, rows: Array<Record<string, unknown>>) =>
            insertedScores.push(...rows),
        }),
    },
    createQueryBuilder: jest.fn(),
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
        ]),
      manager: { connection },
    } as never,
    { find: jest.fn().mockResolvedValue([{ id: "r1" }]) } as never,
    {
      find: jest.fn().mockResolvedValue([curve]),
      manager: { connection },
    } as never,
    { find: jest.fn().mockResolvedValue([parkDay]) } as never,
    scoreRepository as never,
    {
      getHeadlinerAttractions: jest
        .fn()
        .mockResolvedValue([{ attractionId: "r1" }]),
    } as never,
    redis as never,
  );
  return { service, sql, deleted, insertedScores, scoreRepository, redis };
}

describe("ForecastArchiveScoringService.scoreDue", () => {
  it("catches up the unscored dates only, replaces each date's rows, and retires the board cache", async () => {
    const { service, deleted, insertedScores, redis, sql } = build({
      alreadyScored: ["2026-10-06", "2026-10-07"],
    });
    const res = await service.scoreDue(T("2026-10-09T12:00:00Z"));
    expect(res.dates).toEqual(["2026-10-08"]);
    // Delete-then-insert inside one transaction, for that date only.
    expect(deleted).toEqual([{ targetDate: "2026-10-08" }]);
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
});
