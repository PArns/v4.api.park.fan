import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import {
  PARK_DAY_IS_CLOSED_SQL,
  PredictionAccuracyService,
} from "./prediction-accuracy.service";
import {
  PredictionAccuracy,
  scoredSlotSql,
} from "../entities/prediction-accuracy.entity";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import { AttractionAccuracyStats } from "../entities/attraction-accuracy-stats.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Coverage for PredictionAccuracyService — drives the
 * /attractions/{id} prediction-quality badge. The service is large
 * (1800+ lines) so this file targets the public surfaces that are
 * user-visible: `calculateAccuracyBadge`, `recordPredictions`' upsert
 * contract, and `getAttractionAccuracyWithBadge`'s 3-layer cache
 * fallback (Redis → pre-aggregated table → raw SQL).
 */
describe("PredictionAccuracyService", () => {
  let service: PredictionAccuracyService;

  const accuracyRepo = {
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
    findOne: jest.fn(),
    count: jest.fn(),
    find: jest.fn().mockResolvedValue([]),
    query: jest.fn().mockResolvedValue([]),
    manager: {
      transaction: jest.fn(
        (cb: (em: unknown) => Promise<void>): Promise<void> =>
          cb({
            query: jest.fn().mockResolvedValue(undefined),
            getRepository: () => accuracyRepo,
          }),
      ),
    },
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      select: jest.fn().mockReturnThis(),
      addSelect: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getRawOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
    })),
  };
  const statsRepo = { findOne: jest.fn() };
  const predictionRepo = { findOne: jest.fn(), find: jest.fn() };
  const queueDataRepo = {
    findOne: jest.fn(),
    // The two PAR-912 lookups (ride ran earlier today, ride publishes waits).
    query: jest.fn().mockResolvedValue([]),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getOne: jest.fn().mockResolvedValue(null),
    })),
  };
  const redisStore = new Map<string, string>();
  const redis = {
    get: jest.fn((k: string) => Promise.resolve(redisStore.get(k) ?? null)),
    set: jest.fn((k: string, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    del: jest.fn(),
  };

  beforeEach(async () => {
    redisStore.clear();
    jest.clearAllMocks();
    queueDataRepo.query.mockImplementation(() => Promise.resolve([]));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PredictionAccuracyService,
        {
          provide: getRepositoryToken(PredictionAccuracy),
          useValue: accuracyRepo,
        },
        {
          provide: getRepositoryToken(AttractionAccuracyStats),
          useValue: statsRepo,
        },
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: predictionRepo,
        },
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(PredictionAccuracyService);
  });

  describe("getServedIntradayAccuracy — served (PCN) vs stored (CatBoost)", () => {
    // to_regclass returns the table's regclass name when it exists, NULL otherwise.
    const boardExists = () => [{ t: "pcn_intraday_comparisons" }];

    it("returns the n-weighted PCN MAE with the CatBoost delta", async () => {
      accuracyRepo.query
        .mockResolvedValueOnce(boardExists())
        .mockResolvedValueOnce([
          { model: "pcn", n: "39399", mae: "6.12" },
          { model: "catboost", n: "39399", mae: "7.48" },
        ]);

      const res = await service.getServedIntradayAccuracy(14);

      expect(res).toEqual({
        servedModel: "pcn",
        mae: 6.1,
        n: 39399,
        catboostMae: 7.5,
        delta: 1.4, // 7.5 − 6.1 ⇒ served beats the fallback
        days: 14,
      });
    });

    it("returns null (and skips the aggregate) when the board table is absent", async () => {
      accuracyRepo.query.mockResolvedValueOnce([{ t: null }]);

      const res = await service.getServedIntradayAccuracy();

      expect(res).toBeNull();
      expect(accuracyRepo.query).toHaveBeenCalledTimes(1); // guard short-circuits
    });

    it("returns null when no PCN rows exist in-window (override inactive)", async () => {
      accuracyRepo.query
        .mockResolvedValueOnce(boardExists())
        .mockResolvedValueOnce([{ model: "catboost", n: "100", mae: "7.0" }]);

      expect(await service.getServedIntradayAccuracy()).toBeNull();
    });

    it("keeps delta/catboostMae null when CatBoost is absent from the window", async () => {
      accuracyRepo.query
        .mockResolvedValueOnce(boardExists())
        .mockResolvedValueOnce([{ model: "pcn", n: "10", mae: "5.0" }]);

      expect(await service.getServedIntradayAccuracy()).toMatchObject({
        mae: 5,
        catboostMae: null,
        delta: null,
      });
    });
  });

  describe("system accuracy stats — one row per prediction (PAR-640)", () => {
    // Event rows (TICKETED_EVENT etc.) can share a day with OPERATING/CLOSED.
    // A JOIN onto schedule_entries counted each prediction once per row of its
    // day; the CLOSED test now runs as an EXISTS beside the aggregate.
    const recordingBuilder = (calls: Array<[string, unknown[]]>) => {
      const qb: Record<string, unknown> = {};
      for (const m of [
        "innerJoin",
        "leftJoin",
        "select",
        "addSelect",
        "where",
        "andWhere",
        "groupBy",
        "orderBy",
      ]) {
        qb[m] = jest.fn((...args: unknown[]) => {
          calls.push([m, args]);
          return qb;
        });
      }
      qb.getRawOne = jest.fn().mockResolvedValue({});
      qb.getRawMany = jest.fn().mockResolvedValue([]);
      return qb;
    };

    it("does not join schedule_entries and filters CLOSED days via EXISTS", async () => {
      const calls: Array<[string, unknown[]]> = [];
      (accuracyRepo as any).count = jest.fn().mockResolvedValue(0);
      const original = accuracyRepo.createQueryBuilder.getMockImplementation();
      accuracyRepo.createQueryBuilder.mockImplementation(
        () => recordingBuilder(calls) as any,
      );
      try {
        await (service as any).computeSystemAccuracyStats(7);
      } finally {
        accuracyRepo.createQueryBuilder.mockImplementation(original!);
      }

      const joinsSchedule = calls.some(
        ([m, args]) =>
          (m === "leftJoin" || m === "innerJoin") &&
          args[0] === "schedule_entries",
      );
      expect(joinsSchedule).toBe(false);
      expect(
        calls.some(
          ([m, args]) =>
            m === "andWhere" &&
            String(args[0]).includes(`NOT ${PARK_DAY_IS_CLOSED_SQL}`),
        ),
      ).toBe(true);
    });

    // PAR-912: the overall MAE used to admit every 0 on a day the park was not
    // shut, and the HOURLY breakdown averaged every compared row — closures and
    // status-only zeros included (10.1 vs 6.9 over the same 7 days).
    it("averages forecast errors only, in the overall figure and the HOURLY breakdown", async () => {
      const calls: Array<[string, unknown[]]> = [];
      (accuracyRepo as any).count = jest.fn().mockResolvedValue(0);
      const original = accuracyRepo.createQueryBuilder.getMockImplementation();
      accuracyRepo.createQueryBuilder.mockImplementation(
        () => recordingBuilder(calls) as any,
      );
      try {
        await (service as any).computeSystemAccuracyStats(7);
      } finally {
        accuracyRepo.createQueryBuilder.mockImplementation(original!);
      }

      const scored = scoredSlotSql("pa", MAX_PLAUSIBLE_WAIT_TIME);
      expect(
        calls.some(([m, args]) => m === "andWhere" && args[0] === scored),
      ).toBe(true);
      expect(
        calls.some(
          ([m, args]) =>
            m === "addSelect" &&
            args[1] === "mae" &&
            String(args[0]).includes(`FILTER (WHERE ${scored})`),
        ),
      ).toBe(true);
      // The old "every 0 on an open day" clause is gone.
      expect(
        calls.some(([, args]) =>
          String(args[0]).includes("pa.actual_wait_time >= 0"),
        ),
      ).toBe(false);
    });

    it("calls a day CLOSED only when no OPERATING row shares it", () => {
      const sql = PARK_DAY_IS_CLOSED_SQL.replace(/\s+/g, " ");
      expect(sql).toContain(`se."scheduleType" = 'CLOSED'`);
      expect(sql).toContain(`se."attractionId" IS NULL`);
      expect(sql).toMatch(
        /NOT EXISTS \(.*operating_day\."scheduleType" = 'OPERATING'/,
      );
    });

    // PAR-707. `schedule_entries.date` is park-local; `DATE(pa.target_time)`
    // reads the session timezone, which is UTC in production. Measured over 30
    // days of production, 334 rows were charged to the neighbouring day — 270
    // at Sesame Place Langhorne (UTC-4, evening rows carried the NEXT UTC date)
    // and 64 at Six Flags Qiddiya City (UTC+3, after-midnight rows carried the
    // PREVIOUS one). Both directions, so this is not only a negative-offset bug.
    it("reads the prediction's day in the park's timezone, not UTC", () => {
      const sql = PARK_DAY_IS_CLOSED_SQL.replace(/\s+/g, " ");
      expect(sql).toContain(
        "se.date = (pa.target_time AT TIME ZONE p.timezone)::date",
      );
      expect(sql).not.toContain("DATE(pa.target_time)");
    });

    it("joins parks, because the day expression needs p.timezone", async () => {
      const calls: Array<[string, unknown[]]> = [];
      (accuracyRepo as any).count = jest.fn().mockResolvedValue(0);
      const original = accuracyRepo.createQueryBuilder.getMockImplementation();
      accuracyRepo.createQueryBuilder.mockImplementation(
        () => recordingBuilder(calls) as any,
      );
      try {
        await (service as any).computeSystemAccuracyStats(7);
      } finally {
        accuracyRepo.createQueryBuilder.mockImplementation(original!);
      }

      // An INNER JOIN, not a LEFT one: a prediction whose park vanished must not
      // silently turn its day expression NULL and pass the NOT EXISTS gate.
      expect(
        calls.some(
          ([m, args]) =>
            m === "innerJoin" &&
            args[0] === "parks" &&
            String(args[2]).includes('p.id = a."parkId"'),
        ),
      ).toBe(true);
    });
  });

  describe("calculateAccuracyBadge — public ladder", () => {
    // The badge thresholds drive the prediction-quality UI on attraction
    // detail pages. A slide here = different chip shown to users.
    it.each([
      [4.9, 100, "excellent"],
      [9.9, 100, "good"],
      [14.9, 100, "fair"],
      [25, 100, "poor"],
      [2, 9, "insufficient_data"], // under 10 comparisons → no badge
      [2, 10, "excellent"], // exactly 10 → badge kicks in
    ])(
      "MAE=%d compared=%d → badge '%s'",
      (mae: number, compared: number, expected: string) => {
        const result = service.calculateAccuracyBadge(mae, compared);
        expect(result.badge).toBe(expected);
      },
    );

    it("emits a sample-count message when below 10 compared predictions", () => {
      const result = service.calculateAccuracyBadge(2, 5);
      expect(result.message).toMatch(/at least 10/i);
      expect(result.message).toContain("5");
    });

    it("rounds the MAE in the 'poor' message for readability", () => {
      const result = service.calculateAccuracyBadge(25.6, 100);
      expect(result.badge).toBe("poor");
      // The message uses Math.round so the displayed value is integer
      expect(result.message).toContain("26");
      expect(result.message).not.toContain(".6");
    });
  });

  describe("recordPredictions — upsert contract", () => {
    it("upserts on (attractionId, targetTime) — never inserts duplicates", async () => {
      const prediction = {
        attractionId: "a-1",
        createdAt: new Date(),
        predictedTime: new Date(),
        predictedWaitTime: 30,
        modelVersion: "v1",
        predictionType: "hourly",
        features: { foo: 1 },
      } as unknown as WaitTimePrediction;

      await service.recordPredictions([prediction]);

      expect(accuracyRepo.upsert).toHaveBeenCalledTimes(1);
      const [, options] = accuracyRepo.upsert.mock.calls[0];
      expect(options).toMatchObject({
        conflictPaths: ["attractionId", "targetTime"],
      });
    });

    it("stores the served value and its source beside the raw one (PAR-854)", async () => {
      const at = (h: number) =>
        ({
          attractionId: `a-${h}`,
          createdAt: new Date(),
          predictedTime: new Date(`2026-07-01T${h}:00:00Z`),
          predictedWaitTime: 30,
          modelVersion: "v1",
          predictionType: "hourly",
          features: {},
        }) as unknown as WaitTimePrediction;

      await service.recordPredictions(
        [at(10), at(11), at(12)],
        [{ wait: 55, source: "pcn" }, { wait: 30, source: "catboost" }, null],
      );

      // Two statements: TypeORM overwrites a column for EVERY row of an upsert
      // once one row carries it, so a mixed batch would write NULL over the
      // served value an earlier, longer-lead run recorded for the third slot.
      expect(accuracyRepo.upsert).toHaveBeenCalledTimes(2);
      const [withServed] = accuracyRepo.upsert.mock.calls[0] as [
        Array<Record<string, unknown>>,
      ];
      const [withoutServed] = accuracyRepo.upsert.mock.calls[1] as [
        Array<Record<string, unknown>>,
      ];
      expect(
        withServed.map((r) => [
          r.attractionId,
          r.predictedWaitTime,
          r.servedWaitTime,
          r.servedSource,
        ]),
      ).toEqual([
        ["a-10", 30, 55, "pcn"],
        ["a-11", 30, 30, "catboost"],
      ]);
      expect(withoutServed.map((r) => r.attractionId)).toEqual(["a-12"]);
      // Undefined, not null: the column stays out of the statement entirely.
      expect(withoutServed[0].servedWaitTime).toBeUndefined();
      expect(withoutServed[0].servedSource).toBeUndefined();
      // No quantiles handed over → written as NULL, never left stale.
      expect(withServed[0].predictedQ80).toBeNull();
      expect(withoutServed[0].predictedQ95).toBeNull();
    });

    it("writes q0.8/q0.95 on every row, served or not (PAR-912)", async () => {
      const at = (h: number) =>
        ({
          attractionId: `a-${h}`,
          createdAt: new Date(),
          predictedTime: new Date(`2026-07-01T${h}:00:00Z`),
          predictedWaitTime: 30,
          modelVersion: "v1",
          predictionType: "hourly",
          features: {},
        }) as unknown as WaitTimePrediction;

      await service.recordPredictions(
        [at(10), at(11), at(12)],
        [{ wait: 55, source: "pcn" }, null, null],
        [{ q80: 41, q95: 70 }, null, { q80: 12, q95: null }],
      );

      const rows = accuracyRepo.upsert.mock.calls.flatMap(
        ([batch]) => batch as Array<Record<string, unknown>>,
      );
      const byId = Object.fromEntries(rows.map((r) => [r.attractionId, r]));
      // Latest-write semantics, like predicted_wait_time: a run without a value
      // writes NULL instead of leaving an older run's quantile beside its median.
      expect(byId["a-10"]).toMatchObject({
        predictedQ80: 41,
        predictedQ95: 70,
      });
      expect(byId["a-11"]).toMatchObject({
        predictedQ80: null,
        predictedQ95: null,
      });
      expect(byId["a-12"]).toMatchObject({
        predictedQ80: 12,
        predictedQ95: null,
      });
    });
  });

  describe("getAttractionAccuracyWithBadge — cache layers", () => {
    it("layer 1: returns the Redis-cached payload without DB touches", async () => {
      const cached = {
        badge: "good",
        last30Days: {
          mae: 7.5,
          mape: 0.15,
          rmse: 9,
          comparedPredictions: 200,
          totalPredictions: 250,
        },
      };
      redisStore.set("accuracy:badge:v2:a-1:30d", JSON.stringify(cached));

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      expect(result).toEqual(cached);
      // No repo lookups on cache hit.
      expect(statsRepo.findOne).not.toHaveBeenCalled();
    });

    it("layer 2: serves from the pre-aggregated table when Redis misses", async () => {
      statsRepo.findOne.mockResolvedValueOnce({
        attractionId: "a-1",
        badge: "fair",
        mae: 12,
        comparedPredictions: 50,
        totalPredictions: 60,
        message: "Predictions provide general guidance",
      });

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      expect(result.badge).toBe("fair");
      expect(result.last30Days.mae).toBe(12);
      expect(result.last30Days.comparedPredictions).toBe(50);
      // Cache primed for the next call.
      expect(redisStore.get("accuracy:badge:v2:a-1:30d")).toBeDefined();
    });

    it("layer 3: falls through to raw SQL aggregation when nothing else has data", async () => {
      statsRepo.findOne.mockResolvedValueOnce(null);
      // Layer 3 uses repo.query() (raw SQL), not createQueryBuilder.
      accuracyRepo.query.mockResolvedValueOnce([
        {
          total_predictions: "50",
          compared_predictions: "40",
          mae: "8.5",
          mape: "0.2",
          rmse: "10.5",
        },
      ]);

      const result = await service.getAttractionAccuracyWithBadge("a-1");

      // Badge derived from MAE=8.5 and compared=40 → "good"
      expect(result.badge).toBe("good");
      // On the served basis (PAR-854): served error, served rows only.
      const [sql] = accuracyRepo.query.mock.calls[0] as [string];
      expect(sql).toContain("AVG(served_absolute_error)");
      expect(sql).toContain("AND served_source IS NOT NULL");
      // Layer 3 also primes the Redis cache so the next hit is L1.
      expect(redisStore.get("accuracy:badge:v2:a-1:30d")).toBeDefined();
    });
  });
  describe("compareWithActuals — matching predictions to queue data", () => {
    const MIN = 60 * 1000;
    const target = (minutesAgo: number) =>
      new Date(Date.now() - minutesAgo * MIN);

    const pending = (over: Record<string, unknown>) => ({
      id: "p",
      attractionId: "a-1",
      targetTime: target(60),
      predictedWaitTime: 20,
      comparisonStatus: "PENDING",
      actualWaitTime: null,
      absoluteError: null,
      percentageError: null,
      wasUnplannedClosure: false,
      ...over,
    });

    const actual = (
      attractionId: string,
      at: Date,
      status: string,
      waitTime: number | null,
    ) => ({ attractionId, timestamp: at, status, waitTime });

    /** Stubs the pending batch and the queue_data fetch. */
    const arrange = (
      predictions: Array<ReturnType<typeof pending>>,
      records: Array<ReturnType<typeof actual>>,
    ) => {
      accuracyRepo.find.mockResolvedValueOnce(predictions);
      queueDataRepo.createQueryBuilder.mockReturnValueOnce({
        select: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue(records),
      } as never);
    };

    const lastCheck = () =>
      JSON.parse(redisStore.get("ml:last-accuracy-check") ?? "{}");

    /**
     * Answers the two PAR-912 lookups: which CLOSED rows' ride had already run
     * that park-local day (by row id), and which rides published a wait above 0
     * in the last seven days (by attraction id).
     */
    const facts = (f: { ranEarlierToday?: string[]; withSignal?: string[] }) =>
      queueDataRepo.query.mockImplementation((sql: string) =>
        Promise.resolve(
          (sql.includes("date_trunc")
            ? (f.ranEarlierToday ?? [])
            : (f.withSignal ?? [])
          ).map((id) => ({ id })),
        ),
      );

    it("returns 0 and writes the check marker when nothing is ready", async () => {
      accuracyRepo.find.mockResolvedValueOnce([]);

      const res = await service.compareWithActuals();

      expect(res).toEqual({ newComparisons: 0 });
      expect(accuracyRepo.query).not.toHaveBeenCalled();
      expect(lastCheck().newComparisonsAdded).toBe(0);
    });

    it("issues no retention DELETE — that is the daily cleanup-old job's (PAR-868)", async () => {
      accuracyRepo.find.mockResolvedValueOnce([]);

      await service.compareWithActuals();

      expect(accuracyRepo.createQueryBuilder).not.toHaveBeenCalled();
    });

    it("computes absolute and percentage error from an operating match", async () => {
      // predicted 20, actual 30 → |20−30| = 10, 10/30·100 = 33.33…
      const p = pending({ predictedWaitTime: 20 });
      arrange([p], [actual("a-1", p.targetTime, "OPERATING", 30)]);

      const res = await service.compareWithActuals();

      expect(res).toEqual({ newComparisons: 1 });
      expect(p.comparisonStatus).toBe("COMPLETED");
      expect(p.actualWaitTime).toBe(30);
      expect(p.absoluteError).toBe(10);
      expect(p.percentageError).toBeCloseTo(33.333, 2);
      expect(p.wasUnplannedClosure).toBe(false);
      expect(lastCheck().newComparisonsAdded).toBe(1);
    });

    it("scores a real 0-minute wait, with percentageError null, on a ride that publishes waits", async () => {
      const p = pending({ predictedWaitTime: 15 });
      arrange([p], [actual("a-1", p.targetTime, "OPERATING", 0)]);
      facts({ withSignal: ["a-1"] });

      await service.compareWithActuals();

      expect(p.comparisonStatus).toBe("COMPLETED");
      expect((p as Record<string, unknown>).scoreClass).toBe("scored");
      expect(p.actualWaitTime).toBe(0);
      expect(p.absoluteError).toBe(15);
      expect(p.percentageError).toBeNull();
    });

    it("counts CLOSED after the ride had run that day as an unplanned closure with the full predicted error", async () => {
      const p = pending({ predictedWaitTime: 25 });
      arrange([p], [actual("a-1", p.targetTime, "CLOSED", null)]);
      facts({ ranEarlierToday: ["p"] });

      const res = await service.compareWithActuals();

      expect(res.newComparisons).toBe(1);
      expect(p.wasUnplannedClosure).toBe(true);
      expect((p as Record<string, unknown>).scoreClass).toBe(
        "unplanned_closure",
      );
      expect(p.actualWaitTime).toBe(0);
      expect(p.absoluteError).toBe(25);
      expect(p.percentageError).toBeNull();
    });

    // PAR-912: 65-70 % of the compared hourly rows had an actual of 0. These
    // are the classes that are not forecast errors.
    it("does not score CLOSED before the ride's first run of the day — it was not scheduled to run", async () => {
      const p = pending({ predictedWaitTime: 25, servedWaitTime: 30 });
      arrange([p], [actual("a-1", p.targetTime, "CLOSED", null)]);
      facts({ ranEarlierToday: [] });

      const res = await service.compareWithActuals();

      expect(res.newComparisons).toBe(1); // checked, so it counts as covered
      expect(p.comparisonStatus).toBe("COMPLETED");
      expect((p as Record<string, unknown>).scoreClass).toBe(
        "ride_not_operating",
      );
      expect(p.wasUnplannedClosure).toBe(false);
      expect(p.actualWaitTime).toBeNull();
      expect(p.absoluteError).toBeNull();
      expect((p as Record<string, unknown>).servedAbsoluteError).toBeNull();
    });

    it("does not score REFURBISHMENT, and asks no lookup for it", async () => {
      const p = pending({ predictedWaitTime: 25 });
      arrange([p], [actual("a-1", p.targetTime, "REFURBISHMENT", null)]);

      await service.compareWithActuals();

      expect((p as Record<string, unknown>).scoreClass).toBe(
        "ride_not_operating",
      );
      expect(p.actualWaitTime).toBeNull();
      expect(queueDataRepo.query).not.toHaveBeenCalled();
    });

    it("keeps DOWN an unplanned closure even before the ride's first run", async () => {
      const p = pending({ predictedWaitTime: 40 });
      arrange([p], [actual("a-1", p.targetTime, "DOWN", null)]);

      await service.compareWithActuals();

      expect((p as Record<string, unknown>).scoreClass).toBe(
        "unplanned_closure",
      );
      expect(p.wasUnplannedClosure).toBe(true);
      expect(p.absoluteError).toBe(40);
    });

    it("does not score OPERATING 0 on a ride that publishes no wait at all", async () => {
      const p = pending({ predictedWaitTime: 15 });
      arrange([p], [actual("a-1", p.targetTime, "OPERATING", 0)]);
      facts({ withSignal: [] });

      await service.compareWithActuals();

      expect(p.comparisonStatus).toBe("COMPLETED");
      expect((p as Record<string, unknown>).scoreClass).toBe("no_wait_signal");
      expect(p.actualWaitTime).toBeNull();
      expect(p.absoluteError).toBeNull();
    });

    it("takes the wait signal from the batch's own readings without a lookup", async () => {
      const early = pending({ id: "early", targetTime: target(100) });
      const late = pending({ id: "late", targetTime: target(40) });
      arrange(
        [early, late],
        [
          actual("a-1", early.targetTime, "OPERATING", 25),
          actual("a-1", late.targetTime, "OPERATING", 0),
        ],
      );

      await service.compareWithActuals();

      expect((late as Record<string, unknown>).scoreClass).toBe("scored");
      expect(late.actualWaitTime).toBe(0);
      expect(queueDataRepo.query).not.toHaveBeenCalled();
    });

    it("treats an unknown reading status as no reading, never a closure", async () => {
      const young = pending({ id: "young", targetTime: target(60) });
      const old = pending({ id: "old", targetTime: target(180) });
      arrange(
        [old, young],
        [
          actual("a-1", old.targetTime, "UNKNOWN", null),
          actual("a-1", young.targetTime, "UNKNOWN", null),
        ],
      );

      const res = await service.compareWithActuals();

      expect(young.comparisonStatus).toBe("PENDING");
      expect(old.comparisonStatus).toBe("MISSED");
      expect(old.wasUnplannedClosure).toBe(false);
      expect(res.newComparisons).toBe(0);
    });

    it("asks the ran-earlier-today lookup for CLOSED rows only, in the park's day", async () => {
      const shut = pending({ id: "shut", attractionId: "a-2" });
      const open = pending({ id: "open", attractionId: "a-1" });
      arrange(
        [shut, open],
        [
          actual("a-2", shut.targetTime, "CLOSED", null),
          actual("a-1", open.targetTime, "OPERATING", 20),
        ],
      );

      await service.compareWithActuals();

      expect(queueDataRepo.query).toHaveBeenCalledTimes(1);
      const [sql, args] = queueDataRepo.query.mock.calls[0];
      expect(sql).toContain("AT TIME ZONE p.timezone");
      expect(sql).toContain("q.timestamp < u.t");
      expect(args).toEqual([["shut"], ["a-2"], [shut.targetTime]]);
    });

    it("does not fabricate a 0-wait row for OPERATING without a wait value", async () => {
      const recent = pending({ id: "recent", targetTime: target(60) });
      const old = pending({ id: "old", targetTime: target(180) });
      arrange(
        [old, recent],
        [
          actual("a-1", old.targetTime, "OPERATING", null),
          actual("a-1", recent.targetTime, "OPERATING", null),
        ],
      );

      const res = await service.compareWithActuals();

      // 60 min old: stays PENDING; 180 min old (> 2 h): MISSED, never COMPLETED.
      expect(recent.comparisonStatus).toBe("PENDING");
      expect(old.comparisonStatus).toBe("MISSED");
      expect(res.newComparisons).toBe(0);
    });

    it("marks an unmatched prediction MISSED only after 2 hours", async () => {
      const young = pending({ id: "young", targetTime: target(90) });
      const stale = pending({ id: "stale", targetTime: target(150) });
      arrange([stale, young], []);

      await service.compareWithActuals();

      expect(young.comparisonStatus).toBe("PENDING");
      expect(stale.comparisonStatus).toBe("MISSED");
    });

    it("ignores queue data further than 30 minutes from the target and picks the closest record", async () => {
      const farAway = pending({ id: "far", attractionId: "a-2" });
      const near = pending({ id: "near", attractionId: "a-1" });
      arrange(
        [near, farAway],
        [
          // 10 min off, 5 min off → the 5 min record (wait 40) wins for a-1.
          actual(
            "a-1",
            new Date(near.targetTime.getTime() - 10 * MIN),
            "OPERATING",
            10,
          ),
          actual(
            "a-1",
            new Date(near.targetTime.getTime() + 5 * MIN),
            "OPERATING",
            40,
          ),
          // 31 min off → outside the window for a-2.
          actual(
            "a-2",
            new Date(farAway.targetTime.getTime() + 31 * MIN),
            "OPERATING",
            50,
          ),
        ],
      );

      await service.compareWithActuals();

      expect(near.actualWaitTime).toBe(40);
      expect(farAway.comparisonStatus).toBe("PENDING");
    });

    it("writes MISSED and COMPLETED rows in two bulk statements", async () => {
      const done = pending({ id: "done", predictedWaitTime: 20 });
      const gone = pending({
        id: "gone",
        attractionId: "a-2",
        targetTime: target(200),
      });
      arrange([gone, done], [actual("a-1", done.targetTime, "OPERATING", 30)]);

      await service.compareWithActuals();

      expect(accuracyRepo.query).toHaveBeenCalledTimes(2);
      const [missedSql, missedArgs] = accuracyRepo.query.mock.calls[0];
      expect(missedSql).toContain("MISSED");
      expect(missedArgs).toEqual([["gone"]]);
      const [completedSql, completedArgs] = accuracyRepo.query.mock.calls[1];
      expect(completedSql).toContain("score_class          = u.score_class");
      expect(completedArgs).toEqual([
        ["done"],
        [30],
        [10],
        [expect.closeTo(33.333, 2)],
        [false],
        // No served value recorded (a pre-PAR-854 row) → no served error.
        [null],
        ["scored"],
      ]);
    });

    // PAR-854: the served value is scored beside the raw one, against the same
    // actual, so the badge can measure what the reader saw.
    it("scores the served value against the same actual as the raw one", async () => {
      const row = pending({
        id: "both",
        predictedWaitTime: 20, // CatBoost
        servedWaitTime: 35, // PCN + blend, what the reader saw
      });
      arrange([row], [actual("a-1", row.targetTime, "OPERATING", 30)]);

      await service.compareWithActuals();

      expect(row.absoluteError).toBe(10);
      expect((row as Record<string, unknown>).servedAbsoluteError).toBe(5);
      const [sql, args] = accuracyRepo.query.mock.calls[0];
      expect(sql).toContain("served_absolute_error = u.served_absolute_error");
      expect(args[5]).toEqual([5]);
    });

    it("charges an unplanned closure the full served wait, as it does the raw one", async () => {
      const row = pending({
        id: "shut",
        predictedWaitTime: 20,
        servedWaitTime: 45,
      });
      arrange([row], [actual("a-1", row.targetTime, "CLOSED", null)]);
      facts({ ranEarlierToday: ["shut"] });

      await service.compareWithActuals();

      expect(row.wasUnplannedClosure).toBe(true);
      expect(row.absoluteError).toBe(20);
      expect((row as Record<string, unknown>).servedAbsoluteError).toBe(45);
    });

    it("asks the database for the served value with the pending batch", async () => {
      arrange([], []);
      await service.compareWithActuals();

      const [options] = accuracyRepo.find.mock.calls[0];
      expect(
        (options as { select: Record<string, boolean> }).select,
      ).toHaveProperty("servedWaitTime", true);
    });
  });

  describe("getAttractionAccuracyStats — rounding and empty window", () => {
    it("rounds the SQL aggregates to one decimal", async () => {
      accuracyRepo.query.mockResolvedValueOnce([
        {
          total_predictions: "120",
          compared_predictions: "80",
          mae: "6.449",
          mape: "21.351",
          rmse: "8.05",
        },
      ]);

      const res = await service.getAttractionAccuracyStats("a-1", 14);

      expect(res).toEqual({
        totalPredictions: 120,
        comparedPredictions: 80,
        averageAbsoluteError: 6.4,
        averagePercentageError: 21.4,
        rmse: 8.1,
      });
      const [, params] = accuracyRepo.query.mock.calls[0];
      expect(params[0]).toBe("a-1");
      expect(params[2]).toBe(400);
    });

    it("returns zero error figures but keeps the total when nothing was compared", async () => {
      accuracyRepo.query.mockResolvedValueOnce([
        {
          total_predictions: "12",
          compared_predictions: "0",
          mae: null,
          mape: null,
          rmse: null,
        },
      ]);

      expect(await service.getAttractionAccuracyStats("a-1")).toEqual({
        totalPredictions: 12,
        comparedPredictions: 0,
        averageAbsoluteError: 0,
        averagePercentageError: 0,
        rmse: 0,
      });
    });
  });

  describe("getRecentComparisons", () => {
    it("maps rows and bounds the actual wait below the sentinel codes", async () => {
      const t = new Date("2026-10-01T10:00:00Z");
      accuracyRepo.find.mockResolvedValueOnce([
        {
          targetTime: t,
          predictedWaitTime: 20,
          actualWaitTime: 25,
          absoluteError: 5,
          percentageError: 20,
          modelVersion: "v9",
          id: "ignored",
        },
      ]);

      const res = await service.getRecentComparisons("a-1", 5);

      expect(res).toEqual([
        {
          targetTime: t,
          predictedWaitTime: 20,
          actualWaitTime: 25,
          absoluteError: 5,
          percentageError: 20,
          modelVersion: "v9",
        },
      ]);
      const arg = accuracyRepo.find.mock.calls[0][0];
      expect(arg.take).toBe(5);
      expect(arg.order).toEqual({ targetTime: "DESC" });
      expect(arg.where.attractionId).toBe("a-1");
    });
  });

  describe("calculateMetricsFromRecords — hand-computed", () => {
    const calc = (records: unknown[]) =>
      (
        service as unknown as {
          calculateMetricsFromRecords: (r: unknown[]) => {
            mae: number;
            rmse: number;
            mape: number;
            r2Score: number;
          };
        }
      ).calculateMetricsFromRecords(records);

    it("returns zeros for an empty set", () => {
      expect(calc([])).toEqual({ mae: 0, rmse: 0, mape: 0, r2Score: 0 });
    });

    it("computes MAE, RMSE, MAPE and R² from errors 2 and 4", () => {
      // actual 10/20, predicted 12/16: errors 2, 4.
      // MAE = 3; RMSE = √((4+16)/2) = √10 = 3.16; MAPE = (20+20)/2 = 20.
      // mean actual 15 → SSTot = 25+25 = 50, SSRes = 4+16 = 20 → R² = 1 − 20/50 = 0.6.
      const res = calc([
        {
          actualWaitTime: 10,
          predictedWaitTime: 12,
          absoluteError: 2,
          percentageError: 20,
        },
        {
          actualWaitTime: 20,
          predictedWaitTime: 16,
          absoluteError: 4,
          percentageError: 20,
        },
      ]);
      expect(res).toEqual({ mae: 3, rmse: 3.2, mape: 20, r2Score: 0.6 });
    });

    it("skips null percentage errors in MAPE and gives R² 0 when actuals are constant", () => {
      const res = calc([
        {
          actualWaitTime: 10,
          predictedWaitTime: 15,
          absoluteError: 5,
          percentageError: 50,
        },
        {
          actualWaitTime: 10,
          predictedWaitTime: 10,
          absoluteError: 0,
          percentageError: null,
        },
      ]);
      expect(res.mape).toBe(50);
      expect(res.r2Score).toBe(0);
    });
  });

  describe("checkRetrainingNeeded — thresholds", () => {
    const withOverall = (mae: number, mape: number) =>
      jest.spyOn(service, "getSystemAccuracyStats").mockResolvedValue({
        overall: {
          mae,
          rmse: 0,
          mape,
          r2Score: 0,
          totalPredictions: 100,
          matchedPredictions: 100,
          coveragePercent: 100,
        },
      } as never);

    it("recommends retraining above MAE 8", async () => {
      withOverall(8.1, 10);
      const res = await service.checkRetrainingNeeded();
      expect(res.needed).toBe(true);
      expect(res.reason).toBe("accuracy_degradation");
    });

    it("recommends retraining above MAPE 35 when MAE is fine", async () => {
      withOverall(8, 35.1);
      const res = await service.checkRetrainingNeeded();
      expect(res.needed).toBe(true);
      expect(res.reason).toBe("high_percentage_error");
    });

    it("does not recommend retraining at exactly MAE 8 and MAPE 35", async () => {
      withOverall(8, 35);
      const res = await service.checkRetrainingNeeded();
      expect(res.needed).toBe(false);
      expect(res.metrics).not.toBeNull();
    });

    it("reports not needed with null metrics when the stats query fails", async () => {
      jest
        .spyOn(service, "getSystemAccuracyStats")
        .mockRejectedValue(new Error("boom"));
      expect(await service.checkRetrainingNeeded()).toEqual({
        needed: false,
        metrics: null,
      });
    });
  });
  describe("getHealthStatus — counts and success rates", () => {
    it("derives active, stalled and the rounded success rates from the counts", async () => {
      // Call order in the service: pending, stalled, missed7, completed7,
      // completed30, closures24h, total30.
      accuracyRepo.count
        .mockResolvedValueOnce(50)
        .mockResolvedValueOnce(12)
        .mockResolvedValueOnce(25)
        .mockResolvedValueOnce(75)
        .mockResolvedValueOnce(600)
        .mockResolvedValueOnce(4)
        .mockResolvedValueOnce(800);
      accuracyRepo.find.mockResolvedValueOnce([
        {
          attractionId: "a-1",
          targetTime: new Date("2026-10-01T10:00:00Z"),
          predictedWaitTime: 20,
          actualWaitTime: 25,
          absoluteError: 5,
          comparisonStatus: "COMPLETED",
          createdAt: new Date("2026-10-01T09:00:00Z"),
        },
      ]);

      const res = await service.getHealthStatus();

      expect(res.pendingComparisons).toEqual({
        total: 50,
        stalled: 12,
        active: 38,
      });
      expect(res.missedComparisons.total7Days).toBe(25);
      expect(res.unplannedClosures24h).toBe(4);
      // 75 / (75 + 25) = 75 %; 600 / 800 = 75 %.
      expect(res.successRate).toEqual({ last7Days: 75, last30Days: 75 });
      expect(res.recentSamples).toHaveLength(1);
      expect(res.recentSamples[0].status).toBe("COMPLETED");
    });

    it("reports 0 % instead of dividing by zero on an empty window", async () => {
      accuracyRepo.count.mockResolvedValue(0);
      accuracyRepo.find.mockResolvedValueOnce([]);

      const res = await service.getHealthStatus();

      expect(res.successRate).toEqual({ last7Days: 0, last30Days: 0 });
      expect(res.recentSamples).toEqual([]);
      accuracyRepo.count.mockReset();
    });
  });

  describe("cachedAgg — read-through cache", () => {
    const agg = (key: string, compute: () => Promise<unknown>) =>
      (
        service as unknown as {
          cachedAgg: (
            k: string,
            ttl: number,
            c: () => Promise<unknown>,
          ) => Promise<unknown>;
        }
      ).cachedAgg(key, 60, compute);

    it("computes on a miss, stores the JSON and serves the next call from Redis", async () => {
      const compute = jest.fn().mockResolvedValue({ mae: 4.2 });

      expect(await agg("k", compute)).toEqual({ mae: 4.2 });
      expect(await agg("k", compute)).toEqual({ mae: 4.2 });

      expect(compute).toHaveBeenCalledTimes(1);
      expect(redis.set).toHaveBeenCalledWith(
        "k",
        JSON.stringify({ mae: 4.2 }),
        "EX",
        60,
      );
    });

    it("does not cache a failed computation", async () => {
      const compute = jest.fn().mockRejectedValue(new Error("db"));

      await expect(agg("k2", compute)).rejects.toThrow("db");

      expect(redisStore.has("k2")).toBe(false);
    });
  });

  describe("calculateMAE", () => {
    const mae = (records: unknown[]) =>
      (
        service as unknown as { calculateMAE: (r: unknown[]) => number }
      ).calculateMAE(records);

    it("averages absolute errors to one decimal and treats null as 0", () => {
      // (3 + 4 + 0) / 3 = 2.333… → 2.3
      expect(
        mae([
          { absoluteError: 3 },
          { absoluteError: 4 },
          { absoluteError: null },
        ]),
      ).toBe(2.3);
    });

    it("returns 0 for an empty set", () => {
      expect(mae([])).toBe(0);
    });
  });
  describe("getBatchAttractionAccuracy — one query for many rides", () => {
    it("returns an empty map without a query for an empty id list", async () => {
      const res = await service.getBatchAttractionAccuracy([]);
      expect(res.size).toBe(0);
      expect(accuracyRepo.query).not.toHaveBeenCalled();
    });

    it("derives each badge from its own MAE and fills missing rides with insufficient_data", async () => {
      accuracyRepo.query.mockResolvedValueOnce([
        {
          attractionId: "a-1",
          totalPredictions: "60",
          comparedPredictions: "40",
          mae: "3.5",
        },
        {
          attractionId: "a-2",
          totalPredictions: "30",
          comparedPredictions: "9",
          mae: "2",
        },
      ]);

      const res = await service.getBatchAttractionAccuracy([
        "a-1",
        "a-2",
        "a-3",
      ]);

      expect(res.get("a-1")).toMatchObject({
        badge: "excellent",
        last30Days: { mae: 3.5, comparedPredictions: 40, totalPredictions: 60 },
      });
      // 9 compared predictions is below the 10 needed, whatever the MAE.
      expect(res.get("a-2")?.badge).toBe("insufficient_data");
      expect(res.get("a-3")).toMatchObject({
        badge: "insufficient_data",
        last30Days: { mae: 0, comparedPredictions: 0, totalPredictions: 0 },
      });
    });

    it("falls back to insufficient_data for every ride when the query fails", async () => {
      accuracyRepo.query.mockRejectedValueOnce(new Error("timeout"));

      const res = await service.getBatchAttractionAccuracy(["a-1", "a-2"]);

      expect([...res.keys()]).toEqual(["a-1", "a-2"]);
      expect(res.get("a-1")?.message).toBe("Error fetching accuracy data");
    });
  });
});
