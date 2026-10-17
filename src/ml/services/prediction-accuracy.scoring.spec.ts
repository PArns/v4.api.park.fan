import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import {
  PredictionAccuracy,
  isScoredSlot,
  scoredSlotSql,
} from "../entities/prediction-accuracy.entity";
import { AttractionAccuracyStats } from "../entities/attraction-accuracy-stats.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  PredictionAccuracyService,
  classifyComparison,
  operatingAccuracyRows,
} from "./prediction-accuracy.service";
import { predictionQuantilesOf, withoutInternalQuantiles } from "../ml.service";

/**
 * PAR-912. 65–70 % of the hourly COMPLETED rows had an actual of 0 (measured
 * 2026-10-17 over six days), and with them every CatBoost version read MAE
 * 11–12 / bias +10. One day, re-derived against queue_data, split those zeros
 * into: 41.8 % rides not scheduled to run (CLOSED before their first run of the
 * day, REFURBISHMENT), 37.8 % rides that publish no wait at all, 11.1 %
 * unplanned closures (DOWN, or CLOSED after the ride had run) and 9.2 % real
 * 0-minute waits. Only the last are forecast errors.
 */
describe("PAR-912 — which compared slot is a forecast error", () => {
  const none = { rideRanEarlierToday: false, rideHasWaitSignal: false };

  describe("classifyComparison", () => {
    it.each([
      // reading, context, expected class, expected actual
      [{ status: "OPERATING", waitTime: 35 }, none, "scored", 35],
      [
        { status: "OPERATING", waitTime: 0 },
        { ...none, rideHasWaitSignal: true },
        "scored",
        0,
      ],
      [{ status: "OPERATING", waitTime: 0 }, none, "no_wait_signal", null],
      [{ status: "DOWN", waitTime: null }, none, "unplanned_closure", 0],
      [
        { status: "CLOSED", waitTime: null },
        { ...none, rideRanEarlierToday: true },
        "unplanned_closure",
        0,
      ],
      [{ status: "CLOSED", waitTime: null }, none, "ride_not_operating", null],
      [
        { status: "REFURBISHMENT", waitTime: null },
        { ...none, rideRanEarlierToday: true },
        "ride_not_operating",
        null,
      ],
    ])("%j %j → %s", (reading, ctx, scoreClass, actual) => {
      expect(classifyComparison(reading, ctx)).toEqual({
        kind: "compared",
        scoreClass,
        actual,
      });
    });

    it("has nothing to compare for OPERATING without a wait value", () => {
      expect(
        classifyComparison({ status: "OPERATING", waitTime: null }, none),
      ).toEqual({ kind: "no_reading" });
    });

    it("reads an unknown status as no reading — an absent fact is no closure", () => {
      for (const status of ["UNKNOWN", ""]) {
        expect(
          classifyComparison(
            { status, waitTime: 0 },
            { rideRanEarlierToday: true, rideHasWaitSignal: true },
          ),
        ).toEqual({ kind: "no_reading" });
      }
    });
  });

  describe("scoredSlotSql / isScoredSlot — one rule for new and old rows", () => {
    type Row = Pick<
      PredictionAccuracy,
      | "comparisonStatus"
      | "actualWaitTime"
      | "wasUnplannedClosure"
      | "scoreClass"
    >;
    const row = (over: Partial<Row>): Row => ({
      comparisonStatus: "COMPLETED",
      actualWaitTime: 20,
      wasUnplannedClosure: false,
      scoreClass: "scored",
      ...over,
    });

    it.each<[string, Partial<Row>, boolean]>([
      ["a scored wait", {}, true],
      ["a scored real 0-minute wait", { actualWaitTime: 0 }, true],
      [
        "an unplanned closure",
        {
          scoreClass: "unplanned_closure",
          wasUnplannedClosure: true,
          actualWaitTime: 0,
        },
        false,
      ],
      [
        "a ride not scheduled to run",
        { scoreClass: "ride_not_operating", actualWaitTime: null },
        false,
      ],
      [
        "a feed without a wait",
        { scoreClass: "no_wait_signal", actualWaitTime: null },
        false,
      ],
      ["a data-source sentinel", { actualWaitTime: 999 }, false],
      ["a PENDING row", { comparisonStatus: "PENDING" }, false],
      // Rows compared before PAR-912 carry no class: only a positive wait
      // outside a closure is trusted, so their zeros all drop.
      ["an old positive wait", { scoreClass: null }, true],
      ["an old zero", { scoreClass: null, actualWaitTime: 0 }, false],
      [
        "an old closure",
        { scoreClass: null, wasUnplannedClosure: true, actualWaitTime: 0 },
        false,
      ],
    ])("%s → %s", (_label, over, expected) => {
      expect(isScoredSlot(row(over), 400)).toBe(expected);
    });

    it("states the same rule in SQL", () => {
      const sql = scoredSlotSql("pa", 400).replace(/\s+/g, " ");
      expect(sql).toContain("pa.comparison_status = 'COMPLETED'");
      expect(sql).toContain("pa.actual_wait_time IS NOT NULL");
      expect(sql).toContain("pa.actual_wait_time <= 400");
      expect(sql).toContain("pa.score_class = 'scored'");
      expect(sql).toContain(
        `pa.score_class IS NULL AND pa."wasUnplannedClosure" = false AND pa.actual_wait_time > 0`,
      );
    });
  });

  describe("operatingAccuracyRows", () => {
    const sqlRow = (over: Record<string, unknown>) => ({
      model_version: "v20261016_0600",
      is_total: 0,
      n: "100",
      mae: "7.456",
      bias: "4.444",
      cov50: "0.8612",
      n_q80: "40",
      cov80: "0.9",
      n_q95: "40",
      cov95: null,
      ...over,
    });

    it("puts the all-versions row first, then versions newest first", () => {
      const rows = operatingAccuracyRows([
        sqlRow({ model_version: "v20261014_0600" }),
        sqlRow({ model_version: null, is_total: 1, n: "300" }),
        sqlRow({ model_version: "v20261016_0600" }),
      ] as never);
      expect(rows.map((r) => r.modelVersion)).toEqual([
        null,
        "v20261016_0600",
        "v20261014_0600",
      ]);
      expect(rows[0].n).toBe(300);
    });

    it("rounds and keeps a missing quantile's coverage null", () => {
      const [r] = operatingAccuracyRows([sqlRow({})] as never);
      expect(r).toEqual({
        modelVersion: "v20261016_0600",
        n: 100,
        mae: 7.46,
        bias: 4.44,
        coverage: { q50: 0.861, q80: 0.9, q95: null },
        quantileSamples: { q80: 40, q95: 40 },
      });
    });

    it("drops empty groups", () => {
      expect(operatingAccuracyRows([sqlRow({ n: "0" })] as never)).toEqual([]);
    });
  });

  describe("getOperatingAccuracy", () => {
    it("aggregates scored hourly slots per version plus a total, and caches", async () => {
      const store = new Map<string, string>();
      const accuracyRepo = { query: jest.fn().mockResolvedValue([]) };
      const module = await Test.createTestingModule({
        providers: [
          PredictionAccuracyService,
          {
            provide: getRepositoryToken(PredictionAccuracy),
            useValue: accuracyRepo,
          },
          {
            provide: getRepositoryToken(AttractionAccuracyStats),
            useValue: {},
          },
          { provide: getRepositoryToken(WaitTimePrediction), useValue: {} },
          { provide: getRepositoryToken(QueueData), useValue: {} },
          {
            provide: REDIS_CLIENT,
            useValue: {
              get: jest.fn((k: string) =>
                Promise.resolve(store.get(k) ?? null),
              ),
              set: jest.fn((k: string, v: string) => {
                store.set(k, v);
                return Promise.resolve("OK");
              }),
            },
          },
        ],
      }).compile();
      const service = module.get(PredictionAccuracyService);

      await service.getOperatingAccuracy(7);
      await service.getOperatingAccuracy(7);

      expect(accuracyRepo.query).toHaveBeenCalledTimes(1);
      const [sql] = accuracyRepo.query.mock.calls[0];
      const flat = String(sql).replace(/\s+/g, " ");
      expect(flat).toContain(scoredSlotSql("pa", 400).replace(/\s+/g, " "));
      expect(flat).toContain(`pa."predictionType" = 'hourly'`);
      expect(flat).toContain(`GROUPING SETS ((pa."modelVersion"), ())`);
      expect(flat).toContain("pa.actual_wait_time <= pa.predicted_q80");
      expect(flat).toContain("pa.actual_wait_time <= pa.predicted_q95");
      expect([...store.keys()]).toEqual(["accuracy:operating:v1:7"]);
    });
  });

  describe("quantiles on the prediction (MLService helpers)", () => {
    it("rounds q0.8/q0.95 to whole minutes and drops what is not a wait", () => {
      expect(
        predictionQuantilesOf({ predictedQ80: 17.4, predictedQ95: 31.6 }),
      ).toEqual({ q80: 17, q95: 32 });
      expect(
        predictionQuantilesOf({ predictedQ80: -1, predictedQ95: 20 }),
      ).toEqual({ q80: null, q95: 20 });
      expect(predictionQuantilesOf({})).toBeNull();
      expect(
        predictionQuantilesOf({ predictedQ80: NaN, predictedQ95: null }),
      ).toBeNull();
    });

    it("strips them from served reads without touching the cached object", () => {
      const raw = {
        count: 1,
        modelVersion: "v1",
        predictions: [
          {
            attractionId: "a",
            predictedTime: "2026-10-17T10:00:00Z",
            predictedWaitTime: 20,
            predictionType: "hourly" as const,
            confidence: 80,
            crowdLevel: "moderate" as const,
            baseline: 20,
            modelVersion: "v1",
            predictedQ80: 30,
            predictedQ95: 45,
          },
        ],
      };
      const served = withoutInternalQuantiles(raw);
      expect(served.predictions[0]).not.toHaveProperty("predictedQ80");
      expect(served.predictions[0]).not.toHaveProperty("predictedQ95");
      expect(served.predictions[0].predictedWaitTime).toBe(20);
      expect(raw.predictions[0].predictedQ80).toBe(30);
    });
  });
});
