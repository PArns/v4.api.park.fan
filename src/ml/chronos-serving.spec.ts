import {
  CHRONOS_MAX_CURVE_AGE_MS,
  ChronosServeEntry,
  applyChronosOverride,
  chronosServeEntries,
  chronosServeKey,
  chronosSlot,
  readChronosServeCache,
  serveChronosIntraday,
  stripServedSuffix,
  writeChronosServeCache,
} from "./chronos-serving";
import type { PredictionDto } from "./dto";

const MIN = 60_000;
const BASE = Date.UTC(2026, 9, 17, 12, 0, 0); // a slot start

/** A curve from the origin BASE: slot i = BASE + 15 min · i, q50 = 30 + i. */
function entry(
  over: Partial<ChronosServeEntry> = {},
  n = 17,
): ChronosServeEntry {
  return {
    originAt: BASE,
    revision: "29ec376",
    segments: [
      {
        slotStart: BASE,
        slotMinutes: 15,
        q50: Array.from({ length: n }, (_, i) => 30 + i),
        q80: Array.from({ length: n }, (_, i) => 50 + i),
        q95: Array.from({ length: n }, (_, i) => 70 + i),
      },
    ],
    ...over,
  };
}

function pred(leadMin: number, over: Partial<PredictionDto> = {}) {
  return {
    attractionId: "a1",
    predictedTime: new Date(BASE + leadMin * MIN).toISOString(),
    predictedWaitTime: 20,
    predictionType: "hourly",
    confidence: 80,
    crowdLevel: "low",
    baseline: 40,
    modelVersion: "v1+pcn",
    uncertaintyMinutes: 9,
    ...over,
  } as PredictionDto;
}

describe("chronos-serving (PAR-911)", () => {
  describe("serveChronosIntraday", () => {
    const prev = process.env.SERVE_CHRONOS_INTRADAY;
    afterEach(() => {
      if (prev === undefined) delete process.env.SERVE_CHRONOS_INTRADAY;
      else process.env.SERVE_CHRONOS_INTRADAY = prev;
    });

    it("is off unless set to exactly `true`", () => {
      delete process.env.SERVE_CHRONOS_INTRADAY;
      expect(serveChronosIntraday()).toBe(false);
      process.env.SERVE_CHRONOS_INTRADAY = "1";
      expect(serveChronosIntraday()).toBe(false);
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      expect(serveChronosIntraday()).toBe(true);
    });
  });

  describe("applyChronosOverride — the lead window", () => {
    const run = (leads: number[]) => {
      const preds = leads.map((l) => pred(l));
      const done = applyChronosOverride(
        preds,
        new Map([["a1", entry()]]),
        BASE,
        BASE + 5 * MIN,
      );
      return { preds, done };
    };

    it("leaves ≤ 30 min to the persistence blend", () => {
      const { preds, done } = run([0, 15, 30]);
      expect(done.size).toBe(0);
      expect(preds.every((p) => p.predictedWaitTime === 20)).toBe(true);
      expect(preds.every((p) => p.modelVersion === "v1+pcn")).toBe(true);
    });

    it("serves Chronos q50 from 45 min up to and including 120 min", () => {
      const { preds, done } = run([45, 60, 120]);
      expect(done.size).toBe(3);
      // q50 at slot 3 = 33 → rounded to the served 5-minute step.
      expect(preds[0].predictedWaitTime).toBe(35);
      expect(preds[1].predictedWaitTime).toBe(35); // 34 → 35
      expect(preds[2].predictedWaitTime).toBe(40); // 38 → 40
      expect(preds.every((p) => p.modelVersion === "v1+chronos")).toBe(true);
    });

    it("leaves > 120 min to PCN", () => {
      const { preds, done } = run([135, 180, 240]);
      expect(done.size).toBe(0);
      expect(preds.every((p) => p.predictedWaitTime === 20)).toBe(true);
    });

    it("never touches daily rows", () => {
      const p = pred(60, { predictionType: "daily" });
      applyChronosOverride([p], new Map([["a1", entry()]]), BASE, BASE);
      expect(p.predictedWaitTime).toBe(20);
    });
  });

  describe("applyChronosOverride — band and crowd level", () => {
    it("takes the band from q95 − q50 and the crowd level from q80 vs P50", () => {
      const p = pred(60); // slot 4: q50 34, q80 54, q95 74
      applyChronosOverride([p], new Map([["a1", entry()]]), BASE, BASE);
      expect(p.uncertaintyMinutes).toBe(40);
      // q80 54 → 55 served; 55 / 40 = 137.5 % → high (111–150).
      expect(p.crowdLevel).toBe("high");
    });

    it("keeps the crowd wait at or above the shown wait", () => {
      const e = entry();
      e.segments[0].q80 = e.segments[0].q80.map(() => 1);
      const p = pred(60);
      applyChronosOverride([p], new Map([["a1", e]]), BASE, BASE);
      // q80 below q50 (34 → 35): clamped to 35 / 40 = 87.5 % → low.
      expect(p.crowdLevel).toBe("low");
    });

    it("leaves the crowd level alone without a baseline", () => {
      const p = pred(60, { baseline: 0, crowdLevel: "moderate" });
      applyChronosOverride([p], new Map([["a1", entry()]]), BASE, BASE);
      expect(p.crowdLevel).toBe("moderate");
      expect(p.predictedWaitTime).toBe(35);
    });

    it("keeps the existing band when the curve has no q95", () => {
      const e = entry();
      e.segments[0].q95 = [];
      const p = pred(60);
      applyChronosOverride([p], new Map([["a1", e]]), BASE, BASE);
      expect(p.uncertaintyMinutes).toBe(9);
    });
  });

  describe("applyChronosOverride — fallbacks", () => {
    it("serves nothing from a curve older than the staleness guard", () => {
      const p = pred(60);
      const done = applyChronosOverride(
        [p],
        new Map([["a1", entry()]]),
        BASE,
        BASE + CHRONOS_MAX_CURVE_AGE_MS + 1,
      );
      expect(done.size).toBe(0);
      expect(p.predictedWaitTime).toBe(20);
    });

    it("serves a curve exactly at the guard", () => {
      const done = applyChronosOverride(
        [pred(60)],
        new Map([["a1", entry()]]),
        BASE,
        BASE + CHRONOS_MAX_CURVE_AGE_MS,
      );
      expect(done.size).toBe(1);
    });

    it("leaves a ride without a curve, and a slot the curve has no value for", () => {
      const e = entry();
      e.segments[0].q50[4] = null; // outside the published window
      const other = pred(60, { attractionId: "a2" });
      const closed = pred(60);
      const done = applyChronosOverride(
        [other, closed],
        new Map([["a1", e]]),
        BASE,
        BASE,
      );
      expect(done.size).toBe(0);
      expect(other.predictedWaitTime).toBe(20);
      expect(closed.predictedWaitTime).toBe(20);
    });

    it("labels an unversioned row catboost+chronos", () => {
      const p = pred(60, { modelVersion: undefined as never });
      applyChronosOverride([p], new Map([["a1", entry()]]), BASE, BASE);
      expect(p.modelVersion).toBe("catboost+chronos");
    });
  });

  describe("chronosSlot", () => {
    it("finds the slot in the second segment (a curve that crosses midnight)", () => {
      const e = entry({}, 2);
      e.segments.push({
        slotStart: BASE + 12 * 60 * MIN,
        slotMinutes: 15,
        q50: [11, 12],
        q80: [13, 14],
        q95: [],
      });
      expect(chronosSlot(e, BASE + 12 * 60 * MIN + 15 * MIN)).toEqual({
        q50: 12,
        q80: 14,
        q95: null,
      });
    });

    it("refuses an instant between two slots", () => {
      expect(chronosSlot(entry(), BASE + 7 * MIN)).toBeNull();
    });
  });

  describe("chronosServeEntries and the cache", () => {
    const origin = new Date(BASE);
    const curves = [
      {
        attractionId: "a1",
        slotStart: new Date(BASE).toISOString(),
        slotMinutes: 15,
        q50: [10, 20],
        q80: [15, 25],
        q95: [20, 30],
      },
      {
        attractionId: "a1",
        slotStart: new Date(BASE + 12 * 60 * MIN).toISOString(),
        slotMinutes: 15,
        q50: [5],
        q80: [6],
        q95: [7],
      },
      {
        attractionId: "a2",
        slotStart: new Date(BASE).toISOString(),
        slotMinutes: 15,
        q50: [null, null],
        q80: [],
        q95: [],
      },
    ];

    it("groups a ride's curves and drops a ride with no value at all", () => {
      const out = chronosServeEntries(curves, origin, "rev");
      expect([...out.keys()]).toEqual(["a1"]);
      expect(out.get("a1")!.segments).toHaveLength(2);
      expect(out.get("a1")!.originAt).toBe(BASE);
      expect(out.get("a1")!.revision).toBe("rev");
    });

    it("round-trips through Redis, one key per ride with a TTL", async () => {
      const store = new Map<string, string>();
      const set = jest.fn((k: string, v: string) => store.set(k, v));
      const redis = {
        pipeline: () => ({ set, exec: jest.fn().mockResolvedValue([]) }),
        mget: jest.fn((...keys: string[]) =>
          Promise.resolve(keys.map((k) => store.get(k) ?? null)),
        ),
      } as never;
      const written = await writeChronosServeCache(
        redis,
        chronosServeEntries(curves, origin, "rev"),
      );
      expect(written).toBe(1);
      expect(set).toHaveBeenCalledWith(
        chronosServeKey("a1"),
        expect.any(String),
        "EX",
        expect.any(Number),
      );
      const read = await readChronosServeCache(redis, ["a1", "a2"]);
      expect([...read.keys()]).toEqual(["a1"]);
      expect(read.get("a1")!.segments[0].q50).toEqual([10, 20]);
    });

    it("reads nothing (never throws) when Redis fails or a key is corrupt", async () => {
      const down = {
        mget: jest.fn().mockRejectedValue(new Error("down")),
      } as never;
      expect((await readChronosServeCache(down, ["a1"])).size).toBe(0);
      const corrupt = {
        mget: jest.fn().mockResolvedValue(["{not json"]),
      } as never;
      expect((await readChronosServeCache(corrupt, ["a1"])).size).toBe(0);
    });
  });

  it("stripServedSuffix removes either served-model suffix", () => {
    expect(stripServedSuffix("v1+pcn")).toBe("v1");
    expect(stripServedSuffix("v1+chronos")).toBe("v1");
    expect(stripServedSuffix("v1")).toBe("v1");
    expect(stripServedSuffix(null)).toBeNull();
  });
});
