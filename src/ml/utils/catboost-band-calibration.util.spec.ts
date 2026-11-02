import {
  BAND_MIN_OWN_EVIDENCE,
  BAND_MIN_SAMPLE,
  ScoreHistogramRow,
  applyBandCalibration,
  bandFactor,
  fitBandCalibration,
} from "./catboost-band-calibration.util";

/** n slots with score s = bin × 0.05 spread evenly over bins 1..100 (s 0.05..5). */
const uniform = (lead: string, segment: string, perBin: number) =>
  Array.from({ length: 100 }, (_, i): ScoreHistogramRow => ({
    lead,
    segment,
    bin: i + 1,
    n: perBin,
  }));

describe("fitBandCalibration (PAR-1060)", () => {
  it("picks the smallest k that covers 95 % of the cell", () => {
    const t = fitBandCalibration(uniform("h2-6", "busy", 10)); // n = 1000
    const cell = t.cells["h2-6|busy"];
    expect(cell.source).toBe("cell");
    expect(cell.k).toBe(4.75); // 95th of 100 bins → 95 × 0.05
    expect(cell.coverageCalibrated).toBeCloseTo(0.95);
    expect(cell.coverageRaw).toBeCloseTo(0.2); // s ≤ 1 → bins 1..20
  });

  it("a cell under the minimum sample borrows its lead bucket's pool", () => {
    const rows = [
      ...uniform("h2-6", "busy", 10),
      // 40 slots, all at s = 0.5: too few to trust its own k = 0.5.
      { lead: "h2-6", segment: "quiet", bin: 10, n: 40 },
    ];
    const t = fitBandCalibration(rows);
    const small = t.cells["h2-6|quiet"];
    expect(small.n).toBeLessThan(BAND_MIN_OWN_EVIDENCE);
    expect(small.source).toBe("pooled");
    expect(small.k).toBe(t.pooledByLead["h2-6"].k);
    expect(small.k).toBe(4.75); // 1040 slots: 10·95 + 40 ≥ 988, own would be 0.5
    expect(small.coverageCalibrated).toBe(1);
  });

  it("a borrowing cell whose own slots already cover never takes a wider pool factor", () => {
    // Production 2026-11-02: h24+|mid had 424 slots, raw coverage 100 %, yet
    // took the quiet-dominated pool's 2.35 → Taron's band doubled to ±50.
    const rows = [
      ...uniform("h24+", "quiet", 10), // pool k 4.75
      { lead: "h24+", segment: "mid", bin: 10, n: 424 }, // all at s = 0.5
    ];
    const t = fitBandCalibration(rows);
    const mid = t.cells["h24+|mid"];
    expect(mid.n).toBeLessThan(BAND_MIN_SAMPLE);
    expect(mid.source).toBe("pooled");
    expect(mid.k).toBe(0.5);
    expect(mid.coverageCalibrated).toBe(1);
    expect(bandFactor(t, 30 * 24 * 60, 30)).toBe(1); // served: raw band, not doubled
  });

  it("a borrowing cell that over-forecasts (own 95th score ≤ 0) serves the raw band, not the pool", () => {
    // The production shape behind PAR-1222: h24+|mid raw coverage 100 %, own fit
    // has no factor at all — the first fix still lent it the pool's 2.15.
    const rows = [
      ...uniform("h24+", "quiet", 10),
      { lead: "h24+", segment: "mid", bin: -2, n: 432 },
    ];
    const t = fitBandCalibration(rows);
    expect(t.cells["h24+|mid"].k).toBeNull();
    expect(bandFactor(t, 30 * 24 * 60, 30)).toBeNull(); // raw
    expect(bandFactor(t, 30 * 24 * 60, 10)).toBe(2); // quiet keeps its widened band
  });

  it("a borrowing cell that needs MORE than the pool still gets the pool", () => {
    const rows = [
      ...uniform("h6-24", "quiet", 10), // pool k ≈ 4.75
      { lead: "h6-24", segment: "busy", bin: 100, n: 60 }, // own would be 5
    ];
    expect(fitBandCalibration(rows).cells["h6-24|busy"].k).toBe(
      fitBandCalibration(rows).pooledByLead["h6-24"].k,
    );
  });

  it("never lends a short-lead factor to a long lead bucket (PAR-1060 review)", () => {
    const t = fitBandCalibration([
      ...uniform("h0-2", "busy", 10), // well-sampled short lead, k 4.75
      { lead: "h24+", segment: "busy", bin: 30, n: 40 }, // thin long lead
    ]);
    expect(t.cells["h0-2|busy"].k).toBe(4.75);
    expect(t.cells["h24+|busy"].k).toBeNull();
    expect(bandFactor(t, 60, 70)).toBe(2); // fitted 4.75, served clamped (PAR-1083)
    expect(bandFactor(t, 30 * 60, 70)).toBeNull(); // h24+ busy: raw
    expect(bandFactor(t, 30 * 60, 10)).toBeNull(); // h24+ quiet, no cell: raw
    expect(bandFactor(t, 8 * 60, 70)).toBeNull(); // h6-24, no data at all: raw
  });

  it("no k anywhere when even the pool is too small", () => {
    const t = fitBandCalibration([
      { lead: "h0-2", segment: "busy", bin: 30, n: 10 },
    ]);
    expect(t.pooledByLead["h0-2"].k).toBeNull();
    expect(t.cells["h0-2|busy"].k).toBeNull();
    expect(bandFactor(t, 60, 70)).toBeNull();
  });

  it("a cell whose 95th score is not above 0 gets no factor", () => {
    const t = fitBandCalibration([
      { lead: "h0-2", segment: "busy", bin: -3, n: 1000 },
    ]);
    expect(t.pooledByLead["h0-2"].k).toBeNull();
    expect(bandFactor(t, 60, 70)).toBeNull();
  });
});

describe("bandFactor clamp (PAR-1083)", () => {
  /** A table whose only cell carries the given k, as Redis would hand it back. */
  const withK = (k: number) =>
    ({
      cells: { "h0-2|busy": { n: 1000, k, source: "cell" } },
      pooledByLead: {},
    }) as never;

  it("serves a stored k of 4.2 as 2 and a stored k of 0.25 as 1", () => {
    expect(bandFactor(withK(4.2), 60, 70)).toBe(2);
    expect(bandFactor(withK(0.25), 60, 70)).toBe(1);
    expect(bandFactor(withK(1.4), 60, 70)).toBe(1.4);
  });
});

describe("applyBandCalibration", () => {
  const table = fitBandCalibration(uniform("h2-6", "busy", 10));
  const now = Date.UTC(2026, 9, 24, 12);
  const at = (hours: number) => new Date(now + hours * 3_600_000).toISOString();

  it("scales hourly bands by the slot's cell and rounds to whole minutes", () => {
    const preds = [
      {
        predictionType: "hourly",
        predictedTime: at(3),
        predictedWaitTime: 70,
        uncertaintyMinutes: 9,
      },
      {
        predictionType: "hourly",
        predictedTime: at(3),
        predictedWaitTime: 70,
        uncertaintyMinutes: null,
      },
      {
        predictionType: "daily",
        predictedTime: at(3),
        predictedWaitTime: 70,
        uncertaintyMinutes: 9,
      },
    ];
    expect(applyBandCalibration(preds, table, now)).toBe(1);
    expect(preds[0].uncertaintyMinutes).toBe(18); // 9 × min(4.75, 2)
    expect(preds[1].uncertaintyMinutes).toBeNull();
    expect(preds[2].uncertaintyMinutes).toBe(9);
  });

  it("leaves the band raw without a table or with something that is not one", () => {
    const p = {
      predictionType: "hourly",
      predictedTime: at(3),
      predictedWaitTime: 70,
      uncertaintyMinutes: 9,
    };
    applyBandCalibration([p], null, now);
    applyBandCalibration([p], { predictions: [] } as never, now);
    expect(p.uncertaintyMinutes).toBe(9);
  });
});
