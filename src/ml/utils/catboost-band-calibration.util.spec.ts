import {
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

  it("a cell under the minimum sample borrows the pooled k", () => {
    const rows = [
      ...uniform("h2-6", "busy", 10),
      // 50 slots, all at s = 0.5: alone it would read k = 0.5.
      { lead: "h0-2", segment: "quiet", bin: 10, n: 50 },
    ];
    const t = fitBandCalibration(rows);
    const small = t.cells["h0-2|quiet"];
    expect(small.n).toBeLessThan(BAND_MIN_SAMPLE);
    expect(small.source).toBe("pooled");
    expect(small.k).toBe(t.pooled.k);
    expect(small.k).not.toBe(0.5);
    expect(small.coverageCalibrated).toBe(1);
  });

  it("no k anywhere when even the pool is too small", () => {
    const t = fitBandCalibration([
      { lead: "h0-2", segment: "busy", bin: 30, n: 10 },
    ]);
    expect(t.pooled.k).toBeNull();
    expect(t.cells["h0-2|busy"].k).toBeNull();
    expect(bandFactor(t, 60, 70)).toBeNull();
  });

  it("a cell whose 95th score is not above 0 gets no factor", () => {
    const t = fitBandCalibration([
      { lead: "h0-2", segment: "busy", bin: -3, n: 1000 },
    ]);
    expect(t.pooled.k).toBeNull();
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
    expect(preds[0].uncertaintyMinutes).toBe(43); // 9 × 4.75 = 42.75
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
