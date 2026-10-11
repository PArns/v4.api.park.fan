import {
  CHRONOS_MIN_DAYS,
  CHRONOS_MIN_PARKS,
  PairedRow,
  buildChronosBoard,
  compare,
  parkClusterBootstrap,
  seededRandom,
} from "./forecast-archive-paired.util";

function rows(
  parks: number,
  days: number,
  perPark: (p: number, d: number) => { a: number; b: number },
  reference = "served",
): PairedRow[] {
  const out: PairedRow[] = [];
  for (let p = 0; p < parks; p++) {
    for (let d = 0; d < days; d++) {
      const { a, b } = perPark(p, d);
      out.push({
        targetDate: `2026-10-${String(d + 1).padStart(2, "0")}`,
        parkId: `p${p}`,
        useCase: "UC1",
        lead: "h0-1",
        model: "chronos2",
        reference,
        segment: "all",
        n: 10,
        saeModel: a,
        saeRef: b,
        seModel: 0,
        seRef: 0,
      });
    }
  }
  return out;
}

describe("forward-archive paired comparison (PAR-852)", () => {
  it("is deterministic for a seed", () => {
    const a = seededRandom(7);
    const b = seededRandom(7);
    expect([a(), a(), a()]).toEqual([b(), b(), b()]);
  });

  it("gives no CI for a single park — one cluster cannot be resampled", () => {
    expect(parkClusterBootstrap([{ n: 10, a: 1, b: 2 }])).toBeNull();
  });

  it("pools the paired difference over slots, then divides once", () => {
    const c = compare(
      "served",
      rows(2, 1, (p) => (p === 0 ? { a: 10, b: 30 } : { a: 50, b: 50 })),
    );
    // (Σ|e_model| − Σ|e_ref|) / Σn = (60 − 80) / 20.
    expect(c).toMatchObject({ n: 20, modelMae: 3, referenceMae: 4, diff: -1 });
  });

  it("calls a consistent win only with enough parks and days", () => {
    const win = rows(CHRONOS_MIN_PARKS, CHRONOS_MIN_DAYS, (p, d) => ({
      a: 40 + ((p * 7 + d) % 5),
      b: 60 + ((p * 3 + d) % 5),
    }));
    const c = compare("served", win);
    expect(c.enoughData).toBe(true);
    expect(c.ciHigh!).toBeLessThan(0);
    expect(c.verdict).toBe("chronos_better");

    const few = compare(
      "served",
      win.filter((r) => r.parkId !== "p0"),
    );
    expect(few.parks).toBe(CHRONOS_MIN_PARKS - 1);
    expect(few.verdict).toBe("insufficient_data");
  });

  it("reports no significant difference when parks disagree", () => {
    const mixed = rows(12, 14, (p) =>
      p % 2 === 0 ? { a: 40, b: 60 } : { a: 60, b: 40 },
    );
    const c = compare("persistence", mixed);
    expect(c.ciLow!).toBeLessThan(0);
    expect(c.ciHigh!).toBeGreaterThan(0);
    expect(c.verdict).toBe("no_significant_difference");
  });

  it("builds one row per cell with its comparisons in a fixed order", () => {
    const board = buildChronosBoard(
      [
        ...rows(3, 2, () => ({ a: 1, b: 2 }), "persistence"),
        ...rows(3, 2, () => ({ a: 1, b: 2 }), "served"),
      ],
      [
        {
          useCase: "UC1",
          lead: "h0-1",
          source: "chronos2",
          segment: "all",
          days: 2,
          metrics: { mae: 0.5, bias: 0.1, bandCoverage: 0.8 },
          sums: { n: 70 },
        },
        // Served rows are not part of this section.
        {
          useCase: "UC1",
          lead: "h0-1",
          source: "all",
          segment: "all",
          days: 2,
          metrics: { mae: 9 },
          sums: { n: 70 },
        },
      ],
      true,
    );
    expect(board.captureEnabled).toBe(true);
    expect(board.rows).toHaveLength(1);
    expect(board.rows[0]).toMatchObject({
      model: "chronos2",
      surface: "chronos_live",
      mae: 0.5,
      n: 70,
      days: 2,
    });
    expect(board.rows[0].comparisons.map((c) => c.reference)).toEqual([
      "served",
      "persistence",
    ]);
  });
});
