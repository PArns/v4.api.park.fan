import {
  CHRONOS_MIN_DAYS,
  CHRONOS_MIN_PARKS,
  BootstrapCell,
  PairedRow,
  blockBootstrap,
  buildChronosBoard,
  compare,
  gateDays,
  parkClusterBootstrap,
  seededRandom,
  stratumMeanDiff,
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

  it("counts the days the 10th-best park reached (PAR-1024)", () => {
    const set = (n: number) =>
      new Set(Array.from({ length: n }, (_, i) => `d${i}`));
    expect(gateDays([])).toBe(0);
    // Fewer than 10 parks: the weakest park's count.
    expect(gateDays([set(14), set(3)])).toBe(3);
    expect(gateDays([...Array(10)].map(() => set(14)).concat(set(1)))).toBe(14);
    // One park × 14 days + nine × 1 day: 14 dates in all, 1 per the gate.
    const sparse = [
      ...rows(1, 14, () => ({ a: 1, b: 9 })),
      ...rows(9, 1, () => ({ a: 1, b: 9 })).map((r, i) => ({
        ...r,
        parkId: `q${i}`,
      })),
    ];
    const c = compare("served", sparse);
    expect(c.days).toBe(1);
    expect(c.enoughData).toBe(false);
    expect(c.verdict).toBe("insufficient_data");
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

describe("blockBootstrap — park × date (PAR-1036)", () => {
  /** 12 parks × 20 dates; `shock(d)` moves every park on date d alike. */
  function grid(shock: (d: number) => number): BootstrapCell[] {
    const out: BootstrapCell[] = [];
    for (let p = 0; p < 12; p++) {
      for (let d = 0; d < 20; d++) {
        const diff = -0.5 + (p % 4) * 0.1 + shock(d);
        out.push({
          park: `p${p}`,
          date: `2026-10-${String(d + 1).padStart(2, "0")}`,
          stratum: "d1",
          n: 10,
          a: 10 * (10 + diff),
          b: 100,
        });
      }
    }
    return out;
  }

  it("is the park-cluster bootstrap, draw for draw, with one lead", () => {
    const cells = grid(() => 0);
    const perPark = new Map<string, { n: number; a: number; b: number }>();
    for (const c of cells) {
      const x = perPark.get(c.park) ?? { n: 0, a: 0, b: 0 };
      x.n += c.n;
      x.a += c.a;
      x.b += c.b;
      perPark.set(c.park, x);
    }
    const old = parkClusterBootstrap([...perPark.values()], 500, 7)!;
    const now = blockBootstrap(cells, { twoWay: false }, 500, 7)!;
    expect(now.low).toBeCloseTo(old.low, 10);
    expect(now.high).toBeCloseTo(old.high, 10);
  });

  it("widens the interval when a day moves every park at once", () => {
    // Four dates where every park did 3 minutes worse (a holiday, a storm):
    // the park-only interval treats that as twelve independent draws.
    const shocked = grid((d) => (d % 5 === 0 ? 3 : 0));
    const park = blockBootstrap(shocked, { twoWay: false }, 1000, 3)!;
    const two = blockBootstrap(shocked, { twoWay: true }, 1000, 3)!;
    expect(two.high - two.low).toBeGreaterThan(1.5 * (park.high - park.low));
    // Same centre, wider on both sides.
    expect(two.low).toBeLessThan(park.low);
    expect(two.high).toBeGreaterThan(park.high);
  });

  it("needs two parks, and two dates for the date draw", () => {
    const one = grid(() => 0).filter((c) => c.park === "p0");
    expect(blockBootstrap(one, { twoWay: false })).toBeNull();
    const oneDay = grid(() => 0).filter((c) => c.date === "2026-10-01");
    expect(blockBootstrap(oneDay, { twoWay: true })).toBeNull();
    expect(blockBootstrap(oneDay, { twoWay: false })).not.toBeNull();
  });

  it("weighs leads equally inside the statistic", () => {
    const cells = [
      { stratum: "d1", n: 1000, a: 8000, b: 10000 }, // −2 on many slots
      { stratum: "d60", n: 10, a: 100, b: 100 }, // 0 on few
    ];
    expect(stratumMeanDiff(cells)).toBeCloseTo(-1, 10);
  });
});
