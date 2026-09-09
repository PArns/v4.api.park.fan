import { MIN_ESTIMATE_SAMPLE, estimateOutage } from "./downtime-estimate.util";
import type { DowntimeRecoveryCurve } from "../entities/downtime-recovery-curve.entity";

/**
 * The pooled curve as measured against production on 2026-09-06, 180 days of
 * reconstructed intervals. Kept as real numbers rather than round ones so a
 * change in the estimator shows up as a change against reality.
 */
const POOLED: Array<
  [number, number, number, number, number | null, number | null, number | null]
> = [
  // elapsed, atRisk, rec30, rec60, p25, median, p75
  [5, 128412, 0.542, 0.705, 10, 25, 75],
  [10, 118248, 0.5, 0.67, 10, 30, 87],
  [15, 97054, 0.47, 0.638, 15, 35, 97],
  [20, 82812, 0.426, 0.603, 15, 45, 112],
  [30, 62109, 0.37, 0.558, 20, 50, 140],
  [45, 44653, 0.317, 0.504, 25, 60, 192],
  [60, 34190, 0.298, 0.468, 25, 70, 255],
  [90, 21883, 0.242, 0.38, 35, 110, 460],
  [120, 14957, 0.183, 0.291, 50, 165, null],
  [180, 8545, 0.107, 0.202, 85, 305, null],
  [240, 5924, 0.085, 0.148, 121, 1168, null],
];

const row = (
  [elapsedMinutes, atRisk, r30, r60, p25, median, p75]: (typeof POOLED)[number],
  parkId: string | null = null,
): DowntimeRecoveryCurve =>
  ({
    id: `${parkId ?? "pooled"}:${elapsedMinutes}`,
    parkId,
    elapsedMinutes,
    atRisk,
    recoveryWithin30: r30.toFixed(3),
    recoveryWithin60: r60.toFixed(3),
    remainingP25: p25,
    remainingMedian: median,
    remainingP75: p75,
    generatedAt: new Date(),
  }) as DowntimeRecoveryCurve;

const pooled = POOLED.map((r) => row(r));

describe("estimateOutage", () => {
  it("reads the bucket at or below the elapsed figure, never the one above", () => {
    // 44 minutes has passed 30 but not 45. Reading 45 would report a recovery
    // share the outage has not earned.
    const e = estimateOutage({ park: [], pooled }, 44);
    expect(e?.bucketMinutes).toBe(30);
    expect(e?.recoveryWithin30).toBeCloseTo(0.37);
  });

  it("does not interpolate between buckets", () => {
    const a = estimateOutage({ park: [], pooled }, 60);
    const b = estimateOutage({ park: [], pooled }, 89);
    expect(a?.recoveryWithin30).toBe(b?.recoveryWithin30);
  });

  it("says nothing below the first bucket", () => {
    // No reconstructed interval is shorter than MIN_OUTAGE_OPERATING_MINUTES,
    // so there is no measurement to read at two minutes.
    expect(estimateOutage({ park: [], pooled }, 2)).toBeUndefined();
  });

  it("leaves the range open rather than dropping it past two hours", () => {
    const late = estimateOutage({ park: [], pooled }, 130);
    expect(late).toBeDefined();
    // "At least fifty minutes more, no upper bound we can measure" is the most
    // useful thing there is to say about a long outage, and it renders as an
    // open range. Collapsing it would show nothing on exactly the outages a
    // visitor most wants to understand.
    expect(late?.remaining).toEqual({ p25: 50, median: 165, p75: null });
    expect(late?.recoveryWithin60).toBeCloseTo(0.291);
  });

  it("keeps the range while all three quantiles resolve", () => {
    expect(estimateOutage({ park: [], pooled }, 60)?.remaining).toEqual({
      p25: 25,
      median: 70,
      p75: 255,
    });
  });

  it("prefers the park's own curve when it is dense enough", () => {
    const park = [row([30, 900, 0.52, 0.7, 10, 20, 60], "p1")];
    const e = estimateOutage({ park, pooled }, 35);
    expect(e?.basis).toBe("park");
    expect(e?.recoveryWithin30).toBeCloseTo(0.52);
  });

  it("falls back per bucket, not per park", () => {
    // The park carries 30 minutes but nothing at four hours, where only a
    // handful of its spells ever reach.
    const park = [row([30, 900, 0.52, 0.7, 10, 20, 60], "p1")];
    expect(estimateOutage({ park, pooled }, 240)?.basis).toBe("pooled");
    expect(estimateOutage({ park, pooled }, 35)?.basis).toBe("park");
  });

  it("ignores a park bucket under the sample floor", () => {
    const park = [
      row([30, MIN_ESTIMATE_SAMPLE - 1, 0.99, 0.99, 1, 1, 1], "p1"),
    ];
    const e = estimateOutage({ park, pooled }, 35);
    expect(e?.basis).toBe("pooled");
    expect(e?.recoveryWithin30).toBeCloseTo(0.37);
  });

  it("the hazard falls: a longer outage is never reported as more likely to end", () => {
    let previous = 1;
    for (const t of [5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240]) {
      const e = estimateOutage({ park: [], pooled }, t);
      expect(e).toBeDefined();
      expect(e!.recoveryWithin30).toBeLessThanOrEqual(previous);
      previous = e!.recoveryWithin30;
    }
  });

  it("returns nothing rather than guessing on a negative or absurd elapsed", () => {
    expect(estimateOutage({ park: [], pooled }, -1)).toBeUndefined();
    expect(estimateOutage({ park: [], pooled }, NaN)).toBeUndefined();
  });
});

describe("estimateOutage — the recovery window", () => {
  const asOf = new Date("2026-09-09T13:00:00.000Z");
  const windows = [
    {
      opensAt: new Date("2026-09-09T08:00:00.000Z"),
      closesAt: new Date("2026-09-09T18:00:00.000Z"),
    },
    {
      opensAt: new Date("2026-09-10T08:00:00.000Z"),
      closesAt: new Date("2026-09-10T18:00:00.000Z"),
    },
  ];

  it("places the quartiles on the calendar when one is handed over", () => {
    // The 60-minute bucket: p25 25, p75 255, both inside today.
    const e = estimateOutage({ park: [], pooled }, 60, { windows, asOf });
    expect(e?.recoveryWindow).toEqual({
      from: "2026-09-09T13:25:00.000Z",
      to: "2026-09-09T17:15:00.000Z",
    });
    // And the operating-minute pair is untouched beside it: the frontend still
    // renders `remaining` until it moves over.
    expect(e?.remaining).toEqual({ p25: 25, median: 70, p75: 255 });
  });

  it("carries a window past today's closing rather than into the night", () => {
    // 17:20, park shuts at 18:00. The 120-minute bucket's p25 of 50 operating
    // minutes lands ten minutes into tomorrow, not at 18:10 tonight.
    const e = estimateOutage({ park: [], pooled }, 120, {
      windows,
      asOf: new Date("2026-09-09T17:20:00.000Z"),
    });
    expect(e?.recoveryWindow).toEqual({
      from: "2026-09-10T08:10:00.000Z",
      to: null,
    });
  });

  it("omits the window, not the estimate, when no calendar is handed over", () => {
    // A park with no published hours still gets its probabilities: the
    // recovery share is measured in operating minutes on both sides and does
    // not need a clock.
    const e = estimateOutage({ park: [], pooled }, 60);
    expect(e).toBeDefined();
    expect(e?.recoveryWindow).toBeUndefined();
    expect(e?.recoveryWithin30).toBeCloseTo(0.298);
  });

  it("omits the window where the curve gave no quartiles to place", () => {
    // Nothing to derive it from, and a window built off the median alone would
    // be the single instant the pair exists to avoid.
    const thin = [row([5, 128412, 0.542, 0.705, null, null, null])];
    const e = estimateOutage({ park: [], pooled: thin }, 30, { windows, asOf });
    expect(e?.remaining).toBeUndefined();
    expect(e?.recoveryWindow).toBeUndefined();
  });
});
