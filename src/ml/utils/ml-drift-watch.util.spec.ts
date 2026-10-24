import {
  DRIFT_THRESHOLDS,
  DriftWindowSums,
  buildDriftWatch,
  driftModel,
} from "./ml-drift-watch.util";

/** A window of `n` slots: MAE 8, weekday-median MAE 10 (skill 0.8), coverages as given. */
function sums(
  n: number,
  { skill = 0.8, cov80 = 0.8, cov95 = 0.95 } = {},
): DriftWindowSums {
  return {
    n,
    sae: 8 * n,
    nBase: n,
    saeOnBase: skill * 10 * n,
    saeBase: 10 * n,
    nQ: n,
    hitQ80: cov80 * n,
    hitQ95: cov95 * n,
  };
}

describe("driftModel", () => {
  it("is ok inside every threshold", () => {
    const m = driftModel(
      "catboost",
      sums(5000, { cov80: 0.751, cov95: 0.921, skill: 0.87 }),
      sums(20000),
    );
    expect(m.status).toBe("ok");
    expect(m.reasons).toEqual([]);
    expect(m.recent).toMatchObject({ mae: 8, baselineMae: 10, skill: 0.87 });
  });

  it("flags q0.8 coverage moving more than 5 points", () => {
    const m = driftModel("catboost", sums(5000, { cov80: 0.74 }), sums(20000));
    expect(m.status).toBe("drift");
    expect(m.reasons).toHaveLength(1);
    expect(m.reasons[0]).toContain("q0,8");
  });

  it("flags q0.95 coverage moving more than 3 points", () => {
    const m = driftModel("catboost", sums(5000, { cov95: 0.915 }), sums(20000));
    expect(m.status).toBe("drift");
    expect(m.reasons[0]).toContain("q0,95");
  });

  it("flags the MAE rising more than 10 % against the weekday median", () => {
    // 0.8 → 0.89: +11 %. A busier week that raises both MAEs alike is not drift.
    expect(
      driftModel("pcn", sums(5000, { skill: 0.89 }), sums(20000)).status,
    ).toBe("drift");
    expect(
      driftModel("pcn", sums(5000, { skill: 0.87 }), sums(20000)).status,
    ).toBe("ok");
  });

  it("reads no_data below the slot floor, never a verdict", () => {
    const m = driftModel(
      "chronos",
      sums(DRIFT_THRESHOLDS.minSlots - 1, { skill: 2 }),
      sums(20000),
    );
    expect(m.status).toBe("no_data");
    expect(driftModel("chronos", undefined, undefined).status).toBe("no_data");
  });

  it("ignores coverage where no quantile is stored", () => {
    const noQ = (n: number) => ({ ...sums(n), nQ: 0, hitQ80: 0, hitQ95: 0 });
    const m = driftModel("pcn", noQ(5000), noQ(20000));
    expect(m.recent.coverageQ80).toBeNull();
    expect(m.status).toBe("ok");
  });
});

describe("buildDriftWatch", () => {
  it("carries every served model and the worst status", () => {
    const w = buildDriftWatch(
      new Map([
        ["catboost", { recent: sums(5000), previous: sums(20000) }],
        ["pcn", { recent: sums(5000, { skill: 1.2 }), previous: sums(20000) }],
      ]),
      new Date("2026-11-01T12:30:00Z"),
    );
    expect(w.models.map((m) => [m.model, m.status])).toEqual([
      ["catboost", "ok"],
      ["pcn", "drift"],
      ["chronos", "no_data"],
    ]);
    expect(w.status).toBe("drift");
    expect(w.computedAt).toBe("2026-11-01T12:30:00.000Z");
  });
});
