import { ForecastAccuracyService } from "./forecast-accuracy.service";
import { ForecastAccuracyProfile } from "../entities/forecast-accuracy-profile.entity";

/** A profile row with only the fields the lookup and its callers read. */
const cell = (
  predictedBand: string,
  leadBucket: string,
  mae: number,
  sampleSize = 1000,
  uncertaintyP95 = mae * 2.5,
): ForecastAccuracyProfile =>
  ({
    predictedBand,
    leadBucket,
    mae,
    sampleSize,
    meanActual: 40,
    uncertaintyP95,
    computedAt: new Date("2026-09-11T03:10:00Z"),
  }) as ForecastAccuracyProfile;

const profileOf = (...rows: ForecastAccuracyProfile[]) =>
  new Map(rows.map((r) => [`${r.predictedBand}|${r.leadBucket}`, r]));

/**
 * The two pure classifiers. They are static and tiny, and they decide which
 * measured cell a served answer is allowed to quote — a wrong edge here hands a
 * planner the error figure from a different distance or a different band, which
 * is not a crash and would never show up as one.
 *
 * The bucket list grew from four to six in PAR-17; these pin the edges so the
 * two new ones cannot silently drift apart from the forward archive's
 * `PredictionLeadSnapshotService.LEAD_BUCKETS` again.
 */
describe("ForecastAccuracyService.bucketFor", () => {
  it("answers with the bucket a distance falls in, at every edge", () => {
    // Each bucket is an UPPER edge: the boundary day belongs to the bucket it
    // names, and the day after it moves on.
    expect(ForecastAccuracyService.bucketFor(0)).toBe("d1");
    expect(ForecastAccuracyService.bucketFor(1)).toBe("d1");
    expect(ForecastAccuracyService.bucketFor(2)).toBe("d3");
    expect(ForecastAccuracyService.bucketFor(3)).toBe("d3");
    expect(ForecastAccuracyService.bucketFor(4)).toBe("d7");
    expect(ForecastAccuracyService.bucketFor(7)).toBe("d7");
    expect(ForecastAccuracyService.bucketFor(8)).toBe("d14");
    expect(ForecastAccuracyService.bucketFor(14)).toBe("d14");
    expect(ForecastAccuracyService.bucketFor(15)).toBe("d30");
    expect(ForecastAccuracyService.bucketFor(30)).toBe("d30");
    expect(ForecastAccuracyService.bucketFor(31)).toBe("d60");
    expect(ForecastAccuracyService.bucketFor(60)).toBe("d60");
  });

  it("rounds a distance UP into the next bucket, never down", () => {
    // The direction matters and is the opposite of the forward archive's rule.
    // Here the bucket is an upper edge, so a 20-day question is answered by the
    // 30-day cell: quoting `d14` would understate the error at that distance.
    expect(ForecastAccuracyService.bucketFor(20)).toBe("d30");
    expect(ForecastAccuracyService.bucketFor(45)).toBe("d60");
  });

  it("returns null past the last bucket rather than the nearest one", () => {
    // Past 60 days the only forecast is CatBoost's, whose accuracy at that
    // distance has never been measured. `null` is what makes `/plan/day` say
    // `unmeasured` instead of handing over a 60-day figure as if it applied.
    expect(ForecastAccuracyService.bucketFor(61)).toBeNull();
    expect(ForecastAccuracyService.bucketFor(365)).toBeNull();
  });

  it("returns null for a distance in the past", () => {
    expect(ForecastAccuracyService.bucketFor(-1)).toBeNull();
  });
});

describe("ForecastAccuracyService.bandFor", () => {
  it("bands the predicted wait at 30 and 60 minutes", () => {
    expect(ForecastAccuracyService.bandFor(0)).toBe("quiet");
    expect(ForecastAccuracyService.bandFor(29)).toBe("quiet");
    expect(ForecastAccuracyService.bandFor(30)).toBe("mid");
    expect(ForecastAccuracyService.bandFor(59)).toBe("mid");
    expect(ForecastAccuracyService.bandFor(60)).toBe("busy");
    expect(ForecastAccuracyService.bandFor(240)).toBe("busy");
  });

  it("keys off the predicted wait, so a walk-on is a real band and not missing data", () => {
    // A 0-minute prediction is a prediction. It bands `quiet` and gets `quiet`'s
    // measured error; it must not fall out of the profile.
    expect(ForecastAccuracyService.bandFor(0)).toBe("quiet");
  });
});

describe("ForecastAccuracyService.lookup", () => {
  /** The full grid the nightly rebuild writes once this ships. */
  const full = profileOf(
    cell("quiet", "d1", 8.6),
    cell("quiet", "d3", 8.7),
    cell("quiet", "d7", 9.0),
    cell("quiet", "d14", 9.9),
    cell("quiet", "d30", 10.7),
    cell("quiet", "d60", 12.5),
    cell("busy", "d1", 21.5),
    cell("busy", "d3", 21.6),
    cell("busy", "d14", 24.3),
  );

  it("returns the exact cell for the distance and band", () => {
    expect(full.size).toBe(9);
    expect(ForecastAccuracyService.lookup(full, 10, 3)?.mae).toBe(8.7);
    expect(ForecastAccuracyService.lookup(full, 90, 1)?.mae).toBe(21.5);
    expect(ForecastAccuracyService.lookup(full, 10, 60)?.mae).toBe(12.5);
  });

  it("widens to the next coarser bucket when the exact cell is missing", () => {
    // `busy` has no d7 row here, so a 5-day question takes d14 — a longer
    // distance and therefore a larger error, never a shorter one.
    expect(ForecastAccuracyService.lookup(full, 90, 5)?.leadBucket).toBe("d14");
    expect(ForecastAccuracyService.lookup(full, 90, 5)?.mae).toBe(24.3);
  });

  it("survives the day after a deploy that adds a bucket", () => {
    // THE REGRESSION THIS METHOD EXISTS FOR. `rebuild()` replaces the grid once a
    // night, so right after a deploy production still holds the previous four
    // buckets. A plain `band|bucket` get would miss on the two new keys and drop
    // a measured day to `unmeasured`; widening keeps a figure on the wire.
    const preDeploy = profileOf(
      cell("quiet", "d1", 8.6),
      cell("quiet", "d7", 9.0),
      cell("quiet", "d30", 10.7),
      cell("quiet", "d60", 12.5),
    );
    // 2 days out wants the new `d3`; 10 days out wants the new `d14`.
    expect(ForecastAccuracyService.lookup(preDeploy, 10, 2)?.leadBucket).toBe(
      "d7",
    );
    expect(ForecastAccuracyService.lookup(preDeploy, 10, 10)?.leadBucket).toBe(
      "d30",
    );
    // And the pre-existing distances are untouched.
    expect(ForecastAccuracyService.lookup(preDeploy, 10, 1)?.leadBucket).toBe(
      "d1",
    );
  });

  it("never reaches past the last bucket", () => {
    // Past 60 days there is no measurement at all, and widening must not invent
    // one by handing back the 60-day cell.
    expect(ForecastAccuracyService.lookup(full, 10, 61)).toBeUndefined();
    expect(ForecastAccuracyService.lookup(full, 10, 365)).toBeUndefined();
  });

  it("returns undefined for a band with no rows at any distance", () => {
    // `mid` is absent from the fixture entirely.
    expect(ForecastAccuracyService.lookup(full, 45, 7)).toBeUndefined();
  });

  it("returns undefined on an empty profile rather than throwing", () => {
    expect(ForecastAccuracyService.lookup(new Map(), 45, 7)).toBeUndefined();
  });

  it("returns undefined for a distance in the past", () => {
    expect(ForecastAccuracyService.lookup(full, 10, -1)).toBeUndefined();
  });

  it("only ever widens AWAY from the distance asked about", () => {
    // The property the "errs larger" invariant rests on: the bucket a widened
    // lookup lands on must have an upper edge at or beyond the ideal one, so its
    // mean is taken over distances reaching further out and never nearer. If a
    // future bucket re-cuts the existing edges instead of refining them, this is
    // the assertion that should fail.
    const edges: Record<string, number> = {
      d1: 1,
      d3: 3,
      d7: 7,
      d14: 14,
      d30: 30,
      d60: 60,
    };
    const preDeploy = profileOf(
      cell("quiet", "d1", 8.6),
      cell("quiet", "d7", 9.0),
      cell("quiet", "d30", 10.7),
      cell("quiet", "d60", 12.5),
    );
    for (let lead = 0; lead <= 60; lead++) {
      const ideal = ForecastAccuracyService.bucketFor(lead);
      const got = ForecastAccuracyService.lookup(preDeploy, 10, lead);
      expect(got).toBeDefined();
      expect(edges[got!.leadBucket]).toBeGreaterThanOrEqual(edges[ideal!]);
      // And it still covers the distance asked about.
      expect(edges[got!.leadBucket]).toBeGreaterThanOrEqual(lead);
    }
  });
});

describe("the two classifiers together", () => {
  it("produce the 18 keys the profile is stored under", () => {
    const bands = [10, 45, 90];
    const leads = [1, 3, 7, 14, 30, 60];
    const keys = bands.flatMap((w) =>
      leads.map(
        (d) =>
          `${ForecastAccuracyService.bandFor(w)}|${ForecastAccuracyService.bucketFor(d)}`,
      ),
    );
    expect(new Set(keys).size).toBe(18);
    // The key shape `plan-day.service.ts` builds when it looks a ride up.
    expect(keys).toContain("busy|d3");
    expect(keys).toContain("quiet|d14");
  });
});

/**
 * The band is a second figure on the same row, and the two must not be confused
 * at the point of use: `mae` is `/plan/day`'s `expectedError` ("a typical miss")
 * and `uncertaintyP95` is the calendar's `uncertaintyMinutes` ("it can reach this
 * far"). A lookup returns the row, so the guard that matters is that a caller
 * asking for one distance cannot be handed the other's row.
 */
describe("ForecastAccuracyService.lookup carries the band", () => {
  it("returns the p95 of the cell the distance actually falls in", () => {
    const profile = profileOf(
      cell("quiet", "d1", 8.6, 1000, 23.8),
      cell("quiet", "d7", 9.0, 1000, 26.4),
      cell("quiet", "d60", 13.2, 1000, 38.1),
    );

    expect(ForecastAccuracyService.lookup(profile, 20, 1)?.uncertaintyP95).toBe(
      23.8,
    );
    expect(ForecastAccuracyService.lookup(profile, 20, 7)?.uncertaintyP95).toBe(
      26.4,
    );
    expect(
      ForecastAccuracyService.lookup(profile, 20, 45)?.uncertaintyP95,
    ).toBe(38.1);
  });

  it("widens to the coarser cell's band, which is the wider one", () => {
    // `d1` and `d3` absent — the state between a deploy that adds a bucket and
    // the next nightly rebuild. Widening must not narrow the band.
    const profile = profileOf(cell("mid", "d7", 13.6, 1000, 32.8));

    const narrow = ForecastAccuracyService.lookup(profile, 45, 1);
    expect(narrow?.leadBucket).toBe("d7");
    expect(narrow?.uncertaintyP95).toBe(32.8);
  });

  it("has nothing to widen to past the last bucket", () => {
    const profile = profileOf(cell("mid", "d60", 16.4, 1000, 45.7));

    expect(ForecastAccuracyService.lookup(profile, 45, 61)).toBeUndefined();
  });
});
