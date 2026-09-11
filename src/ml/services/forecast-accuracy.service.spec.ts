import { ForecastAccuracyService } from "./forecast-accuracy.service";

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
