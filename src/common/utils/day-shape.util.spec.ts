import { composeDayCurve, unfoldedCloseHour } from "./day-shape.util";

/**
 * The composition is the only place a per-ride, per-hour number for a future day
 * comes from — no model produces one. Everything here is a decision that would
 * be invisible once rendered: a curve scaled off the wrong statistic, or
 * extrapolated into hours nobody measured, looks exactly as confident as a
 * correct one.
 *
 * Phantasialand is the recurring case: its hourly profile really does return
 * [10, 11, 12, 13, 17] against Europa-Park's [9 … 19], so gaps and short
 * profiles are the normal input here, not the edge case.
 */
describe("composeDayCurve", () => {
  const shape = {
    // A rising morning, a midday plateau, a quiet evening.
    shapeHours: [9, 12, 18],
    shapeP50: [20, 40, 10],
  };

  it("puts the curve's peak at the predicted day level", () => {
    // The daily prediction is a day-PEAK proxy (predict.py collapses the
    // peak-window hours to a per-day MAX), so the maximum is what it names.
    const curve = composeDayCurve({
      ...shape,
      dayPeak: 60,
      openHour: 9,
      closeHour: 18,
    })!;

    expect(Math.max(...curve.map((p) => p.wait))).toBe(60);
    // And it lands on the hour the shape peaks at, not somewhere else.
    expect(curve.find((p) => p.wait === 60)!.hour).toBe(12);
  });

  it("keeps the shape's proportions while rescaling", () => {
    const curve = composeDayCurve({
      ...shape,
      dayPeak: 80, // 2× the shape's own peak of 40
      openHour: 9,
      closeHour: 18,
    })!;

    expect(curve.find((p) => p.hour === 9)!.wait).toBe(40); // 20 × 2
    expect(curve.find((p) => p.hour === 12)!.wait).toBe(80); // 40 × 2
    expect(curve.find((p) => p.hour === 18)!.wait).toBe(20); // 10 × 2
  });

  it("interpolates the hours between measurements", () => {
    const curve = composeDayCurve({
      shapeHours: [10, 14],
      shapeP50: [20, 40],
      dayPeak: 40, // factor 1
      openHour: 10,
      closeHour: 14,
    })!;

    // Halfway between 20 and 40 at hour 12, rounded to 5.
    expect(curve.find((p) => p.hour === 12)!.wait).toBe(30);
    expect(curve.find((p) => p.hour === 11)!.wait).toBe(25);
  });

  it("holds the end values flat instead of extrapolating", () => {
    // Phantasialand's shape stops at 13:00 while the park runs to 20:00. A
    // continued downward trend would reach zero and read as "no queue after
    // 17:00", which is a claim about the ride rather than about our data.
    const curve = composeDayCurve({
      shapeHours: [10, 13],
      shapeP50: [40, 20],
      dayPeak: 40,
      openHour: 9,
      closeHour: 20,
    })!;

    expect(curve.find((p) => p.hour === 9)!.wait).toBe(40); // before the shape
    for (const hour of [14, 17, 20]) {
      expect(curve.find((p) => p.hour === hour)!.wait).toBe(20); // after it
    }
  });

  it("treats a gap as unknown, never as zero minutes", () => {
    // A null in the profile means the hour produced no bucket (feed gap,
    // refurbishment, too few readings). Scaling a zero into the curve would
    // both dent the shape and, being the maximum's denominator, inflate every
    // other hour.
    const withGap = composeDayCurve({
      shapeHours: [10, 12, 14],
      shapeP50: [40, null, 40],
      dayPeak: 40,
      openHour: 10,
      closeHour: 14,
    })!;

    // 12:00 interpolates between its measured neighbours, both at 40.
    expect(withGap.find((p) => p.hour === 12)!.wait).toBe(40);
    expect(withGap.every((p) => p.wait === 40)).toBe(true);
  });

  it("returns null when there is no measured shape at all", () => {
    // Not a flat line at the day's level: that would read as "the same queue
    // all day", which is a statement about the ride. Null lets the caller
    // render nothing.
    expect(
      composeDayCurve({
        shapeHours: [10, 11],
        shapeP50: [null, null],
        dayPeak: 50,
        openHour: 9,
        closeHour: 18,
      }),
    ).toBeNull();

    expect(
      composeDayCurve({
        shapeHours: [],
        shapeP50: [],
        dayPeak: 50,
        openHour: 9,
        closeHour: 18,
      }),
    ).toBeNull();
  });

  it("gives a flat day from a single measured hour", () => {
    // One hour carries no shape, but it does carry a level, and the day
    // prediction carries the rest. Flat is the honest reading here.
    const curve = composeDayCurve({
      shapeHours: [12],
      shapeP50: [30],
      dayPeak: 45,
      openHour: 10,
      closeHour: 14,
    })!;

    expect(curve).toHaveLength(5);
    expect(curve.every((p) => p.wait === 45)).toBe(true);
  });

  it("answers zero for a day the model expects no queue on", () => {
    const curve = composeDayCurve({
      ...shape,
      dayPeak: 0,
      openHour: 9,
      closeHour: 12,
    })!;

    expect(curve.map((p) => p.wait)).toEqual([0, 0, 0, 0]);
  });

  it("covers exactly the open hours, inclusive", () => {
    const curve = composeDayCurve({
      ...shape,
      dayPeak: 40,
      openHour: 11,
      closeHour: 15,
    })!;

    expect(curve.map((p) => p.hour)).toEqual([11, 12, 13, 14, 15]);
  });

  it("refuses a closing hour before the opening one", () => {
    expect(
      composeDayCurve({
        ...shape,
        dayPeak: 40,
        openHour: 18,
        closeHour: 9,
      }),
    ).toBeNull();
  });

  it("rounds only the output, not the working values", () => {
    // Rounding the shape before scaling changes the answer, and the two are
    // easy to confuse because they agree on most inputs. These values are
    // chosen so they disagree:
    //
    //   late  (correct): peak 19, factor 40/19 = 2.105 → 11×2.105 = 23.2 → 25
    //   early (wrong):   11→10, 19→20, factor 2       → 10×2     = 20   → 20
    //
    // Both give 40 at the peak, so only the first hour tells them apart.
    const curve = composeDayCurve({
      shapeHours: [10, 11],
      shapeP50: [11, 19],
      dayPeak: 40,
      openHour: 10,
      closeHour: 11,
    })!;

    expect(curve.find((p) => p.hour === 10)!.wait).toBe(25);
    expect(curve.find((p) => p.hour === 11)!.wait).toBe(40);
  });

  // ── A day that runs past midnight ─────────────────────────────────────────
  describe("past midnight", () => {
    it("draws every hour of a day that ends after midnight", () => {
      // La Ronde's day is 10 → 0, which arrives here unfolded as 10 → 24. The
      // folded pair returns null (the test above), and that null was the whole
      // of what /plan/day answered for such a day.
      const curve = composeDayCurve({
        shapeHours: [10, 14, 22],
        shapeP50: [20, 40, 30],
        dayPeak: 40,
        openHour: 10,
        closeHour: unfoldedCloseHour(10, 0),
      })!;

      expect(curve[0].hour).toBe(10);
      expect(curve[curve.length - 1].hour).toBe(24);
      expect(curve.map((p) => p.hour)).toEqual([
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
      ]);
    });

    it("reads a midnight bucket in the shape as the end of the day", () => {
      // The profile buckets by WALL-CLOCK hour, so a 00:00 reading is written
      // as 0. Left where it sits it sorts in front of the morning, and every
      // hour of the evening is then interpolated towards it: the 22:00 value
      // below would be dragged down to the 5-minute midnight reading instead of
      // holding its own until the last hour.
      const curve = composeDayCurve({
        shapeHours: [0, 10, 22],
        shapeP50: [5, 20, 40],
        dayPeak: 40, // factor 1: the shape's own peak is already 40
        openHour: 10,
        closeHour: unfoldedCloseHour(10, 0),
      })!;

      expect(curve.find((p) => p.hour === 22)!.wait).toBe(40);
      // Midnight is the shape's own 00:00 reading, at the end where it belongs.
      expect(curve.find((p) => p.hour === 24)!.wait).toBe(5);
      // And the hour between the two is interpolated across the pair, not held
      // flat: halfway from 40 to 5 is 22.5, rounded up to 25.
      expect(curve.find((p) => p.hour === 23)!.wait).toBe(25);
    });

    it("unfolds a close hour only when the day actually wraps", () => {
      expect(unfoldedCloseHour(10, 18)).toBe(18);
      expect(unfoldedCloseHour(10, 0)).toBe(24);
      expect(unfoldedCloseHour(16, 1)).toBe(25);
      // Not a wrap: a park open for a single hour.
      expect(unfoldedCloseHour(10, 10)).toBe(10);
    });
  });

  it("lands the peak on the prediction only as closely as fives allow", () => {
    // The scale is exact, the output is rounded. A dayPeak that is not itself a
    // multiple of five therefore comes back as its nearest five — which is
    // correct, because the number is displayed as a wait time and parks post
    // those in fives. In practice predict.py has already rounded it.
    const curve = composeDayCurve({
      shapeHours: [10],
      shapeP50: [30],
      dayPeak: 43,
      openHour: 10,
      closeHour: 10,
    })!;

    expect(curve[0].wait).toBe(45);
  });
});
