import { roundToNearest5Minutes } from "./wait-time.utils";

/**
 * Turn a day-level prediction into an hour-by-hour curve.
 *
 * The planner asks "what will this ride's queue be at 14:00 on 17 October", and
 * no model answers that. What exists is a prediction for the DAY (one number per
 * ride, reaching 60 days out) and a historical profile of what that ride's day
 * normally LOOKS like (P50 per hour, from `/stats/hourly`). This composes the
 * two: the level is predicted, the shape is historical.
 *
 * Three things this deliberately does not do.
 *
 * **It does not extrapolate past the measured hours.** A ride with data for
 * 10:00–13:00 in a park open until 20:00 gets its last known value held flat to
 * closing, not a trend continued into hours nobody has measured. Holding a value
 * is visibly a guess; a continued trend looks like knowledge.
 *
 * **It does not invent a shape.** With no measured hour at all it returns null,
 * and the caller renders nothing rather than a flat line at the day's level —
 * which would read as "the queue is the same all day", a claim about the ride
 * rather than an admission of ignorance.
 *
 * **It does not round the intermediate maths.** Only the output is rounded to
 * five, because parks post waits in fives; scaling and interpolating on
 * pre-rounded values compounds the error and invents ties between hours that the
 * data does not have.
 *
 * WHY THE PEAK IS THE ANCHOR. The daily prediction is a day-PEAK proxy —
 * `predict.py` collapses the peak-window hours to a per-day MAX, and the
 * calendar scores it against the realised day-P90 for exactly that reason. So
 * the shape is scaled to put its maximum at the predicted level. Anchoring on
 * the mean instead would push every hour up by the ratio between a day's peak
 * and its average, which on a headliner is most of the number.
 */

export interface DayCurvePoint {
  /**
   * Park-local hour. 0–23 on an ordinary day; on a day that runs past midnight
   * it continues past 23 rather than wrapping — 24 is that day's midnight — so
   * the curve stays one ascending series. See {@link unfoldedCloseHour}.
   */
  hour: number;
  /** Expected wait in minutes, rounded to 5. */
  wait: number;
}

export interface ComposeDayCurveOptions {
  /**
   * Hours the historical profile has data for, ascending. These come from
   * `/stats/hourly`, where they are a filter over observed hours and NOT a
   * range — Phantasialand returns [10, 11, 12, 13, 17] against Europa-Park's
   * [9 … 19], and the gaps are real.
   */
  shapeHours: readonly number[];
  /** Positional against `shapeHours`. `null` is a gap, never zero minutes. */
  shapeP50: readonly (number | null)[];
  /** The day-level prediction for this ride, as a day-peak proxy. */
  dayPeak: number;
  /**
   * First and last park-local hour the park is open, inclusive, and `closeHour`
   * **unfolded** — 24, not 0, for a day that ends at midnight. Pass it through
   * {@link unfoldedCloseHour}; a still-wrapped pair returns null below, because
   * the alternative is a curve of zero points served as if it were an answer.
   */
  openHour: number;
  closeHour: number;
}

/**
 * The day's last hour, counted from its own opening rather than from midnight.
 *
 * A park whose day runs past midnight publishes `closeHour < openHour` — La
 * Ronde is `10 → 0`, Six Flags Magic Mountain on Halloween is `10 → 1` — and
 * every loop of the form `for (h = openHour; h <= closeHour; h++)` runs **zero
 * times** for one. Unfolding turns those into `10 → 24` and `10 → 25`, so an
 * operating day is one ascending run of hours and everything that walks it
 * needs to know nothing about midnight.
 *
 * The rule lives here and nowhere else on purpose. It was written inline in two
 * places on the frontend that then disagreed — the grid unfolded the axis to
 * 01:00 while the estimator tested `hour > closeHour` and so called every hour
 * of that day out-of-hours — and this endpoint had the third copy of it, in the
 * guard that returned an empty ride list for the whole day.
 *
 * Hours past 23 are real hours of that day and are sent as such: 24 is the
 * midnight that ends a `10 → 0` day, 25 the 01:00 that ends a `10 → 1` one.
 * `context.closeHour` stays the operator's wall-clock number, because that is
 * what the operator published.
 */
export function unfoldedCloseHour(openHour: number, closeHour: number): number {
  return closeHour < openHour ? closeHour + 24 : closeHour;
}

/**
 * @returns one point per open hour, or `null` when the ride has no measured
 *          shape to scale — which is an answer, not a failure.
 */
export function composeDayCurve(
  options: ComposeDayCurveOptions,
): DayCurvePoint[] | null {
  const { shapeHours, shapeP50, dayPeak, openHour, closeHour } = options;

  if (closeHour < openHour) return null;

  // The day may be unfolded past midnight; the SHAPE never is, because the
  // profile buckets by wall-clock hour and a 00:00 bucket is written as 0. So
  // the small hours are moved to the end of the day before the two are read
  // against each other — without it, `interpolate` sorts midnight in FRONT of
  // the evening and then holds the last measured value flat across the night.
  const wraps = closeHour > 23;

  // Measured hours only. A null in shapeP50 means "no reading", and treating it
  // as a zero would drag the whole curve down through the scaling step.
  const measured: Array<{ hour: number; value: number }> = [];
  for (let i = 0; i < shapeHours.length; i++) {
    const value = shapeP50[i];
    if (value === null || value === undefined || !Number.isFinite(value)) {
      continue;
    }
    const hour = shapeHours[i];
    measured.push({ hour: wraps && hour < openHour ? hour + 24 : hour, value });
  }
  if (measured.length === 0) return null;
  measured.sort((a, b) => a.hour - b.hour);

  // A closed day, or a ride the model expects no queue for. Zero is a real
  // answer here and does not need a shape to carry it.
  if (dayPeak <= 0) {
    return rangeOf(openHour, closeHour).map((hour) => ({ hour, wait: 0 }));
  }

  const raw = rangeOf(openHour, closeHour).map((hour) => ({
    hour,
    value: interpolate(measured, hour),
  }));

  // Scale so the curve's own maximum sits at the predicted day peak. `peak` is
  // taken from the interpolated curve rather than from `measured`, because an
  // hour the park is closed for must not set the scale for the hours it is open.
  const peak = Math.max(...raw.map((p) => p.value));
  if (peak <= 0) {
    return raw.map((p) => ({ hour: p.hour, wait: 0 }));
  }
  const factor = dayPeak / peak;

  return raw.map((p) => ({
    hour: p.hour,
    wait: Math.max(0, roundToNearest5Minutes(p.value * factor)),
  }));
}

/**
 * Linear between the two nearest measured hours; the nearest value itself
 * outside their span. See the note above on why the ends are held rather than
 * extended.
 */
function interpolate(
  measured: ReadonlyArray<{ hour: number; value: number }>,
  hour: number,
): number {
  const first = measured[0];
  const last = measured[measured.length - 1];
  if (hour <= first.hour) return first.value;
  if (hour >= last.hour) return last.value;

  for (let i = 0; i < measured.length - 1; i++) {
    const a = measured[i];
    const b = measured[i + 1];
    if (hour >= a.hour && hour <= b.hour) {
      if (b.hour === a.hour) return a.value;
      const t = (hour - a.hour) / (b.hour - a.hour);
      return a.value + (b.value - a.value) * t;
    }
  }
  // Unreachable while `measured` is sorted and the bounds above hold.
  return last.value;
}

function rangeOf(from: number, to: number): number[] {
  const out: number[] = [];
  for (let h = from; h <= to; h++) out.push(h);
  return out;
}
