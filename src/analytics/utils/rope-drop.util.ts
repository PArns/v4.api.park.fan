import { RopeDropDayBucket } from "../../common/types/rope-drop.type";

/**
 * Pure rope-drop aggregation — separated from the service so it is unit-testable
 * without a database. See docs / plan for the validated two-layer model:
 * shape (opening-relative ratio curve, pooled over history) + levels (absolute
 * minutes on a trailing window, weekend/weekday buckets).
 */

/** One operating day's opening-relative wait slots for one attraction. */
export interface RopeDropDayInput {
  /** Park-local date string (YYYY-MM-DD). */
  date: string;
  /** Day of week, 0=Sunday … 6=Saturday (Postgres EXTRACT(DOW) convention). */
  dow: number;
  /** 15-min slots: minutes after opening (>=0) and that slot's P90 wait. */
  slots: Array<{ minutesAfterOpen: number; p90: number }>;
}

export interface RopeDropThresholds {
  /** Trailing window (days) for the level layer. */
  windowDays: number;
  /** Day-peak floor for a day to contribute to the shape curve (noise guard). */
  shapeMinDayPeak: number;
  /** Ratio of peak that ends the rope-drop advantage window. */
  rideByRatio: number;
  /** worth: busyPeak must reach at least this many minutes. */
  worthPeakFloor: number;
  /** worth: savings must be at least this many minutes. */
  worthSavingsFloor: number;
  /** strength 'high': busyPeak floor. */
  strengthPeakFloor: number;
  /** strength 'high': savings floor. */
  strengthSavingsFloor: number;
}

export const DEFAULT_ROPE_DROP_THRESHOLDS: RopeDropThresholds = {
  windowDays: 70,
  shapeMinDayPeak: 20,
  rideByRatio: 0.5,
  worthPeakFloor: 60,
  worthSavingsFloor: 45,
  strengthPeakFloor: 90,
  strengthSavingsFloor: 60,
};

const BIN_MINUTES = 15;

/** Result of the pure computation — everything persisted except the UTC resolution. */
export interface RopeDropComputeResult {
  worth: boolean;
  strength: "high" | "moderate" | null;
  confidence: "high" | "medium" | "low";
  busyPeak: number;
  openWait: number;
  savings: number;
  rideByMinutesAfterOpen: number;
  bestSlotMinutesAfterOpen: number;
  byDaytype: { weekend: RopeDropDayBucket; weekday: RopeDropDayBucket };
  windowDays: number;
  sampleDays: number;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

/** Max P90 across a day's slots (the daily peak). */
function dayPeak(day: RopeDropDayInput): number {
  let peak = 0;
  for (const s of day.slots) {
    if (s.minutesAfterOpen >= 0 && s.p90 > peak) peak = s.p90;
  }
  return peak;
}

/** P90 of the first 15-min slot after opening (the rope-drop wait), or null. */
function openWaitForDay(day: RopeDropDayInput): number | null {
  let val: number | null = null;
  for (const s of day.slots) {
    if (s.minutesAfterOpen >= 0 && s.minutesAfterOpen < BIN_MINUTES) {
      val = val === null ? s.p90 : Math.min(val, s.p90);
    }
  }
  return val;
}

function bucketLevels(days: RopeDropDayInput[]): RopeDropDayBucket {
  const peaks: number[] = [];
  const opens: number[] = [];
  for (const day of days) {
    const ow = openWaitForDay(day);
    const peak = dayPeak(day);
    if (ow === null || peak <= 0) continue;
    peaks.push(peak);
    opens.push(ow);
  }
  if (peaks.length === 0) {
    return { openWait: 0, busyPeak: 0, savings: 0 };
  }
  const busyPeak = Math.round(median(peaks));
  const openWait = Math.round(median(opens));
  return { openWait, busyPeak, savings: Math.max(0, busyPeak - openWait) };
}

/**
 * Compute the rope-drop recommendation for one attraction.
 *
 * @param days       all available operating-day slot data for the attraction
 * @param windowStart park-local YYYY-MM-DD; days >= this feed the level layer
 * @param thresholds tuning knobs (defaults validated against live data)
 * @returns the computation result, or null if there is no usable data
 */
export function computeRopeDrop(
  days: RopeDropDayInput[],
  windowStart: string,
  thresholds: RopeDropThresholds = DEFAULT_ROPE_DROP_THRESHOLDS,
): RopeDropComputeResult | null {
  if (days.length === 0) return null;

  // --- Shape layer: opening-relative ratio curve, pooled over ALL history ---
  // Accumulate p90/day_peak per 15-min bin (only on meaningful days).
  const ratiosByBin = new Map<number, number[]>();
  for (const day of days) {
    const peak = dayPeak(day);
    if (peak < thresholds.shapeMinDayPeak) continue;
    for (const s of day.slots) {
      if (s.minutesAfterOpen < 0) continue;
      const binStart =
        Math.floor(s.minutesAfterOpen / BIN_MINUTES) * BIN_MINUTES;
      const arr = ratiosByBin.get(binStart);
      const ratio = s.p90 / peak;
      if (arr) arr.push(ratio);
      else ratiosByBin.set(binStart, [ratio]);
    }
  }

  if (ratiosByBin.size === 0) return null;

  const bins = Array.from(ratiosByBin.keys()).sort((a, b) => a - b);
  const curve = bins.map((bin) => ({
    bin,
    ratio: median(ratiosByBin.get(bin)!),
  }));

  // rideBy = first bin whose median ratio exceeds the threshold (advantage end).
  let rideByMinutesAfterOpen = curve[curve.length - 1].bin;
  for (const point of curve) {
    if (point.ratio > thresholds.rideByRatio) {
      rideByMinutesAfterOpen = point.bin;
      break;
    }
  }

  // bestSlot = bin with the minimum median ratio (often the evening).
  let bestSlotMinutesAfterOpen = curve[0].bin;
  let minRatio = curve[0].ratio;
  for (const point of curve) {
    if (point.ratio < minRatio) {
      minRatio = point.ratio;
      bestSlotMinutesAfterOpen = point.bin;
    }
  }

  // --- Level layer: absolute minutes on the trailing window ---
  const windowDays = days.filter((d) => d.date >= windowStart);
  const weekend = bucketLevels(
    windowDays.filter((d) => d.dow === 0 || d.dow === 6),
  );
  const weekday = bucketLevels(
    windowDays.filter((d) => d.dow >= 1 && d.dow <= 5),
  );

  // Headline = the busier bucket (the realistic full-day scenario).
  const headline = weekend.busyPeak >= weekday.busyPeak ? weekend : weekday;
  const sampleDays = windowDays.filter((d) => {
    const ow = openWaitForDay(d);
    return ow !== null && dayPeak(d) > 0;
  }).length;

  let confidence: "high" | "medium" | "low" = "low";
  if (sampleDays >= 40) confidence = "high";
  else if (sampleDays >= 20) confidence = "medium";

  // worth anchors on absolute minutes: a genuinely long ride (busyPeak floor)
  // where rope-dropping saves a lot (savings floor). We deliberately do NOT
  // gate on openWait/busyPeak ratio — that would only ever exclude the highest-
  // peak rides (the flagship rope-drop targets like Anna & Elsa / Seven Dwarfs),
  // since the savings floor already keeps mid-peak rides' opening low. The
  // "morning isn't a real trough" nuance is carried by rideByMinutesAfterOpen
  // (≈0 for always-busy rides), not by hard exclusion.
  const worth =
    headline.busyPeak >= thresholds.worthPeakFloor &&
    headline.savings >= thresholds.worthSavingsFloor;

  const strength: "high" | "moderate" | null = !worth
    ? null
    : headline.busyPeak >= thresholds.strengthPeakFloor &&
        headline.savings >= thresholds.strengthSavingsFloor
      ? "high"
      : "moderate";

  return {
    worth,
    strength,
    confidence,
    busyPeak: headline.busyPeak,
    openWait: headline.openWait,
    savings: headline.savings,
    rideByMinutesAfterOpen,
    bestSlotMinutesAfterOpen,
    byDaytype: { weekend, weekday },
    windowDays: thresholds.windowDays,
    sampleDays,
  };
}
