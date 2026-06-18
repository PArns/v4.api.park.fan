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
  /**
   * Minutes before a day's last recorded slot that are dropped as unreliable.
   * Parks stop admitting to the queue shortly before closing (e.g. ~17:55 for an
   * 18:00 close), so the final slots show an artificially draining line — never a
   * real "come back later" trough. Guarded out of every per-day computation.
   */
  closingGuardMinutes: number;
  /**
   * Fraction of the (guarded) operating day after which the trough counts as
   * "end of day". The end-of-day recommendation only fires past this point.
   */
  eveningFraction: number;
  /**
   * Minimum share of shape-contributing days a 15-min bin must cover to enter
   * the shape curve. Parks vary their hours through the year (e.g. summer
   * 08:00–22:00 vs. regular 10:00–18:00), so the far bins only exist on the
   * long days. Without this floor those rare-day bins can win the trough and
   * the offset then resolves PAST closing on a regular day (e.g. "best slot
   * 825 min after open" → 23:45 for a 10:00–21:00 day). The floor keeps the
   * curve describing the typical operating day.
   */
  shapeBinMinSupport: number;
}

export const DEFAULT_ROPE_DROP_THRESHOLDS: RopeDropThresholds = {
  windowDays: 70,
  shapeMinDayPeak: 20,
  rideByRatio: 0.5,
  worthPeakFloor: 60,
  worthSavingsFloor: 45,
  strengthPeakFloor: 90,
  strengthSavingsFloor: 60,
  closingGuardMinutes: 30,
  eveningFraction: 0.6,
  shapeBinMinSupport: 0.5,
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
  bestSlotWait: number;
  endOfDayWorth: boolean;
  endOfDaySavings: number;
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

/**
 * Drop the slots within `guardMinutes` of a day's last recorded slot.
 *
 * The tail of the operating day is unreliable: parks stop letting guests into
 * the queue shortly before closing, so those slots show a draining line rather
 * than the real demand. Using the last recorded slot as a closing proxy, we
 * trim that tail so it can never be mistaken for an evening "come back later"
 * trough. The opening slots (the rope-drop signal) are untouched.
 */
function trimClosingTail(
  day: RopeDropDayInput,
  guardMinutes: number,
): RopeDropDayInput {
  let lastMao = -Infinity;
  for (const s of day.slots) {
    if (s.minutesAfterOpen >= 0 && s.minutesAfterOpen > lastMao) {
      lastMao = s.minutesAfterOpen;
    }
  }
  if (!Number.isFinite(lastMao)) return day;
  const cutoff = lastMao - guardMinutes;
  return {
    ...day,
    slots: day.slots.filter((s) => s.minutesAfterOpen <= cutoff),
  };
}

/** Max P90 across a day's slots (the daily peak). */
function dayPeak(day: RopeDropDayInput): number {
  let peak = 0;
  for (const s of day.slots) {
    if (s.minutesAfterOpen >= 0 && s.p90 > peak) peak = s.p90;
  }
  return peak;
}

/**
 * P90 at the ride's opening (the rope-drop wait), or null when the day has no
 * post-open slot at all.
 *
 * Anchored on the ride's **earliest reported slot**, NOT a fixed [0,15)-min
 * window after PARK opening. Many parks stagger ride openings behind the gates
 * — Phantasialand opens at 09:00 but its rides at 10:00, so the first slot is
 * 45-180 min after park open. A fixed park-open window returned null for every
 * such day, dropping the ride from `sampleDays` entirely (busyPeak=0 →
 * worth=false). Using the first populated 15-min bin measures the wait when you
 * can actually first ride, and is identical to the old behaviour for rides that
 * do open with the park (earliest bin = 0).
 */
function openWaitForDay(day: RopeDropDayInput): number | null {
  let earliestBin: number | null = null;
  for (const s of day.slots) {
    if (s.minutesAfterOpen < 0) continue;
    const bin = Math.floor(s.minutesAfterOpen / BIN_MINUTES) * BIN_MINUTES;
    if (earliestBin === null || bin < earliestBin) earliestBin = bin;
  }
  if (earliestBin === null) return null;

  let val: number | null = null;
  for (const s of day.slots) {
    if (s.minutesAfterOpen < 0) continue;
    const bin = Math.floor(s.minutesAfterOpen / BIN_MINUTES) * BIN_MINUTES;
    if (bin === earliestBin) {
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

  // Guard out the unreliable pre-closing tail of every day up front, so it can
  // never contaminate the shape trough, the levels or the day-length estimate.
  const guardedDays = days.map((d) =>
    trimClosingTail(d, thresholds.closingGuardMinutes),
  );

  // --- Shape layer: opening-relative ratio curve, pooled over ALL history ---
  // Accumulate p90/day_peak per 15-min bin (only on meaningful days).
  const ratiosByBin = new Map<number, number[]>();
  let shapeDayCount = 0;
  for (const day of guardedDays) {
    const peak = dayPeak(day);
    if (peak < thresholds.shapeMinDayPeak) continue;
    shapeDayCount++;
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

  // Keep only bins present on a meaningful share of shape days (each day
  // contributes at most ~one slot per 15-min bin, so sample count ≈ day
  // support). Bins past the typical closing exist only on extended-hours days
  // and would otherwise resolve to instants after a regular day's close.
  const minSupport = Math.max(
    1,
    Math.ceil(shapeDayCount * thresholds.shapeBinMinSupport),
  );
  let bins = Array.from(ratiosByBin.keys())
    .filter((bin) => ratiosByBin.get(bin)!.length >= minSupport)
    .sort((a, b) => a - b);
  if (bins.length === 0) {
    // Degenerate data (no bin reaches the floor) — fall back to everything
    // rather than dropping the recommendation outright.
    bins = Array.from(ratiosByBin.keys()).sort((a, b) => a - b);
  }
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
  const windowDays = guardedDays.filter((d) => d.date >= windowStart);
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

  // Absolute wait at the day's trough (the "come back later" payoff): the pooled
  // shape ratio at the best slot resolved against the busy-day peak (shape×level,
  // the same two-layer composition the model uses elsewhere). Capped at openWait
  // so the trough is never reported as worse than rope-dropping.
  const bestSlotWait = Math.min(
    headline.openWait,
    Math.round(minRatio * headline.busyPeak),
  );

  // End-of-day verdict: is this ride better saved for late in the day than
  // rope-dropped? Only when the trough actually falls in the back of the
  // (closing-guarded) operating day and waiting there beats the busy peak by the
  // same savings floor rope drop uses. `endOfDaySavings` mirrors `savings` but
  // measured against the evening trough instead of the opening wait.
  const dayLength = curve[curve.length - 1].bin;
  const bestSlotIsEvening =
    dayLength > 0 &&
    bestSlotMinutesAfterOpen >= thresholds.eveningFraction * dayLength;
  const endOfDaySavings = Math.max(0, headline.busyPeak - bestSlotWait);
  const endOfDayWorth =
    bestSlotIsEvening &&
    headline.busyPeak >= thresholds.worthPeakFloor &&
    endOfDaySavings >= thresholds.worthSavingsFloor;

  return {
    worth,
    strength,
    confidence,
    busyPeak: headline.busyPeak,
    openWait: headline.openWait,
    savings: headline.savings,
    rideByMinutesAfterOpen,
    bestSlotMinutesAfterOpen,
    bestSlotWait,
    endOfDayWorth,
    endOfDaySavings,
    byDaytype: { weekend, weekday },
    windowDays: thresholds.windowDays,
    sampleDays,
  };
}
