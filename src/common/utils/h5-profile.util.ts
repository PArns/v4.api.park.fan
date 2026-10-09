/**
 * The H5 hybrid day profile and its composition into 15-minute slots (PAR-834).
 *
 * A port of baselines 4 and 5 of the ML review's offline benchmark
 * (BENCH-SPEC.md; reference implementation `ml-bench/mlbench/baselines.py`,
 * columns `h5` and `lvlh5_tft`). Measured there on 15-minute truth, H5 is the
 * only profile that beats a plain weekday median at every lead, while the
 * composer `/plan/day` used before it (`composeDayCurve`: a year's hourly P50
 * profile stretched so its maximum equals the day level) is worse than that
 * weekday median from d3 on — +0.5 to +1.1 min MAE, and +2.3 min of bias in the
 * first hour, because putting the profile's MAXIMUM at a P90-like level
 * inflates every hour below it.
 *
 * WHAT H5 IS, slot by slot, on a 15-minute grid of the service day:
 *
 * - **first hour** (the four slots after the PUBLISHED opening): the ride's
 *   median at that many minutes after opening. Aligned to the opening rather
 *   than to the clock, because the rope-drop ramp is a property of the opening:
 *   a 09:00 and a 10:00 day both start empty and fill over their first hour.
 * - **last hour** (the four slots before the published closing): the median at
 *   that many minutes before closing, for the same reason.
 * - **in between**: the ride's hourly median (by wall-clock hour), linearly
 *   interpolated between hour centres onto the quarter-hours.
 * - fallback where the hour has too few days: the 15-minute wall-clock median.
 *
 * Every median is over the ride's own last {@link H5_WINDOW_DAYS} days, split by
 * weekday/weekend where that split has enough days (≥ {@link MIN_DAYS_DAYTYPE}),
 * else over all days (≥ {@link MIN_DAYS_ALL}).
 *
 * LEVEL. With a day-level forecast the profile is scaled by a RATIO —
 * `level ÷ the ride's median daily P90 over the same window` — never by putting
 * its maximum at the level. Both sides of the ratio are daily P90s, so the
 * ratio says "this day is 20 % busier than the ride's typical day" and the
 * profile's own shape and height survive. The first hour is NOT scaled: the
 * rope-drop ramp is the same on a busy and a quiet morning. With a level the
 * weekday/weekend split is not used, for profile and reference alike: the
 * benchmark found it adds nothing once the level carries the day.
 *
 * Only a TFT level is applied. CatBoost's daily level made the benchmark worse
 * (+2.6 min MAE, bias −5.7), so without a TFT level the plain profile is served.
 *
 * MINUTES ARE WALL-CLOCK MINUTES OF THE SERVICE DAY, unfolded past midnight:
 * 600 is 10:00, 1440 the midnight that ends the day, 1500 its 01:00 — the same
 * axis as `PlanDayHourDto.hour` (× 60). History rows are stored per park-local
 * DATE with park-local `HH:MM` slots, so this axis is the one the data already
 * speaks. It differs from elapsed time only across a DST switch inside the open
 * window (02:00–03:00), where no park we track is open.
 */

/** Days of history the profile is built from, before today. */
export const H5_WINDOW_DAYS = 56;
/** A weekday- or weekend-only median needs this many days at that index. */
export const MIN_DAYS_DAYTYPE = 3;
/** An all-days median needs this many days at that index. */
export const MIN_DAYS_ALL = 4;
/** A ride-day needs this many filled slots before its P90 counts. */
export const MIN_RIDE_DAY_SLOTS = 8;
/** `queue_data` is a change log: a reading older than this is not in force. */
export const MAX_STALENESS_MINUTES = 180;
/** Readings below this are not a queue (the truth's own floor). */
export const MIN_WAIT = 5;
/** Slots after the opening, and before the closing, that are edge-aligned. */
export const EDGE_SLOTS = 4;
export const SLOT_MINUTES = 15;

/** Median per index, for weekdays, weekends and all days. Thresholded. */
export interface H5Split {
  /** Monday–Friday. */
  wd: Record<number, number>;
  /** Saturday and Sunday. */
  we: Record<number, number>;
  all: Record<number, number>;
}

/** One ride's H5 profile, as cached. */
export interface H5RideProfile {
  /** Index = slots since the published opening (0–3). */
  open: H5Split;
  /** Index = slots before the published closing (0 = the last slot). */
  close: H5Split;
  /** Index = wall-clock hour of the service day, unfolded (24 = midnight). */
  hourly: H5Split;
  /** Index = wall-clock quarter-hour of the service day (minute / 15). */
  slot: H5Split;
  /** Median daily P90 over all window days; null below {@link MIN_DAYS_ALL}. */
  refLevel: number | null;
  /** Window days that contributed at least one slot. */
  days: number;
}

/** One service day of one ride, as read from `attraction_hourly_history`. */
export interface H5HistoryDay {
  /** Saturday or Sunday. */
  weekend: boolean;
  /** Published opening, wall-clock minutes of the service day. */
  openMin: number;
  /** Published closing, unfolded (> 1440 on a day that crosses midnight). */
  closeMin: number;
  /**
   * The ride's 15-minute readings on that axis — the slot's mean wait, already
   * filtered to OPERATING, STANDBY and wait ≥ 5 by the rollup. Unsorted is fine.
   */
  readings: ReadonlyArray<{ minute: number; wait: number }>;
}

/**
 * The truth slots of one history day: the 15-minute grid of the published
 * window, each slot holding the reading in force at it.
 *
 * The rollup keeps only quarter-hours that HAD a reading, and `queue_data`
 * writes on change, so a ride holding 30 minutes all afternoon has one slot at
 * 14:00 and nothing after. Forward-filling it — at most
 * {@link MAX_STALENESS_MINUTES} — is what turns that back into a day; a median
 * over the stored slots alone would weigh a jittery hour over a calm one. The
 * same rule the forward archive's q90 uses (`ForecastArchiveService.rideQ90`).
 *
 * One known difference from the benchmark's truth: the rollup drops DOWN
 * readings rather than storing them, so a breakdown is filled with the last
 * operating wait for up to three hours instead of leaving a gap. The profile is
 * a median over 56 days, which an occasional outage barely moves.
 */
export function truthSlotsOf(
  day: H5HistoryDay,
): Array<{ minute: number; ko: number; kc: number; wait: number }> {
  const readings = [...day.readings]
    .filter((r) => Number.isFinite(r.wait) && r.wait >= MIN_WAIT)
    .sort((a, b) => a.minute - b.minute);
  const out: Array<{ minute: number; ko: number; kc: number; wait: number }> =
    [];
  let i = 0;
  let current: { minute: number; wait: number } | null = null;
  for (const minute of gridOf(day.openMin, day.closeMin)) {
    while (i < readings.length && readings[i].minute <= minute) {
      current = readings[i];
      i++;
    }
    if (!current || minute - current.minute > MAX_STALENESS_MINUTES) continue;
    out.push({
      minute,
      ...edgeIndices(minute, day.openMin, day.closeMin),
      wait: current.wait,
    });
  }
  return out;
}

/**
 * One ride's window days, from its rollup rows and the park's published
 * windows. A day without a published window contributes nothing — the truth
 * the profile imitates is defined inside the published window only.
 *
 * A day that crosses midnight reads the NEXT date's row as well, shifted by
 * 1440, because the rollup is keyed by park-local date and the 00:30 of a
 * `10 → 1` day is stored under tomorrow.
 *
 * @param rows date → that date's readings, `minute` counted from that date's own midnight
 * @param windows date → published window, wall-clock minutes of that date (close unfolded)
 */
export function historyDaysOf(
  rows: ReadonlyMap<string, ReadonlyArray<{ minute: number; wait: number }>>,
  windows: ReadonlyMap<string, { openMin: number; closeMin: number }>,
): H5HistoryDay[] {
  const out: H5HistoryDay[] = [];
  for (const [date, window] of windows) {
    const own = rows.get(date) ?? [];
    const readings = [...own];
    if (window.closeMin > 1440) {
      for (const r of rows.get(nextDate(date)) ?? []) {
        readings.push({ minute: r.minute + 1440, wait: r.wait });
      }
    }
    if (readings.length === 0) continue;
    out.push({
      weekend: isWeekendDate(date),
      openMin: window.openMin,
      closeMin: window.closeMin,
      readings,
    });
  }
  return out;
}

/** Saturday or Sunday, from the date string itself (no timezone involved). */
export function isWeekendDate(date: string): boolean {
  const day = new Date(`${date}T00:00:00Z`).getUTCDay();
  return day === 0 || day === 6;
}

function nextDate(date: string): string {
  return new Date(Date.parse(`${date}T00:00:00Z`) + 86_400_000)
    .toISOString()
    .slice(0, 10);
}

/** Builds one ride's profile from its window days. Null with no usable day. */
export function buildH5Profile(
  days: ReadonlyArray<H5HistoryDay>,
): H5RideProfile | null {
  const open = new Accumulator();
  const close = new Accumulator();
  const hourly = new Accumulator();
  const slot = new Accumulator();
  const dayP90s: number[] = [];
  let usable = 0;

  for (const day of days) {
    const slots = truthSlotsOf(day);
    if (slots.length === 0) continue;
    usable++;
    // `hourly` counts DAYS per hour (the benchmark's count(DISTINCT date)) but
    // takes the median over every slot value in it.
    const hoursSeen = new Set<number>();
    for (const s of slots) {
      if (s.ko < EDGE_SLOTS) open.add(s.ko, s.wait, day.weekend, true);
      if (s.kc < EDGE_SLOTS) close.add(s.kc, s.wait, day.weekend, true);
      const hour = Math.floor(s.minute / 60);
      hourly.add(hour, s.wait, day.weekend, !hoursSeen.has(hour));
      hoursSeen.add(hour);
      slot.add(s.minute / SLOT_MINUTES, s.wait, day.weekend, true);
    }
    if (slots.length >= MIN_RIDE_DAY_SLOTS) {
      dayP90s.push(
        quantile(
          slots.map((s) => s.wait),
          0.9,
        ),
      );
    }
  }
  if (usable === 0) return null;

  return {
    open: open.medians(),
    close: close.medians(),
    hourly: hourly.medians(),
    slot: slot.medians(),
    refLevel: dayP90s.length >= MIN_DAYS_ALL ? quantile(dayP90s, 0.5) : null,
    days: usable,
  };
}

export interface ComposeH5Options {
  profile: H5RideProfile;
  /** First slot's lower bound, wall-clock minutes of the service day. */
  openMin: number;
  /** Unfolded closing minute; the last slot starts before it. */
  closeMin: number;
  /**
   * Whether `openMin`/`closeMin` are the operator's PUBLISHED hours. Only then
   * are the first and last hour edge-aligned: past the publishing horizon the
   * window is the hours the queues were measured in, and a rope-drop ramp
   * pinned to "the first hour we have readings for" would be a ramp in the
   * wrong place. The benchmark's opening-aligned gain was measured on
   * published schedules, so it is not claimed where there is none.
   */
  alignToSchedule: boolean;
  weekend: boolean;
  /**
   * The TFT day level (a daily P90 forecast), or null for none. CatBoost's
   * level is never passed here — see the file comment.
   */
  level: number | null;
}

export interface H5Slot {
  /** Wall-clock minute of the service day, unfolded, on the 15-min grid. */
  minute: number;
  /** Expected wait in minutes, unrounded. */
  wait: number;
}

export interface H5Composition {
  slots: H5Slot[];
  /** Whether the TFT level was applied (`h5_tft`) or the plain profile (`h5`). */
  levelApplied: boolean;
}

/**
 * The composed day: one value per quarter-hour of the window that the profile
 * has a value for. Slots the profile cannot answer (an hour nobody measured)
 * are absent rather than invented.
 */
export function composeH5Day(options: ComposeH5Options): H5Composition {
  const { profile, openMin, closeMin, alignToSchedule, weekend, level } =
    options;
  const refLevel = profile.refLevel;
  const levelApplied =
    level !== null &&
    Number.isFinite(level) &&
    refLevel !== null &&
    refLevel > 0;
  const ratio = levelApplied ? Math.max(0, level) / refLevel : 1;
  // With a level the split adds nothing (benchmark), so all days only.
  const read = (split: H5Split, index: number): number | null =>
    levelApplied ? pick(split.all, index) : resolve(split, index, weekend);

  const slots: H5Slot[] = [];
  for (const minute of gridOf(openMin, closeMin)) {
    const { ko, kc } = edgeIndices(minute, openMin, closeMin);
    const rampSlot = alignToSchedule && ko < EDGE_SLOTS;

    // First the edge profile, and only one of them: in the first hour the
    // opening-aligned median or nothing, exactly as the benchmark's CASE does.
    let value: number | null = null;
    if (rampSlot) value = read(profile.open, ko);
    else if (alignToSchedule && kc < EDGE_SLOTS)
      value = read(profile.close, kc);

    if (value === null) {
      const hour = Math.floor(minute / 60);
      const h0 = read(profile.hourly, hour);
      if (h0 !== null) {
        // Between hour CENTRES: a :07.5 slot midpoint sits 22.5 minutes before
        // its hour's centre, so it takes 22.5/60 of the step to the hour before.
        const mid = (minute % 60) + SLOT_MINUTES / 2;
        if (mid < 30) {
          const prev = read(profile.hourly, hour - 1) ?? h0;
          value = h0 + ((30 - mid) / 60) * (prev - h0);
        } else {
          const next = read(profile.hourly, hour + 1) ?? h0;
          value = h0 + ((mid - 30) / 60) * (next - h0);
        }
      }
    }
    if (value === null) value = read(profile.slot, minute / SLOT_MINUTES);
    if (value === null) continue;

    slots.push({ minute, wait: rampSlot ? value : value * ratio });
  }
  return { slots, levelApplied };
}

/**
 * The quarter-hours of a window: every slot whose START lies in
 * [open, close). An opening off the grid (09:10) starts at the next slot.
 */
export function gridOf(openMin: number, closeMin: number): number[] {
  const out: number[] = [];
  const first = Math.ceil(openMin / SLOT_MINUTES) * SLOT_MINUTES;
  for (let m = first; m < closeMin; m += SLOT_MINUTES) out.push(m);
  return out;
}

/** Slots since the opening and slots before the closing (0 = the last). */
function edgeIndices(
  minute: number,
  openMin: number,
  closeMin: number,
): { ko: number; kc: number } {
  return {
    ko: Math.floor((minute - openMin) / SLOT_MINUTES),
    kc: Math.floor((closeMin - minute - 1) / SLOT_MINUTES),
  };
}

/** Day-type first, all days second, per index (the benchmark's coalesce). */
function resolve(
  split: H5Split,
  index: number,
  weekend: boolean,
): number | null {
  return pick(weekend ? split.we : split.wd, index) ?? pick(split.all, index);
}

function pick(map: Record<number, number>, index: number): number | null {
  const v = map[index];
  return v === undefined || !Number.isFinite(v) ? null : v;
}

/** Values and day counts per index, for the three day types. */
class Accumulator {
  private readonly values = {
    wd: new Map<number, number[]>(),
    we: new Map<number, number[]>(),
    all: new Map<number, number[]>(),
  };
  private readonly counts = {
    wd: new Map<number, number>(),
    we: new Map<number, number>(),
    all: new Map<number, number>(),
  };

  add(index: number, value: number, weekend: boolean, newDay: boolean): void {
    for (const key of [weekend ? "we" : "wd", "all"] as const) {
      const list = this.values[key].get(index) ?? [];
      list.push(value);
      this.values[key].set(index, list);
      if (newDay) {
        this.counts[key].set(index, (this.counts[key].get(index) ?? 0) + 1);
      }
    }
  }

  medians(): H5Split {
    const out: H5Split = { wd: {}, we: {}, all: {} };
    for (const key of ["wd", "we", "all"] as const) {
      const floor = key === "all" ? MIN_DAYS_ALL : MIN_DAYS_DAYTYPE;
      for (const [index, list] of this.values[key]) {
        if ((this.counts[key].get(index) ?? 0) < floor) continue;
        out[key][index] = round2(quantile(list, 0.5));
      }
    }
    return out;
  }
}

/** Linear-interpolated quantile (Postgres `percentile_cont`, DuckDB `quantile_cont`). */
export function quantile(values: ReadonlyArray<number>, q: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return NaN;
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

/** Two decimals: enough for a median of 5-minute values, smaller in cache. */
function round2(v: number): number {
  return Math.round(v * 100) / 100;
}
