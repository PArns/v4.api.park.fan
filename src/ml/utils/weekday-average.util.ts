/**
 * The ex-ante weekday average (PAR-855): "what was typical on this weekday in
 * the last weeks" — the yardstick every planner and calendar model has to
 * beat, scored as model `weekday_avg` in `forecast_archive_paired`.
 *
 * For a ride, a target date D and an origin whose park-local date is O, the
 * value at a park-local quarter-hour HH:MM is the mean, over the
 * {@link WEEKDAY_AVG_WEEKS} most recent dates D − 7k that lie STRICTLY BEFORE
 * O, of that date's wait at HH:MM. Nothing at or after the origin's date enters
 * — not even the morning of the origin day, which the 06:00 origin has not
 * seen. That is the leakage guard; {@link weekdayAverageAt} applies it itself
 * rather than trusting the caller's date list.
 *
 * The input is the `attraction_hourly_history` rollup (one row per ride and
 * park-local date, written at 04:30 for the previous day): its `slots` hold the
 * mean STANDBY wait of each quarter-hour that had an OPERATING reading ≥ 5 min.
 * `queue_data` is a change log, so a quarter-hour without a reading still had
 * the previous value in force: each day is forward-filled for at most 3 h
 * (the truth's own staleness rule) before it is averaged.
 *
 * Pure: the scoring service reads the rollup and hands it in.
 */
import { addIsoDays } from "../../common/utils/date.util";

/** How many same-weekday dates are averaged. */
export const WEEKDAY_AVG_WEEKS = 8;
/** A quarter-hour needs at least this many dates with a value. */
export const WEEKDAY_AVG_MIN_DAYS = 2;
/** Forward-fill reach inside one day, in quarter-hours (3 h). */
const FILL_STEPS = 12;

/** ride-day: park-local "HH:MM" → wait. */
export type DaySlots = Map<string, number>;

/** "HH:MM" of quarter-hour i (0 … 95). */
function hhmmOf(i: number): string {
  const h = Math.floor(i / 4);
  const m = (i % 4) * 15;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

function indexOf(hhmm: string): number | null {
  const m = /^(\d{2}):(\d{2})/.exec(hhmm);
  if (!m) return null;
  const i = Number(m[1]) * 4 + Math.floor(Number(m[2]) / 15);
  return i >= 0 && i < 96 ? i : null;
}

/**
 * One day's rollup slots forward-filled onto the 96 quarter-hours: a value is
 * carried for at most {@link FILL_STEPS} quarter-hours after its reading.
 */
export function forwardFillDay(
  slots: Array<{ time_slot: string; avgWait: number }>,
): DaySlots {
  const raw: (number | null)[] = new Array(96).fill(null);
  for (const s of slots) {
    const i = indexOf(s.time_slot);
    const w = Number(s.avgWait);
    if (i === null || !Number.isFinite(w)) continue;
    raw[i] = w;
  }
  const out: DaySlots = new Map();
  let last: number | null = null;
  let age = 0;
  for (let i = 0; i < 96; i++) {
    if (raw[i] !== null) {
      last = raw[i];
      age = 0;
    } else if (last !== null) {
      age++;
      if (age > FILL_STEPS) last = null;
    }
    if (last !== null) out.set(hhmmOf(i), last);
  }
  return out;
}

/**
 * The dates a target date's weekday average can draw on for leads up to
 * `maxLeadDays`: D − 7k for k = 1 … ⌊maxLead / 7⌋ + {@link WEEKDAY_AVG_WEEKS}.
 * The scorer reads exactly these rows of the rollup — at most 20 per ride for
 * the d90 lead — rather than a range.
 */
export function weekdayHistoryDates(
  targetDate: string,
  maxLeadDays: number,
): string[] {
  const k = Math.floor(Math.max(0, maxLeadDays) / 7) + WEEKDAY_AVG_WEEKS;
  const out: string[] = [];
  for (let i = 1; i <= k; i++) out.push(addIsoDays(targetDate, -7 * i));
  return out;
}

function daysBetween(a: string, b: string): number {
  return Math.round(
    (Date.parse(`${a}T12:00:00Z`) - Date.parse(`${b}T12:00:00Z`)) / 86_400_000,
  );
}

/**
 * The dates actually averaged for (target, origin): same weekday as the
 * target, strictly before the origin's park-local date, newest first, at most
 * {@link WEEKDAY_AVG_WEEKS}. Dates with no rollup row are skipped rather than
 * counted as zero (a closed day is not a quiet one).
 */
export function weekdayAverageDates(
  history: Map<string, DaySlots>,
  targetDate: string,
  originDate: string,
): string[] {
  return [...history.keys()]
    .filter((d) => d < originDate)
    .filter((d) => {
      const diff = daysBetween(targetDate, d);
      return diff > 0 && diff % 7 === 0;
    })
    .sort()
    .reverse()
    .slice(0, WEEKDAY_AVG_WEEKS);
}

/**
 * The weekday average at one park-local quarter-hour, or null with fewer than
 * {@link WEEKDAY_AVG_MIN_DAYS} dates holding a value there.
 */
export function weekdayAverageAt(
  history: Map<string, DaySlots>,
  targetDate: string,
  originDate: string,
  hhmm: string,
): number | null {
  let s = 0;
  let n = 0;
  for (const d of weekdayAverageDates(history, targetDate, originDate)) {
    const v = history.get(d)?.get(hhmm);
    if (v === undefined) continue;
    s += v;
    n++;
  }
  return n >= WEEKDAY_AVG_MIN_DAYS ? s / n : null;
}

/**
 * {@link weekdayAverageAt} for a run of quarter-hours at once — the dates are
 * chosen once, not per slot.
 */
export function weekdayAverageCurve(
  history: Map<string, DaySlots>,
  targetDate: string,
  originDate: string,
  hhmms: string[],
): (number | null)[] {
  const days = weekdayAverageDates(history, targetDate, originDate).map((d) =>
    history.get(d)!,
  );
  return hhmms.map((t) => {
    let s = 0;
    let n = 0;
    for (const day of days) {
      const v = day.get(t);
      if (v === undefined) continue;
      s += v;
      n++;
    }
    return n >= WEEKDAY_AVG_MIN_DAYS ? s / n : null;
  });
}
