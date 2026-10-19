/**
 * How far back a ride's operating days are read, in days. Eight weeks: long
 * enough that each weekday occurs eight times, short enough that a schedule
 * change (a Halloween festival starting, a summer ending) shows within weeks.
 */
export const RUN_WEEKDAYS_WINDOW_DAYS = 56;

/**
 * History a ride needs before it gets a pattern at all, in days since its first
 * operating day inside the window. Two weeks puts every weekday at least twice
 * into the denominator; below that one missed Friday reads as „does not run on
 * Fridays".
 */
export const RUN_WEEKDAYS_MIN_HISTORY_DAYS = 14;

/**
 * A ride whose last operating day is older than this has no pattern. It is
 * closed for something longer than a week, and „runs Fri–Sun" would promise a
 * weekend nobody announced.
 */
export const RUN_WEEKDAYS_MAX_IDLE_DAYS = 7;

/**
 * Share of a weekday's occurrences the ride must have run on for the weekday to
 * count, measured from the ride's first operating day in the window rather than
 * from the window's start — a season that began three weeks ago would otherwise
 * put Fri–Sun at 3 of 8 and drop them.
 *
 * Half, inclusive, because the two errors are not equal. A weekday kept wrongly
 * shows `opensAt` as before; a weekday dropped wrongly tells a visitor the ride
 * is not on that day's programme. Measured at Movie Park on 2026-10-19: the
 * mazes ran on 1 of 3 Wednesdays (30.09., a one-off) and on 3 of 4 Fridays.
 */
export const RUN_WEEKDAYS_MIN_SHARE = 0.5;

/** ISO weekday (Monday 1 … Sunday 7) of a `YYYY-MM-DD` calendar date. */
export function isoWeekdayOf(date: string): number {
  const day = new Date(`${date}T00:00:00Z`).getUTCDay();
  return day === 0 ? 7 : day;
}

function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function daysBetween(from: string, to: string): number {
  return Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );
}

/**
 * The ISO weekdays a ride usually runs on, ascending, or `null` when there is
 * no pattern to tell: less than two weeks of history, no operating day in the
 * last week, or no weekday that clears the share.
 *
 * `operatingDays` are the park-local dates with at least one observed
 * OPERATING reading inside the window; `today` is the park-local date. Today
 * counts towards „ran recently" but not towards the shares, because the day is
 * not over.
 */
export function runWeekdays(
  operatingDays: readonly string[],
  today: string,
): number[] | null {
  const days = [...new Set(operatingDays)].filter((d) => d <= today).sort();
  if (days.length === 0) return null;
  const first = days[0];
  const last = days[days.length - 1];
  if (daysBetween(first, today) < RUN_WEEKDAYS_MIN_HISTORY_DAYS) return null;
  if (daysBetween(last, today) > RUN_WEEKDAYS_MAX_IDLE_DAYS) return null;

  const occurrences = new Array<number>(8).fill(0);
  for (let d = first; d < today; d = addDays(d, 1)) {
    occurrences[isoWeekdayOf(d)]++;
  }
  const ran = new Array<number>(8).fill(0);
  for (const d of days) if (d < today) ran[isoWeekdayOf(d)]++;

  const out: number[] = [];
  for (let w = 1; w <= 7; w++) {
    if (occurrences[w] > 0 && ran[w] / occurrences[w] >= RUN_WEEKDAYS_MIN_SHARE)
      out.push(w);
  }
  // A ride that runs now and then on no weekday in particular has no pattern,
  // and an empty list would read as „never runs".
  return out.length > 0 ? out : null;
}
