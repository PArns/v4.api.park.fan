/**
 * How far back a ride's operating days are read, in days. Eight weeks: long
 * enough that each weekday occurs eight times, short enough that a schedule
 * change (a Halloween festival starting, a summer ending) shows within weeks.
 */
export const RUN_WEEKDAYS_WINDOW_DAYS = 56;

/**
 * History a ride needs before it gets a pattern at all, in days since its first
 * operating day inside the window. Below two weeks one missed Friday reads as
 * „does not run on Fridays".
 */
export const RUN_WEEKDAYS_MIN_HISTORY_DAYS = 14;

/**
 * A ride whose last operating day is older than this has no pattern. It is
 * closed for something longer than a week, and „runs Fri–Sun" would promise a
 * weekend nobody announced.
 */
export const RUN_WEEKDAYS_MAX_IDLE_DAYS = 7;

/**
 * Share of the park's open days of a weekday the ride must have run on for the
 * weekday to count.
 *
 * The denominator is the days the PARK was open, not the calendar: a ride that
 * runs whenever the park does must not lose a weekday because the park is shut
 * on it most weeks. Movie Park opens on Mondays only in holiday weeks, and on a
 * calendar denominator Star Trek read „not on Mondays" on the holiday Monday it
 * was running.
 *
 * Half, inclusive, because the two errors are not equal. A weekday kept wrongly
 * shows `opensAt` as before; a weekday dropped wrongly tells a visitor the ride
 * is not on that day's programme.
 */
export const RUN_WEEKDAYS_MIN_SHARE = 0.5;

/**
 * Open days of a weekday the park needs before that weekday's own share is
 * trusted. One open day is an anecdote: Movie Park opened on one Wednesday
 * (30.09.) since the Halloween mazes began, the mazes ran on it, and 1 of 1
 * would make them a Wednesday attraction.
 */
export const RUN_WEEKDAYS_MIN_OPEN_DAYS = 2;

/**
 * Below `RUN_WEEKDAYS_MIN_OPEN_DAYS`, a weekday counts only for a ride that ran
 * on at least this share of ALL the park's open days since its first run — a
 * ride that runs whenever the park does. Measured on 2026-10-19: Star Trek ran
 * on every open day, the mazes on 11 of 15.
 */
export const RUN_WEEKDAYS_EVERY_OPEN_DAY_SHARE = 0.9;

/** ISO weekday (Monday 1 … Sunday 7) of a `YYYY-MM-DD` calendar date. */
export function isoWeekdayOf(date: string): number {
  const day = new Date(`${date}T00:00:00Z`).getUTCDay();
  return day === 0 ? 7 : day;
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
 * `operatingDays` are the park-local dates the ride has an observed OPERATING
 * reading on; `parkOpenDays` the dates ANY ride of the park has one on, which
 * is how „the park was open" is read — Movie Park's calendar does not say.
 * `today` is the park-local date. Today counts towards „ran recently" but not
 * towards the shares, because the day is not over. A weekday the park was
 * never open on since the ride's first run is not in the pattern, unless the
 * park is open on it today and the ride runs whenever the park does.
 */
export function runWeekdays(
  operatingDays: readonly string[],
  parkOpenDays: readonly string[],
  today: string,
): number[] | null {
  const days = [...new Set(operatingDays)].filter((d) => d <= today).sort();
  if (days.length === 0) return null;
  const first = days[0];
  const last = days[days.length - 1];
  if (daysBetween(first, today) < RUN_WEEKDAYS_MIN_HISTORY_DAYS) return null;
  if (daysBetween(last, today) > RUN_WEEKDAYS_MAX_IDLE_DAYS) return null;

  const ran = new Set(days);
  const openDays = new Array<number>(8).fill(0);
  const ranDays = new Array<number>(8).fill(0);
  // A day the ride ran on is a day the park was open, whatever the other list
  // says.
  for (const d of new Set([...parkOpenDays, ...days])) {
    if (d < first || d >= today) continue;
    const w = isoWeekdayOf(d);
    openDays[w]++;
    if (ran.has(d)) ranDays[w]++;
  }
  const totalOpen = openDays.reduce((a, b) => a + b, 0);
  const totalRan = ranDays.reduce((a, b) => a + b, 0);
  const runsWheneverOpen =
    totalOpen > 0 && totalRan / totalOpen >= RUN_WEEKDAYS_EVERY_OPEN_DAY_SHARE;

  // The park is open today once any of its rides has run, and on the first
  // holiday Monday there is no earlier open Monday to count. A ride that runs
  // whenever the park does runs today too.
  const openToday = parkOpenDays.includes(today);
  const todayWeekday = isoWeekdayOf(today);

  const out: number[] = [];
  for (let w = 1; w <= 7; w++) {
    if (openDays[w] === 0) {
      if (w === todayWeekday && openToday && runsWheneverOpen) out.push(w);
      continue;
    }
    const share = ranDays[w] / openDays[w];
    const counts =
      openDays[w] >= RUN_WEEKDAYS_MIN_OPEN_DAYS
        ? share >= RUN_WEEKDAYS_MIN_SHARE
        : runsWheneverOpen && share > 0;
    if (counts) out.push(w);
  }
  // A ride that runs now and then on no weekday in particular has no pattern,
  // and an empty list would read as „never runs".
  return out.length > 0 ? out : null;
}
