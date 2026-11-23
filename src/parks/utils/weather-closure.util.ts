/**
 * Whether the weather closed a ride, learned per ride from its own history.
 *
 * Measured on Europa-Park and Phantasialand (PAR-1414): over the hours a park
 * was open, a ride is "closed" in an hour it read CLOSED or DOWN and never
 * OPERATING. Two causes separate cleanly and must be kept apart:
 *
 * - **Cold.** Poseidon is shut in 66 % of dry hours under 4 °C and in 0.2 % of
 *   hours at 12–25 °C. It is the temperature.
 * - **Snow.** Taron is shut in 0 % of dry hours under 4 °C and in 29 % of the
 *   hours after a snowfall. Learned on all cold hours together, Taron would
 *   read as cold-sensitive, which it is not.
 *
 * So the cold profile is learned on dry hours only (no snowfall in the hour or
 * the 24 before it), and snow gets its own profile from the snowy hours.
 *
 * The share of the condition's closures the condition explains is the
 * attributable fraction `(p_condition - p_warm) / p_condition`, and that is
 * the probability a page may print. Both rates come from a handful of winter
 * days, so the figure is computed from the pessimistic end of both: the Wilson
 * lower bound of `p_condition` against the upper bound of `p_warm`, with one
 * park-local day counted as one observation.
 *
 * What would otherwise pass for weather, and is removed before learning:
 * a ride out of season (it must have run within a week on BOTH sides of the
 * day, so the days after its last run of the year do not count), a streak of
 * whole days shut (a rehab, not a cold morning), and hours in which a large
 * part of the park was shut (a storm, a feed outage).
 */

/** One park-open hour of one ride. */
export interface WeatherClosureSample {
  hourStart: Date;
  /** The park-local date of the hour. */
  day: string;
  temperatureC: number;
  /** At least {@link SNOW_MIN_CM} fell in this hour or in the 24 before it. */
  snow: boolean;
  closed: boolean;
}

/** One ride in one park-open hour, as `WEATHER_CLOSURE_HOURS_SQL` reads it. */
export interface RideHourState {
  attractionId: string;
  hourStart: Date;
  /** Read CLOSED or DOWN in the hour and never OPERATING. */
  closed: boolean;
}

/** The park's weather in one hour, from the archive. */
export interface ArchiveHour {
  temperatureC: number | null;
  snowfallCm: number | null;
}

/** Less than this is model trace, not snow on a track (cm). */
export const SNOW_MIN_CM = 0.1;

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;
/** A ride that ran within this many days on both sides is in its season. */
const SEASON_REACH_DAYS = 7;
/** Above this share of the park's other in-season rides shut, the hour is about the park. */
const PARK_WIDE_SHARE = 0.3;
const PARK_WIDE_MIN_RIDES = 5;
/**
 * This many consecutive open days shut from first hour to last are a rehab or
 * a season's end, not the weather; a cold morning shuts a ride for hours.
 */
const CLOSED_STREAK_DAYS = 4;

function dayNumber(day: string): number {
  return Math.round(Date.parse(`${day}T00:00:00Z`) / DAY_MS);
}

/**
 * Turns a park's ride-hours and its hourly archive into one sample list per
 * ride, dropping what would teach the wrong lesson (see the file comment).
 *
 * @param dayOf - The park-local date of an instant, `YYYY-MM-DD`.
 */
export function buildWeatherClosureSamples(
  states: readonly RideHourState[],
  weather: ReadonlyMap<number, ArchiveHour>,
  dayOf: (instant: Date) => string,
): Map<string, WeatherClosureSample[]> {
  const dayCache = new Map<number, string>();
  const localDay = (d: Date): string => {
    const t = d.getTime();
    let v = dayCache.get(t);
    if (v === undefined) {
      v = dayOf(d);
      dayCache.set(t, v);
    }
    return v;
  };

  const ranOn = new Map<string, Set<number>>();
  for (const s of states) {
    if (s.closed) continue;
    const days = ranOn.get(s.attractionId) ?? new Set<number>();
    days.add(dayNumber(localDay(s.hourStart)));
    ranOn.set(s.attractionId, days);
  }
  const ranWithin = (
    days: Set<number> | undefined,
    from: number,
    to: number,
  ) => {
    for (let d = from; d <= to; d++) if (days?.has(d)) return true;
    return false;
  };
  const inSeason = states.filter((s) => {
    const days = ranOn.get(s.attractionId);
    const d = dayNumber(localDay(s.hourStart));
    return (
      ranWithin(days, d - SEASON_REACH_DAYS, d) &&
      ranWithin(days, d, d + SEASON_REACH_DAYS)
    );
  });

  const perHour = new Map<number, { closed: number; total: number }>();
  for (const s of inSeason) {
    const t = s.hourStart.getTime();
    const agg = perHour.get(t) ?? { closed: 0, total: 0 };
    agg.total += 1;
    if (s.closed) agg.closed += 1;
    perHour.set(t, agg);
  }

  const snowWithin24h = (t: number): boolean => {
    for (let k = 0; k <= 24; k++) {
      if ((weather.get(t - k * HOUR_MS)?.snowfallCm ?? 0) >= SNOW_MIN_CM) {
        return true;
      }
    }
    return false;
  };

  const out = new Map<string, WeatherClosureSample[]>();
  for (const s of inSeason) {
    const t = s.hourStart.getTime();
    const wx = weather.get(t);
    if (!wx || wx.temperatureC === null) continue;
    const agg = perHour.get(t)!;
    const others = agg.total - 1;
    const othersClosed = agg.closed - (s.closed ? 1 : 0);
    if (
      others > PARK_WIDE_MIN_RIDES &&
      othersClosed / others > PARK_WIDE_SHARE
    ) {
      continue;
    }
    const list = out.get(s.attractionId) ?? [];
    list.push({
      hourStart: s.hourStart,
      day: localDay(s.hourStart),
      temperatureC: wx.temperatureC,
      snow: snowWithin24h(t),
      closed: s.closed,
    });
    out.set(s.attractionId, list);
  }

  for (const [id, list] of out) out.set(id, withoutClosedStreaks(list));
  return out;
}

/** Drops runs of {@link CLOSED_STREAK_DAYS}+ consecutive open days shut all day. */
function withoutClosedStreaks(
  list: WeatherClosureSample[],
): WeatherClosureSample[] {
  const fullyClosed = new Map<string, boolean>();
  for (const s of list) {
    fullyClosed.set(s.day, (fullyClosed.get(s.day) ?? true) && s.closed);
  }
  const days = [...fullyClosed.keys()].sort();
  const drop = new Set<string>();
  let run: string[] = [];
  const flush = () => {
    if (run.length >= CLOSED_STREAK_DAYS) run.forEach((d) => drop.add(d));
    run = [];
  };
  for (const day of days) {
    if (fullyClosed.get(day)) run.push(day);
    else flush();
  }
  flush();
  return drop.size ? list.filter((s) => !drop.has(s.day)) : list;
}

/** What the weekly job stores per ride and cause. */
export interface WeatherClosureProfile {
  /**
   * Cold: the ride usually runs from this temperature on (°C). Snow: null,
   * every snowy hour counts.
   */
  thresholdC: number | null;
  /** Share of the condition's park-open hours the ride was shut. */
  closedShare: number;
  /** Its lower bound, days counted once. */
  closedShareLower: number;
  /** Share of hours at 12–25 °C it was shut — the ride's ordinary rate. */
  baselineShare: number;
  /** Conservative probability that a closure under the condition is its doing. */
  attributableShare: number;
  hours: number;
  days: number;
}

export interface WeatherClosureProfiles {
  cold: WeatherClosureProfile | null;
  snow: WeatherClosureProfile | null;
}

/** The temperatures that count as "nothing about the weather". */
const WARM_FROM_C = 12;
const WARM_TO_C = 25;
/** Fewer warm hours than this and the ride's ordinary rate is unknown. */
const MIN_WARM_HOURS = 100;
/** A profile needs this many condition hours over this many days behind it. */
const MIN_HOURS = 30;
const MIN_DAYS = 8;
/** One-sided 95 %. */
const Z = 1.645;
/** What a page may print at all; anything weaker is not a statement. */
export const MIN_PUBLISHED_PROBABILITY = 0.9;
/**
 * The condition must also shut the ride in a fair share of its hours: a ride
 * that never closes in mild weather and closed on one cold afternoon has a
 * perfect ratio and no weather sensitivity anybody could plan around.
 */
export const MIN_PUBLISHED_CLOSED_SHARE_LOWER = 0.15;
/** Snow is not named above this: a ride shut at 10 °C after yesterday's snow is shut for something else. */
const SNOW_MAX_PLAUSIBLE_C = 6;

const CANDIDATE_THRESHOLDS_C = Array.from({ length: 25 }, (_, i) => -2 + i / 2);

function wilson(successes: number, n: number): { lo: number; hi: number } {
  if (n === 0) return { lo: 0, hi: 1 };
  const p = successes / n;
  const z2 = Z * Z;
  const centre = p + z2 / (2 * n);
  const spread = Z * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n));
  const denom = 1 + z2 / n;
  return {
    lo: Math.max(0, (centre - spread) / denom),
    hi: Math.min(1, (centre + spread) / denom),
  };
}

function profileOf(
  hit: readonly WeatherClosureSample[],
  warm: readonly WeatherClosureSample[],
  thresholdC: number | null,
): WeatherClosureProfile | null {
  const days = new Set(hit.map((s) => s.day)).size;
  if (hit.length < MIN_HOURS || days < MIN_DAYS) return null;
  const closed = hit.filter((s) => s.closed).length;
  const warmClosed = warm.filter((s) => s.closed).length;
  const warmDays = new Set(warm.map((s) => s.day)).size;
  // The hours of one day are one observation, not ten: a single snowy
  // afternoon with a long fault would otherwise vouch for itself ten times.
  // So the bounds take the day count as the sample size.
  const condition = wilson((closed / hit.length) * days, days);
  const ordinary = wilson((warmClosed / warm.length) * warmDays, warmDays);
  return {
    thresholdC,
    closedShare: closed / hit.length,
    closedShareLower: condition.lo,
    baselineShare: warmClosed / warm.length,
    attributableShare:
      condition.lo > 0
        ? Math.max(0, (condition.lo - ordinary.hi) / condition.lo)
        : 0,
    hours: hit.length,
    days,
  };
}

/** Whether a profile may be printed. */
export function isPublishable(
  p: WeatherClosureProfile | null | undefined,
): boolean {
  return (
    !!p &&
    p.attributableShare >= MIN_PUBLISHED_PROBABILITY &&
    (p.closedShareLower ?? 0) >= MIN_PUBLISHED_CLOSED_SHARE_LOWER
  );
}

/**
 * The cold and the snow profile of one ride; either is null when there is too
 * little behind it.
 *
 * The cold threshold is the one that best separates dry cold-hour closures
 * from the rest (F1 of "closed" against "below the threshold", over the dry
 * hours under 12 °C); the probability is then judged at that threshold alone,
 * on the same hours, which flatters it somewhat — the day-counted bound and
 * the closed-share floor are there to absorb that. Snow is learned on the
 * snowy hours at or above a publishable cold threshold, so a ride the cold
 * already explains does not learn a snow profile out of cold hours.
 *
 * A profile is stored whatever its probability, so the admin can see a ride
 * approach the bar; {@link isPublishable} is the bar.
 */
export function learnWeatherClosureProfiles(
  samples: readonly WeatherClosureSample[],
): WeatherClosureProfiles {
  const none = { cold: null, snow: null };
  const warm = samples.filter(
    (s) =>
      !s.snow && s.temperatureC >= WARM_FROM_C && s.temperatureC < WARM_TO_C,
  );
  if (warm.length < MIN_WARM_HOURS) return none;

  const dry = samples.filter((s) => !s.snow);
  const dryColdClosed = dry.filter(
    (s) => s.closed && s.temperatureC < WARM_FROM_C,
  ).length;
  let best: { f1: number; thresholdC: number } | null = null;
  for (const thresholdC of CANDIDATE_THRESHOLDS_C) {
    const below = dry.filter((s) => s.temperatureC < thresholdC);
    if (below.length < MIN_HOURS) continue;
    const hit = below.filter((s) => s.closed).length;
    if (hit === 0) continue;
    const precision = hit / below.length;
    const recall = hit / dryColdClosed;
    const f1 = (2 * precision * recall) / (precision + recall);
    if (!best || f1 > best.f1) best = { f1, thresholdC };
  }
  const cold = best
    ? profileOf(
        dry.filter((s) => s.temperatureC < best.thresholdC),
        warm,
        best.thresholdC,
      )
    : null;
  const coldFloor =
    isPublishable(cold) && cold!.thresholdC !== null
      ? cold!.thresholdC
      : -Infinity;
  const snow = profileOf(
    samples.filter((s) => s.snow && s.temperatureC >= coldFloor),
    warm,
    null,
  );
  return { cold, snow };
}

/** One hour of today's weather, observed or forecast. */
export interface WeatherHour {
  /** Start of the hour, UTC. */
  time: Date;
  temperatureC: number | null;
  snowfallCm: number | null;
}

export type WeatherClosureOutlook = "opens_when_warmer" | "likely_closed_today";

export interface WeatherClosureVerdict {
  cause: "cold" | "snow";
  probability: number;
  temperatureC: number;
  /** Cold only. */
  thresholdC: number | null;
  /** Cold only: snow gives no clock, the clearing of a track is not forecast. */
  outlook: WeatherClosureOutlook | null;
  expectedOpenAt: Date | null;
}

/**
 * Whether a ride that is shut during opening hours is shut for the weather,
 * and what today's temperature curve says about the rest of the day.
 *
 * Cold is asked first: its profile is learned on dry hours only, so a ride it
 * names is one the cold shuts with or without snow, and the cold verdict
 * carries the opening time. It needs the ride to be colder than its threshold
 * right now — a ride shut at 15 °C is shut for something else — and the
 * outlook walks the forecast to the park's close for the first hour that
 * reaches it; none means the ride most likely stays shut. Snow is asked when
 * the cold does not answer, snow fell in the last 24 hours and it is still
 * cold enough for it to lie.
 *
 * Null unless the matching profile clears {@link isPublishable}.
 */
export function decideWeatherClosure(
  profiles: WeatherClosureProfiles | null | undefined,
  hours: readonly WeatherHour[],
  snowedYesterday: boolean,
  asOf: Date,
  closesAt: Date,
): WeatherClosureVerdict | null {
  if (!profiles) return null;
  const sorted = [...hours].sort((a, b) => a.time.getTime() - b.time.getTime());
  const now = asOf.getTime();
  const current = sorted.find(
    (h) => h.time.getTime() <= now && now < h.time.getTime() + HOUR_MS,
  );
  if (!current || current.temperatureC === null) return null;

  const cold = profiles.cold;
  if (
    isPublishable(cold) &&
    cold!.thresholdC !== null &&
    current.temperatureC < cold!.thresholdC
  ) {
    const thresholdC = cold!.thresholdC;
    const warmEnough = sorted.find(
      (h) =>
        h.time.getTime() > now &&
        h.time.getTime() < closesAt.getTime() &&
        h.temperatureC !== null &&
        h.temperatureC >= thresholdC,
    );
    return {
      cause: "cold",
      probability: Math.round(cold!.attributableShare * 100) / 100,
      temperatureC: current.temperatureC,
      thresholdC,
      outlook: warmEnough ? "opens_when_warmer" : "likely_closed_today",
      expectedOpenAt: warmEnough ? warmEnough.time : null,
    };
  }

  // shortcut: the forecast starts at 00:00 UTC, so "the last 24 hours" is
  // today's UTC hours so far plus yesterday's park-local daily sum; parks far
  // east of UTC miss their local early morning. Upgrade with `past_days=1`
  // on the forecast call if a snow verdict ever reaches one of them.
  const snowRecently =
    snowedYesterday ||
    sorted.some(
      (h) =>
        h.time.getTime() <= now &&
        h.time.getTime() > now - DAY_MS &&
        (h.snowfallCm ?? 0) >= SNOW_MIN_CM,
    );
  if (
    snowRecently &&
    isPublishable(profiles.snow) &&
    current.temperatureC < SNOW_MAX_PLAUSIBLE_C
  ) {
    return {
      cause: "snow",
      probability: Math.round(profiles.snow!.attributableShare * 100) / 100,
      temperatureC: current.temperatureC,
      thresholdC: null,
      outlook: null,
      expectedOpenAt: null,
    };
  }
  return null;
}
