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
 * lower bound of `p_condition` against the upper bound of `p_warm`.
 */

/** One park-open hour of one ride. */
export interface WeatherClosureSample {
  hourStart: Date;
  temperatureC: number;
  /** Snow fell in this hour or in the 24 before it. */
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

const DAY_MS = 24 * 60 * 60 * 1000;
/** A ride that ran within this many days either side is in its season. */
const SEASON_REACH_DAYS = 7;
/** Above this share of the park's other rides shut, the hour is about the park. */
const PARK_WIDE_SHARE = 0.3;
const PARK_WIDE_MIN_RIDES = 5;

/**
 * Turns a park's ride-hours and its hourly archive into one sample list per
 * ride. Three hours are dropped, each because it would teach the wrong lesson:
 *
 * - out of season (the ride ran on no day within a week): a water ride shut
 *   from November to March is closed for the season, not for today's weather;
 * - park-wide (over 30 % of the park's other rides shut that hour): a storm
 *   evacuation or a feed outage is not this ride's sensitivity;
 * - no temperature in the archive for the hour.
 */
export function buildWeatherClosureSamples(
  states: readonly RideHourState[],
  weather: ReadonlyMap<number, ArchiveHour>,
): Map<string, WeatherClosureSample[]> {
  const perHour = new Map<number, { closed: number; total: number }>();
  const ranOn = new Map<string, Set<number>>();
  for (const s of states) {
    const t = s.hourStart.getTime();
    const agg = perHour.get(t) ?? { closed: 0, total: 0 };
    agg.total += 1;
    if (s.closed) agg.closed += 1;
    perHour.set(t, agg);
    if (!s.closed) {
      const days = ranOn.get(s.attractionId) ?? new Set<number>();
      days.add(Math.floor(t / DAY_MS));
      ranOn.set(s.attractionId, days);
    }
  }

  const snowWithin24h = (t: number): boolean => {
    for (let k = 0; k <= 24; k++) {
      if ((weather.get(t - k * HOUR_MS)?.snowfallCm ?? 0) > 0) return true;
    }
    return false;
  };

  const out = new Map<string, WeatherClosureSample[]>();
  for (const s of states) {
    const t = s.hourStart.getTime();
    const wx = weather.get(t);
    if (!wx || wx.temperatureC === null) continue;

    const days = ranOn.get(s.attractionId);
    const day = Math.floor(t / DAY_MS);
    let inSeason = false;
    for (let k = -SEASON_REACH_DAYS; k <= SEASON_REACH_DAYS && !inSeason; k++) {
      inSeason = days?.has(day + k) ?? false;
    }
    if (!inSeason) continue;

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
      temperatureC: wx.temperatureC,
      snow: snowWithin24h(t),
      closed: s.closed,
    });
    out.set(s.attractionId, list);
  }
  return out;
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
const MIN_DAYS = 5;
/** One-sided 95 %. */
const Z = 1.645;
/** What a page may print at all; anything weaker is not a statement. */
export const MIN_PUBLISHED_PROBABILITY = 0.9;

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
  const days = new Set(hit.map((s) => s.hourStart.toISOString().slice(0, 10)))
    .size;
  if (hit.length < MIN_HOURS || days < MIN_DAYS) return null;
  const closed = hit.filter((s) => s.closed).length;
  const warmClosed = warm.filter((s) => s.closed).length;
  const warmDays = new Set(
    warm.map((s) => s.hourStart.toISOString().slice(0, 10)),
  ).size;
  // The hours of one day are one observation, not ten: a single snowy
  // afternoon with a long fault would otherwise vouch for itself ten times.
  // So the bounds take the day count as the sample size.
  const condition = wilson((closed / hit.length) * days, days);
  const ordinary = wilson((warmClosed / warm.length) * warmDays, warmDays);
  return {
    thresholdC,
    closedShare: closed / hit.length,
    baselineShare: warmClosed / warm.length,
    attributableShare:
      condition.lo > 0
        ? Math.max(0, (condition.lo - ordinary.hi) / condition.lo)
        : 0,
    hours: hit.length,
    days,
  };
}

/**
 * The cold and the snow profile of one ride; either is null when there is too
 * little behind it.
 *
 * The cold threshold is the one that best separates dry cold-hour closures
 * from the rest (F1 of "closed" against "below the threshold", over the dry
 * hours under 12 °C); the probability is then judged at that threshold alone.
 * A profile is stored whatever its probability, so the admin can see a ride
 * approach the bar; {@link decideWeatherClosure} applies the bar.
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
        dry.filter((s) => s.temperatureC < best!.thresholdC),
        warm,
        best.thresholdC,
      )
    : null;
  const snow = profileOf(
    samples.filter((s) => s.snow),
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

const HOUR_MS = 60 * 60 * 1000;

function publishable(p: WeatherClosureProfile | null | undefined): boolean {
  return !!p && p.attributableShare >= MIN_PUBLISHED_PROBABILITY;
}

/**
 * Whether a ride that is shut during opening hours is shut for the weather,
 * and what today's temperature curve says about the rest of the day.
 *
 * Snow is asked first: when snow fell in the last 24 hours, a cold verdict
 * would name the wrong cause for exactly the rides that close for snow and
 * not for cold, so the cold is not asked at all. Without snow, the cold
 * verdict needs the ride to be colder than its threshold right now — a ride
 * shut at 15 °C is shut for something else — and the outlook walks the
 * forecast to the park's close for the first hour that reaches it; none means
 * the ride most likely stays shut.
 *
 * Null unless the matching profile clears {@link MIN_PUBLISHED_PROBABILITY}.
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

  // shortcut: the forecast starts at 00:00 UTC, so "the last 24 hours" is
  // today's hours so far plus yesterday's daily snowfall sum.
  const snowRecently =
    snowedYesterday ||
    sorted.some(
      (h) =>
        h.time.getTime() <= now &&
        h.time.getTime() > now - 24 * HOUR_MS &&
        (h.snowfallCm ?? 0) > 0,
    );

  if (snowRecently) {
    if (!publishable(profiles.snow)) return null;
    return {
      cause: "snow",
      probability: Math.round(profiles.snow!.attributableShare * 100) / 100,
      temperatureC: current.temperatureC,
      thresholdC: null,
      outlook: null,
      expectedOpenAt: null,
    };
  }

  const cold = profiles.cold;
  if (!publishable(cold) || cold!.thresholdC === null) return null;
  const thresholdC = cold!.thresholdC;
  if (current.temperatureC >= thresholdC) return null;

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
