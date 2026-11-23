/**
 * Climate normals: the long-run mean of a place's weather for a calendar day.
 *
 * A normal is a statement about a date's average over the years, never about
 * one particular day. It therefore lives apart from `weather_data` (the table
 * the wait-time model reads its weather from) and travels with its own marker,
 * `basis: "climate_normal"`, on every response that carries it. See
 * `docs/architecture/weather.md`.
 */

/** First and last year of the averaging period (ERA5 reanalysis, full years). */
export const CLIMATE_NORMAL_FIRST_YEAR = 2015;
export const CLIMATE_NORMAL_LAST_YEAR = 2024;

/** Human-readable reference period as served in the API. */
export const CLIMATE_NORMAL_PERIOD = `${CLIMATE_NORMAL_FIRST_YEAR}-${CLIMATE_NORMAL_LAST_YEAR}`;

/** A calendar day is averaged with this many days on each side, in every year. */
export const CLIMATE_NORMAL_WINDOW_DAYS = 3;

/**
 * A wet day has at least this much precipitation: the WMO's "precipitation
 * day" (≥ 1.0 mm), the threshold most climatologies count rain days by. It also
 * keeps ERA5's known excess of drizzle (too many days of a few tenths of a
 * millimetre) out of the count.
 */
export const WET_DAY_MIN_MM = 1;

/** Key of one calendar day, `MM-DD`. */
export type MonthDay = string;

export interface ClimateNormalDay {
  temperatureMax: number | null;
  temperatureMin: number | null;
  /**
   * The typical range of the day's high: the 25th and 75th percentile of the
   * daily highs in the window (70 days over ten years), so half of those days
   * reached a high between the two. Absent in normals cached before PAR-1411.
   */
  temperatureMaxP25?: number | null;
  temperatureMaxP75?: number | null;
  /** The same for the day's low. */
  temperatureMinP25?: number | null;
  temperatureMinP75?: number | null;
  /**
   * The share of the window's days with at least {@link WET_DAY_MIN_MM} of
   * precipitation, 0…1, two decimals: the chance that such a date is a rain
   * day. Absent in normals cached before PAR-1411.
   */
  wetDayShare?: number | null;
  precipitationSum: number | null;
  rainSum: number | null;
  snowfallSum: number | null;
  windSpeedMax: number | null;
  /** Most frequent WMO code of the window, `null` when none was reported. */
  weatherCode: number | null;
}

/** One `MM-DD` → normal entry per calendar day, including `02-29`. */
export type ClimateNormals = Record<MonthDay, ClimateNormalDay>;

/** Daily archive arrays as Open-Meteo returns them (one entry per `time`). */
export interface ArchiveDaily {
  time: string[];
  temperature_2m_max?: (number | null)[];
  temperature_2m_min?: (number | null)[];
  precipitation_sum?: (number | null)[];
  rain_sum?: (number | null)[];
  snowfall_sum?: (number | null)[];
  weathercode?: (number | null)[];
  windspeed_10m_max?: (number | null)[];
}

const MS_PER_DAY = 24 * 60 * 60 * 1000;
/** A leap year, so that every `MM-DD` (including 02-29) exists once. */
const REFERENCE_LEAP_YEAR = 2024;

/** `MM-DD` of a Date taken in UTC. */
function monthDayOf(date: Date): MonthDay {
  return date.toISOString().slice(5, 10);
}

/** All 366 `MM-DD` keys, in calendar order. */
function allMonthDays(): MonthDay[] {
  const start = Date.UTC(REFERENCE_LEAP_YEAR, 0, 1);
  return Array.from({ length: 366 }, (_, i) =>
    monthDayOf(new Date(start + i * MS_PER_DAY)),
  );
}

/** Mean of the finite values, rounded to one decimal; `null` when there are none. */
function mean(values: (number | null | undefined)[]): number | null {
  const finite = values.filter(
    (v): v is number => typeof v === "number" && Number.isFinite(v),
  );
  if (finite.length === 0) return null;
  const sum = finite.reduce((a, b) => a + b, 0);
  return Math.round((sum / finite.length) * 10) / 10;
}

/** The finite values among `values`. */
function finiteOf(values: (number | null | undefined)[]): number[] {
  return values.filter(
    (v): v is number => typeof v === "number" && Number.isFinite(v),
  );
}

/**
 * The `p` quantile (0…1) of the finite values, linearly interpolated between
 * the two nearest ranks (the common "type 7" estimator), rounded to one
 * decimal; `null` when there are none.
 */
export function quantile(
  values: (number | null | undefined)[],
  p: number,
): number | null {
  const sorted = finiteOf(values).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const rank = (sorted.length - 1) * p;
  const lower = Math.floor(rank);
  const upper = Math.ceil(rank);
  const value =
    sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower);
  return Math.round(value * 10) / 10;
}

/** The share of the finite values of at least `min`, two decimals; `null` when there are none. */
function shareAtLeast(
  values: (number | null | undefined)[],
  min: number,
): number | null {
  const finite = finiteOf(values);
  if (finite.length === 0) return null;
  const share = finite.filter((v) => v >= min).length / finite.length;
  return Math.round(share * 100) / 100;
}

function mode(values: (number | null | undefined)[]): number | null {
  const counts = new Map<number, number>();
  for (const v of values) {
    if (typeof v !== "number" || !Number.isFinite(v)) continue;
    counts.set(v, (counts.get(v) ?? 0) + 1);
  }
  let best: number | null = null;
  let bestCount = 0;
  for (const [value, count] of counts) {
    // Ties go to the lower (clearer) code.
    if (count > bestCount || (count === bestCount && value < (best ?? 0))) {
      best = value;
      bestCount = count;
    }
  }
  return best;
}

/**
 * Averages daily archive rows into one normal per calendar day.
 *
 * Only rows inside the reference period count, so a response that reaches
 * further than asked cannot shift the period the API names. Each calendar day
 * is averaged over `±CLIMATE_NORMAL_WINDOW_DAYS` days in every year; with ten
 * years that is 70 samples per day instead of 10, which keeps one freak day
 * from becoming the "normal". A day with no temperature at all (no sample in
 * the window) is left out rather than stored as nulls.
 *
 * Beside each mean the same samples give the spread a visitor can expect
 * (PAR-1411): the 25th and 75th percentile of the highs and lows, so "16–18 °C"
 * is the range half of those days fell in, and the share of rain days. A mean
 * alone says neither: a mean of 3 mm is one wet day in five as easily as
 * drizzle on every one.
 */
export function buildClimateNormals(daily: ArchiveDaily): ClimateNormals {
  const byDay = new Map<MonthDay, number[]>();
  daily.time.forEach((date, index) => {
    const year = Number(date.slice(0, 4));
    if (year < CLIMATE_NORMAL_FIRST_YEAR || year > CLIMATE_NORMAL_LAST_YEAR) {
      return;
    }
    const key = date.slice(5, 10);
    const list = byDay.get(key) ?? [];
    list.push(index);
    byDay.set(key, list);
  });

  const days = allMonthDays();
  const normals: ClimateNormals = {};

  days.forEach((key, dayIndex) => {
    const indexes: number[] = [];
    for (
      let offset = -CLIMATE_NORMAL_WINDOW_DAYS;
      offset <= CLIMATE_NORMAL_WINDOW_DAYS;
      offset++
    ) {
      const neighbour = days[(dayIndex + offset + days.length) % days.length];
      indexes.push(...(byDay.get(neighbour) ?? []));
    }
    const pick = (series?: (number | null)[]) =>
      indexes.map((i) => series?.[i] ?? null);

    const highs = pick(daily.temperature_2m_max);
    const lows = pick(daily.temperature_2m_min);
    const precipitation = pick(daily.precipitation_sum);
    const temperatureMax = mean(highs);
    const temperatureMin = mean(lows);
    if (temperatureMax === null && temperatureMin === null) return;

    normals[key] = {
      temperatureMax,
      temperatureMin,
      temperatureMaxP25: quantile(highs, 0.25),
      temperatureMaxP75: quantile(highs, 0.75),
      temperatureMinP25: quantile(lows, 0.25),
      temperatureMinP75: quantile(lows, 0.75),
      wetDayShare: shareAtLeast(precipitation, WET_DAY_MIN_MM),
      precipitationSum: mean(precipitation),
      rainSum: mean(pick(daily.rain_sum)),
      snowfallSum: mean(pick(daily.snowfall_sum)),
      windSpeedMax: mean(pick(daily.windspeed_10m_max)),
      weatherCode: mode(pick(daily.weathercode)),
    };
  });

  return normals;
}

/**
 * Whether cached normals carry the spread (PAR-1411): normals cached before it
 * have the means only, and are fetched again in the background.
 */
export function hasClimateSpread(normals: ClimateNormals): boolean {
  return Object.values(normals).some(
    (day) => day.temperatureMaxP25 !== undefined,
  );
}

/** `MM-DD` key of a `YYYY-MM-DD` date string. */
export function monthDayKey(dateStr: string): MonthDay {
  return dateStr.slice(5, 10);
}
