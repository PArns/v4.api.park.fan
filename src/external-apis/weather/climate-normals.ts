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

/** Key of one calendar day, `MM-DD`. */
export type MonthDay = string;

export interface ClimateNormalDay {
  temperatureMax: number | null;
  temperatureMin: number | null;
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

    const temperatureMax = mean(pick(daily.temperature_2m_max));
    const temperatureMin = mean(pick(daily.temperature_2m_min));
    if (temperatureMax === null && temperatureMin === null) return;

    normals[key] = {
      temperatureMax,
      temperatureMin,
      precipitationSum: mean(pick(daily.precipitation_sum)),
      rainSum: mean(pick(daily.rain_sum)),
      snowfallSum: mean(pick(daily.snowfall_sum)),
      windSpeedMax: mean(pick(daily.windspeed_10m_max)),
      weatherCode: mode(pick(daily.weathercode)),
    };
  });

  return normals;
}

/** `MM-DD` key of a `YYYY-MM-DD` date string. */
export function monthDayKey(dateStr: string): MonthDay {
  return dateStr.slice(5, 10);
}
