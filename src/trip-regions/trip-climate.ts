/**
 * The typical weather of a trip's days: a plan day, a `/when` window, a
 * calendar month, read from a park's climate normals (2015–2024, ERA5; see
 * `climate-normals.ts`). Never a forecast: every block says
 * `basis: "climate_normal"` and its period. Contract:
 * docs/frontend/trip-regions.md#typical-weather.
 */
import {
  CLIMATE_NORMAL_PERIOD,
  type ClimateNormalDay,
  type ClimateNormals,
} from "../external-apis/weather/climate-normals";
import type { RegionConfig } from "./regions";

/**
 * A span's typical weather. The means say what such dates are like on
 * average; the percentiles the range a visitor can expect: on half of the
 * days around such a date the high lay between `tempMaxP25` and `tempMaxP75`
 * ("typically 16–18 °C"). The percentiles and `wetDayPercent` are absent while
 * the park's normals were cached before PAR-1411 (they are fetched again in
 * the background).
 */
export interface TripClimate {
  /** The mean daily high, °C, one decimal. */
  tempMax: number;
  /** The mean daily low, °C, one decimal. */
  tempMin: number;
  /** The 25th and 75th percentile of the daily high, °C, one decimal. */
  tempMaxP25?: number;
  tempMaxP75?: number;
  /** The 25th and 75th percentile of the daily low, °C, one decimal. */
  tempMinP25?: number;
  tempMinP75?: number;
  /**
   * The precipitation over the span, mm: a day's mean (one decimal), a
   * window's or a month's total (whole millimetres).
   */
  precipitationMm: number;
  /** The chance a day of the span is a rain day (at least 1 mm), in percent. */
  wetDayPercent?: number;
  basis: "climate_normal";
  period: string;
}

const round1 = (v: number) => Math.round(v * 10) / 10;

/**
 * The mean of a field over the days that have it; undefined when none does.
 * One park's table is cached whole, so its days carry the same fields: a day
 * without one is a gap in the archive, not an older shape.
 */
function meanOf(
  days: readonly ClimateNormalDay[],
  field: keyof ClimateNormalDay,
): number | undefined {
  const values = days
    .map((d) => d[field])
    .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  if (values.length === 0) return undefined;
  return values.reduce((s, v) => s + v, 0) / values.length;
}

/**
 * The typical weather of the given normals, one per day of a span of
 * `spanDays` days (a month passes all its normals and its length). Undefined
 * without a high, a low or precipitation on any of them.
 */
export function climateOfNormals(
  days: readonly ClimateNormalDay[],
  spanDays: number,
): TripClimate | undefined {
  if (days.length === 0) return undefined;
  const high = meanOf(days, "temperatureMax");
  const low = meanOf(days, "temperatureMin");
  const rain = meanOf(days, "precipitationSum");
  if (high === undefined || low === undefined || rain === undefined) {
    return undefined;
  }
  const maxP25 = meanOf(days, "temperatureMaxP25");
  const maxP75 = meanOf(days, "temperatureMaxP75");
  const minP25 = meanOf(days, "temperatureMinP25");
  const minP75 = meanOf(days, "temperatureMinP75");
  const wet = meanOf(days, "wetDayShare");
  return {
    tempMax: round1(high),
    tempMin: round1(low),
    ...(maxP25 !== undefined &&
      maxP75 !== undefined && {
        tempMaxP25: round1(maxP25),
        tempMaxP75: round1(maxP75),
      }),
    ...(minP25 !== undefined &&
      minP75 !== undefined && {
        tempMinP25: round1(minP25),
        tempMinP75: round1(minP75),
      }),
    precipitationMm:
      spanDays === 1 ? round1(rain) : Math.round(rain * spanDays),
    ...(wet !== undefined && { wetDayPercent: Math.round(wet * 100) }),
    basis: "climate_normal",
    period: CLIMATE_NORMAL_PERIOD,
  };
}

/** The typical weather of the given dates (`YYYY-MM-DD`), from the `MM-DD` table. */
export function climateOfDates(
  normals: ClimateNormals | null | undefined,
  dates: readonly string[],
): TripClimate | undefined {
  if (!normals || dates.length === 0) return undefined;
  const days = dates.map((d) => normals[d.slice(5, 10)]);
  if (days.some((d) => !d)) return undefined;
  return climateOfNormals(days, dates.length);
}

/**
 * The park whose normals stand for a plan day's weather: the day's (first)
 * park; on a day without one, the nearest park of the base the night is spent
 * in (a region with bases: Osaka's days read Universal Studios Japan, not
 * Tokyo Disneyland); else the region's climate park.
 */
export function climateParkOfDay(
  region: Pick<RegionConfig, "bases" | "weather">,
  day: { parks: ReadonlyArray<{ slug: string }>; base?: string },
): string {
  if (day.parks.length > 0) return day.parks[0].slug;
  const base = day.base
    ? region.bases?.find((b) => b.key === day.base)
    : undefined;
  const nearest = base?.parks.reduce<
    { slug: string; minutes: number } | undefined
  >((best, p) => (!best || p.minutes < best.minutes ? p : best), undefined);
  return nearest?.slug ?? region.weather.climatePark;
}
