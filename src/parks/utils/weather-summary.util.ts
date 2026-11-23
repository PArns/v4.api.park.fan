import type { WeatherSummary } from "../dto/integrated-calendar.dto";
import {
  CLIMATE_NORMAL_PERIOD,
  ClimateNormalDay,
} from "../../external-apis/weather/climate-normals";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";

/**
 * Turns one `weather_data` row into the calendar's weather block — or into
 * nothing, which is the point of this file.
 *
 * `weather_data` holds a row per park per day, and the row at the edge of the
 * forecast window exists before the model reaches that far: every column null,
 * a placeholder written by the sync. The calendar used to map it inline with
 * `Number(x) ?? 0`, which cannot do what it looks like it does — `Number(null)`
 * is `0`, so the `?? 0` is dead code and a missing temperature arrived at the
 * frontend as a real one. On 2026-08-28 that put "0°–0°" on the tile of 22
 * parks, all on the sixteenth day, which is the same rule this codebase keeps
 * relearning: an absent fact must not be served as a confident one.
 *
 * A function rather than an inline block because the rule is now stated once
 * and tested once. The row it is handed comes from TypeORM, where `numeric`
 * columns arrive as strings, so every value goes through `num`.
 */

interface WeatherRow {
  temperatureMin?: number | string | null;
  temperatureMax?: number | string | null;
  precipitationSum?: number | string | null;
  snowfallSum?: number | string | null;
  windSpeedMax?: number | string | null;
  windGustsMax?: number | string | null;
  precipitationProbabilityMax?: number | string | null;
  precipitationHours?: number | string | null;
  uvIndexMax?: number | string | null;
  sunshineHours?: number | string | null;
  humidity?: number | string | null;
  apparentTemperature?: number | string | null;
  weatherCode?: number | null;
}

/** A finite number, or undefined — never 0 standing in for "no value". */
function num(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function toWeatherSummary(
  weather: WeatherRow | null | undefined,
): WeatherSummary | undefined {
  if (!weather) return undefined;

  const tempMin = num(weather.temperatureMin);
  const tempMax = num(weather.temperatureMax);

  // No temperature at all means no forecast for this day, whatever else the row
  // happens to carry. Nothing else in the block is worth a tile on its own, and
  // a weather icon over an empty range reads as a broken page.
  //
  // Note the asymmetry with 0: a winter day really is 0 °C, and `num` keeps it.
  // Only null is missing.
  if (tempMin === undefined && tempMax === undefined) return undefined;

  return {
    basis: "forecast",
    condition:
      weather.weatherCode != null
        ? getWeatherDescription(weather.weatherCode)
        : "unknown",
    // One known end fills the other: a single figure is honest, a range with an
    // invented end is not.
    tempMin: tempMin ?? tempMax!,
    tempMax: tempMax ?? tempMin!,
    // `rainChance` keeps its misleading legacy name — it is precipitation in mm,
    // not a percentage — and its legacy 0 default, because it is a required
    // field of the published contract. The honest one is `precipitationMm`
    // beside it, which stays absent when nothing was measured.
    rainChance: num(weather.precipitationSum) ?? 0,
    precipitationMm: num(weather.precipitationSum),
    snowMm: num(weather.snowfallSum),
    windMax: num(weather.windSpeedMax),
    windGustsMax: num(weather.windGustsMax),
    precipitationProbability: num(weather.precipitationProbabilityMax),
    precipitationHours: num(weather.precipitationHours),
    uvIndexMax: num(weather.uvIndexMax),
    sunshineHours: num(weather.sunshineHours),
    humidity: num(weather.humidity),
    apparentTemp: num(weather.apparentTemperature),
    icon: weather.weatherCode ?? 0,
  };
}

/** What {@link weatherView} holds, as the tools that return it describe it. */
export const WEATHER_VIEW_FIELDS =
  "condition, °C (feelsLikeC today), precipitation in mm with its highest hourly chance in % " +
  "and its hours, snow in cm, wind and gusts in km/h, UV index, sunshine hours; " +
  "`basis: climate_normal` is the historical average for the date (note), with typicalHighC " +
  "and typicalLowC (the range half such days fell in) and rainDayChancePercent";

/**
 * A day's weather as Parky and the admin MCP read it: every figure the calendar
 * has, each named with its unit, and the legacy `rainChance` left out — it holds
 * millimetres under a name that reads as a percentage, and a model reading it
 * said "12,6 % chance of rain" for 12.6 mm. Absent fields stay absent.
 */
export function weatherView(weather: WeatherSummary | null | undefined) {
  if (!weather) return weather;
  return {
    basis: weather.basis,
    condition: weather.condition,
    // The WMO code, as get_park_weather names it.
    weatherCode: weather.icon ?? null,
    tempMin: weather.tempMin,
    tempMax: weather.tempMax,
    feelsLikeC: weather.apparentTemp,
    precipitationMm: weather.precipitationMm ?? weather.rainChance,
    precipitationChancePercent: weather.precipitationProbability,
    precipitationHours: weather.precipitationHours,
    snowCm: weather.snowMm,
    windMaxKmh: weather.windMax,
    windGustsMaxKmh: weather.windGustsMax,
    uvIndexMax: weather.uvIndexMax,
    sunshineHours: weather.sunshineHours,
    humidityPercent: weather.humidity,
    // A normal is said as what such a day is usually like, never as the day's
    // forecast: the note says so beside the figures, the ranges give the spread.
    ...(weather.basis === "climate_normal" && {
      note: CLIMATE_NORMAL_NOTE,
      typicalHighC: tempRange(weather.tempMaxP25, weather.tempMaxP75),
      typicalLowC: tempRange(weather.tempMinP25, weather.tempMinP75),
      rainDayChancePercent: weather.wetDayPercent,
    }),
  };
}

/**
 * The calendar's weather block for a climate normal.
 *
 * Same shape as a forecast's, so a client that draws a tile can draw this one,
 * and marked `basis: "climate_normal"` so a client that cares can tell. The
 * fields a normal cannot have (humidity, feels-like) stay absent.
 */
export function toClimateNormalSummary(
  normal: ClimateNormalDay | null | undefined,
): WeatherSummary | undefined {
  if (!normal) return undefined;
  const summary = toWeatherSummary(normal);
  if (!summary) return undefined;
  const maxP25 = num(normal.temperatureMaxP25);
  const maxP75 = num(normal.temperatureMaxP75);
  const minP25 = num(normal.temperatureMinP25);
  const minP75 = num(normal.temperatureMinP75);
  const wet = num(normal.wetDayShare);
  return {
    ...summary,
    basis: "climate_normal",
    normalPeriod: CLIMATE_NORMAL_PERIOD,
    // The spread of the days the mean is taken over (PAR-1411); absent in
    // normals cached before it.
    ...(maxP25 !== undefined &&
      maxP75 !== undefined && { tempMaxP25: maxP25, tempMaxP75: maxP75 }),
    ...(minP25 !== undefined &&
      minP75 !== undefined && { tempMinP25: minP25, tempMinP75: minP75 }),
    ...(wet !== undefined && { wetDayPercent: Math.round(wet * 100) }),
  };
}

/** What a climate normal is, in the words Parky and the admin MCP read beside it. */
export const CLIMATE_NORMAL_NOTE =
  `historical average for this calendar date (${CLIMATE_NORMAL_PERIOD}), ` +
  "not a forecast for the day";

/** A range of two temperatures as one short string, "15-19"; one figure where they round alike. */
export function tempRange(low: number | undefined, high: number | undefined) {
  if (low === undefined || high === undefined) return undefined;
  const a = Math.round(low);
  const b = Math.round(high);
  return a === b ? `${a}` : `${a}-${b}`;
}
