import type { WeatherSummary } from "../dto/integrated-calendar.dto";
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
    condition: weather.weatherCode
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
    humidity: num(weather.humidity),
    apparentTemp: num(weather.apparentTemperature),
    icon: weather.weatherCode ?? 0,
  };
}
