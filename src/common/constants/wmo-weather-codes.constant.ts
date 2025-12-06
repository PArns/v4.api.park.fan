/**
 * WMO Weather Code Interpretation
 *
 * Maps WMO (World Meteorological Organization) weather codes
 * to human-readable descriptions.
 *
 * Used by Open-Meteo API for weather data.
 *
 * Reference: https://open-meteo.com/en/docs
 * WMO Code Table: https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM
 */
export const WMO_WEATHER_CODES: Record<number, string> = {
  0: "Clear sky",
  1: "Mainly clear",
  2: "Partly cloudy",
  3: "Overcast",
  45: "Fog",
  48: "Depositing rime fog",
  51: "Drizzle, light intensity",
  53: "Drizzle, moderate intensity",
  55: "Drizzle, dense intensity",
  56: "Freezing drizzle, light intensity",
  57: "Freezing drizzle, dense intensity",
  61: "Rain, slight intensity",
  63: "Rain, moderate intensity",
  65: "Rain, heavy intensity",
  66: "Freezing rain, light intensity",
  67: "Freezing rain, heavy intensity",
  71: "Snow fall, slight intensity",
  73: "Snow fall, moderate intensity",
  75: "Snow fall, heavy intensity",
  77: "Snow grains",
  80: "Rain showers, slight intensity",
  81: "Rain showers, moderate intensity",
  82: "Rain showers, violent intensity",
  85: "Snow showers, slight intensity",
  86: "Snow showers, heavy intensity",
  95: "Thunderstorm, slight or moderate",
  96: "Thunderstorm with slight hail",
  99: "Thunderstorm with heavy hail",
};

/**
 * Gets human-readable weather description from WMO code
 *
 * @param code - WMO weather code
 * @returns Weather description or "Unknown" if code not found
 */
export function getWeatherDescription(code: number): string {
  return WMO_WEATHER_CODES[code] || `Unknown (code: ${code})`;
}

/**
 * Categorizes weather codes into simple categories
 *
 * Useful for filtering and grouping weather data.
 *
 * @param code - WMO weather code
 * @returns Weather category
 */
export function getWeatherCategory(
  code: number,
): "clear" | "cloudy" | "rain" | "snow" | "thunderstorm" | "fog" | "unknown" {
  if (code === 0 || code === 1) return "clear";
  if (code === 2 || code === 3) return "cloudy";
  if (code === 45 || code === 48) return "fog";
  if (code >= 51 && code <= 67) return "rain";
  if (code >= 71 && code <= 77) return "snow";
  if (code >= 80 && code <= 82) return "rain";
  if (code >= 85 && code <= 86) return "snow";
  if (code >= 95 && code <= 99) return "thunderstorm";
  return "unknown";
}
