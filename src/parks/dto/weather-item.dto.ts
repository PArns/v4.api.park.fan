import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";

/**
 * Weather Item DTO
 *
 * Represents a single day's weather data
 */
export class WeatherItemDto {
  date: string; // ISO 8601 date (YYYY-MM-DD)
  dataType: "historical" | "current" | "forecast";
  temperatureMax: number | null;
  temperatureMin: number | null;
  precipitationSum: number | null;
  rainSum: number | null;
  snowfallSum: number | null;
  weatherCode: number | null;
  weatherDescription: string | null; // Human-readable weather (e.g., "Rain, slight intensity")
  windSpeedMax: number | null;

  static fromEntity(weatherData: {
    date: Date | string;
    dataType: "historical" | "current" | "forecast";
    temperatureMax: number | null;
    temperatureMin: number | null;
    precipitationSum: number | null;
    rainSum: number | null;
    snowfallSum: number | null;
    weatherCode: number | null;
    windSpeedMax: number | null;
  }): WeatherItemDto {
    const dto = new WeatherItemDto();
    // Handle both Date objects and string dates
    dto.date =
      typeof weatherData.date === "string"
        ? weatherData.date.split("T")[0]
        : weatherData.date.toISOString().split("T")[0]; // YYYY-MM-DD format
    dto.dataType = weatherData.dataType;
    dto.temperatureMax = weatherData.temperatureMax;
    dto.temperatureMin = weatherData.temperatureMin;
    dto.precipitationSum = weatherData.precipitationSum;
    dto.rainSum = weatherData.rainSum;
    dto.snowfallSum = weatherData.snowfallSum;
    dto.weatherCode = weatherData.weatherCode;
    dto.weatherDescription =
      weatherData.weatherCode !== null
        ? getWeatherDescription(weatherData.weatherCode)
        : null;
    dto.windSpeedMax = weatherData.windSpeedMax;
    return dto;
  }
}
