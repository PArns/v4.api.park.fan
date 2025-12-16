import { ApiProperty } from "@nestjs/swagger";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";

/**
 * Weather Item DTO
 *
 * Represents a single day's weather data
 */
export class WeatherItemDto {
  @ApiProperty({
    description: "Date of the weather data (YYYY-MM-DD)",
    example: "2025-12-17",
  })
  date: string; // ISO 8601 date (YYYY-MM-DD)

  @ApiProperty({
    description: "Type of data source",
    enum: ["historical", "current", "forecast"],
  })
  dataType: "historical" | "current" | "forecast";

  @ApiProperty({
    description: "Maximum temperature (Celsius)",
    required: false,
    nullable: true,
  })
  temperatureMax: number | null;

  @ApiProperty({
    description: "Minimum temperature (Celsius)",
    required: false,
    nullable: true,
  })
  temperatureMin: number | null;

  @ApiProperty({
    description: "Total precipitation (mm)",
    required: false,
    nullable: true,
  })
  precipitationSum: number | null;

  @ApiProperty({
    description: "Total rain (mm)",
    required: false,
    nullable: true,
  })
  rainSum: number | null;

  @ApiProperty({
    description: "Total snowfall (cm)",
    required: false,
    nullable: true,
  })
  snowfallSum: number | null;

  @ApiProperty({
    description: "WMO Weather Code",
    required: false,
    nullable: true,
  })
  weatherCode: number | null;

  @ApiProperty({
    description: "Human-readable weather description",
    required: false,
    nullable: true,
  })
  weatherDescription: string | null; // Human-readable weather (e.g., "Rain, slight intensity")

  @ApiProperty({
    description: "Maximum wind speed (km/h)",
    required: false,
    nullable: true,
  })
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
