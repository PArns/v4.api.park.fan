import { ApiProperty } from "@nestjs/swagger";
import type { ParkHourly } from "../utils/weather-hourly.util";
import {
  OPEN_METEO_ATTRIBUTION,
  WeatherAttributionDto,
} from "./weather-attribution.dto";

export class WeatherHourDto {
  @ApiProperty({
    description:
      "Start of the hour on the park's clock, ISO 8601 with its offset (an instant and the park's wall time in one)",
    example: "2026-11-25T14:00+01:00",
  })
  time: string;

  @ApiProperty({ description: "Air temperature (°C)", nullable: true })
  temperatureC: number | null;

  @ApiProperty({ description: "Feels-like temperature (°C)", nullable: true })
  apparentTemperatureC: number | null;

  @ApiProperty({
    description: "Precipitation in the hour (mm)",
    nullable: true,
  })
  precipitationMm: number | null;

  @ApiProperty({
    description: "Chance of precipitation in the hour (0-100)",
    nullable: true,
  })
  precipitationProbability: number | null;

  @ApiProperty({
    description: "WMO weather code, as on the other weather routes",
    nullable: true,
  })
  weatherCode: number | null;

  @ApiProperty({ description: "Sustained wind speed (km/h)", nullable: true })
  windSpeedKmh: number | null;

  @ApiProperty({ description: "Wind gusts (km/h)", nullable: true })
  windGustsKmh: number | null;

  @ApiProperty({
    description: "Wind direction (degrees, 0-360, the direction it comes FROM)",
    nullable: true,
  })
  windDirectionDeg: number | null;

  @ApiProperty({ description: "Daylight in this hour", nullable: true })
  isDay: boolean | null;
}

export class WeatherHourlyDayDto {
  @ApiProperty({ description: "Park-local date", example: "2026-11-25" })
  date: string;

  @ApiProperty({
    description: "Sunrise on the park's clock, with offset",
    example: "2026-11-25T08:01+01:00",
    nullable: true,
  })
  sunrise: string | null;

  @ApiProperty({
    description: "Sunset on the park's clock, with offset",
    example: "2026-11-25T16:38+01:00",
    nullable: true,
  })
  sunset: string | null;

  @ApiProperty({
    description:
      "The day's hours from midnight to midnight: 24, 23 on a spring-forward day, 25 on a fall-back day",
    type: [WeatherHourDto],
  })
  hours: WeatherHourDto[];
}

export class WeatherHourlyDto {
  @ApiProperty({
    description: "Park information",
    type: "object",
    properties: {
      id: { type: "string", format: "uuid" },
      name: { type: "string", example: "Phantasialand" },
      slug: { type: "string", example: "phantasialand" },
      timezone: { type: "string", example: "Europe/Berlin" },
    },
  })
  park: { id: string; name: string; slug: string; timezone: string };

  @ApiProperty({
    description: "When the forecast behind this answer was fetched (UTC ISO)",
  })
  generatedAt: string;

  @ApiProperty({
    description:
      "When to ask again (UTC ISO): the cache's end, or the park's next midnight if sooner. The response's max-age runs to it.",
  })
  nextUpdateAt: string;

  @ApiProperty({
    description:
      "The park's today first, then up to 14 following days (15 at most)",
    type: [WeatherHourlyDayDto],
  })
  days: WeatherHourlyDayDto[];

  @ApiProperty({
    description: "Attribution metadata for the weather data source",
    type: WeatherAttributionDto,
  })
  attribution: WeatherAttributionDto;

  static from(
    park: { id: string; name: string; slug: string; timezone: string },
    hourly: ParkHourly,
  ): WeatherHourlyDto {
    const dto = new WeatherHourlyDto();
    dto.park = park;
    dto.generatedAt = hourly.generatedAt;
    dto.nextUpdateAt = hourly.nextUpdateAt;
    dto.days = hourly.days;
    dto.attribution = OPEN_METEO_ATTRIBUTION;
    return dto;
  }
}
