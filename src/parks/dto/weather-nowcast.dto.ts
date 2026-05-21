import { ApiProperty } from "@nestjs/swagger";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";
import type { ParkNowcast } from "../weather.service";
import {
  OPEN_METEO_ATTRIBUTION,
  WeatherAttributionDto,
} from "./weather-attribution.dto";

export class NowcastStepDto {
  @ApiProperty({
    description: "Start of the 15-min interval (local park time, ISO 8601)",
    example: "2026-05-21T14:00",
  })
  time: string;

  @ApiProperty({
    description: "Precipitation in this 15-min slot (mm)",
    nullable: true,
  })
  precipitation: number | null;

  @ApiProperty({
    description: "Precipitation probability for this slot (0-100)",
    nullable: true,
  })
  precipitationProbability: number | null;

  @ApiProperty({
    description: "WMO weather code for this slot",
    nullable: true,
  })
  weatherCode: number | null;
}

export class WeatherNowcastDto {
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
    description: "ISO timestamp when this nowcast was computed",
    example: "2026-05-21T13:58:12.000Z",
  })
  observedAt: string;

  @ApiProperty({ description: "True if rain is currently falling" })
  currentlyRaining: boolean;

  @ApiProperty({
    description: "Precipitation in mm for the current 15-min slot",
    nullable: true,
  })
  currentPrecipitationMm: number | null;

  @ApiProperty({ description: "Current WMO weather code", nullable: true })
  currentWeatherCode: number | null;

  @ApiProperty({
    description: "Human-readable description of the current weather",
    nullable: true,
  })
  currentWeatherDescription: string | null;

  @ApiProperty({
    description:
      "ISO timestamp when rain is next expected to start. Null if already raining or no rain in the forecast window.",
    nullable: true,
    example: "2026-05-21T14:15:00.000Z",
  })
  rainStartsAt: string | null;

  @ApiProperty({
    description: "Minutes from now until rain starts",
    nullable: true,
    example: 17,
  })
  rainStartsInMinutes: number | null;

  @ApiProperty({
    description:
      "ISO timestamp when rain is expected to stop. Null if no rain in window or rain continues beyond the forecast window.",
    nullable: true,
    example: "2026-05-21T14:45:00.000Z",
  })
  rainEndsAt: string | null;

  @ApiProperty({
    description: "Minutes from now until rain ends",
    nullable: true,
    example: 47,
  })
  rainEndsInMinutes: number | null;

  @ApiProperty({
    description:
      "ISO timestamp of the next thunderstorm slot (WMO 95/96/99). Null if none in the forecast window.",
    nullable: true,
  })
  thunderstormAt: string | null;

  @ApiProperty({
    description: "Minutes from now until next thunderstorm slot",
    nullable: true,
  })
  thunderstormInMinutes: number | null;

  @ApiProperty({
    description:
      "Raw 15-min forecast series (next ~6h). Useful for clients rendering their own chart.",
    type: [NowcastStepDto],
  })
  steps: NowcastStepDto[];

  @ApiProperty({
    description: "Attribution metadata for the weather data source",
    type: WeatherAttributionDto,
  })
  attribution: WeatherAttributionDto;

  static fromNowcast(
    park: { id: string; name: string; slug: string; timezone: string },
    nowcast: ParkNowcast,
  ): WeatherNowcastDto {
    const dto = new WeatherNowcastDto();
    dto.park = park;
    dto.observedAt = nowcast.observedAt;
    dto.currentlyRaining = nowcast.currentlyRaining;
    dto.currentPrecipitationMm = nowcast.currentPrecipitationMm;
    dto.currentWeatherCode = nowcast.currentWeatherCode;
    dto.currentWeatherDescription =
      nowcast.currentWeatherCode != null
        ? getWeatherDescription(nowcast.currentWeatherCode)
        : null;
    dto.rainStartsAt = nowcast.rainStartsAt;
    dto.rainStartsInMinutes = nowcast.rainStartsInMinutes;
    dto.rainEndsAt = nowcast.rainEndsAt;
    dto.rainEndsInMinutes = nowcast.rainEndsInMinutes;
    dto.thunderstormAt = nowcast.thunderstormAt;
    dto.thunderstormInMinutes = nowcast.thunderstormInMinutes;
    dto.steps = nowcast.steps.map((s) => ({
      time: s.time,
      precipitation: s.precipitation,
      precipitationProbability: s.precipitationProbability,
      weatherCode: s.weatherCode,
    }));
    dto.attribution = OPEN_METEO_ATTRIBUTION;
    return dto;
  }
}
