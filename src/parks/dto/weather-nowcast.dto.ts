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

  @ApiProperty({
    description: "Sustained wind speed in this slot (km/h)",
    nullable: true,
  })
  windSpeed: number | null;

  @ApiProperty({
    description: "Wind gusts in this slot (km/h)",
    nullable: true,
  })
  windGusts: number | null;
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
    description:
      "ISO timestamp when the upstream forecast was last fetched. Acts as the data-freshness indicator.",
    example: "2026-05-21T13:58:12.000Z",
  })
  observedAt: string;

  @ApiProperty({
    description:
      "ISO timestamp when the next nowcast refresh is expected (observedAt + cache TTL of 15 minutes).",
    example: "2026-05-21T14:13:12.000Z",
  })
  nextUpdateAt: string;

  @ApiProperty({
    description:
      "Whether it was raining at `observedAt`. Snapshot value — not recomputed against wall-clock time. Use the absolute event timestamps below for the live picture.",
  })
  currentlyRaining: boolean;

  @ApiProperty({
    description: "Air temperature in °C at `observedAt`",
    nullable: true,
    example: 15.4,
  })
  currentTemperatureC: number | null;

  @ApiProperty({
    description:
      "'Feels like' temperature in °C at `observedAt` (factors in wind chill / humidity)",
    nullable: true,
    example: 13.1,
  })
  currentApparentTemperatureC: number | null;

  @ApiProperty({
    description: "Relative humidity 0-100 (whole percent) at `observedAt`",
    nullable: true,
    example: 78,
  })
  currentHumidity: number | null;

  @ApiProperty({
    description:
      "Precipitation in mm for the 15-min slot containing `observedAt`",
    nullable: true,
  })
  currentPrecipitationMm: number | null;

  @ApiProperty({
    description:
      "WMO weather code for the slot containing `observedAt`. Use this to pick a weather icon.",
    nullable: true,
  })
  currentWeatherCode: number | null;

  @ApiProperty({
    description: "Human-readable description of the weather at `observedAt`",
    nullable: true,
  })
  currentWeatherDescription: string | null;

  @ApiProperty({
    description:
      "Whether it is daytime at `observedAt`. Drives day/night icon variants.",
    nullable: true,
  })
  isDay: boolean | null;

  @ApiProperty({
    description: "Forecast high for today (park's local timezone), in °C",
    nullable: true,
    example: 14,
  })
  temperatureMaxC: number | null;

  @ApiProperty({
    description: "Forecast low for today (park's local timezone), in °C",
    nullable: true,
    example: 8,
  })
  temperatureMinC: number | null;

  @ApiProperty({
    description:
      "ISO timestamp when rain is next expected to start. Null if already raining or no rain in the forecast window.",
    nullable: true,
    example: "2026-05-21T14:15:00.000Z",
  })
  rainStartsAt: string | null;

  @ApiProperty({
    description:
      "Forecast precipitation in mm for the first rainy 15-min slot. Indicates how heavy rain will be when it starts.",
    nullable: true,
    example: 0.8,
  })
  rainStartsIntensityMm: number | null;

  @ApiProperty({
    description:
      "Qualitative rain intensity bucket for the first rainy slot (light < 0.625 mm, moderate < 1.9 mm, heavy ≥ 1.9 mm per 15 min).",
    nullable: true,
    enum: ["light", "moderate", "heavy"],
    example: "moderate",
  })
  rainStartsIntensity: "light" | "moderate" | "heavy" | null;

  @ApiProperty({
    description:
      "ISO timestamp when rain is expected to stop. Null if no rain in window or rain continues beyond the forecast window.",
    nullable: true,
    example: "2026-05-21T14:45:00.000Z",
  })
  rainEndsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp of the next thunderstorm slot (WMO 95/96/99). Null if none in the forecast window.",
    nullable: true,
  })
  thunderstormStartsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp when the thunderstorm block is expected to clear. Null if none in the window or the block continues beyond the window.",
    nullable: true,
  })
  thunderstormEndsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp of the next hail slot (WMO 96/99 - thunderstorm with hail). Null if no hail in the forecast window.",
    nullable: true,
  })
  hailStartsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp when the hail block is expected to clear. Null if none in the window or the block continues beyond the window.",
    nullable: true,
  })
  hailEndsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp of the next slot with storm-force wind gusts (≥ 75 km/h, Beaufort 9). Null if none in the forecast window.",
    nullable: true,
  })
  stormStartsAt: string | null;

  @ApiProperty({
    description:
      "ISO timestamp when storm-force wind gusts are expected to die down. Null if none in the window or they continue beyond the window.",
    nullable: true,
  })
  stormEndsAt: string | null;

  @ApiProperty({
    description: "Sustained wind speed at `observedAt` (km/h)",
    nullable: true,
  })
  currentWindSpeedKmh: number | null;

  @ApiProperty({
    description: "Wind gusts at `observedAt` (km/h)",
    nullable: true,
  })
  currentWindGustsKmh: number | null;

  @ApiProperty({
    description:
      "Peak wind gust forecasted within the nowcast window (km/h). Useful for indicating expected wind strength.",
    nullable: true,
  })
  peakWindGustsKmh: number | null;

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
    dto.nextUpdateAt = nowcast.nextUpdateAt;
    dto.currentlyRaining = nowcast.currentlyRaining;
    dto.currentTemperatureC = nowcast.currentTemperatureC;
    dto.currentApparentTemperatureC = nowcast.currentApparentTemperatureC;
    dto.currentHumidity = nowcast.currentHumidity;
    dto.currentPrecipitationMm = nowcast.currentPrecipitationMm;
    dto.currentWeatherCode = nowcast.currentWeatherCode;
    dto.currentWeatherDescription =
      nowcast.currentWeatherCode != null
        ? getWeatherDescription(nowcast.currentWeatherCode)
        : null;
    dto.isDay = nowcast.isDay;
    dto.temperatureMaxC = nowcast.temperatureMaxC;
    dto.temperatureMinC = nowcast.temperatureMinC;
    dto.rainStartsAt = nowcast.rainStartsAt;
    dto.rainStartsIntensityMm = nowcast.rainStartsIntensityMm;
    dto.rainStartsIntensity = nowcast.rainStartsIntensity;
    dto.rainEndsAt = nowcast.rainEndsAt;
    dto.thunderstormStartsAt = nowcast.thunderstormStartsAt;
    dto.thunderstormEndsAt = nowcast.thunderstormEndsAt;
    dto.hailStartsAt = nowcast.hailStartsAt;
    dto.hailEndsAt = nowcast.hailEndsAt;
    dto.stormStartsAt = nowcast.stormStartsAt;
    dto.stormEndsAt = nowcast.stormEndsAt;
    dto.currentWindSpeedKmh = nowcast.currentWindSpeedKmh;
    dto.currentWindGustsKmh = nowcast.currentWindGustsKmh;
    dto.peakWindGustsKmh = nowcast.peakWindGustsKmh;
    dto.steps = nowcast.steps.map((s) => ({
      time: s.time,
      precipitation: s.precipitation,
      precipitationProbability: s.precipitationProbability,
      weatherCode: s.weatherCode,
      windSpeed: s.windSpeed,
      windGusts: s.windGusts,
    }));
    dto.attribution = OPEN_METEO_ATTRIBUTION;
    return dto;
  }
}
