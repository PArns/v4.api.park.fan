import { ApiProperty } from "@nestjs/swagger";
import { WeatherItemDto } from "./weather-item.dto";
import {
  OPEN_METEO_ATTRIBUTION,
  WeatherAttributionDto,
} from "./weather-attribution.dto";

/**
 * Weather Response DTO
 *
 * Returns weather data for a park
 */
export class WeatherResponseDto {
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };
  weather: WeatherItemDto[];

  @ApiProperty({
    description: "Attribution metadata for the weather data source",
    type: WeatherAttributionDto,
  })
  attribution: WeatherAttributionDto = OPEN_METEO_ATTRIBUTION;
}
