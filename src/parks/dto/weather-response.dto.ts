import { WeatherItemDto } from "./weather-item.dto";

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
}
