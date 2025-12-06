export class WeatherForecastItemDto {
  time: string;
  temperature: number | null;
  precipitation: number | null;
  rain: number | null;
  snowfall: number | null;
  weatherCode: number | null;
  windSpeed: number | null;
}

export class PredictionRequestDto {
  attractionIds: string[];
  parkIds: string[];
  predictionType: "hourly" | "daily";
  baseTime?: string; // ISO format
  weatherForecast?: WeatherForecastItemDto[];
  currentWaitTimes?: Record<string, number>;
}

export class ParkPredictionRequestDto {
  parkId: string;
  predictionType: "hourly" | "daily";
}
