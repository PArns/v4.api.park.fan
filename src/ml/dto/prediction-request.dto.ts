import { ApiProperty } from "@nestjs/swagger";

export class WeatherForecastItemDto {
  @ApiProperty({ description: "Forecast time (ISO 8601)" })
  time: string;

  @ApiProperty({ description: "Temperature in Celsius", nullable: true })
  temperature: number | null;

  @ApiProperty({ description: "Precipitation amount (mm)", nullable: true })
  precipitation: number | null;

  @ApiProperty({ description: "Rain amount (mm)", nullable: true })
  rain: number | null;

  @ApiProperty({ description: "Snowfall amount (cm)", nullable: true })
  snowfall: number | null;

  @ApiProperty({ description: "WMO weather code", nullable: true })
  weatherCode: number | null;

  @ApiProperty({ description: "Wind speed (km/h)", nullable: true })
  windSpeed: number | null;
}

export class PredictionRequestDto {
  @ApiProperty({
    description: "Timestamp to predict for (ISO 8601)",
    required: false,
  })
  timestamp?: string;

  @ApiProperty({
    description: "Base timestamp for prediction (ISO 8601)",
    required: false,
  })
  baseTime?: string;

  @ApiProperty({ description: "Array of attraction IDs to predict for" })
  attractionIds: string[];

  @ApiProperty({ description: "Array of park IDs", required: false })
  parkIds?: string[];

  @ApiProperty({ enum: ["hourly", "daily"], description: "Type of prediction" })
  predictionType: "hourly" | "daily";

  @ApiProperty({
    description: "Weather forecast data override",
    type: [WeatherForecastItemDto],
    required: false,
  })
  weatherForecast?: WeatherForecastItemDto[];

  @ApiProperty({ description: "Current wait times override", required: false })
  currentWaitTimes?: Record<string, number>;

  @ApiProperty({
    description: "Wait times from ~30 mins ago for velocity calculation",
    required: false,
  })
  recentWaitTimes?: Record<string, number>;

  @ApiProperty({
    description: "Feature context for Phase 2 ML features",
    required: false,
  })
  featureContext?: {
    parkOccupancy?: Record<string, number>; // parkId -> occupancy %
    parkOpeningTimes?: Record<string, string>; // parkId -> opening time ISO string
    downtimeCache?: Record<string, number>; // attractionId -> downtime minutes
    queueData?: Record<string, any>; // attractionId -> queue info
    isBridgeDay?: Record<string, boolean>; // parkId -> is bridge day?
  };
}

export class ParkPredictionRequestDto {
  @ApiProperty({ description: "Park ID" })
  parkId: string;

  @ApiProperty({ enum: ["hourly", "daily"], description: "Type of prediction" })
  predictionType: "hourly" | "daily";
}
