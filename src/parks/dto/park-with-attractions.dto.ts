import { ApiProperty } from "@nestjs/swagger";
import { Park } from "../entities/park.entity";
import { WeatherItemDto } from "./weather-item.dto";
import { ScheduleItemDto } from "./schedule-item.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { buildParkUrl } from "../../common/utils/url.util";
import { ParkDailyPredictionDto } from "./park-daily-prediction.dto";

export class ParkPredictioAccuracyDto {
  @ApiProperty({
    description: "Accuracy badge",
    enum: ["excellent", "good", "fair", "poor", "insufficient_data"],
  })
  badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";

  @ApiProperty({ description: "Last 30 days statistics" })
  last30Days: {
    mae: number;
    mape: number;
    rmse: number;
    comparedPredictions: number;
    totalPredictions: number;
  };

  @ApiProperty({ description: "Message" })
  message?: string;
}

export class ParkAttractionPredictionDto {
  @ApiProperty({ description: "Predicted time (ISO 8601)" })
  predictedTime: string;

  @ApiProperty({ description: "Predicted wait time in minutes" })
  predictedWaitTime: number;

  @ApiProperty({
    description: "Confidence percentage",
    required: false,
    nullable: true,
  })
  confidencePercentage: number | null;
}

export class ParkLoadDto {
  @ApiProperty({
    description: "Crowd Level",
    enum: ["very_low", "low", "normal", "higher", "high", "extreme"],
  })
  crowdLevel: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";

  @ApiProperty({ description: "Baseline wait time value" })
  baseline: number;

  @ApiProperty({ description: "Current wait time value" })
  currentWaitTime: number;
}

export class ParkAttractionDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({
    description: "Queues",
    type: [QueueDataItemDto],
    required: false,
  })
  queues?: QueueDataItemDto[];

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({
    description: "Hourly ML predictions",
    type: [ParkAttractionPredictionDto],
    required: false,
  })
  hourlyForecast?: ParkAttractionPredictionDto[];

  @ApiProperty({
    description: "Prediction accuracy",
    type: ParkPredictioAccuracyDto,
    required: false,
    nullable: true,
  })
  predictionAccuracy?: ParkPredictioAccuracyDto | null;

  @ApiProperty({
    description: "Current load",
    type: ParkLoadDto,
    required: false,
    nullable: true,
  })
  currentLoad?: ParkLoadDto | null;
}

export class ParkShowDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({ description: "Showtimes", required: false })
  showtimes?: {
    type: string;
    startTime: string;
    endTime?: string;
  }[];

  @ApiProperty({ description: "Operating hours", required: false })
  operatingHours?: {
    type: string;
    startTime: string;
    endTime: string;
  }[];

  @ApiProperty({ description: "Last updated", required: false })
  lastUpdated?: string;
}

export class ParkRestaurantDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({ description: "Cuisine type", required: false, nullable: true })
  cuisineType: string | null;

  @ApiProperty({ description: "Requires reservation" })
  requiresReservation: boolean;

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({ description: "Wait time", required: false, nullable: true })
  waitTime?: number | null;

  @ApiProperty({ description: "Party size", required: false, nullable: true })
  partySize?: number | null;

  @ApiProperty({ description: "Operating hours", required: false })
  operatingHours?: {
    type: string;
    startTime: string;
    endTime: string;
  }[];

  @ApiProperty({ description: "Last updated", required: false })
  lastUpdated?: string;
}

export class ParkOccupancyDto {
  @ApiProperty({ description: "Current occupancy" })
  current: number;

  @ApiProperty({ description: "Trend", enum: ["up", "stable", "down"] })
  trend: "up" | "stable" | "down";

  @ApiProperty({ description: "Compared to typical" })
  comparedToTypical: number;

  @ApiProperty({
    description: "Comparison status",
    enum: ["lower", "typical", "higher"],
  })
  comparisonStatus: "lower" | "typical" | "higher";

  @ApiProperty({ description: "Baseline 90th percentile" })
  baseline90thPercentile: number;

  @ApiProperty({ description: "Updated at" })
  updatedAt: string;

  @ApiProperty({ description: "Breakdown", required: false })
  breakdown?: {
    currentAvgWait: number;
    typicalAvgWait: number;
    activeAttractions: number;
  };
}

export class ParkStatisticsDto {
  @ApiProperty({ description: "Average wait time" })
  avgWaitTime: number;

  @ApiProperty({ description: "Average wait today" })
  avgWaitToday: number;

  @ApiProperty({ description: "Peak hour", nullable: true })
  peakHour: string | null;

  @ApiProperty({
    description: "Crowd level",
    enum: ["very_low", "low", "moderate", "high", "very_high"],
  })
  crowdLevel: "very_low" | "low" | "moderate" | "high" | "very_high";

  @ApiProperty({ description: "Total attractions count" })
  totalAttractions: number;

  @ApiProperty({ description: "Operating attractions count" })
  operatingAttractions: number;

  @ApiProperty({ description: "Closed attractions count" })
  closedAttractions: number;

  @ApiProperty({ description: "Timestamp" })
  timestamp: string;
}

export class ParkAnalyticsDto {
  @ApiProperty({ description: "Occupancy data", type: ParkOccupancyDto })
  occupancy: ParkOccupancyDto;

  @ApiProperty({ description: "Statistics data", type: ParkStatisticsDto })
  statistics: ParkStatisticsDto;

  @ApiProperty({ description: "Percentiles", required: false })
  percentiles?: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  };
}

/**
 * Park with Attractions Response DTO
 *
 * Used when returning park data with its attractions included.
 * Now includes integrated live data: weather, schedule, wait times, analytics.
 */
export class ParkWithAttractionsDto {
  @ApiProperty({ description: "Unique identifier of the park" })
  id: string;

  @ApiProperty({ description: "Name of the park" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({
    description: "Official website URL",
    required: false,
    nullable: true,
  })
  url: string | null;

  @ApiProperty({ description: "Country name", required: false, nullable: true })
  country: string | null;

  @ApiProperty({ description: "City name", required: false, nullable: true })
  city: string | null;

  @ApiProperty({ description: "Timezone identifier" })
  timezone: string;

  @ApiProperty({
    description: "Current operating status",
    enum: ["OPERATING", "CLOSED"],
    required: false,
  })
  status?: "OPERATING" | "CLOSED";

  @ApiProperty({
    description: "Latitude coordinate",
    required: false,
    nullable: true,
  })
  latitude: number | null;

  @ApiProperty({
    description: "Longitude coordinate",
    required: false,
    nullable: true,
  })
  longitude: number | null;

  @ApiProperty({
    description: "Continent name",
    required: false,
    nullable: true,
  })
  continent: string | null;

  // Live Data
  @ApiProperty({
    description: "Current crowd load rating",
    type: ParkLoadDto,
    required: false,
    nullable: true,
  })
  currentLoad?: ParkLoadDto | null;

  @ApiProperty({ description: "Weather information", required: false })
  weather?: {
    current: WeatherItemDto | null;
    forecast: WeatherItemDto[];
  };

  // Content
  @ApiProperty({
    description: "List of attractions with live wait times",
    type: [ParkAttractionDto],
    required: false,
  })
  attractions: ParkAttractionDto[];

  @ApiProperty({
    description: "List of shows",
    type: [ParkShowDto],
    required: false,
  })
  shows?: ParkShowDto[];

  @ApiProperty({
    description: "List of restaurants",
    type: [ParkRestaurantDto],
    required: false,
  })
  restaurants?: ParkRestaurantDto[];

  // Analytics & Planning
  @ApiProperty({
    description: "Park-wide analytics and occupancy",
    type: ParkAnalyticsDto,
    required: false,
    nullable: true,
  })
  analytics?: ParkAnalyticsDto | null;

  @ApiProperty({
    description: "Today's operating schedule",
    required: false,
    type: [ScheduleItemDto],
  })
  schedule?: ScheduleItemDto[];

  @ApiProperty({
    description: "Daily crowd forecast for the next days",
    required: false,
    type: [ParkDailyPredictionDto],
  })
  crowdForecast?: ParkDailyPredictionDto[];

  static fromEntity(park: Park): ParkWithAttractionsDto {
    return {
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),

      country: park.country || null,
      city: park.city || null,
      continent: park.continent || null,
      timezone: park.timezone,
      status: "CLOSED", // Default to ensure order
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,

      attractions: park.attractions
        ? park.attractions.map((attraction) => ({
            id: attraction.id,
            name: attraction.name,
            slug: attraction.slug,
            latitude:
              attraction.latitude !== undefined ? attraction.latitude : null,
            longitude:
              attraction.longitude !== undefined ? attraction.longitude : null,
            // queue data, forecasts etc will be attached by service
          }))
        : [],
      shows: park.shows
        ? park.shows.map((show) => ({
            id: show.id,
            name: show.name,
            slug: show.slug,
            status: "CLOSED", // Default to ensure order
            latitude: show.latitude !== undefined ? show.latitude : null,
            longitude: show.longitude !== undefined ? show.longitude : null,
          }))
        : [],
      restaurants: park.restaurants
        ? park.restaurants.map((restaurant) => ({
            id: restaurant.id,
            name: restaurant.name,
            slug: restaurant.slug,
            status: "CLOSED", // Default to ensure order
            latitude:
              restaurant.latitude !== undefined ? restaurant.latitude : null,
            longitude:
              restaurant.longitude !== undefined ? restaurant.longitude : null,
            cuisineType: restaurant.cuisineType || null,
            requiresReservation: restaurant.requiresReservation,
          }))
        : [],
    };
  }
}
