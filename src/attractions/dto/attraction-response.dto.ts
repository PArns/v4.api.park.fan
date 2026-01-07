import { ApiProperty } from "@nestjs/swagger";
import { Attraction } from "../entities/attraction.entity";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { ForecastItemDto } from "../../queue-data/dto/forecast-response.dto";
import { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * Attraction Response DTO
 *
 * Used for API responses when returning attraction data.
 * Now includes integrated live data: current queues, status, forecasts, ML predictions, statistics.
 */
export class AttractionResponseDto {
  @ApiProperty({ description: "Unique identifier of the attraction" })
  id: string;

  @ApiProperty({ description: "Name of the attraction" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({ description: "Current status", required: false })
  status?: string; // Overall status: OPERATING, DOWN, CLOSED, REFURBISHMENT

  @ApiProperty({
    description: "Effective status (considering park status)",
    required: false,
  })
  effectiveStatus?: string;

  @ApiProperty({
    description:
      "Themed land name (e.g. 'The Wizarding World of Harry Potter')",
    required: false,
    nullable: true,
  })
  land?: string | null;

  // Live Data
  @ApiProperty({
    description: "Current wait times (all queue types)",
    required: false,
    type: [QueueDataItemDto],
  })
  queues?: QueueDataItemDto[];

  @ApiProperty({
    description: "Hourly ML predictions (internal model)",
    required: false,
  })
  hourlyForecast?: {
    predictedTime: string;
    predictedWaitTime: number;
    confidence: number;
    trend: string;
  }[];

  @ApiProperty({
    description: "External forecasts (e.g. ThemeParks.wiki)",
    required: false,
    type: [ForecastItemDto],
  })
  forecasts?: ForecastItemDto[];

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
    description: "Parent park details",
    required: false,
    nullable: true,
  })
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
    continent: string | null;
    country: string | null;
    city: string | null;
  } | null;

  @ApiProperty({
    description: "Frontend URL to attraction",
    nullable: true,
    required: false,
  })
  url?: string | null;

  @ApiProperty({
    description: "Attraction statistics",
    required: false,
    nullable: true,
  })
  statistics?: {
    avgWaitToday: number | null;
    peakWaitToday: number | null;
    peakWaitTimestamp: string | null;
    minWaitToday: number | null;
    typicalWaitThisHour: number | null;
    percentile95ThisHour: number | null;
    currentVsTypical: number | null;
    dataPoints: number;
    history: {
      timestamp: string;
      waitTime: number;
    }[];
    timestamp: string;
  } | null;

  @ApiProperty({
    description: "Wait time trend direction (up/down/stable)",
    enum: ["up", "down", "stable"],
    required: false,
    nullable: true,
  })
  trend?: "up" | "down" | "stable" | null;

  @ApiProperty({
    description: "Current crowd level badge",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "closed",
    ],
    required: false,
    nullable: true,
  })
  crowdLevel?: CrowdLevel | "closed" | null;

  // Prediction Accuracy (Feedback Loop)
  @ApiProperty({
    description: "Prediction accuracy metrics",
    required: false,
    nullable: true,
  })
  predictionAccuracy?: {
    badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
    last30Days: {
      comparedPredictions: number;
      totalPredictions: number;
    };
    message?: string;
  } | null;

  static fromEntity(attraction: Attraction): AttractionResponseDto {
    return {
      id: attraction.id,
      name: attraction.name,
      slug: attraction.slug,

      status: "CLOSED", // Default

      latitude: attraction.latitude !== undefined ? attraction.latitude : null,
      longitude:
        attraction.longitude !== undefined ? attraction.longitude : null,

      park: attraction.park
        ? {
            id: attraction.park.id,
            name: attraction.park.name,
            slug: attraction.park.slug,
            timezone: attraction.park.timezone,
            continent: attraction.park.continent || null,
            country: attraction.park.country || null,
            city: attraction.park.city || null,
          }
        : null,

      land: attraction.landName || null,

      hourlyForecast: [],
      forecasts: [],
      statistics: null,
    };
  }
}
