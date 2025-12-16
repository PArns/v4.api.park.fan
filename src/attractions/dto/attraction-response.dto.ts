import { Attraction } from "../entities/attraction.entity";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { ForecastItemDto } from "../../queue-data/dto/forecast-response.dto";

/**
 * Attraction Response DTO
 *
 * Used for API responses when returning attraction data.
 * Now includes integrated live data: current queues, status, forecasts, ML predictions, statistics.
 */
export class AttractionResponseDto {
  id: string;
  name: string;
  slug: string;

  status?: string; // Overall status: OPERATING, DOWN, CLOSED, REFURBISHMENT

  // Live Data
  queues?: QueueDataItemDto[]; // Current wait times (all queue types)
  currentLoad?: {
    rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
    baseline: number;
    message?: string;
  } | null;

  predictions?: {
    predictedTime: string;
    predictedWaitTime: number;
    confidence: number;
    trend: string;
    modelVersion: string;
  }[]; // Our ML model predictions (hourly)

  forecasts?: ForecastItemDto[]; // ThemeParks.wiki predictions (next 24 hours)

  latitude: number | null;
  longitude: number | null;

  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
    continent: string | null;
    country: string | null;
    city: string | null;
  } | null;

  statistics?: {
    avgWaitToday: number | null;
    peakWaitToday: number | null;
    minWaitToday: number | null;
    typicalWaitThisHour: number | null;
    percentile95ThisHour: number | null;
    currentVsTypical: number | null;
    dataPoints: number;
    timestamp: string;
  } | null;

  // Prediction Accuracy (Feedback Loop)
  predictionAccuracy?: {
    badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
    last30Days: {
      mae: number; // Mean Absolute Error (minutes)
      mape: number; // Mean Absolute Percentage Error (%)
      rmse: number; // Root Mean Square Error (minutes)
      comparedPredictions: number; // How many predictions were compared
      totalPredictions: number; // Total predictions made
    };
    message?: string; // Optional explanation
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

      predictions: [],
      forecasts: [],
      statistics: null,
    };
  }
}
