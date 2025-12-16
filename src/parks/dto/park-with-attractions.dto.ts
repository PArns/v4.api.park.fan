import { Park } from "../entities/park.entity";
import { WeatherItemDto } from "./weather-item.dto";
import { ScheduleItemDto } from "./schedule-item.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { buildParkUrl } from "../../common/utils/url.util";

import { ParkDailyPredictionDto } from "./park-daily-prediction.dto";

/**
 * Park with Attractions Response DTO
 *
 * Used when returning park data with its attractions included.
 * Now includes integrated live data: weather, schedule, wait times, analytics.
 */
export class ParkWithAttractionsDto {
  id: string;
  name: string;
  slug: string;
  url: string | null;
  dates?: ParkDailyPredictionDto[]; // Daily ML predictions
  timezone: string;
  latitude: number | null;
  longitude: number | null;
  continent: string | null;
  country: string | null;
  city: string | null;
  // Destination field removed - was redundant with park name/slug
  status?: "OPERATING" | "CLOSED";
  currentLoad?: {
    rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
    baseline: number;
    current: number;
  } | null;
  attractions: {
    id: string;
    name: string;
    slug: string;
    latitude: number | null;
    longitude: number | null;
    queues?: QueueDataItemDto[]; // Current wait times for all queue types
    status?: string; // OPERATING, DOWN, CLOSED, REFURBISHMENT
    predictions?: {
      predictedTime: string;
      predictedWaitTime: number;
      confidencePercentage: number | null;
    }[];
    predictionAccuracy?: {
      badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
      last30Days: {
        mae: number;
        mape: number;
        rmse: number;
        comparedPredictions: number;
        totalPredictions: number;
      };
      message?: string;
    } | null;
    currentLoad?: {
      rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
      baseline: number;
      current: number;
    } | null;
  }[];

  // Phase 6: Shows & Restaurants
  shows?: {
    id: string;
    name: string;
    slug: string;
    latitude: number | null;
    longitude: number | null;
    status?: string;
    showtimes?: {
      type: string;
      startTime: string;
      endTime?: string;
    }[];
    operatingHours?: {
      type: string;
      startTime: string;
      endTime: string;
    }[];
    lastUpdated?: string;
  }[];

  restaurants?: {
    id: string;
    name: string;
    slug: string;
    latitude: number | null;
    longitude: number | null;
    cuisineType: string | null;
    requiresReservation: boolean;
    status?: string;
    waitTime?: number | null;
    partySize?: number | null;
    operatingHours?: {
      type: string;
      startTime: string;
      endTime: string;
    }[];
    lastUpdated?: string;
  }[];

  // Integrated live data
  weather?: {
    current: WeatherItemDto | null; // Today's weather
    forecast: WeatherItemDto[]; // 16-day forecast
  };
  schedule?: ScheduleItemDto[]; // Today's schedule only

  // Analytics (Phase 4: with optional percentiles)
  analytics?: {
    occupancy: {
      current: number;
      trend: "up" | "stable" | "down";
      comparedToTypical: number;
      comparisonStatus: "lower" | "typical" | "higher";
      baseline90thPercentile: number;
      updatedAt: string;
      breakdown?: {
        currentAvgWait: number;
        typicalAvgWait: number;
        activeAttractions: number;
      };
    };
    statistics: {
      avgWaitTime: number;
      avgWaitToday: number;
      peakHour: string | null;
      crowdLevel: "very_low" | "low" | "moderate" | "high" | "very_high";
      totalAttractions: number;
      operatingAttractions: number;
      closedAttractions: number;
      timestamp: string;
    };
    percentiles?: {
      p50: number;
      p75: number;
      p90: number;
      p95: number;
    };
  } | null;

  createdAt: Date;
  updatedAt: Date;

  static fromEntity(park: Park): ParkWithAttractionsDto {
    return {
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),
      timezone: park.timezone,
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,
      continent: park.continent || null,
      country: park.country || null,
      city: park.city || null,
      // Destination field removed
      attractions: park.attractions
        ? park.attractions.map((attraction) => ({
            id: attraction.id,
            name: attraction.name,
            slug: attraction.slug,
            latitude:
              attraction.latitude !== undefined ? attraction.latitude : null,
            longitude:
              attraction.longitude !== undefined ? attraction.longitude : null,
          }))
        : [],
      shows: park.shows
        ? park.shows.map((show) => ({
            id: show.id,
            name: show.name,
            slug: show.slug,
            latitude: show.latitude !== undefined ? show.latitude : null,
            longitude: show.longitude !== undefined ? show.longitude : null,
          }))
        : [],
      restaurants: park.restaurants
        ? park.restaurants.map((restaurant) => ({
            id: restaurant.id,
            name: restaurant.name,
            slug: restaurant.slug,
            latitude:
              restaurant.latitude !== undefined ? restaurant.latitude : null,
            longitude:
              restaurant.longitude !== undefined ? restaurant.longitude : null,
            cuisineType: restaurant.cuisineType || null,
            requiresReservation: restaurant.requiresReservation,
          }))
        : [],
      createdAt: park.createdAt,
      updatedAt: park.updatedAt,
    };
  }
}
