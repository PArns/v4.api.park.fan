import { Park } from "../entities/park.entity";
import { buildParkUrl } from "../../common/utils/url.util";
import { WeatherItemDto } from "./weather-item.dto";
import { ScheduleItemDto } from "./schedule-item.dto";

/**
 * Park Response DTO
 *
 * Used for API responses when returning park data.
 */
export class ParkResponseDto {
  id: string;
  name: string;
  slug: string;
  url: string | null;

  country: string | null;
  city: string | null;
  continent: string | null;
  latitude: number | null;
  longitude: number | null;
  timezone: string;

  status: "OPERATING" | "CLOSED";

  // Analytics / Live Data
  currentLoad?: {
    rating: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
    baseline: number;
    current: number;
  } | null;

  weather?: {
    current: WeatherItemDto | null;
    forecast: WeatherItemDto[];
  };

  analytics?: {
    occupancy: {
      current: number;
      trend: "up" | "stable" | "down";
      comparedToTypical: number;
      comparisonStatus: "lower" | "typical" | "higher";
      baseline90thPercentile: number;
      updatedAt: string;
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
  } | null;

  // Additional Data
  schedule?: ScheduleItemDto[];

  static fromEntity(park: Park): ParkResponseDto {
    return {
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),

      country: park.country || null,
      city: park.city || null,
      continent: park.continent || null,
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,
      timezone: park.timezone,

      status: "CLOSED", // Default, overwritten by service
    };
  }
}
