import { ApiProperty } from "@nestjs/swagger";
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
  @ApiProperty({
    description: "Unique identifier of the park",
    example: "ebf830...",
  })
  id: string;

  @ApiProperty({ description: "Name of the park", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug", example: "phantasialand" })
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

  @ApiProperty({ description: "Region name", required: false, nullable: true })
  region: string | null;

  @ApiProperty({ description: "Region code", required: false, nullable: true })
  regionCode: string | null;

  @ApiProperty({
    description: "Continent name",
    required: false,
    nullable: true,
  })
  continent: string | null;

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

  @ApiProperty({ description: "Timezone identifier", example: "Europe/Berlin" })
  timezone: string;

  @ApiProperty({
    description: "Current operating status",
    enum: ["OPERATING", "CLOSED"],
  })
  status: "OPERATING" | "CLOSED";

  // Analytics / Live Data
  @ApiProperty({
    description: "Current load rating",
    required: false,
    nullable: true,
  })
  currentLoad?: {
    crowdLevel: "very_low" | "low" | "normal" | "higher" | "high" | "extreme";
    baseline: number;
    currentWaitTime: number;
  } | null;

  @ApiProperty({
    description: "Weather information (current and forecast)",
    required: false,
  })
  weather?: {
    current: WeatherItemDto | null;
    forecast: WeatherItemDto[];
  };

  @ApiProperty({
    description: "Real-time analytics and statistics",
    required: false,
    nullable: true,
  })
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
  @ApiProperty({
    description: "Operating schedule",
    required: false,
    type: [ScheduleItemDto],
  })
  schedule?: ScheduleItemDto[];

  static fromEntity(park: Park): ParkResponseDto {
    return {
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),

      country: park.country || null,
      city: park.city || null,
      region: park.region || null,
      regionCode: park.regionCode || null,
      continent: park.continent || null,
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,
      timezone: park.timezone,

      status: "CLOSED", // Default, overwritten by service
    };
  }
}
