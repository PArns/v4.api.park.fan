import { ApiProperty } from "@nestjs/swagger";

/**
 * Park with distance DTO
 *
 * Used for API responses when returning park data with distance information.
 * Supports both required distance (nearby) and optional distance (favorites).
 */
export class ParkWithDistanceDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "Park slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Distance from user location in meters",
    example: 15000,
    required: false,
    nullable: true,
  })
  distance?: number | null;

  @ApiProperty({ description: "City name", example: "Brühl", nullable: true })
  city: string | null;

  @ApiProperty({
    description: "Country name",
    example: "Germany",
    nullable: true,
  })
  country: string | null;

  @ApiProperty({
    description: "Park operating status",
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description: "Total number of attractions",
    example: 35,
  })
  totalAttractions: number;

  @ApiProperty({
    description: "Number of operating attractions",
    example: 28,
  })
  operatingAttractions: number;

  @ApiProperty({
    description: "Park analytics",
    required: false,
    nullable: true,
  })
  analytics?: {
    avgWaitTime?: number;
    crowdLevel?: string;
    occupancy?: number;
  };

  @ApiProperty({
    description: "Frontend URL to park",
    example: "/europe/germany/bruehl/phantasialand",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({ description: "Park timezone", example: "Europe/Berlin" })
  timezone: string;

  @ApiProperty({
    description: "Today's operating schedule",
    required: false,
    nullable: true,
  })
  todaySchedule?: {
    openingTime: string;
    closingTime: string;
    scheduleType: string;
  } | null;

  @ApiProperty({
    description: "Next scheduled opening day",
    required: false,
    nullable: true,
  })
  nextSchedule?: {
    openingTime: string;
    closingTime: string;
    scheduleType: string;
  } | null;
}
