import { ApiProperty } from "@nestjs/swagger";

/**
 * User location coordinates
 */
export class UserLocationDto {
  @ApiProperty({ description: "User latitude", example: 48.266 })
  latitude: number;

  @ApiProperty({ description: "User longitude", example: 7.722 })
  longitude: number;
}

/**
 * Ride with distance from user
 */
export class RideWithDistanceDto {
  @ApiProperty({ description: "Attraction ID" })
  id: string;

  @ApiProperty({ description: "Attraction name", example: "Blue Fire" })
  name: string;

  @ApiProperty({ description: "Attraction slug", example: "blue-fire" })
  slug: string;

  @ApiProperty({
    description: "Distance from user in meters",
    example: 250,
  })
  distance: number;

  @ApiProperty({
    description: "Current wait time in minutes",
    example: 35,
    nullable: true,
  })
  waitTime: number | null;

  @ApiProperty({
    description: "Attraction status",
    example: "OPERATING",
    enum: ["OPERATING", "CLOSED", "DOWN"],
  })
  status: string;

  @ApiProperty({
    description: "Analytics data for the ride",
    required: false,
    nullable: true,
  })
  analytics?: {
    p50?: number;
    p90?: number;
    avgWaitToday?: number;
  };

  @ApiProperty({
    description: "Frontend URL to attraction",
    example: "/europe/germany/rust/europa-park/blue-fire",
  })
  url: string;
}

/**
 * Park information when user is inside
 */
export class NearbyParkInfoDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name", example: "Europa-Park" })
  name: string;

  @ApiProperty({ description: "Park slug", example: "europa-park" })
  slug: string;

  @ApiProperty({
    description: "Distance from user to park center in meters",
    example: 150,
  })
  distance: number;

  @ApiProperty({
    description: "Park operating status",
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description: "Park analytics",
    required: false,
    nullable: true,
  })
  analytics?: {
    avgWaitTime?: number;
    crowdLevel?: string;
    operatingAttractions?: number;
  };

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
  };
}

/**
 * Response when user is inside a park
 */
export class NearbyRidesDto {
  @ApiProperty({ description: "Park information" })
  park: NearbyParkInfoDto;

  @ApiProperty({
    description: "List of rides sorted by distance",
    type: [RideWithDistanceDto],
  })
  rides: RideWithDistanceDto[];
}

/**
 * Park with distance from user (for nearby parks response)
 */
export class ParkWithDistanceDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "Park slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Distance from user in meters",
    example: 15000,
  })
  distance: number;

  @ApiProperty({ description: "City name", example: "Brühl" })
  city: string;

  @ApiProperty({ description: "Country name", example: "Germany" })
  country: string;

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
  })
  url: string;

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
  };
}

/**
 * Response when user is outside all parks
 */
export class NearbyParksDto {
  @ApiProperty({
    description: "List of nearby parks (max 5)",
    type: [ParkWithDistanceDto],
  })
  parks: ParkWithDistanceDto[];

  @ApiProperty({
    description: "Total count of parks found",
    example: 5,
  })
  count: number;
}

/**
 * Main response DTO for nearby endpoint
 */
export class NearbyResponseDto {
  @ApiProperty({
    description: "Response type",
    enum: ["in_park", "nearby_parks"],
    example: "in_park",
  })
  type: "in_park" | "nearby_parks";

  @ApiProperty({ description: "User location coordinates" })
  userLocation: UserLocationDto;

  @ApiProperty({
    description: "Response data (rides if in park, parks if outside)",
    oneOf: [
      { $ref: "#/components/schemas/NearbyRidesDto" },
      { $ref: "#/components/schemas/NearbyParksDto" },
    ],
  })
  data: NearbyRidesDto | NearbyParksDto;
}
