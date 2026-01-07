import { ApiProperty } from "@nestjs/swagger";
import { AttractionResponseDto } from "../../attractions/dto/attraction-response.dto";

/**
 * Simplified park with distance (similar to nearby endpoint)
 */
export class ParkWithDistanceDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "Park slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Distance from user location in meters (if lat/lng provided)",
    example: 15000,
    required: false,
    nullable: true,
  })
  distance?: number | null;

  @ApiProperty({ description: "City name", example: "Brühl" })
  city: string | null;

  @ApiProperty({ description: "Country name", example: "Germany" })
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
  };

  @ApiProperty({
    description: "Next scheduled opening day",
    required: false,
    nullable: true,
  })
  nextSchedule?: {
    openingTime: string;
    closingTime: string;
    scheduleType: string;
  };
}

/**
 * Attraction with distance (extends full AttractionResponseDto)
 */
export class AttractionWithDistanceDto extends AttractionResponseDto {
  @ApiProperty({
    description: "Distance from user location in meters (if lat/lng provided)",
    example: 250,
    required: false,
    nullable: true,
  })
  distance?: number | null;

  @ApiProperty({
    description:
      "Overall trend direction extracted from primary queue (up/down/stable)",
    example: "up",
    enum: ["up", "down", "stable"],
    required: false,
    nullable: true,
  })
  trend?: "up" | "down" | "stable" | null;
}

/**
 * Simplified show with distance
 */
export class ShowWithDistanceDto {
  @ApiProperty({ description: "Show ID" })
  id: string;

  @ApiProperty({
    description: "Show name",
    example: "Festival of the Lion King",
  })
  name: string;

  @ApiProperty({
    description: "Show slug",
    example: "festival-of-the-lion-king",
  })
  slug: string;

  @ApiProperty({
    description: "Distance from user location in meters (if lat/lng provided)",
    example: 500,
    required: false,
    nullable: true,
  })
  distance?: number | null;

  @ApiProperty({
    description: "Current operating status",
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description: "Upcoming showtimes",
    required: false,
    nullable: true,
  })
  showtimes: Array<{
    type: string;
    startTime: string;
    endTime?: string;
  }> | null;

  @ApiProperty({
    description: "Frontend URL to show",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description: "Parent park details",
    required: false,
    nullable: true,
  })
  park?: {
    id: string;
    name: string;
    slug: string;
  } | null;
}

/**
 * Simplified restaurant with distance
 */
export class RestaurantWithDistanceDto {
  @ApiProperty({ description: "Restaurant ID" })
  id: string;

  @ApiProperty({ description: "Restaurant name", example: "Be Our Guest" })
  name: string;

  @ApiProperty({ description: "Restaurant slug", example: "be-our-guest" })
  slug: string;

  @ApiProperty({
    description: "Distance from user location in meters (if lat/lng provided)",
    example: 300,
    required: false,
    nullable: true,
  })
  distance?: number | null;

  @ApiProperty({
    description: "Current operating status",
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description: "Current wait time in minutes",
    required: false,
    nullable: true,
  })
  waitTime: number | null;

  @ApiProperty({
    description: "Cuisine type",
    required: false,
    nullable: true,
  })
  cuisineType: string | null;

  @ApiProperty({
    description: "Frontend URL to restaurant",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description: "Parent park details",
    required: false,
    nullable: true,
  })
  park?: {
    id: string;
    name: string;
    slug: string;
  } | null;
}

/**
 * Favorites Response DTO
 *
 * Returns grouped favorite data with all information.
 */
export class FavoritesResponseDto {
  @ApiProperty({
    description: "Favorite parks with full information",
    type: [ParkWithDistanceDto],
  })
  parks: ParkWithDistanceDto[];

  @ApiProperty({
    description: "Favorite attractions (rides) with full information",
    type: [AttractionWithDistanceDto],
  })
  attractions: AttractionWithDistanceDto[];

  @ApiProperty({
    description: "Favorite shows with full information",
    type: [ShowWithDistanceDto],
  })
  shows: ShowWithDistanceDto[];

  @ApiProperty({
    description: "Favorite restaurants with full information",
    type: [RestaurantWithDistanceDto],
  })
  restaurants: RestaurantWithDistanceDto[];

  @ApiProperty({
    description: "User location (if provided)",
    required: false,
    nullable: true,
  })
  userLocation?: {
    latitude: number;
    longitude: number;
  } | null;
}
