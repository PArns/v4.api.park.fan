import { ApiProperty } from "@nestjs/swagger";
import { ParkWithDistanceDto } from "../../common/dto/park-with-distance.dto";
import {
  CROWD_LEVEL_VALUES,
  CrowdLevel,
} from "../../common/types/crowd-level.type";

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
    description: "Current crowd/wait level for the attraction",
    enum: CROWD_LEVEL_VALUES,
    example: "moderate",
    required: false,
    nullable: true,
  })
  crowdLevel?: CrowdLevel | null;

  @ApiProperty({
    description:
      "Whether this is a headliner (top) attraction for this park, based on historical wait-time data",
    example: true,
  })
  isHeadliner: boolean;

  @ApiProperty({
    description: "Whether this attraction only operates during certain seasons",
    example: false,
  })
  isSeasonal: boolean;

  @ApiProperty({
    description:
      "Months (1–12) when this attraction typically operates. Null if not seasonal.",
    required: false,
    nullable: true,
    type: [Number],
  })
  seasonMonths?: number[] | null;

  @ApiProperty({
    description:
      "Whether the attraction is currently in its operating season. " +
      "Null for non-seasonal attractions (or seasonal ones with unknown months). " +
      "Rides that are definitively out of season (false) are omitted from the list.",
    required: false,
    nullable: true,
  })
  isCurrentlyInSeason?: boolean | null;

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
    description: "Whether the park provides official operating hours",
    example: true,
  })
  hasOperatingSchedule: boolean;

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

  @ApiProperty({
    description:
      "Every show of the park (not cut to any radius), nearest first; shows without coordinates last",
    type: () => [ShowWithDistanceDto],
  })
  shows: ShowWithDistanceDto[];
}

/**
 * A show of the park the visitor is in, with today's programme
 */
export class ShowWithDistanceDto {
  @ApiProperty({ description: "Show ID" })
  id: string;

  @ApiProperty({ description: "Show name" })
  name: string;

  @ApiProperty({ description: "Show slug" })
  slug: string;

  @ApiProperty({
    description:
      "Distance from the visitor in metres; null without coordinates",
    nullable: true,
    example: 240,
  })
  distance: number | null;

  @ApiProperty({
    description:
      "The show's newest reading while the park is OPERATING, else CLOSED. CLOSED also means no reading.",
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description:
      "Today's (park-local) performances, past ones included, as ISO 8601 instants",
    example: [{ startTime: "2026-08-29T10:30:00.000Z" }],
  })
  showtimes: Array<{ startTime: string }>;

  @ApiProperty({
    description: "Whether this show only runs in certain seasons",
  })
  isSeasonal: boolean;

  @ApiProperty({
    description:
      "Months (1–12) the show runs. Null if unknown or not seasonal.",
    nullable: true,
    type: [Number],
  })
  seasonMonths: number[] | null;

  @ApiProperty({
    description:
      "In season this park-local month. Null when not seasonal or months unknown.",
    nullable: true,
  })
  isCurrentlyInSeason: boolean | null;

  @ApiProperty({
    description:
      "API path of the show's park; shows have no route of their own",
    nullable: true,
  })
  url: string | null;
}

// ParkWithDistanceDto is now imported from common/dto/park-with-distance.dto

/**
 * The park a short-wait ride stands in.
 */
export class NearbyShortWaitParkDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "Park slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Distance from the user to the park in meters",
    example: 19000,
  })
  distance: number;

  @ApiProperty({
    description: "Frontend URL to the park",
    example: "/europe/germany/bruehl/phantasialand",
    nullable: true,
  })
  url: string | null;
}

/**
 * A ride in one of the nearby parks whose live wait is well below its usual
 * one, as `nearby_parks` lists it under `shortWaits`.
 */
export class NearbyShortWaitDto {
  @ApiProperty({ description: "Attraction ID" })
  id: string;

  @ApiProperty({ description: "Attraction name", example: "Taron" })
  name: string;

  @ApiProperty({ description: "Attraction slug", example: "taron" })
  slug: string;

  @ApiProperty({
    description: "Frontend URL to the attraction",
    example: "/europe/germany/bruehl/phantasialand/taron",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description: "Live standby wait in minutes, rounded to 5 (13 stays 13)",
    example: 15,
  })
  waitTime: number;

  @ApiProperty({
    description:
      "The wait the live one is rated against (the ride's P50, P90 when it has " +
      "none), rounded to 5. Never below 15 and always at least 10 above `waitTime`.",
    example: 40,
  })
  typicalWait: number;

  @ApiProperty({
    description: "Whether this is one of the park's headliners",
    example: true,
  })
  isHeadliner: boolean;

  @ApiProperty({ description: "The park the ride stands in" })
  park: NearbyShortWaitParkDto;
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

  @ApiProperty({
    description:
      "Rides in the open parks of `parks` whose live wait is at most 60 % of " +
      "their usual one and at least 10 minutes below it, the biggest gap first, " +
      "at most three per park and five in all. Empty when nothing qualifies; a " +
      "park whose live payload was not in the cache adds nothing.",
    type: [NearbyShortWaitDto],
  })
  shortWaits: NearbyShortWaitDto[];
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
