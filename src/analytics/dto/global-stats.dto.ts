import { ApiProperty } from "@nestjs/swagger";

export class ParkStatsItemDto {
  @ApiProperty()
  id: string;

  @ApiProperty()
  name: string;

  @ApiProperty()
  slug: string;

  @ApiProperty({ description: "City where the park is located" })
  city: string;

  @ApiProperty({ description: "Country where the park is located" })
  country: string;

  @ApiProperty({ description: "Average wait time in minutes", nullable: true })
  averageWaitTime: number;

  @ApiProperty({
    description: "Internal normalized URL path (geocoded)",
    example: "/united-states/orlando/magic-kingdom",
  })
  url: string;

  @ApiProperty({
    description: "Crowd level (e.g., 'high', 'low')",
    nullable: true,
  })
  crowdLevel: string | null;

  @ApiProperty({
    description: "Occupancy percentage (0-200+)",
    example: 85,
    nullable: true,
  })
  occupancy: number | null;

  @ApiProperty({
    description: "Comparison to typical levels",
    enum: ["higher", "lower", "typical"],
    nullable: true,
  })
  comparedToTypical: string | null;

  @ApiProperty({ description: "Total number of attractions in park" })
  totalAttractions: number;

  @ApiProperty({ description: "Number of currently operating attractions" })
  operatingAttractions: number;

  @ApiProperty({ description: "Number of currently closed attractions" })
  closedAttractions: number;
}

export class AttractionStatsItemDto {
  @ApiProperty()
  id: string;

  @ApiProperty()
  name: string;

  @ApiProperty()
  slug: string;

  @ApiProperty()
  parkName: string;

  @ApiProperty()
  parkSlug: string;

  @ApiProperty({ description: "City where the park is located" })
  parkCity: string;

  @ApiProperty({ description: "Country where the park is located" })
  parkCountry: string;

  @ApiProperty()
  waitTime: number;

  @ApiProperty({
    description: "Internal normalized URL path",
    example: "/united-states/orlando/magic-kingdom/attractions/space-mountain",
  })
  url: string;

  @ApiProperty({
    description: "Crowd level (e.g., 'high', 'low')",
    nullable: true,
  })
  crowdLevel: string | null;

  @ApiProperty({
    description: "90th percentile baseline for this hour/day",
    nullable: true,
  })
  baseline: number | null;

  @ApiProperty({
    description:
      "How current wait compares to baseline (e.g., 'busier', 'quieter', 'typical')",
    nullable: true,
  })
  comparison: string | null;
}

export class GlobalCountsDto {
  @ApiProperty({ description: "Number of parks currently operating" })
  openParks: number;

  @ApiProperty({ description: "Number of parks currently closed" })
  closedParks: number;

  @ApiProperty({ description: "Total number of parks in the system" })
  parks: number;

  @ApiProperty({ description: "Percentage of parks currently open" })
  parksOpenPercentage: number;

  @ApiProperty({ description: "Number of attractions currently operating" })
  openAttractions: number;

  @ApiProperty({ description: "Number of attractions currently closed" })
  closedAttractions: number;

  @ApiProperty({ description: "Total number of attractions" })
  attractions: number;

  @ApiProperty({ description: "Percentage of attractions currently open" })
  attractionsOpenPercentage: number;

  @ApiProperty({ description: "Total number of shows" })
  shows: number;

  @ApiProperty({ description: "Total number of restaurants" })
  restaurants: number;

  @ApiProperty({ description: "Total historical queue data records" })
  queueDataRecords: number;

  @ApiProperty({ description: "Total historical weather data records" })
  weatherDataRecords: number;

  @ApiProperty({ description: "Total park schedule entries" })
  scheduleEntries: number;

  @ApiProperty({
    description: "Total restaurant live data records (including menus/hours)",
  })
  restaurantLiveDataRecords: number;

  @ApiProperty({
    description: "Total show live data records (including showtimes)",
  })
  showLiveDataRecords: number;

  @ApiProperty({ description: "Total wait time prediction records" })
  waitTimePredictions: number;

  @ApiProperty({
    description:
      "Sum of wait times of all currently operating attractions (minutes)",
  })
  totalWaitTime: number;
}

export class GlobalStatsDto {
  @ApiProperty({ type: GlobalCountsDto })
  counts: GlobalCountsDto;

  @ApiProperty({ type: ParkStatsItemDto, nullable: true })
  mostCrowdedPark: ParkStatsItemDto | null;

  @ApiProperty({ type: ParkStatsItemDto, nullable: true })
  leastCrowdedPark: ParkStatsItemDto | null;

  @ApiProperty({ type: AttractionStatsItemDto, nullable: true })
  longestWaitRide: AttractionStatsItemDto | null;

  @ApiProperty({ type: AttractionStatsItemDto, nullable: true })
  shortestWaitRide: AttractionStatsItemDto | null;

  @ApiProperty({ description: "Last updated timestamp" })
  lastUpdated: string;
}
