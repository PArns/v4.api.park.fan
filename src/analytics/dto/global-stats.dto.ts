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

  @ApiProperty({ description: "Country slug for translation" })
  countrySlug: string;

  @ApiProperty({ description: "Average wait time in minutes", nullable: true })
  averageWaitTime: number;

  @ApiProperty({
    description: "Internal normalized URL path (geocoded)",
    example: "/united-states/orlando/magic-kingdom",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description: "Crowd level (e.g., 'high', 'low')",
    nullable: true,
  })
  crowdLevel: string | null;

  @ApiProperty({ description: "Total number of attractions in park" })
  totalAttractions: number;

  @ApiProperty({ description: "Number of currently operating attractions" })
  operatingAttractions: number;
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

  @ApiProperty({ description: "Country slug for translation" })
  parkCountrySlug: string;

  @ApiProperty()
  waitTime: number;

  @ApiProperty({
    description: "Internal normalized URL path",
    example: "/united-states/orlando/magic-kingdom/attractions/space-mountain",
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description: "Crowd level (e.g., 'high', 'low')",
    nullable: true,
  })
  crowdLevel: string | null;

  @ApiProperty({
    description: "Historical wait time data points for sparkline visualization",
    nullable: true,
    type: "array",
    items: {
      type: "object",
      properties: {
        timestamp: { type: "string" },
        waitTime: { type: "number" },
      },
    },
  })
  sparkline: { timestamp: string; waitTime: number }[] | null;
}

export class GlobalCountsDto {
  @ApiProperty({ description: "Number of parks currently operating" })
  openParks: number;

  @ApiProperty({ description: "Total number of parks in the system" })
  parks: number;

  @ApiProperty({ description: "Number of attractions currently operating" })
  openAttractions: number;

  @ApiProperty({ description: "Total number of attractions" })
  attractions: number;

  @ApiProperty({ description: "Total number of shows" })
  shows: number;

  @ApiProperty({ description: "Total number of restaurants" })
  restaurants: number;

  @ApiProperty({ description: "Total historical queue data records" })
  queueDataRecords: number;

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
}
