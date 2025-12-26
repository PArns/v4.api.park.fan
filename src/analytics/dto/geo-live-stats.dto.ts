import { ApiProperty } from "@nestjs/swagger";

/**
 * City-level live statistics
 */
export class CityLiveStatsDto {
  @ApiProperty({
    description: "City slug",
    example: "orlando",
  })
  slug: string;

  @ApiProperty({
    description: "Number of currently open parks",
    example: 5,
  })
  openParkCount: number;

  @ApiProperty({
    description: "Average wait time across all attractions (minutes)",
    example: 45,
    nullable: true,
  })
  averageWaitTime: number | null;
}

/**
 * Country-level live statistics
 */
export class CountryLiveStatsDto {
  @ApiProperty({
    description: "Country slug",
    example: "united-states",
  })
  slug: string;

  @ApiProperty({
    description: "Number of currently open parks",
    example: 15,
  })
  openParkCount: number;

  @ApiProperty({
    description: "Average wait time across all attractions (minutes)",
    example: 42,
    nullable: true,
  })
  averageWaitTime: number | null;

  @ApiProperty({
    description: "City-level statistics",
    type: [CityLiveStatsDto],
  })
  cities: CityLiveStatsDto[];
}

/**
 * Continent-level live statistics
 */
export class ContinentLiveStatsDto {
  @ApiProperty({
    description: "Continent slug",
    example: "north-america",
  })
  slug: string;

  @ApiProperty({
    description: "Number of currently open parks",
    example: 25,
  })
  openParkCount: number;

  @ApiProperty({
    description: "Average wait time across all attractions (minutes)",
    example: 38,
    nullable: true,
  })
  averageWaitTime: number | null;

  @ApiProperty({
    description: "Country-level statistics",
    type: [CountryLiveStatsDto],
  })
  countries: CountryLiveStatsDto[];
}

/**
 * Global geographic live statistics
 */
export class GeoLiveStatsDto {
  @ApiProperty({
    description: "Continent-level live statistics",
    type: [ContinentLiveStatsDto],
  })
  continents: ContinentLiveStatsDto[];

  @ApiProperty({
    description: "Timestamp when statistics were calculated",
    example: "2025-12-26T15:30:00.000Z",
  })
  generatedAt: string;
}
