import { ApiProperty } from "@nestjs/swagger";

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
}
