import { ApiProperty } from "@nestjs/swagger";

/**
 * Park Reference DTO
 *
 * Minimal park information for route generation
 */
export class ParkReferenceDto {
  @ApiProperty({
    description: "Park UUID",
    example: "abc123-def456-ghi789",
  })
  id: string;

  @ApiProperty({
    description: "Park name",
    example: "Europa-Park",
  })
  name: string;

  @ApiProperty({
    description: "Park slug",
    example: "europa-park",
  })
  slug: string;

  @ApiProperty({
    description: "Full URL path to park",
    example: "/europe/germany/rust/europa-park",
  })
  url: string;
}

/**
 * City DTO
 *
 * Represents a city containing parks
 */
export class CityDto {
  @ApiProperty({
    description: "City name",
    example: "Rust",
  })
  name: string;

  @ApiProperty({
    description: "City slug",
    example: "rust",
  })
  slug: string;

  @ApiProperty({
    description: "Parks in this city",
    type: [ParkReferenceDto],
  })
  parks: ParkReferenceDto[];

  @ApiProperty({
    description: "Number of parks in this city",
    example: 2,
  })
  parkCount: number;
}

/**
 * Country DTO
 *
 * Represents a country containing cities
 */
export class CountryDto {
  @ApiProperty({
    description: "Country name",
    example: "Germany",
  })
  name: string;

  @ApiProperty({
    description: "Country slug",
    example: "germany",
  })
  slug: string;

  @ApiProperty({
    description: "ISO 3166-1 alpha-2 country code",
    example: "DE",
  })
  code: string;

  @ApiProperty({
    description: "Cities in this country",
    type: [CityDto],
  })
  cities: CityDto[];

  @ApiProperty({
    description: "Number of cities in this country",
    example: 5,
  })
  cityCount: number;

  @ApiProperty({
    description: "Total number of parks in this country",
    example: 12,
  })
  parkCount: number;
}

/**
 * Continent DTO
 *
 * Represents a continent containing countries
 */
export class ContinentDto {
  @ApiProperty({
    description: "Continent name",
    example: "Europe",
  })
  name: string;

  @ApiProperty({
    description: "Continent slug",
    example: "europe",
  })
  slug: string;

  @ApiProperty({
    description: "Countries in this continent",
    type: [CountryDto],
  })
  countries: CountryDto[];

  @ApiProperty({
    description: "Number of countries in this continent",
    example: 8,
  })
  countryCount: number;

  @ApiProperty({
    description: "Total number of parks in this continent",
    example: 45,
  })
  parkCount: number;
}

/**
 * Geo Structure Response DTO
 *
 * Complete hierarchical geographic structure for route generation
 */
export class GeoStructureDto {
  @ApiProperty({
    description: "All continents with nested countries, cities, and parks",
    type: [ContinentDto],
  })
  continents: ContinentDto[];

  @ApiProperty({
    description: "Total number of continents",
    example: 5,
  })
  continentCount: number;

  @ApiProperty({
    description: "Total number of countries",
    example: 32,
  })
  countryCount: number;

  @ApiProperty({
    description: "Total number of cities",
    example: 78,
  })
  cityCount: number;

  @ApiProperty({
    description: "Total number of parks",
    example: 156,
  })
  parkCount: number;

  @ApiProperty({
    description: "Timestamp when this structure was generated",
    example: "2024-01-15T10:00:00.000Z",
  })
  generatedAt: string;
}
