import { ApiProperty } from "@nestjs/swagger";

/**
 * Response DTO for GET /v1/parks/debug/missing-geocode
 * Returns parks with incomplete geocoding data
 */
export class MissingGeocodeResponseDto {
  @ApiProperty({ description: "Park ID" })
  id: string;

  @ApiProperty({ description: "Park name" })
  name: string;

  @ApiProperty({ description: "Park slug" })
  slug: string;

  @ApiProperty({ description: "Latitude coordinate", nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude coordinate", nullable: true })
  longitude: number | null;

  @ApiProperty({ description: "Continent name", nullable: true })
  continent: string | null;

  @ApiProperty({ description: "Continent slug", nullable: true })
  continentSlug: string | null;

  @ApiProperty({ description: "Country name", nullable: true })
  country: string | null;

  @ApiProperty({ description: "Country slug", nullable: true })
  countrySlug: string | null;

  @ApiProperty({ description: "City name", nullable: true })
  city: string | null;

  @ApiProperty({ description: "City slug", nullable: true })
  citySlug: string | null;

  @ApiProperty({
    description: "Geocoding attempt timestamp",
    nullable: true,
  })
  geocodingAttemptedAt: Date | null;

  @ApiProperty({
    description: "Missing fields indicator",
    example: {
      continent: false,
      country: true,
      city: false,
    },
  })
  missing: {
    continent: boolean;
    country: boolean;
    city: boolean;
  };
}
