import { ApiProperty } from "@nestjs/swagger";

export class SearchResultItemDto {
  @ApiProperty({
    description: "Entity type",
    enum: ["park", "attraction", "show", "restaurant"],
    example: "park",
  })
  type: "park" | "attraction" | "show" | "restaurant";

  @ApiProperty({ description: "Entity ID", example: "uuid-here" })
  id: string;

  @ApiProperty({ description: "Entity name", example: "Disneyland Park" })
  name: string;

  @ApiProperty({ description: "Entity slug", example: "disneyland-park" })
  slug: string;

  @ApiProperty({
    description: "Geocoded URL path (if available)",
    example: "/v1/parks/europe/france/paris/disneyland-park",
    required: false,
  })
  url?: string | null;

  // Location metadata
  @ApiProperty({
    description: "Continent name",
    example: "Europe",
    required: false,
  })
  continent?: string | null;

  @ApiProperty({
    description: "Country name",
    example: "France",
    required: false,
  })
  country?: string | null;

  @ApiProperty({
    description: "ISO country code",
    example: "FR",
    required: false,
  })
  countryCode?: string | null;

  @ApiProperty({ description: "City name", example: "Paris", required: false })
  city?: string | null;

  @ApiProperty({
    description: "Resort/Destination name",
    example: "Disneyland Paris",
    required: false,
  })
  resort?: string | null;

  // Park-specific metadata
  @ApiProperty({
    description: "Park operating status (parks only)",
    enum: ["OPERATING", "CLOSED"],
    example: "OPERATING",
    required: false,
  })
  status?: "OPERATING" | "CLOSED" | null;

  @ApiProperty({
    description: "Current crowd level (parks only)",
    enum: ["very_low", "low", "normal", "higher", "high", "extreme"],
    example: "normal",
    required: false,
  })
  load?: "very_low" | "low" | "normal" | "higher" | "high" | "extreme" | null;

  // Attraction-specific metadata
  @ApiProperty({
    description: "Parent park information (attractions only)",
    required: false,
    example: {
      id: "uuid",
      name: "Europa-Park",
      slug: "europa-park",
      url: "/v1/parks/europe/germany/rust/europa-park",
    },
  })
  parentPark?: {
    id: string;
    name: string;
    slug: string;
    url: string | null;
  } | null;
}

export class SearchResultDto {
  @ApiProperty({
    description: "List of matching entities",
    type: [SearchResultItemDto],
  })
  results: SearchResultItemDto[];

  @ApiProperty({ description: "Total number of matches" })
  total: number;

  @ApiProperty({ description: "Original search query" })
  query: string;

  @ApiProperty({ description: "Types filtered by" })
  searchTypes: string[];
}
