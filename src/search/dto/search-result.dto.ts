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

  // Geographic coordinates
  @ApiProperty({
    description: "Latitude coordinate",
    example: 48.8674,
    required: false,
  })
  latitude?: number | null;

  @ApiProperty({
    description: "Longitude coordinate",
    example: 2.7835,
    required: false,
  })
  longitude?: number | null;

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
    description: "2-character ISO country code (ISO 3166-1 alpha-2)",
    example: "FR",
    required: false,
  })
  countryCode?: string | null;

  @ApiProperty({
    description: "City name where the entity is located",
    example: "Paris",
    required: false,
  })
  city?: string | null;

  @ApiProperty({
    description: "Resort/Destination name (theme park complex)",
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

  @ApiProperty({
    description: "Today's operating hours (parks only)",
    example: {
      open: "2024-12-25T09:00:00.000Z",
      close: "2024-12-25T22:00:00.000Z",
      type: "OPERATING",
    },
    required: false,
  })
  parkHours?: {
    open: string;
    close: string;
    type: string;
  } | null;

  // Attraction-specific metadata
  @ApiProperty({
    description: "Current operating status (attractions, parks)",
    enum: ["OPERATING", "CLOSED", "DOWN", "REFURBISHMENT"],
    example: "OPERATING",
    required: false,
  })
  status?: "OPERATING" | "CLOSED" | "DOWN" | "REFURBISHMENT" | null;

  @ApiProperty({
    description: "Current crowd/wait level (attractions, parks)",
    enum: ["very_low", "low", "normal", "higher", "high", "extreme"],
    example: "normal",
    required: false,
  })
  load?: "very_low" | "low" | "normal" | "higher" | "high" | "extreme" | null;

  @ApiProperty({
    description: "Current wait time in minutes (attractions only)",
    example: 45,
    required: false,
  })
  waitTime?: number | null;

  // Show-specific metadata
  @ApiProperty({
    description: "Today's show times (shows only)",
    example: [
      "2024-12-25T14:00:00.000Z",
      "2024-12-25T16:30:00.000Z",
      "2024-12-25T19:00:00.000Z",
    ],
    required: false,
  })
  showTimes?: string[] | null;

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
  @ApiProperty({ description: "Original search query" })
  query: string;

  @ApiProperty({
    description: "List of matching entities (max 5 per type)",
    type: [SearchResultItemDto],
  })
  results: SearchResultItemDto[];

  @ApiProperty({
    description: "Result counts per type",
    example: {
      park: { returned: 5, total: 12 },
      attraction: { returned: 5, total: 156 },
      show: { returned: 3, total: 3 },
      restaurant: { returned: 0, total: 0 },
    },
  })
  counts: {
    park: { returned: number; total: number };
    attraction: { returned: number; total: number };
    show: { returned: number; total: number };
    restaurant: { returned: number; total: number };
  };
}
