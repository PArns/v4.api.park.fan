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
