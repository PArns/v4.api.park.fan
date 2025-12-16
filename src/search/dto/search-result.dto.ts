import { ApiProperty } from "@nestjs/swagger";

export class SearchResultItemDto {
  @ApiProperty({
    description: "Type of the entity",
    enum: ["park", "attraction", "show", "restaurant"],
  })
  type: "park" | "attraction" | "show" | "restaurant";

  @ApiProperty({ description: "Unique identifier for the entity" })
  id: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({ description: "Name of the entity" })
  name: string;
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
