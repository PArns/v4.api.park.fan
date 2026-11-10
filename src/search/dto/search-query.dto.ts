import {
  IsString,
  MinLength,
  IsOptional,
  IsArray,
  IsInt,
  Min,
  Max,
} from "class-validator";
import { Transform, Type } from "class-transformer";
import { ApiProperty } from "@nestjs/swagger";

/** The longest search query read; anything after it is dropped. */
export const SEARCH_QUERY_MAX = 100;

export class SearchQueryDto {
  @ApiProperty({
    description: "Search query (searches name, city, country, continent)",
    minLength: 2,
    example: "disney",
  })
  @IsString()
  @MinLength(2, { message: "Search query must be at least 2 characters long" })
  // Cut, not refused: a pasted paragraph searches its start, and no query reaches the trigram
  // scans at any length.
  @Transform(({ value }: { value: unknown }) =>
    typeof value === "string" ? value.slice(0, SEARCH_QUERY_MAX) : value,
  )
  q: string;

  @ApiProperty({
    description: "Filter by entity type (returns max 5 results per type)",
    required: false,
    type: [String],
    enum: ["park", "attraction", "show", "restaurant"],
    example: ["park", "attraction"],
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",") : value,
  )
  @IsArray()
  type?: string[];

  @ApiProperty({
    description: "Maximum number of results per type",
    required: false,
    example: 5,
    minimum: 1,
    maximum: 20,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(20)
  limit?: number = 5;
}
