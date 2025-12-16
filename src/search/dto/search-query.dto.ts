import {
  IsString,
  MinLength,
  IsOptional,
  IsArray,
  IsInt,
  Min,
} from "class-validator";
import { Type, Transform } from "class-transformer";
import { ApiProperty } from "@nestjs/swagger";

export class SearchQueryDto {
  @ApiProperty({
    description: "Search query string (min 2 chars)",
    minLength: 2,
    example: "space",
  })
  @IsString()
  @MinLength(2, { message: "Search query must be at least 2 characters long" })
  q: string;

  @ApiProperty({
    description: "Filter by entity type",
    required: false,
    type: [String],
    example: ["park", "attraction"],
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",") : value,
  )
  @IsArray()
  type?: string[];

  @ApiProperty({
    description: "Max results to return",
    required: false,
    default: 20,
    minimum: 1,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  limit?: number = 20;

  @ApiProperty({
    description: "Pagination offset",
    required: false,
    default: 0,
    minimum: 0,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  offset?: number = 0;
}
