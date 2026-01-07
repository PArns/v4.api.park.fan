import { ApiProperty } from "@nestjs/swagger";
import { IsArray, IsOptional, IsNumber, Min, Max } from "class-validator";
import { Type, Transform } from "class-transformer";

/**
 * Favorites Query DTO
 *
 * Used for GET /favorites endpoint to retrieve favorite data by IDs.
 * IDs can be passed as query parameters or in request body.
 */
export class FavoritesQueryDto {
  @ApiProperty({
    description: "Array of park IDs (comma-separated or array)",
    example: "abc-123,def-456",
    required: false,
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",").map((s) => s.trim()) : value,
  )
  @IsArray()
  parkIds?: string[];

  @ApiProperty({
    description: "Array of attraction (ride) IDs (comma-separated or array)",
    example: "xyz-789,uvw-012",
    required: false,
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",").map((s) => s.trim()) : value,
  )
  @IsArray()
  attractionIds?: string[];

  @ApiProperty({
    description: "Array of show IDs (comma-separated or array)",
    example: "show-123,show-456",
    required: false,
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",").map((s) => s.trim()) : value,
  )
  @IsArray()
  showIds?: string[];

  @ApiProperty({
    description: "Array of restaurant IDs (comma-separated or array)",
    example: "rest-123,rest-456",
    required: false,
  })
  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",").map((s) => s.trim()) : value,
  )
  @IsArray()
  restaurantIds?: string[];

  @ApiProperty({
    description: "User latitude for distance calculation",
    example: 48.266,
    required: false,
  })
  @IsNumber()
  @IsOptional()
  @Type(() => Number)
  @Min(-90)
  @Max(90)
  lat?: number;

  @ApiProperty({
    description: "User longitude for distance calculation",
    example: 7.722,
    required: false,
  })
  @IsNumber()
  @IsOptional()
  @Type(() => Number)
  @Min(-180)
  @Max(180)
  lng?: number;
}
