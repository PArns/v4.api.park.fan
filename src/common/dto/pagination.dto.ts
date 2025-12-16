import { IsOptional, IsInt, Min } from "class-validator";
import { Type } from "class-transformer";
import { ApiProperty } from "@nestjs/swagger";

/**
 * Pagination metadata for paginated responses
 */
export class PaginationDto {
  @ApiProperty({ example: 1, description: "Current page number" })
  page: number;

  @ApiProperty({ example: 10, description: "Items per page" })
  limit: number;

  @ApiProperty({ example: 100, description: "Total number of items" })
  total: number;

  @ApiProperty({ example: 10, description: "Total number of pages" })
  totalPages: number;

  @ApiProperty({ example: true, description: "Whether there is a next page" })
  hasNext: boolean;

  @ApiProperty({
    example: false,
    description: "Whether there is a previous page",
  })
  hasPrevious: boolean;

  constructor(page: number, limit: number, total: number) {
    this.page = page;
    this.limit = limit;
    this.total = total;
    this.totalPages = Math.ceil(total / limit);
    this.hasNext = page < this.totalPages;
    this.hasPrevious = page > 1;
  }
}

/**
 * Generic paginated response wrapper
 */
export class PaginatedResponseDto<T> {
  data: T[];

  @ApiProperty({ type: PaginationDto })
  pagination: PaginationDto;
}

/**
 * Query parameters for pagination
 */
export class PaginationQueryDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  page?: number = 1;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  limit?: number = 10;
}
