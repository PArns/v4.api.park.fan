import {
  IsString,
  MinLength,
  IsOptional,
  IsArray,
  IsInt,
  Min,
} from "class-validator";
import { Type, Transform } from "class-transformer";

export class SearchQueryDto {
  @IsString()
  @MinLength(2, { message: "Search query must be at least 2 characters long" })
  q: string;

  @IsOptional()
  @Transform(({ value }) =>
    typeof value === "string" ? value.split(",") : value,
  )
  @IsArray()
  type?: string[];

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  limit?: number = 20;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  offset?: number = 0;
}
