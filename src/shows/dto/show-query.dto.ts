import { IsOptional, IsString, IsInt, Min, IsIn } from "class-validator";
import { Type } from "class-transformer";

export class ShowQueryDto {
  @IsOptional()
  @IsString()
  park?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  durationMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  durationMax?: number;

  @IsOptional()
  @IsString()
  @IsIn(["name:asc", "name:desc", "duration:asc", "duration:desc"])
  sort?: string;
}
