import { IsOptional, IsString, IsIn } from "class-validator";

export class ParkQueryDto {
  @IsOptional()
  @IsString()
  continent?: string;

  @IsOptional()
  @IsString()
  country?: string;

  @IsOptional()
  @IsString()
  city?: string;

  @IsOptional()
  @IsString()
  @IsIn(["name:asc", "name:desc", "openStatus:asc", "openStatus:desc"])
  sort?: string;
}
