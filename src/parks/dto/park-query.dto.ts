import { IsOptional, IsString, IsIn } from "class-validator";
import { PaginationQueryDto } from "../../common/dto/pagination.dto";

export class ParkQueryDto extends PaginationQueryDto {
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
  continentSlug?: string;

  @IsOptional()
  @IsString()
  countrySlug?: string;

  @IsOptional()
  @IsString()
  citySlug?: string;

  @IsOptional()
  @IsString()
  @IsIn(["name:asc", "name:desc", "openStatus:asc", "openStatus:desc"])
  sort?: string;
}
