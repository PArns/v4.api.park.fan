import { IsOptional, IsString, IsBoolean, IsIn } from "class-validator";
import { Transform } from "class-transformer";
import { PaginationQueryDto } from "../../common/dto/pagination.dto";

export class RestaurantQueryDto extends PaginationQueryDto {
  @IsOptional()
  @IsString()
  park?: string;

  @IsOptional()
  @IsString()
  cuisineType?: string;

  @IsOptional()
  @Transform(({ value }) => {
    if (value === "true") return true;
    if (value === "false") return false;
    return value;
  })
  @IsBoolean()
  requiresReservation?: boolean;

  @IsOptional()
  @IsString()
  @IsIn(["name:asc", "name:desc"])
  sort?: string;
}
