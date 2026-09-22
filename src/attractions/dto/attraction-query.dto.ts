import { IsOptional, IsString, IsInt, Min, IsIn } from "class-validator";
import { Type } from "class-transformer";
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";
import { PaginationQueryDto } from "../../common/dto/pagination.dto";

export class AttractionQueryDto extends PaginationQueryDto {
  @IsOptional()
  @IsString()
  park?: string;

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
  @IsIn(["OPERATING", "DOWN", "CLOSED", "REFURBISHMENT"])
  status?: LiveStatus;

  @IsOptional()
  @IsString()
  // Read off the enum rather than retyped: the hand-written copy here was
  // missing VIRTUAL_QUEUE, so a filter for it was rejected as an invalid value
  // for a queue type the rest of the codebase knows about.
  @IsIn(Object.values(QueueType))
  queueType?: QueueType;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  waitTimeMin?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  waitTimeMax?: number;

  @IsOptional()
  @IsString()
  @IsIn([
    "name:asc",
    "name:desc",
    "waitTime:asc",
    "waitTime:desc",
    "status:asc",
    "status:desc",
  ])
  sort?: string;
}
