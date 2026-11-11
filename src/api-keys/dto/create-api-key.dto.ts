import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import {
  ArrayMaxSize,
  IsArray,
  IsIn,
  IsInt,
  IsUUID,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
  MinLength,
  ValidateIf,
} from "class-validator";
import {
  API_KEY_USAGES,
  DEFAULT_REQUESTS_PER_HOUR,
  type ApiKeyUsage,
} from "../api-key.constants";

/** The largest hourly allowance the admin may set; above it, use null. */
export const MAX_REQUESTS_PER_HOUR = 1_000_000;

/** More parks than this on one key is every park, which null says more plainly. */
export const MAX_ALLOWED_PARKS = 200;

/** The body of `POST /v1/admin/api-keys`. */
export class CreateApiKeyDto {
  @ApiProperty({
    description: "Who or what the key is for, shown in the key list.",
    example: "Coaster club website",
  })
  @IsString()
  @MinLength(1)
  @MaxLength(80)
  name: string;

  @ApiPropertyOptional({
    description: "An email or URL to reach the holder.",
    example: "dev@example.org",
  })
  @IsOptional()
  @IsString()
  @MaxLength(200)
  contact?: string;

  @ApiPropertyOptional({
    description:
      "`commercial` only for a holder with a written agreement; the default is `non-commercial`.",
    enum: API_KEY_USAGES,
    default: "non-commercial",
  })
  @IsOptional()
  @IsIn(API_KEY_USAGES)
  usage?: ApiKeyUsage;

  @ApiPropertyOptional({
    description: "Anything the admin wants to remember about the key.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(500)
  note?: string;

  @ApiPropertyOptional({
    description: `Requests the key may make per clock hour; null for no per-key limit. Absent: ${DEFAULT_REQUESTS_PER_HOUR}.`,
    nullable: true,
    example: DEFAULT_REQUESTS_PER_HOUR,
  })
  @IsOptional()
  @ValidateIf((_dto, value) => value !== null)
  @IsInt()
  @Min(1)
  @Max(MAX_REQUESTS_PER_HOUR)
  requestsPerHour?: number | null;

  @ApiPropertyOptional({
    description:
      "The parks (ids) the key may read forecasts for; null or absent for every park.",
    nullable: true,
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_ALLOWED_PARKS)
  @IsUUID("all", { each: true })
  allowedParkIds?: string[] | null;
}

/**
 * The body of `PATCH /v1/admin/api-keys/:id`. A field left out stays as it is;
 * null clears it (no per-key limit, every park).
 */
export class UpdateApiKeyDto {
  @ApiPropertyOptional({
    description:
      "Requests the key may make per clock hour; null for no per-key limit.",
    nullable: true,
    example: DEFAULT_REQUESTS_PER_HOUR,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(MAX_REQUESTS_PER_HOUR)
  requestsPerHour?: number | null;

  @ApiPropertyOptional({
    description:
      "The parks (ids) the key may read forecasts for; null for every park.",
    nullable: true,
    type: [String],
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_ALLOWED_PARKS)
  @IsUUID("all", { each: true })
  allowedParkIds?: string[] | null;
}
