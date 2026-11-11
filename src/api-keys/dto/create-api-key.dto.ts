import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import {
  IsIn,
  IsOptional,
  IsString,
  MaxLength,
  MinLength,
} from "class-validator";
import { API_KEY_USAGES, type ApiKeyUsage } from "../api-key.constants";

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
}
