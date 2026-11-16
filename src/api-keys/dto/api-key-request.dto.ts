import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import {
  ArrayMaxSize,
  ArrayMinSize,
  Equals,
  IsArray,
  IsBoolean,
  IsEmail,
  IsIn,
  IsInt,
  IsISO8601,
  IsOptional,
  IsString,
  IsUrl,
  IsUUID,
  Max,
  MaxLength,
  Min,
  MinLength,
  ValidateIf,
} from "class-validator";
import { API_KEY_USAGES, type ApiKeyUsage } from "../api-key.constants";
import {
  PLATFORMS,
  PROJECT_KINDS,
  REQUEST_LIMITS as L,
  REQUEST_VOLUMES,
  REVENUE_MODELS,
  SOURCE_MODELS,
} from "../api-key-request.policy";
import { API_PERMISSIONS } from "../permissions";
import { MAX_ALLOWED_PARKS, MAX_REQUESTS_PER_HOUR } from "./create-api-key.dto";

const HTTP_URL = { protocols: ["http", "https"], require_protocol: true };

/** The body of `POST /v1/developer/register`. */
export class RegisterDeveloperDto {
  @ApiProperty({ example: "dev@example.org" })
  @IsEmail()
  @MaxLength(320)
  email: string;

  @ApiPropertyOptional({
    description: "Shown in the admin; defaults to the address's local part.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(120)
  displayName?: string;

  @ApiProperty({ description: "At least 12 characters." })
  @IsString()
  @MinLength(12)
  @MaxLength(256)
  password: string;

  @ApiPropertyOptional({
    description: "The solved Turnstile token (action `developer-register`).",
  })
  @IsOptional()
  @IsString()
  @MaxLength(2048)
  turnstileToken?: string;
}

/** The body of `POST /v1/developer/requests`. */
export class CreateApiKeyRequestDto {
  @ApiProperty()
  @IsString()
  @MinLength(1)
  @MaxLength(L.projectName)
  projectName: string;

  @ApiPropertyOptional({ description: "The project's website or store page." })
  @IsOptional()
  @IsUrl(HTTP_URL)
  @MaxLength(L.url)
  website?: string;

  @ApiProperty({ enum: PROJECT_KINDS })
  @IsIn(PROJECT_KINDS)
  kind: string;

  @ApiProperty({ enum: PLATFORMS, isArray: true })
  @IsArray()
  @ArrayMinSize(1)
  @IsIn(PLATFORMS, { each: true })
  platforms: string[];

  @ApiPropertyOptional({ description: "The languages the project is in." })
  @IsOptional()
  @IsString()
  @MaxLength(L.languages)
  languages?: string;

  @ApiProperty({
    description: "What the project does and where park.fan's data appears.",
  })
  @IsString()
  @MinLength(L.descriptionMin)
  @MaxLength(L.description)
  description: string;

  @ApiProperty({ enum: SOURCE_MODELS })
  @IsIn(SOURCE_MODELS)
  source: string;

  @ApiPropertyOptional({ description: "Required for open source." })
  @ValidateIf((dto: CreateApiKeyRequestDto) => dto.source === "open-source")
  @IsUrl(HTTP_URL)
  @MaxLength(L.url)
  repository?: string;

  @ApiPropertyOptional({ description: "The repository's licence, e.g. MIT." })
  @IsOptional()
  @IsString()
  @MaxLength(L.license)
  license?: string;

  @ApiProperty({ enum: REVENUE_MODELS, isArray: true })
  @IsArray()
  @ArrayMinSize(1)
  @IsIn(REVENUE_MODELS, { each: true })
  revenue: string[];

  @ApiProperty({ enum: API_PERMISSIONS, isArray: true })
  @IsArray()
  @ArrayMinSize(1)
  @IsIn(API_PERMISSIONS, { each: true })
  permissions: string[];

  @ApiPropertyOptional({ description: "The routes the project will call." })
  @IsOptional()
  @IsString()
  @MaxLength(L.endpoints)
  endpoints?: string;

  @ApiProperty({ enum: REQUEST_VOLUMES })
  @IsIn(REQUEST_VOLUMES)
  volume: string;

  @ApiPropertyOptional({ description: "The parks it needs; empty for all." })
  @IsOptional()
  @IsString()
  @MaxLength(L.parks)
  parks?: string;

  @ApiProperty({ description: "The key stays on the applicant's server." })
  @IsBoolean()
  serverSide: boolean;

  @ApiPropertyOptional({ description: "How answers are cached." })
  @IsOptional()
  @IsString()
  @MaxLength(L.caching)
  caching?: string;

  @ApiProperty({
    description: "The page with the visible backlink to park.fan.",
  })
  @IsUrl(HTTP_URL)
  @MaxLength(L.url)
  attributionUrl: string;

  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(L.message)
  message?: string;

  @ApiProperty({ enum: ["en", "de", "nl", "fr", "es", "it"] })
  @IsIn(["en", "de", "nl", "fr", "es", "it"])
  locale: string;

  @ApiProperty({
    description: "The applicant accepts the terms, the backlink among them.",
  })
  @Equals(true)
  terms: boolean;
}

/** The body of `POST /v1/admin/api-key-requests/:id/approve`. */
export class ApproveApiKeyRequestDto {
  @ApiPropertyOptional({
    description: "The key's name; the project's name when absent.",
  })
  @IsOptional()
  @IsString()
  @MinLength(1)
  @MaxLength(80)
  name?: string;

  @ApiProperty({ enum: API_PERMISSIONS, isArray: true })
  @IsArray()
  @ArrayMinSize(1)
  @IsIn(API_PERMISSIONS, { each: true })
  permissions: string[];

  @ApiPropertyOptional({ enum: API_KEY_USAGES, default: "non-commercial" })
  @IsOptional()
  @IsIn(API_KEY_USAGES)
  usage?: ApiKeyUsage;

  @ApiPropertyOptional({ nullable: true })
  @IsOptional()
  @ValidateIf((_dto, value) => value !== null)
  @IsInt()
  @Min(1)
  @Max(MAX_REQUESTS_PER_HOUR)
  requestsPerHour?: number | null;

  @ApiPropertyOptional({ nullable: true, type: [String] })
  @IsOptional()
  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(MAX_ALLOWED_PARKS)
  @IsUUID("all", { each: true })
  allowedParkIds?: string[] | null;

  @ApiPropertyOptional({ nullable: true })
  @IsOptional()
  @IsISO8601({ strict: true })
  expiresAt?: string | null;

  @ApiPropertyOptional({ description: "For the admin." })
  @IsOptional()
  @IsString()
  @MaxLength(500)
  note?: string;

  @ApiPropertyOptional({
    description: "What the developer reads beside the approval.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(L.reason)
  message?: string;
}

/** The body of `POST /v1/admin/api-key-requests/:id/reject`. */
export class RejectApiKeyRequestDto {
  @ApiPropertyOptional({
    description: "Why; the developer reads it on the dashboard.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(L.reason)
  reason?: string;
}
