import { ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  ArrayMaxSize,
  IsArray,
  IsDefined,
  IsIn,
  IsOptional,
  IsString,
  IsUUID,
  MaxLength,
  MinLength,
  ValidateNested,
} from "class-validator";

/** The most ids a list keeps; far beyond a real visitor, short of a free store. */
export const MAX_ACCOUNT_IDS = 1000;

/** Favorites by kind, each a list of ids. */
export class AccountFavoritesDto {
  @IsArray()
  @ArrayMaxSize(MAX_ACCOUNT_IDS)
  @IsUUID("all", { each: true })
  parks: string[];

  @IsArray()
  @ArrayMaxSize(MAX_ACCOUNT_IDS)
  @IsUUID("all", { each: true })
  attractions: string[];

  @IsArray()
  @ArrayMaxSize(MAX_ACCOUNT_IDS)
  @IsUUID("all", { each: true })
  shows: string[];

  @IsArray()
  @ArrayMaxSize(MAX_ACCOUNT_IDS)
  @IsUUID("all", { each: true })
  restaurants: string[];
}

/** The body of `PUT /v1/account/data`: any field left out stays as it is. */
export class UpdateAccountDataDto {
  @ApiPropertyOptional({ type: AccountFavoritesDto })
  @IsOptional()
  @ValidateNested()
  @Type(() => AccountFavoritesDto)
  favorites?: AccountFavoritesDto;

  @ApiPropertyOptional({ type: [String] })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(MAX_ACCOUNT_IDS)
  @IsUUID("all", { each: true })
  riddenRides?: string[];

  @ApiPropertyOptional({
    enum: ["C", "F"],
    nullable: true,
    description: "null clears the unit.",
  })
  @IsOptional()
  @IsIn(["C", "F"])
  temperatureUnit?: "C" | "F" | null;
}

/** The body of `DELETE /v1/account`: the password confirms it. */
export class DeleteOwnAccountDto {
  @IsString()
  @MaxLength(256)
  password: string;
}

/** A storage key: lowercase letters, digits, `.`, `_` and `-`, starting with a letter or digit. */
export const ACCOUNT_STORAGE_KEY = /^[a-z0-9][a-z0-9._-]{0,63}$/;
/** The most keys one account keeps. */
export const MAX_ACCOUNT_STORAGE_KEYS = 100;
/** The largest value, as JSON, in bytes. */
export const MAX_ACCOUNT_STORAGE_BYTES = 64 * 1024;

/** The body of `PUT /v1/account/storage/:key`: any JSON value. */
export class PutAccountStorageDto {
  @ApiPropertyOptional({ description: "Any JSON value, at most 64 KB." })
  @IsDefined()
  value: unknown;
}

/** The body of `PATCH /v1/account/profile`. */
export class UpdateAccountProfileDto {
  @ApiPropertyOptional({ description: "The name the account shows." })
  @IsString()
  @MinLength(1)
  @MaxLength(120)
  displayName: string;
}
