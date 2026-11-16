import { ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  ArrayMaxSize,
  IsArray,
  IsIn,
  IsOptional,
  IsString,
  IsUUID,
  MaxLength,
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

  @ApiPropertyOptional({ enum: ["C", "F"], nullable: true })
  @IsOptional()
  @IsIn(["C", "F"])
  temperatureUnit?: "C" | "F";
}

/** The body of `DELETE /v1/account`: the password confirms it. */
export class DeleteOwnAccountDto {
  @IsString()
  @MaxLength(256)
  password: string;
}
