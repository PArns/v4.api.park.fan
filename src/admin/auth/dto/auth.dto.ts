import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import {
  IsBoolean,
  IsEmail,
  IsIn,
  IsOptional,
  IsString,
  Matches,
  MaxLength,
  MinLength,
} from "class-validator";
import { ADMIN_ROLES, type AdminRole } from "../entities/admin-user.entity";
import { PAT_EXPIRY_DAYS } from "../admin-api-token.constants";

export class AdminLoginDto {
  @ApiProperty({ example: "you@park.fan" })
  @IsEmail({}, { message: "A valid email address is required" })
  @MaxLength(320)
  email: string;

  @ApiProperty({ example: "correct horse battery staple" })
  @IsString()
  @MaxLength(256)
  password: string;

  @ApiPropertyOptional({
    description:
      "Six-digit TOTP code. Omit it on the first call: when the account has " +
      "two-factor enabled the response says `totp-required` and asks again.",
    example: "492013",
  })
  @IsOptional()
  @IsString()
  @Matches(/^\d{6}$/, { message: "A TOTP code is six digits" })
  totpCode?: string;

  @ApiPropertyOptional({
    description:
      "A solved Cloudflare Turnstile token. Required of callers that do not " +
      "present a valid frontend key, and ignored for those that do — park.fan's " +
      "admin proxy verifies the challenge on its own side before it forwards " +
      "anything here, and a token may be redeemed only once.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(4096)
  turnstileToken?: string;
}

export class AdminChangePasswordDto {
  @ApiProperty()
  @IsString()
  @MaxLength(256)
  currentPassword: string;

  @ApiProperty({
    description: "At least 12 characters. Length only — no composition rules.",
  })
  @IsString()
  @MinLength(12)
  @MaxLength(256)
  newPassword: string;
}

export class AdminCreateUserDto {
  @ApiProperty()
  @IsEmail({}, { message: "A valid email address is required" })
  @MaxLength(320)
  email: string;

  @ApiPropertyOptional({
    description: "Defaults to the local part of the email.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(120)
  displayName?: string;

  @ApiProperty({ enum: ADMIN_ROLES })
  @IsIn(ADMIN_ROLES as unknown as string[])
  role: AdminRole;

  @ApiProperty({
    description:
      "Temporary password. The account is created owing a password change, " +
      "so this one works exactly once.",
  })
  @IsString()
  @MinLength(12)
  @MaxLength(256)
  password: string;
}

export class AdminUpdateUserDto {
  @ApiPropertyOptional()
  @IsOptional()
  @IsString()
  @MaxLength(120)
  displayName?: string;

  @ApiPropertyOptional({ enum: ADMIN_ROLES })
  @IsOptional()
  @IsIn(ADMIN_ROLES as unknown as string[])
  role?: AdminRole;

  @ApiPropertyOptional()
  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

export class AdminResetPasswordDto {
  @ApiProperty()
  @IsString()
  @MinLength(12)
  @MaxLength(256)
  newPassword: string;
}

export class AdminTotpBeginDto {
  @ApiProperty({
    description:
      "The account password. Enrolment needs it for the same reason disabling " +
      "does — a stolen session must not be able to put its own authenticator " +
      "on somebody else's account.",
  })
  @IsString()
  @MaxLength(256)
  password: string;
}

export class AdminTotpConfirmDto {
  @ApiProperty({ example: "492013" })
  @IsString()
  @Matches(/^\d{6}$/, { message: "A TOTP code is six digits" })
  code: string;
}

export class AdminTotpDisableDto {
  @ApiProperty()
  @IsString()
  @MaxLength(256)
  password: string;

  @ApiProperty({ example: "492013" })
  @IsString()
  @Matches(/^\d{6}$/, { message: "A TOTP code is six digits" })
  code: string;
}

export class AdminCreateApiTokenDto {
  @ApiProperty({
    description: "What the token is for, shown in the token list.",
    example: "Laptop",
  })
  @IsString()
  @MinLength(1)
  @MaxLength(80)
  name: string;

  @ApiPropertyOptional({
    description: "Days until it expires; null or absent never expires.",
    enum: PAT_EXPIRY_DAYS,
    nullable: true,
    example: 90,
  })
  @IsOptional()
  @IsIn(PAT_EXPIRY_DAYS)
  expiresInDays?: (typeof PAT_EXPIRY_DAYS)[number] | null;

  @ApiProperty({
    description:
      "The account password, asked again: a session alone does not mint a " +
      "credential that outlives it. Wrong → 401 `invalid-password`.",
  })
  @IsString()
  @MaxLength(256)
  password: string;

  @ApiPropertyOptional({
    description:
      "Six-digit TOTP code, required when the account has two-factor on " +
      "(401 `totp-required` without it, `invalid-totp` when it does not " +
      "match or was already used).",
    example: "492013",
  })
  @IsOptional()
  @IsString()
  @Matches(/^\d{6}$/, { message: "A TOTP code is six digits" })
  totpCode?: string;
}
