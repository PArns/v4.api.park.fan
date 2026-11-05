import { ApiProperty } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  IsDefined,
  IsIn,
  IsString,
  MaxLength,
  ValidateNested,
} from "class-validator";

export const REDIRECT_KINDS = ["park", "attraction"] as const;
export type RedirectKind = (typeof REDIRECT_KINDS)[number];

export class RedirectTargetDto {
  @ApiProperty({ enum: REDIRECT_KINDS })
  @IsIn(REDIRECT_KINDS as unknown as string[])
  kind: RedirectKind;

  @ApiProperty({ description: "The park's or ride's id (uuid)" })
  @IsString()
  id: string;
}

export class CreateRedirectDto {
  @ApiProperty({
    description:
      "The dead frontend path, with or without its locale: " +
      "`/de/parks/<continent>/<country>/<city>/<park>` for a park, " +
      "the same plus `/<ride>` for a ride inside the target's current park.",
    example: "/de/parks/europe/germany/bruehl/phantasialand/old-ride-name",
  })
  @IsString()
  @MaxLength(512)
  fromPath: string;

  @ApiProperty({ type: RedirectTargetDto })
  // Without it a missing target passes ValidateNested and fails later as a 500.
  @IsDefined()
  @ValidateNested()
  @Type(() => RedirectTargetDto)
  target: RedirectTargetDto;
}
