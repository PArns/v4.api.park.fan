import { ApiProperty } from "@nestjs/swagger";
import {
  IsInt,
  IsNotEmpty,
  IsString,
  IsUUID,
  Max,
  MaxLength,
  Min,
} from "class-validator";

/**
 * Every field carries a class-validator decorator for the same reason as
 * `PushSubscribeDto` — the global `ValidationPipe({ whitelist: true,
 * forbidNonWhitelisted: true })` strips (and 400s) anything undecorated.
 */
export class CreateRideAlertDto {
  @ApiProperty({
    description: "The subscribing browser's push endpoint.",
    example: "https://fcm.googleapis.com/fcm/send/e8s...",
  })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;

  @ApiProperty({ description: "The attraction to watch." })
  @IsUUID()
  attractionId: string;

  @ApiProperty({
    description:
      "Notify once the STANDBY wait drops below this many minutes. " +
      "Re-sending for an attraction already watched replaces the threshold " +
      "and re-arms the alert — a changed number is a fresh ask, not a no-op.",
    example: 20,
    minimum: 1,
    maximum: 240,
  })
  @IsInt()
  @Min(1)
  @Max(240)
  thresholdMinutes: number;
}

export class DeleteRideAlertDto {
  @ApiProperty({ description: "The subscribing browser's push endpoint." })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;

  @ApiProperty({ description: "The attraction to stop watching." })
  @IsUUID()
  attractionId: string;
}

export class RideAlertResponseDto {
  @ApiProperty() attractionId: string;
  @ApiProperty() attractionName: string;
  @ApiProperty() attractionSlug: string;
  @ApiProperty() parkId: string;
  @ApiProperty() parkName: string;
  @ApiProperty() parkSlug: string;
  @ApiProperty({
    nullable: true,
    description:
      "The ride's page on the site, relative to the origin. Null when the " +
      "park's geo slugs are incomplete — the same gap `frontendAttractionPath` " +
      "guards elsewhere.",
  })
  path: string | null;
  @ApiProperty({ example: 20 })
  thresholdMinutes: number;
  @ApiProperty({
    description:
      "Whether the ride is currently out of season (per the curated " +
      "seasonal-attraction data) — accepted at write time regardless, since " +
      "a visitor may set this up ahead of a future visit, but the sweep " +
      "skips an out-of-season ride entirely, so the alert cannot fire " +
      "until the ride is back in season.",
  })
  outOfSeason: boolean;
  @ApiProperty({
    description:
      "Whether this ride has been retired since the alert was created — it " +
      "can never fire again, and the visitor should be told to remove it.",
  })
  retired: boolean;
  @ApiProperty({
    description:
      "Whether the next qualifying reading will fire. False right after this " +
      "alert has already triggered and the wait has not yet risen back to the " +
      "threshold.",
  })
  armed: boolean;
  @ApiProperty({ example: "2026-09-08T09:12:44.000Z" })
  createdAt: string;
}
