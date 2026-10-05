import { ApiProperty } from "@nestjs/swagger";
import {
  IsIn,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  Max,
  MaxLength,
  Min,
  ValidateIf,
} from "class-validator";
import { RIDE_ALERT_KIND_REOPEN } from "../entities/ride-alert.entity";

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
    required: false,
    enum: [RIDE_ALERT_KIND_REOPEN],
    description:
      "What to be told about. Omitted (or null) is the wait-time alert and " +
      "needs `thresholdMinutes`. `reopen` notifies once when the ride opens " +
      "after being seen closed, down or in refurbishment, and ignores " +
      "`thresholdMinutes`. A ride has one alert per browser, so sending the " +
      "other kind replaces the existing one.",
  })
  @IsOptional()
  @IsIn([RIDE_ALERT_KIND_REOPEN])
  kind?: typeof RIDE_ALERT_KIND_REOPEN | null;

  @ApiProperty({
    required: false,
    description:
      "Notify once the STANDBY wait drops below this many minutes. Required " +
      "unless `kind` is `reopen`. " +
      "Re-sending for an attraction already watched replaces the threshold " +
      "and re-arms the alert — a changed number is a fresh ask, not a no-op.",
    example: 20,
    minimum: 1,
    maximum: 240,
  })
  @ValidateIf((o: CreateRideAlertDto) => o.kind !== RIDE_ALERT_KIND_REOPEN)
  @IsInt()
  @Min(1)
  @Max(240)
  thresholdMinutes?: number;
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
  @ApiProperty({
    nullable: true,
    enum: [RIDE_ALERT_KIND_REOPEN],
    description:
      "What this alert waits for. `null` is the wait-time alert (fires below " +
      "`thresholdMinutes`); `reopen` fires when the ride opens again.",
  })
  kind: string | null;
  @ApiProperty({
    example: 20,
    nullable: true,
    description:
      "`null` for a `reopen` alert, which has no wait-time threshold.",
  })
  thresholdMinutes: number | null;
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
