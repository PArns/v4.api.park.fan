import { ApiProperty } from "@nestjs/swagger";
import {
  IsISO8601,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  MaxLength,
} from "class-validator";

/**
 * Every field carries a class-validator decorator for the same reason as
 * `PushSubscribeDto` — the global `ValidationPipe({ whitelist: true,
 * forbidNonWhitelisted: true })` strips (and 400s) anything undecorated.
 */
export class CreateShowFollowDto {
  @ApiProperty({
    description: "The subscribing browser's push endpoint.",
    example: "https://fcm.googleapis.com/fcm/send/e8s...",
  })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;

  @ApiProperty({ description: "The show to follow." })
  @IsUUID()
  showId: string;

  @ApiProperty({
    required: false,
    nullable: true,
    description:
      "Which performance to be reminded about, as a full ISO instant. " +
      "Omitted means whichever performance comes next, on any day — the " +
      "open-ended follow a show card's bell files. Sent when the visitor " +
      "picked one of the day's showtimes instead, which is the case the " +
      "next performance is NOT the one they want.",
    example: "2026-09-08T23:10:00.000Z",
  })
  @IsOptional()
  @IsISO8601()
  startTime?: string;
}

export class DeleteShowFollowDto {
  @ApiProperty({ description: "The subscribing browser's push endpoint." })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;

  @ApiProperty({ description: "The show to stop following." })
  @IsUUID()
  showId: string;
}

export class ShowFollowResponseDto {
  @ApiProperty() showId: string;
  @ApiProperty() showName: string;
  @ApiProperty() showSlug: string;
  @ApiProperty() parkId: string;
  @ApiProperty() parkName: string;
  @ApiProperty() parkSlug: string;
  @ApiProperty({
    nullable: true,
    description:
      "The park's shows tab, relative to the origin. Null when the park's " +
      "geo slugs are incomplete.",
  })
  path: string | null;
  @ApiProperty({
    nullable: true,
    description:
      "The performance this follow is about, or null for whichever comes " +
      "next. A full ISO instant — read it in `timezone` to get the clock " +
      "time the park posts.",
    example: "2026-09-08T23:10:00.000Z",
  })
  startTime: string | null;
  @ApiProperty({
    nullable: true,
    description:
      "The park's IANA zone, so a caller can render `startTime` as the park " +
      "reads it rather than as the reader's own phone does.",
    example: "America/New_York",
  })
  timezone: string | null;
  @ApiProperty({ example: "2026-09-08T09:12:44.000Z" })
  createdAt: string;
}
