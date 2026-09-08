import { ApiProperty } from "@nestjs/swagger";
import { IsNotEmpty, IsString, IsUUID, MaxLength } from "class-validator";

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
  @ApiProperty({ example: "2026-09-08T09:12:44.000Z" })
  createdAt: string;
}
