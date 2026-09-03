import { ApiProperty } from "@nestjs/swagger";
import {
  IsArray,
  IsOptional,
  IsString,
  MaxLength,
  IsNotEmpty,
} from "class-validator";
import { PUSH_TOPICS } from "../push-config";

/**
 * The subscribe body.
 *
 * Every field carries a class-validator decorator because the global
 * `ValidationPipe({ whitelist: true, forbidNonWhitelisted: true })` in
 * `main.ts` keeps only decorated properties and 400s the rest — a DTO with no
 * decorators rejects its own fields, which is exactly what this endpoint did
 * (`["property endpoint should not exist", …]`) for every request.
 *
 * The decorators are a GATE, not the check: they bound type and length so a
 * hostile body cannot reach the column, and `push.controller.ts` still does the
 * semantic work (https-only endpoint against the known push services, known
 * topics only, a locale that is a BCP-47 tag, an IANA zone). The lengths match
 * the columns in `push-subscription.entity.ts`, so an over-long value answers
 * 400 rather than failing at insert time.
 */
export class PushSubscribeDto {
  @ApiProperty({
    description:
      "The push service's URL for this browser, from `PushSubscription.endpoint`.",
    example: "https://fcm.googleapis.com/fcm/send/e8s...",
  })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;

  @ApiProperty({ description: "The browser's public key, base64url." })
  @IsString()
  @IsNotEmpty()
  @MaxLength(512)
  p256dh: string;

  @ApiProperty({ description: "The browser's auth secret, base64url." })
  @IsString()
  @IsNotEmpty()
  @MaxLength(512)
  auth: string;

  @ApiProperty({
    description:
      "Which stored trip this browser wants to hear about. The trip must exist — " +
      "a subscription against an id nobody created can never produce a " +
      "notification, and would leave the visitor looking at a switch that is on " +
      "and does nothing.",
    example: "n7Qk2Fd3Xb9pLmZa",
  })
  @IsString()
  @IsNotEmpty()
  @MaxLength(32)
  tripId: string;

  @ApiProperty({
    description:
      "The language to write in. The SUBSCRIBER's, not the park's, and stored " +
      "rather than derived: the job runs with no request to read it from, and a " +
      "notification in the wrong language is an unreadable interruption. " +
      "Anything unrecognised falls back to English rather than being refused.",
    example: "de",
    required: false,
  })
  @IsOptional()
  @IsString()
  @MaxLength(64)
  locale?: string;

  @ApiProperty({ required: false, example: "Europe/Berlin" })
  @IsOptional()
  @IsString()
  @MaxLength(64)
  timezone?: string;

  @ApiProperty({
    description:
      "What to be told about. Unknown topics are dropped, and a body with no " +
      "known topic left is refused — a stored subscription for a topic nothing " +
      "sends is a switch that is on and does nothing.",
    enum: PUSH_TOPICS,
    isArray: true,
    // The example is the whole list, generated from it. Writing a topic here
    // that `isPushTopic` rejects would advertise a switch nothing sends —
    // `push-config.ts` describes that as the one failure this module is
    // arranged against, and the example said `ride-down`.
    example: [...PUSH_TOPICS],
    required: false,
  })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  @MaxLength(64, { each: true })
  topics?: string[];
}

export class PushUnsubscribeDto {
  @ApiProperty({ description: "The endpoint to forget." })
  @IsString()
  @IsNotEmpty()
  @MaxLength(2048)
  endpoint: string;
}

export class PushStatusDto {
  @ApiProperty({
    description:
      "Whether this deploy can send at all. False when no VAPID keypair is " +
      "configured — the browser must not offer a switch that cannot work.",
  })
  available: boolean;

  @ApiProperty({
    required: false,
    description:
      "The VAPID public key, which the browser needs to subscribe. Public by " +
      "design: it is the half of the pair a push service checks a signature " +
      "against. Absent when push is not configured.",
  })
  publicKey?: string;

  @ApiProperty({ enum: PUSH_TOPICS, isArray: true })
  topics: string[];
}
