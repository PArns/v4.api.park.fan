import { ApiProperty } from "@nestjs/swagger";
import {
  ArrayMaxSize,
  IsArray,
  IsIn,
  IsOptional,
  IsString,
  IsUUID,
  Matches,
  MaxLength,
  IsNotEmpty,
} from "class-validator";
import { DEVICE_PUSH_KINDS, PUSH_TOPICS } from "../push-config";
import { APNS_ENVIRONMENTS, type ApnsEnvironment } from "../apns";
import { PUSH_SELF_TEST_KINDS } from "../push-self-test";

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
      "Which stored trip this browser wants to hear about. Omit entirely for a " +
      "subscription that is not about the trip planner (a followed show, a " +
      "ride's wait-time alert) — omitting never clears a trip already stored " +
      "for this endpoint. When sent, the trip must exist — a subscription " +
      "against an id nobody created can never produce a notification, and " +
      "would leave the visitor looking at a switch that is on and does nothing.",
    example: "n7Qk2Fd3Xb9pLmZa",
    required: false,
  })
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  @MaxLength(32)
  tripId?: string;

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

  @ApiProperty({
    description:
      "The SUBSCRIBER's IANA zone, not the park's. Nothing is sent between " +
      "23:00 and 07:00 by this clock, which is the point of storing it: every " +
      "trigger fires inside the park's opening hours, so the only way to be " +
      "woken at 03:00 is to be somewhere else than the park. Omitting it never " +
      "clears a zone already stored for this endpoint — but a browser with no " +
      "stored zone is sent to at any hour, so send it. A zone this deploy " +
      "cannot resolve is treated the same as none.",
    example: "Europe/Berlin",
    required: false,
  })
  @IsOptional()
  @IsString()
  @MaxLength(64)
  timezone?: string;

  @ApiProperty({
    description:
      "What to be told about, for the trip planner. Omit entirely outside the " +
      "trip planner — omitting never clears topics already stored for this " +
      "endpoint. When sent, unknown topics are dropped, and a body with no " +
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

  @ApiProperty({
    description:
      "Scopes this to the trip planner alone: only `tripId`/`topics` are " +
      "cleared, and a ride alert or followed show on the same endpoint is " +
      "left running. Omit to forget the browser entirely.",
    example: "n7Qk2Fd3Xb9pLmZa",
    required: false,
  })
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  @MaxLength(32)
  tripId?: string;
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

  @ApiProperty({
    description:
      "Whether this deploy can send to the iOS app (an APNs key is " +
      "configured). False: the app must not offer its switches.",
  })
  apns: boolean;

  @ApiProperty({ enum: DEVICE_PUSH_KINDS, isArray: true })
  deviceKinds: string[];
}

/**
 * An iOS app install. No account: `installId` is a random UUID the app keeps
 * in its own storage and the identity of the row (`apns:<installId>`), the
 * same bearer role the endpoint URL plays for a browser.
 */
export class PushDeviceDto {
  @ApiProperty({ description: "The app's random install id (UUID)." })
  @IsUUID()
  installId: string;

  @ApiProperty({ description: "The APNs device token, hex." })
  @IsString()
  @Matches(/^[0-9a-fA-F]{64,200}$/)
  token: string;

  @ApiProperty({
    description:
      "Every kind the device wants, its whole current choice: replaces what " +
      "was stored. Unknown kinds are dropped; empty means nothing is sent " +
      "while alerts and follows stay stored.",
    enum: DEVICE_PUSH_KINDS,
    isArray: true,
  })
  @IsArray()
  @ArrayMaxSize(32)
  @IsString({ each: true })
  @MaxLength(64, { each: true })
  kinds: string[];

  @ApiProperty({
    required: false,
    nullable: true,
    description:
      "News only about these parks (API geo paths); omitted or null for all news. Does not narrow `guides`.",
    example: ["europe/germany/rust/europa-park"],
  })
  @IsOptional()
  @IsArray()
  @ArrayMaxSize(200)
  @IsString({ each: true })
  @MaxLength(200, { each: true })
  newsParks?: string[] | null;

  @ApiProperty({ required: false, example: "de" })
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
    required: false,
    nullable: true,
    description:
      "The stored plan (`/v1/trips`) the trip kinds read. Omitted keeps what " +
      "was stored, null clears it; an id that names no trip is a 404.",
  })
  @IsOptional()
  @IsString()
  @Matches(/^[A-Za-z0-9_-]{1,32}$/)
  tripId?: string | null;

  @ApiProperty({
    required: false,
    enum: APNS_ENVIRONMENTS,
    description:
      "Which APNs environment issued the token: `sandbox` for a build " +
      "installed from Xcode (`aps-environment` `development`), `production` " +
      "for TestFlight and the App Store. Omitted, the server finds out with " +
      "the first notification.",
  })
  @IsOptional()
  @IsIn(APNS_ENVIRONMENTS)
  environment?: ApnsEnvironment;
}

export class PushDeviceRemoveDto {
  @ApiProperty({ description: "The install id to forget." })
  @IsUUID()
  installId: string;
}

/** A device asking for a sample of one kind, sent to itself. */
export class PushDeviceTestDto {
  @ApiProperty({
    description:
      "The install that asks; the sample goes to its own registered token.",
  })
  @IsUUID()
  installId: string;

  @ApiProperty({ enum: PUSH_SELF_TEST_KINDS })
  @IsIn(PUSH_SELF_TEST_KINDS)
  kind: string;

  @ApiProperty({
    required: false,
    description: "The language to write it in; omitted, the stored one.",
  })
  @IsOptional()
  @IsString()
  @MaxLength(12)
  locale?: string;
}

export class PushDeviceTestResponseDto {
  @ApiProperty({ enum: ["sent", "apns-error", "not-configured", "no-token"] })
  state: "sent" | "apns-error" | "not-configured" | "no-token";

  @ApiProperty({ required: false, description: "Apple's HTTP status." })
  status?: number;

  @ApiProperty({ required: false, description: "Apple's `reason`." })
  reason?: string;

  @ApiProperty({ required: false, enum: APNS_ENVIRONMENTS })
  environment?: ApnsEnvironment;
}

export class PushDeviceResponseDto {
  @ApiProperty({
    description:
      "The identity to send as `endpoint` to `/v1/push/ride-alerts` and " +
      "`/v1/push/show-follows`.",
    example: "apns:6f1c…",
  })
  endpoint: string;

  @ApiProperty({ enum: DEVICE_PUSH_KINDS, isArray: true })
  kinds: string[];

  @ApiProperty({ nullable: true, type: [String] })
  newsParks: string[] | null;

  @ApiProperty({ nullable: true, type: String })
  tripId: string | null;
}
