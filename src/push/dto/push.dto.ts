import { ApiProperty } from "@nestjs/swagger";
import { PUSH_TOPICS } from "../push-config";

export class PushSubscribeDto {
  @ApiProperty({
    description:
      "The push service's URL for this browser, from `PushSubscription.endpoint`.",
    example: "https://fcm.googleapis.com/fcm/send/e8s...",
  })
  endpoint: string;

  @ApiProperty({ description: "The browser's public key, base64url." })
  p256dh: string;

  @ApiProperty({ description: "The browser's auth secret, base64url." })
  auth: string;

  @ApiProperty({
    description:
      "Which stored trip this browser wants to hear about. The trip must exist — " +
      "a subscription against an id nobody created can never produce a " +
      "notification, and would leave the visitor looking at a switch that is on " +
      "and does nothing.",
    example: "n7Qk2Fd3Xb9pLmZa",
  })
  tripId: string;

  @ApiProperty({
    description:
      "The language to write in. The SUBSCRIBER's, not the park's, and stored " +
      "rather than derived: the job runs with no request to read it from, and a " +
      "notification in the wrong language is an unreadable interruption.",
    example: "de",
  })
  locale?: string;

  @ApiProperty({ required: false, example: "Europe/Berlin" })
  timezone?: string;

  @ApiProperty({
    description:
      "What to be told about. Unknown topics are rejected, not stored.",
    enum: PUSH_TOPICS,
    isArray: true,
    example: ["next-up", "ride-down"],
  })
  topics?: string[];
}

export class PushUnsubscribeDto {
  @ApiProperty({ description: "The endpoint to forget." })
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
