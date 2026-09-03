import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Post,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { PushService } from "./push.service";
import { TripsService } from "../trips/trips.service";
import {
  PUSH_TOPICS,
  getVapidConfig,
  isPushConfigured,
  isPushTopic,
  type PushTopic,
} from "./push-config";
import {
  PushStatusDto,
  PushSubscribeDto,
  PushUnsubscribeDto,
} from "./dto/push.dto";

/**
 * Web-push subscriptions.
 *
 * Three endpoints and one rule running through all of them: **never accept a
 * subscription this deploy cannot send to.** A stored subscription against a
 * server with no VAPID keys, or against a trip nobody created, produces no
 * notification and no error — the visitor is left looking at a switch that is
 * on and does nothing, which is the worst state this feature has.
 *
 * So `GET /v1/push` exists at all: the browser asks whether push is available
 * BEFORE it offers the control, and gets the public key it needs in the same
 * answer.
 */
@ApiTags("push")
@Controller("push")
@UseInterceptors(new NoCdnCacheInterceptor())
export class PushController {
  constructor(
    private readonly pushService: PushService,
    private readonly tripsService: TripsService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Whether push works here, and the key to subscribe with",
    description:
      "The browser asks this before offering the control. `available: false` " +
      "means this deploy has no VAPID keypair — the switch must not be shown.",
  })
  @ApiResponse({ status: 200, type: PushStatusDto })
  status(): PushStatusDto {
    const vapid = getVapidConfig();
    return {
      available: vapid !== null,
      // Public by design: it is the half of the pair a push service checks a
      // signature against, and the browser cannot subscribe without it.
      ...(vapid ? { publicKey: vapid.publicKey } : {}),
      topics: [...PUSH_TOPICS],
    };
  }

  @Post("subscriptions")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Subscribe a browser to a trip",
    description:
      "An upsert on the endpoint, never an insert: a push service hands back " +
      "the same URL every time a page re-subscribes, and inserting would " +
      "deliver every notification once per page load.",
  })
  @ApiResponse({ status: 204, description: "Stored." })
  @ApiResponse({
    status: 400,
    description: "Malformed subscription, or an unknown topic.",
  })
  @ApiResponse({ status: 404, description: "No such trip." })
  @ApiResponse({
    status: 503,
    description: "Push is not configured on this deploy.",
  })
  async subscribe(@Body() body: PushSubscribeDto): Promise<void> {
    if (!isPushConfigured()) {
      throw new HttpException(
        "Push is not configured on this deploy",
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }

    const endpoint = requireUrl(body?.endpoint, "endpoint");
    const p256dh = requireString(body?.p256dh, "p256dh");
    const auth = requireString(body?.auth, "auth");
    const tripId = requireString(body?.tripId, "tripId");

    // The trip has to exist. A subscription against an id nobody created can
    // never produce a notification — the job walks trips, not subscriptions —
    // and the visitor would see the switch stay on for a month.
    const trip = await this.tripsService.find(tripId);
    if (!trip) throw new HttpException("Trip not found", HttpStatus.NOT_FOUND);

    const topics = normalizeTopics(body?.topics);
    if (topics.length === 0) {
      throw new BadRequestException("No known topics requested");
    }

    const stored = await this.pushService.subscribe({
      endpoint,
      p256dh,
      auth,
      tripId,
      locale: normalizeLocale(body?.locale),
      timezone: normalizeTimezone(body?.timezone),
      topics,
    });

    // `subscribe` answers null only when VAPID went away between the check
    // above and the write, which is a restart mid-request. Say so rather than
    // returning 204 for a subscription that was not stored.
    if (!stored) {
      throw new HttpException(
        "Push is not configured on this deploy",
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }
  }

  @Delete("subscriptions")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Forget a browser",
    description:
      "Idempotent. Unsubscribing an endpoint that is not stored is not an " +
      "error: a browser that revoked permission has no way to know whether its " +
      "subscription ever reached us.",
  })
  @ApiResponse({ status: 204, description: "Gone, or was never there." })
  async unsubscribe(@Body() body: PushUnsubscribeDto): Promise<void> {
    const endpoint = requireUrl(body?.endpoint, "endpoint");
    await this.pushService.unsubscribe(endpoint);
  }
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new BadRequestException(`Missing ${field}`);
  }
  return value.trim();
}

/**
 * An endpoint has to be an https URL.
 *
 * This string is handed to `web-push`, which makes a request to it. Accepting
 * anything else would turn the subscribe endpoint into a request forwarder
 * pointed wherever the caller likes — the classic shape of an SSRF, and the
 * only thing standing between it and this API's own network is this check.
 */
function requireUrl(value: unknown, field: string): string {
  const raw = requireString(value, field);
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new BadRequestException(`${field} is not a URL`);
  }
  if (parsed.protocol !== "https:") {
    throw new BadRequestException(`${field} must be https`);
  }
  return raw;
}

/** Known topics only, deduplicated. Unknown ones are dropped, not stored. */
function normalizeTopics(value: unknown): PushTopic[] {
  const list = Array.isArray(value) ? value : [];
  return [...new Set(list.filter(isPushTopic))];
}

/**
 * A BCP-47 tag, or `en`.
 *
 * Stored and then written into a notification, so it is bounded here rather
 * than trusted: the column is 12 characters and an unbounded string would be
 * either a truncation error at insert time or a very long tag in a query.
 */
function normalizeLocale(value: unknown): string {
  if (typeof value !== "string") return "en";
  const tag = value.trim().slice(0, 12);
  return /^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$/.test(tag) ? tag : "en";
}

/** An IANA zone, or null. Same bounding, same reason. */
function normalizeTimezone(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const zone = value.trim();
  if (zone.length === 0 || zone.length > 64) return null;
  return /^[A-Za-z0-9_+\-/]+$/.test(zone) ? zone : null;
}
