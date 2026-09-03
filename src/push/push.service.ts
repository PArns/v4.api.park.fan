import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import * as webpush from "web-push";
import { PushSubscription } from "./entities/push-subscription.entity";
import {
  getVapidConfig,
  isPushConfigured,
  type PushTopic,
} from "./push-config";

/** What a browser hands over when the visitor says yes. */
export interface SubscribeInput {
  endpoint: string;
  p256dh: string;
  auth: string;
  tripId: string;
  locale: string;
  timezone: string | null;
  topics: PushTopic[];
}

/** One notification, already written in the subscriber's language. */
export interface PushMessage {
  title: string;
  body: string;
  /** Where a tap should land. Relative to the site's origin. */
  url: string;
  /**
   * Collapses an older undelivered notification with the same tag.
   *
   * Load-bearing rather than a nicety: a phone in a pocket for two hours would
   * otherwise surface four stale "your next ride starts in 10 minutes" banners
   * at once, every one of them about a moment that has passed.
   */
  tag: string;
}

/**
 * Subscriptions, and sending to them.
 *
 * Everything here degrades rather than throws when VAPID is unconfigured — see
 * `push-config.ts` for why that is the right failure for this feature and the
 * wrong one for a login. The one thing it must not do is accept a subscription
 * it can never send to, which would leave the visitor looking at a switch that
 * is on and does nothing; `subscribe` refuses in that case and the controller
 * turns it into a 503.
 */
@Injectable()
export class PushService {
  private readonly logger = new Logger(PushService.name);

  /**
   * How many consecutive non-fatal failures before a subscription is dropped.
   *
   * A 404 or 410 is fatal on the first answer — the browser is gone and the
   * push service is telling us so. Everything else is the service having a bad
   * minute, and dropping a subscriber over one 500 costs them the feature for a
   * reason on our side.
   */
  private static readonly MAX_FAILURES = 8;

  constructor(
    @InjectRepository(PushSubscription)
    private readonly repository: Repository<PushSubscription>,
  ) {}

  /**
   * Store a browser's subscription. An UPSERT on the endpoint, never an insert.
   *
   * A push service issues one URL per browser and hands the same one back every
   * time the page re-subscribes — which happens on every load that calls
   * `pushManager.subscribe()`. Inserting would accumulate a row per page load
   * and then deliver the same notification four times to the same phone.
   *
   * `null` when this deploy has no VAPID keys, so the caller can say so instead
   * of storing a subscription nothing will ever send to.
   */
  async subscribe(input: SubscribeInput): Promise<PushSubscription | null> {
    if (!isPushConfigured()) return null;

    const existing = await this.repository.findOne({
      where: { endpoint: input.endpoint },
    });

    const row =
      existing ?? this.repository.create({ endpoint: input.endpoint });
    row.p256dh = input.p256dh;
    row.auth = input.auth;
    row.tripId = input.tripId;
    row.locale = input.locale;
    row.timezone = input.timezone;
    row.topics = input.topics;
    // A re-subscribe is the browser saying it is alive. Whatever went wrong
    // before this is not evidence about the subscription that exists now.
    row.failureCount = 0;

    return this.repository.save(row);
  }

  /** Forget a browser. Idempotent — unsubscribing twice is not an error. */
  async unsubscribe(endpoint: string): Promise<void> {
    await this.repository.delete({ endpoint });
  }

  /** Every subscription for one trip. */
  async forTrip(tripId: string): Promise<PushSubscription[]> {
    return this.repository.find({ where: { tripId } });
  }

  /**
   * Every subscription there is, for the job that walks them.
   *
   * Unbounded on purpose and safe for exactly one reason: this table holds one
   * row per browser that opted IN, which is a number bounded by people rather
   * than by traffic, and dead rows are deleted the first time a push service
   * answers 404 or 410 rather than left to accumulate. If that ever stops being
   * true the fix is a cursor here, not a filter — the job has to see every
   * subscriber or it silently stops notifying the ones past the limit.
   */
  async allSubscriptions(): Promise<PushSubscription[]> {
    return this.repository.find();
  }

  /**
   * Send one message to one browser.
   *
   * Returns whether it landed. The two failure kinds are handled differently and
   * the distinction is the whole of the bookkeeping: 404 and 410 mean the
   * subscription is dead and the row goes now, anything else counts toward
   * `MAX_FAILURES` and resets on the next success.
   */
  async send(
    subscription: PushSubscription,
    message: PushMessage,
  ): Promise<boolean> {
    const vapid = getVapidConfig();
    if (!vapid) return false;

    try {
      await webpush.sendNotification(
        {
          endpoint: subscription.endpoint,
          keys: { p256dh: subscription.p256dh, auth: subscription.auth },
        },
        JSON.stringify(message),
        {
          vapidDetails: {
            subject: vapid.subject,
            publicKey: vapid.publicKey,
            privateKey: vapid.privateKey,
          },
          // A notification about the next ten minutes is worthless in an hour.
          // The push service holds it this long for a phone that is offline and
          // then drops it, rather than delivering a banner about a moment that
          // has passed.
          TTL: 900,
        },
      );

      subscription.failureCount = 0;
      subscription.lastNotifiedAt = new Date();
      await this.repository.save(subscription);
      return true;
    } catch (error) {
      await this.recordFailure(subscription, error);
      return false;
    }
  }

  /**
   * What a failed send means for the row.
   *
   * `statusCode` is what `web-push` puts on its error, and the two dead codes
   * are the push services telling us the browser is gone. Believing them the
   * first time is the difference between a table that stays the size of the
   * subscriber base and one that grows forever.
   */
  private async recordFailure(
    subscription: PushSubscription,
    error: unknown,
  ): Promise<void> {
    const status = (error as { statusCode?: number })?.statusCode;

    if (status === 404 || status === 410) {
      await this.repository.delete({ id: subscription.id });
      return;
    }

    subscription.failureCount += 1;
    if (subscription.failureCount >= PushService.MAX_FAILURES) {
      this.logger.warn(
        `Dropping push subscription after ${subscription.failureCount} failures`,
      );
      await this.repository.delete({ id: subscription.id });
      return;
    }
    await this.repository.save(subscription);
  }
}
