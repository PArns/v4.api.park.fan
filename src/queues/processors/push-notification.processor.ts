import { Process, Processor } from "@nestjs/bull";
import { Inject, Logger } from "@nestjs/common";
import { Job } from "bull";
import { createHash } from "crypto";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { PushService } from "../../push/push.service";
import { TripsService } from "../../trips/trips.service";
import { isPushConfigured } from "../../push/push-config";
import { dueNotifications } from "../../push/notification-planner";
import { writeMessage } from "../../push/push-messages";

/**
 * The job that decides whose phone buzzes.
 *
 * Every five minutes: for each trip that somebody subscribed to, ask
 * `dueNotifications` what the plan and the clock say is starting soon, and send
 * it once per subscriber who asked for that topic.
 *
 * The deciding is not here. `dueNotifications` is pure — plan plus a timestamp
 * in, notifications out — so the question "does this wake somebody at the wrong
 * hour" is answered by a unit test rather than by watching a queue. This file is
 * the I/O around it, and the two things it owns are both about not sending the
 * same thing twice.
 *
 * The first is the WINDOW OVERLAP. `dueNotifications` looks 10–20 minutes ahead
 * while this runs every 5, so a block is due on two or three consecutive ticks.
 * That overlap is deliberate — at exactly one tick's width a single missed run
 * drops the notification and nothing retries — and it is paid for here with a
 * Redis key per (endpoint, event). Redis is evictable on this instance, so the
 * worst case is a duplicate banner, which the message's `tag` then collapses on
 * the device. That is a bounded failure; the alternative, a Postgres table of
 * every notification ever sent, is a growing one.
 *
 * The second is that a trip is read ONCE per tick, not once per subscriber. A
 * family sharing a plan is four subscriptions against one id.
 */
@Processor("push-notifications")
export class PushNotificationProcessor {
  private readonly logger = new Logger(PushNotificationProcessor.name);

  /**
   * How long a "we sent this" marker lives.
   *
   * Longer than the lead window it guards, so the last tick that still sees an
   * event finds the marker from the first. Not much longer: a block that is
   * moved and moved back is a new event, and the key carries its start minute,
   * so nothing here needs to remember a day.
   */
  private static readonly SENT_TTL_SECONDS = 45 * 60;

  constructor(
    private readonly pushService: PushService,
    private readonly tripsService: TripsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("send-due-notifications")
  async handleDue(_job: Job): Promise<void> {
    // No keypair, no work. Checked here as well as at the subscribe endpoint
    // because a deploy can lose its configuration without losing its table.
    if (!isPushConfigured()) return;

    const started = Date.now();
    const byTrip = await this.subscriptionsByTrip();
    if (byTrip.size === 0) return;

    let sent = 0;
    for (const [tripId, subscriptions] of byTrip) {
      // One read per trip, not one per subscriber: a family sharing a plan is
      // four subscriptions against one id.
      const trip = await this.tripsService.find(tripId);
      if (!trip) continue;

      const due = dueNotifications(trip.payload, started);
      if (due.length === 0) continue;

      for (const subscription of subscriptions) {
        for (const notification of due) {
          if (!subscription.topics?.includes(notification.topic)) continue;
          if (
            await this.alreadySent(
              subscription.endpoint,
              notification.dedupeKey,
            )
          ) {
            continue;
          }
          const message = writeMessage(notification, subscription.locale);
          const ok = await this.pushService.send(subscription, message);
          if (ok) {
            await this.markSent(subscription.endpoint, notification.dedupeKey);
            sent++;
          }
        }
      }
    }

    if (sent > 0) {
      this.logger.log(
        `Sent ${sent} push notification(s) across ${byTrip.size} trip(s) in ${Date.now() - started}ms`,
      );
    }
  }

  /**
   * Subscriptions grouped by the trip they follow.
   *
   * Grouped in memory rather than with a `DISTINCT` and a second query: this
   * table is one row per browser that opted in, which is a number bounded by
   * people rather than by traffic, and the job runs twelve times an hour.
   */
  private async subscriptionsByTrip() {
    const all = await this.pushService.allSubscriptions();
    const byTrip = new Map<
      string,
      Awaited<ReturnType<PushService["allSubscriptions"]>>
    >();
    for (const subscription of all) {
      const list = byTrip.get(subscription.tripId) ?? [];
      list.push(subscription);
      byTrip.set(subscription.tripId, list);
    }
    return byTrip;
  }

  /**
   * Never throws. Redis being down must not stop notifications — a duplicate
   * banner is a smaller failure than silence, and the device's `tag` still
   * collapses the pair.
   */
  private async alreadySent(endpoint: string, key: string): Promise<boolean> {
    try {
      return (await this.redis.exists(this.sentKey(endpoint, key))) === 1;
    } catch {
      return false;
    }
  }

  private async markSent(endpoint: string, key: string): Promise<void> {
    try {
      await this.redis.set(
        this.sentKey(endpoint, key),
        "1",
        "EX",
        PushNotificationProcessor.SENT_TTL_SECONDS,
      );
    } catch {
      // Same reason as above, one layer on: failing to write the marker costs a
      // duplicate, failing the job costs everybody their notification.
    }
  }

  /**
   * The endpoint is hashed into the key rather than pasted into it.
   *
   * A push endpoint is a capability — anyone holding it can send that browser a
   * notification — and a Redis key list ends up in a support ticket far more
   * casually than a database row does.
   */
  private sentKey(endpoint: string, dedupeKey: string): string {
    return `push:sent:${hash(endpoint)}:${hash(dedupeKey)}`;
  }
}

function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}
