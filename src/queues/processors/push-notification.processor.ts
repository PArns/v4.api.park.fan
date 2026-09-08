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
import { ShowFollowsService } from "../../show-follows/show-follows.service";
import { ShowsService } from "../../shows/shows.service";
import {
  dueShowNotifications,
  type FollowedShowStatus,
} from "../../show-follows/show-follow-notifications";
import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";
import { frontendShowsPath } from "../../common/utils/frontend-url.util";

/**
 * The job that decides whose phone buzzes.
 *
 * Every five minutes, two unrelated things starting soon: for each trip
 * someone subscribed to, what `dueNotifications` says the plan and the clock
 * mean is starting soon; and for each show someone follows, what
 * `dueShowNotifications` says about its next showtime. Both deciding
 * functions are pure — the question "does this wake somebody at the wrong
 * hour" is answered by a unit test rather than by watching a queue. This
 * file is the I/O around them, and the two things it owns are both about not
 * sending the same thing twice.
 *
 * The first is the WINDOW OVERLAP. Both lookaheads are wider than this five-
 * minute tick (10–20 min for a trip block, 25–35 for a show), so an event is
 * due on two or three consecutive ticks. That overlap is deliberate — at
 * exactly one tick's width a single missed run drops the notification and
 * nothing retries — and it is paid for here with a Redis key per (endpoint,
 * event). Redis is evictable on this instance, so the worst case is a
 * duplicate banner, which the message's `tag` then collapses on the device.
 * That is a bounded failure; the alternative, a Postgres table of every
 * notification ever sent, is a growing one. A ride-alert crossing has no such
 * overlap — see `RideAlertsService.checkAndNotify`, which dedupes through its
 * own `armed` column instead and does not run through this job at all.
 *
 * The second is that a trip (or a show's live data) is read ONCE per tick,
 * not once per subscriber. A family sharing a plan is four subscriptions
 * against one id; a popular show is many subscribers against one show.
 */
@Processor("push-notifications")
export class PushNotificationProcessor {
  private readonly logger = new Logger(PushNotificationProcessor.name);

  /**
   * How long a "we sent this" marker lives.
   *
   * Longer than the wider of the two lookahead windows, so the last tick that
   * still sees an event finds the marker from the first. Not much longer: an
   * event that is moved and moved back is a new one, and both dedupe keys
   * carry the specific instant, so nothing here needs to remember a day.
   */
  private static readonly SENT_TTL_SECONDS = 45 * 60;

  constructor(
    private readonly pushService: PushService,
    private readonly tripsService: TripsService,
    private readonly showFollowsService: ShowFollowsService,
    private readonly showsService: ShowsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Process("send-due-notifications")
  async handleDue(_job: Job): Promise<void> {
    // No keypair, no work. Checked here as well as at the subscribe endpoint
    // because a deploy can lose its configuration without losing its table.
    if (!isPushConfigured()) return;

    const started = Date.now();
    const tripSent = await this.handleTripNotifications(started);
    const showSent = await this.handleShowFollowNotifications(started);

    const sent = tripSent + showSent;
    if (sent > 0) {
      this.logger.log(
        `Sent ${sent} push notification(s) (${tripSent} trip, ${showSent} show-follow) in ${Date.now() - started}ms`,
      );
    }
  }

  /** The trip-planner half — unchanged behaviour, only moved out of `handleDue`. */
  private async handleTripNotifications(startedMs: number): Promise<number> {
    const byTrip = await this.subscriptionsByTrip();
    if (byTrip.size === 0) return 0;

    let sent = 0;
    for (const [tripId, subscriptions] of byTrip) {
      // One read per trip, not one per subscriber: a family sharing a plan is
      // four subscriptions against one id.
      const trip = await this.tripsService.find(tripId);
      if (!trip) continue;

      const due = dueNotifications(trip.payload, startedMs);
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
    return sent;
  }

  /**
   * The followed-shows half. Unlike a trip topic, a `ShowFollow` row IS the
   * consent — there is no topics list to check, since following a specific
   * show already says everything a subscriber wants to be told about it.
   */
  private async handleShowFollowNotifications(
    startedMs: number,
  ): Promise<number> {
    const follows = await this.showFollowsService.allFollows();
    if (follows.length === 0) return 0;

    const showIds = [...new Set(follows.map((f) => f.showId))];
    const statusByShow =
      await this.showsService.findBatchCurrentStatusByShows(showIds);

    const shows: FollowedShowStatus[] = [];
    for (const showId of showIds) {
      const liveData = statusByShow.get(showId);
      // Not operating, or no live data at all (stale >48h — see
      // findBatchCurrentStatusByShows): nothing to be due about today.
      if (!liveData || liveData.status !== LiveStatus.OPERATING) continue;
      if (!liveData.showtimes || liveData.showtimes.length === 0) continue;

      const show = liveData.show;
      const park = show?.park;
      if (!show || !park) continue;

      shows.push({
        showId,
        showName: show.name,
        parkName: park.name,
        timezone: park.timezone ?? null,
        url: frontendShowsPath(park) ?? "/",
        showtimes: liveData.showtimes,
      });
    }
    if (shows.length === 0) return 0;

    const due = dueShowNotifications(shows, startedMs);
    if (due.length === 0) return 0;

    const followsByShow = new Map<string, typeof follows>();
    for (const follow of follows) {
      const list = followsByShow.get(follow.showId) ?? [];
      list.push(follow);
      followsByShow.set(follow.showId, list);
    }

    const subscriptions = await this.pushService.findByIds(
      follows.map((f) => f.subscriptionId),
    );

    let sent = 0;
    for (const notification of due) {
      for (const follow of followsByShow.get(notification.showId) ?? []) {
        const subscription = subscriptions.get(follow.subscriptionId);
        if (!subscription) continue;
        if (
          await this.alreadySent(subscription.endpoint, notification.dedupeKey)
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
    return sent;
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
      // A ride-alert or show-follow subscription has no trip at all — not
      // this loop's concern, and `tripsService.find(null)` is not a lookup
      // that means anything.
      if (!subscription.tripId) continue;
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
