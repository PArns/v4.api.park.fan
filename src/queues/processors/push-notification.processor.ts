import { Process, Processor } from "@nestjs/bull";
import { Inject, Logger } from "@nestjs/common";
import { Job } from "bull";
import { createHash } from "crypto";
import { Redis } from "ioredis";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { PushService } from "../../push/push.service";
import { TripsService } from "../../trips/trips.service";
import { isPushConfigured } from "../../push/push-config";
import { dueNotifications } from "../../push/notification-planner";
import { writeMessage } from "../../push/push-messages";
import { ShowFollowsService } from "../../show-follows/show-follows.service";
import { ShowFollow } from "../../show-follows/entities/show-follow.entity";
import { ShowsService } from "../../shows/shows.service";
import {
  dueShowNotifications,
  type DueShowNotification,
  type FollowedShowStatus,
} from "../../show-follows/show-follow-notifications";
import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";
import { frontendShowsPath } from "../../common/utils/frontend-url.util";
import {
  formatInParkTimezone,
  getTomorrowDateInTimezoneAt,
} from "../../common/utils/date.util";
import { PushSubscription } from "../../push/entities/push-subscription.entity";

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
 * minute tick (10–20 min for a trip block, 25–35 and again 8–14 for a
 * show — see `SHOW_LATE_LEAD_MIN`), so an event is
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

  /**
   * How many show-follow sends to run concurrently — same value and same
   * reasoning as `RideAlertsService.SEND_BATCH_SIZE`: a popular show
   * followed by hundreds of browsers must not turn into hundreds of
   * concurrent outbound HTTPS calls.
   */
  private static readonly SEND_BATCH_SIZE = 20;

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
    // Isolated, like `WaitTimesProcessor` isolates one park's failure from
    // the rest of its cycle: a throw in one half (a DB error listing trips,
    // a hypertable query erroring on the show side) must not also silence
    // the other half's notifications for this tick, and must not fail the
    // Bull job into an `attempts: 3` retry that resends whichever half DID
    // succeed the first time. Run concurrently, not sequentially — the two
    // halves touch unrelated tables and neither result depends on the
    // other, so awaiting them one after another only ever cost wall-clock
    // time on a five-minute cron with no other reason to serialize them.
    const [tripSent, showSent] = await Promise.all([
      this.runHalf("trip", () => this.handleTripNotifications(started)),
      this.runHalf("show-follow", () =>
        this.handleShowFollowNotifications(started),
      ),
    ]);

    const sent = tripSent + showSent;
    if (sent > 0) {
      this.logger.log(
        `Sent ${sent} push notification(s) (${tripSent} trip, ${showSent} show-follow) in ${Date.now() - started}ms`,
      );
    }
  }

  private async runHalf(
    label: string,
    fn: () => Promise<number>,
  ): Promise<number> {
    try {
      return await fn();
    } catch (error) {
      this.logger.error(
        `${label} push notifications failed: ${(error as Error)?.message ?? error}`,
      );
      return 0;
    }
  }

  /** The trip-planner half — unchanged behaviour, only moved out of `handleDue`. */
  private async handleTripNotifications(startedMs: number): Promise<number> {
    const byTrip = await this.subscriptionsByTrip();
    if (byTrip.size === 0) return 0;

    // Its own try/catch, not just `runHalf`'s outer one: a throw partway
    // through (one bad trip's payload, a Redis blip on the tenth iteration)
    // used to discard every `sent++` from the iterations that already
    // succeeded, since `runHalf`'s catch answers a flat 0 — reporting a
    // cycle that sent nine notifications as having sent none, in the one
    // log line that ever states how many the job wrote to a phone.
    let sent = 0;
    try {
      for (const [tripId, subscriptions] of byTrip) {
        // One read per trip, not one per subscriber: a family sharing a plan
        // is four subscriptions against one id.
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
              await this.markSent(
                subscription.endpoint,
                notification.dedupeKey,
              );
              sent++;
            }
          }
        }
      }
    } catch (error) {
      this.logger.error(
        `trip push notifications failed after sending ${sent}: ${(error as Error)?.message ?? error}`,
      );
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
    const shows = await this.followedShowsDueToday(showIds, startedMs);
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

    const tasks: Array<{
      notification: DueShowNotification;
      follow: ShowFollow;
    }> = [];
    for (const notification of due) {
      for (const follow of followsByShow.get(notification.showId) ?? []) {
        // A follow that named a performance is about THAT one and no other.
        // Somebody who picked the 19:10 badge at 17:00 did so because the
        // 17:30 one is no use to them; sending it anyway would be the app
        // overruling a choice it asked for. A null `startTime` is the
        // open-ended follow — whichever performance is next — which is what
        // a card's bell files and what every row written before the column
        // existed still means.
        if (
          follow.startTime &&
          follow.startTime.toISOString() !== notification.startTime
        ) {
          continue;
        }
        tasks.push({ notification, follow });
      }
    }

    // Batched like `RideAlertsService.sendTrigger`, not one at a time and
    // not all at once: a popular show followed by hundreds of browsers must
    // not turn into hundreds of concurrent outbound HTTPS calls, nor stall
    // behind them one at a time for minutes. `Promise.allSettled` rather
    // than `Promise.all` because one follower's failed send (a push service
    // having a bad minute) must not stop the rest of the batch — the same
    // reason `sendShowFollowNotification` never throws.
    let sent = 0;
    for (
      let i = 0;
      i < tasks.length;
      i += PushNotificationProcessor.SEND_BATCH_SIZE
    ) {
      const batch = tasks.slice(
        i,
        i + PushNotificationProcessor.SEND_BATCH_SIZE,
      );
      const results = await Promise.allSettled(
        batch.map((task) =>
          this.sendShowFollowNotification(
            task.notification,
            task.follow,
            subscriptions,
          ),
        ),
      );
      for (const result of results) {
        if (result.status === "fulfilled") {
          if (result.value) sent++;
        } else {
          this.logger.warn(
            `show-follow send failed: ${(result.reason as Error)?.message ?? result.reason}`,
          );
        }
      }
    }
    return sent;
  }

  /** One follower's send. Never throws — a failure here must not stop the rest of the batch. */
  private async sendShowFollowNotification(
    notification: DueShowNotification,
    follow: ShowFollow,
    subscriptions: Map<string, PushSubscription>,
  ): Promise<boolean> {
    const subscription = subscriptions.get(follow.subscriptionId);
    if (!subscription) return false;
    if (await this.alreadySent(subscription.endpoint, notification.dedupeKey)) {
      return false;
    }
    const message = writeMessage(notification, subscription.locale);
    const sent = await this.pushService.send(subscription, message);
    if (sent) {
      await this.markSent(subscription.endpoint, notification.dedupeKey);
    }
    return sent;
  }

  /**
   * Followed shows with a VERIFIED showtime for today — never
   * `findBatchCurrentStatusByShows`'s own `showtimes`, which
   * `projectShowtimesToToday` remaps onto today's date whatever the
   * original date actually was (ThemeParks.wiki still serves entries from
   * 2022) and which has no park-schedule check, so a park shut today but
   * still showing yesterday's OPERATING row would keep notifying about a
   * performance that is not happening. `ShowsService.getShowtimesOnDate`
   * is the one reader that checks a showtime against its OWN embedded date
   * instead of projecting it, so it is used here for the showtimes
   * themselves — `findBatchCurrentStatusByShows` is kept only for the
   * show/park metadata (name, timezone) that reader does not return.
   */
  private async followedShowsDueToday(
    showIds: string[],
    startedMs: number,
  ): Promise<FollowedShowStatus[]> {
    const statusByShow =
      await this.showsService.findBatchCurrentStatusByShows(showIds);

    interface ShowMeta {
      showId: string;
      showName: string;
      parkName: string;
      timezone: string;
      url: string;
      parkId: string;
    }
    const metaByShow = new Map<string, ShowMeta>();
    const timezoneByPark = new Map<string, string>();
    for (const showId of showIds) {
      const liveData = statusByShow.get(showId);
      // Not operating, or no live data at all (stale >48h — see
      // findBatchCurrentStatusByShows): nothing to be due about today.
      if (!liveData || liveData.status !== LiveStatus.OPERATING) continue;
      const show = liveData.show;
      const park = show?.park;
      // No timezone, no verified date to check a showtime against — same
      // "unknown means skip" rule `dueShowNotifications` applies further on.
      if (!show || !park || !park.timezone) continue;

      metaByShow.set(showId, {
        showId,
        showName: show.name,
        parkName: park.name,
        timezone: park.timezone,
        url: frontendShowsPath(park) ?? "/",
        parkId: park.id,
      });
      timezoneByPark.set(park.id, park.timezone);
    }
    if (metaByShow.size === 0) return [];

    // Two `getShowtimesOnDate` calls per distinct PARK, not per show — a
    // popular park with many followed shows shares them. Both today's AND
    // tomorrow's date: a showtime in the first ~35 minutes after local
    // midnight has a lead window that opens BEFORE that midnight, while
    // `todayStr` at that moment still names the day before it — querying
    // only "today" means the one tick where such a showtime is due asks the
    // wrong day and never finds it.
    //
    // All of them fired at once, not one park-date at a time: each query is
    // independent (a different park, a different date) and nothing here
    // reads `verifiedTimesByShow` before every write to it, so awaiting them
    // one by one only serialized round trips that had no reason to wait on
    // each other. Merged rather than overwritten once a query resolves,
    // since each date can contribute showtimes for the same show — safe
    // without a lock because every write below runs to completion with no
    // `await` in between the read and the `.set()`, so two results can never
    // interleave mid-merge.
    const dateQueries = [...timezoneByPark].flatMap(([parkId, timezone]) => {
      const todayStr = formatInParkTimezone(new Date(startedMs), timezone);
      const tomorrowStr = getTomorrowDateInTimezoneAt(startedMs, timezone);
      return [todayStr, tomorrowStr].map((dateStr) => ({
        parkId,
        timezone,
        dateStr,
      }));
    });

    const verifiedTimesByShow = new Map<string, string[]>();
    await Promise.all(
      dateQueries.map(async ({ parkId, timezone, dateStr }) => {
        let timesByShow: Map<string, string[]>;
        try {
          timesByShow = await this.showsService.getShowtimesOnDate(
            parkId,
            timezone,
            dateStr,
          );
        } catch (error) {
          this.logger.warn(
            `getShowtimesOnDate failed for park ${parkId} on ${dateStr}: ${(error as Error)?.message ?? error}`,
          );
          return;
        }
        for (const [showId, hhmmTimes] of timesByShow) {
          if (!metaByShow.has(showId)) continue; // a show in this park nobody follows
          const isoTimes = verifiedTimesByShow.get(showId) ?? [];
          for (const hhmm of hhmmTimes) {
            try {
              const instant = fromZonedTime(`${dateStr}T${hhmm}:00`, timezone);
              // A local wall-clock time a spring-forward transition skips
              // (e.g. 02:30 on the one day the clock jumps 02:00 -> 03:00)
              // has no real instant at all — `fromZonedTime` still returns
              // one, silently shifted by the DST offset, which round-trips
              // to a DIFFERENT local time than the one asked for. Caught
              // here rather than sent: a show cannot start at a time that
              // did not happen.
              const roundTrip = formatInTimeZone(
                instant,
                timezone,
                "yyyy-MM-dd'T'HH:mm",
              );
              if (roundTrip !== `${dateStr}T${hhmm}`) continue;
              isoTimes.push(instant.toISOString());
            } catch {
              // A malformed time from the aggregate query — skip just this one.
            }
          }
          verifiedTimesByShow.set(showId, isoTimes);
        }
      }),
    );

    const shows: FollowedShowStatus[] = [];
    for (const meta of metaByShow.values()) {
      const isoTimes = verifiedTimesByShow.get(meta.showId);
      if (!isoTimes || isoTimes.length === 0) continue;
      shows.push({
        showId: meta.showId,
        showName: meta.showName,
        parkName: meta.parkName,
        timezone: meta.timezone,
        url: meta.url,
        showtimes: isoTimes.map((startTime) => ({ startTime })),
      });
    }
    return shows;
  }

  /**
   * Subscriptions grouped by the trip they follow.
   *
   * Grouped in memory rather than with a `DISTINCT` and a second query: this
   * table is one row per browser that opted in, which is a number bounded by
   * people rather than by traffic, and the job runs twelve times an hour.
   * `subscriptionsWithTrip` already scopes to `tripId IS NOT NULL` at the
   * query — a ride-alert or show-follow-only row never reaches this method
   * at all, rather than arriving and being discarded a line later.
   */
  private async subscriptionsByTrip() {
    const all = await this.pushService.subscriptionsWithTrip();
    const byTrip = new Map<
      string,
      Awaited<ReturnType<PushService["subscriptionsWithTrip"]>>
    >();
    for (const subscription of all) {
      // Defensive, not the real filter (the query above is): `tripId` is
      // typed nullable on the entity, so this just narrows the type for the
      // `Map<string, …>` key below — `tripsService.find(null)` is not a
      // lookup that means anything.
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
