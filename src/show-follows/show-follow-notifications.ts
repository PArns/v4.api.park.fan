import { formatInTimeZone } from "date-fns-tz";
import type { ScheduledStartCopy } from "../push/push-messages";

/**
 * Deciding which followed shows start soon enough to notify about.
 *
 * Pure, mirroring `notification-planner.ts`'s `dueNotifications` — but
 * simpler, because the caller (`PushNotificationProcessor.followedShowsDueToday`)
 * has already turned each `startTime` into a full ISO instant for today
 * before this function ever sees it, verified against `ShowsService
 * .getShowtimesOnDate` rather than taken from `projectShowtimesToToday`
 * (which remaps a showtime onto today's date whatever the original date
 * actually was, with no check that the park is even open today). Comparing
 * two absolute instants needs no timezone at all — the zone is only read
 * here to FORMAT the display time (`atTime`) in the park's local clock, not
 * to decide whether a showtime is due.
 */

/**
 * How far ahead of a showtime the notification goes out — the first of two
 * windows, and the one almost every follower is served by. The second is
 * {@link SHOW_LATE_LEAD_MIN}, for a follow made after this one had passed.
 */
export const SHOW_LEAD_MIN = 25;

/**
 * The far edge of the window — ten minutes wide, same shape as
 * `LEAD_MIN`/`LEAD_MAX_MIN`: wider than the five-minute cron tick this runs
 * on, so one missed run does not drop the notification entirely, and the
 * overlap is absorbed by `dedupeKey` (here, the showtime's own ISO string is
 * already unique, so no separate "today" component is needed the way
 * `dueNotifications`'s minutes-since-midnight key requires one).
 */
export const SHOW_LEAD_MAX_MIN = 35;

/**
 * A second, later window, for a show followed too late for the first one.
 *
 * Somebody standing in the park who taps the bell twenty minutes before a
 * performance is past {@link SHOW_LEAD_MAX_MIN} the moment they tap it: the
 * window they needed closed before the follow existed, and the next one is
 * tomorrow's performance. They asked to be reminded about a show starting
 * soon and got nothing at all, which is the one outcome this feature exists
 * to prevent.
 *
 * It is a plain second window rather than a per-follow lead time, and the
 * reason is `sendShowFollowNotification`'s dedupe: the marker is keyed by
 * (endpoint, {@link DueShowNotification.dedupeKey}) and the key is the
 * showtime's own instant, identical in both windows. So a follower already
 * notified at half an hour is silently skipped here, and only the ones who
 * were never told — because they followed too late, or because their send
 * failed — are reached. The alternative, filtering by each follow's
 * `createdAt`, would need this pure function to know about rows it has
 * deliberately never seen.
 *
 * Six minutes wide, like the first window and for the same reason: wider
 * than the five-minute cron tick, so a late or missed run cannot drop the
 * notification. It stops well short of the first window's near edge — the
 * gap between them is nobody's loss, since a follow landing in it is caught
 * here on the way down.
 */
export const SHOW_LATE_LEAD_MIN = 8;
export const SHOW_LATE_LEAD_MAX_MIN = 14;

/** Whether a lead falls in either send window. */
function isDueLead(leadMin: number): boolean {
  const standard = leadMin >= SHOW_LEAD_MIN && leadMin <= SHOW_LEAD_MAX_MIN;
  const late =
    leadMin >= SHOW_LATE_LEAD_MIN && leadMin <= SHOW_LATE_LEAD_MAX_MIN;
  return standard || late;
}

export interface FollowedShowtime {
  /** Full ISO instant, already projected onto the park's "today". */
  startTime: string;
}

export interface FollowedShowStatus {
  showId: string;
  showName: string;
  parkName: string;
  /** The SHOW's park's zone. Unknown means skip, never default to UTC — a
   *  notification with the wrong clock time is worse than none. */
  timezone: string | null;
  /** Where a tap should land. Relative to the site's origin. */
  url: string;
  showtimes: FollowedShowtime[];
}

export interface DueShowNotification extends ScheduledStartCopy {
  /** Which show this is about — the caller resolves subscribers by this. */
  showId: string;
  /**
   * The exact performance, as the ISO instant it was resolved from.
   *
   * The caller needs it to honour a follow that named a showtime of its own:
   * `dedupeKey` already carries the same string, but reading it back out of a
   * formatted key would be parsing our own output. A follow with a null
   * `startTime` ignores this and takes whichever performance comes next.
   */
  startTime: string;
}

/**
 * Followed shows starting soon, across every show anybody follows.
 *
 * A show with no timezone, or one this deploy cannot format, is skipped
 * entirely — same rule as `dueNotifications`, for the same reason: a banner
 * naming the wrong minute is worse than no banner. A malformed `startTime` is
 * skipped rather than thrown over, since one bad row from an upstream feed
 * must not stop the job notifying about every other show.
 */
export function dueShowNotifications(
  shows: FollowedShowStatus[],
  nowMs: number,
): DueShowNotification[] {
  const out: DueShowNotification[] = [];

  for (const show of shows) {
    if (!show.timezone) continue;

    for (const showtime of show.showtimes) {
      const startMs = Date.parse(showtime.startTime);
      if (!Number.isFinite(startMs)) continue;

      const leadMin = (startMs - nowMs) / 60_000;
      if (!isDueLead(leadMin)) continue;

      let atTime: string;
      try {
        atTime = formatInTimeZone(new Date(startMs), show.timezone, "HH:mm");
      } catch {
        // An unknown IANA zone. Same answer as no zone at all.
        continue;
      }

      out.push({
        showId: show.showId,
        startTime: showtime.startTime,
        dedupeKey: `show-start:${show.showId}:${showtime.startTime}`,
        parkName: show.parkName,
        what: show.showName,
        inMinutes: Math.round(leadMin / 5) * 5,
        atTime,
        url: show.url,
      });
    }
  }

  // Earliest first, same reasoning as dueNotifications: a tick that finds two
  // shows due notifies about the nearer one first.
  return out.sort((a, b) => a.inMinutes - b.inMinutes);
}
