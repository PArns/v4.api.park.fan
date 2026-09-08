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

/** How far ahead of a showtime the notification goes out. */
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
      if (leadMin < SHOW_LEAD_MIN || leadMin > SHOW_LEAD_MAX_MIN) continue;

      let atTime: string;
      try {
        atTime = formatInTimeZone(new Date(startMs), show.timezone, "HH:mm");
      } catch {
        // An unknown IANA zone. Same answer as no zone at all.
        continue;
      }

      out.push({
        showId: show.showId,
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
