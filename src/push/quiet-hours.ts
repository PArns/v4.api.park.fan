import { formatInTimeZone } from "date-fns-tz";

/**
 * The hours in which this API does not push, reckoned where the subscriber is.
 *
 * Pure — no database, no push service, no `Date.now()` that is not passed in —
 * for the same reason `notification-planner.ts` is: the thing that decides
 * whether somebody's phone stays quiet at three in the morning should be
 * testable against a zone and a timestamp rather than against a running queue.
 *
 * **The zone is the subscriber's, and that is the whole point.** All three of
 * today's triggers fire inside the PARK's opening hours — `next-up` ten minutes
 * before a block, a show start before a performance, a ride alert only off an
 * `OPERATING` reading — so nothing here is about a job running through the
 * night. It is about the subscriber who is not in the park's zone: a Magic
 * Kingdom plan read on a phone in Berlin puts a 21:00 block at 03:00, and
 * `push_subscriptions.timezone` is stored per subscriber precisely because it
 * is not the park's.
 *
 * **It is the zone the browser last sent, not the zone the phone is in now**,
 * and the difference has teeth in exactly the case above. The frontend writes
 * the column when it first registers and when somebody works the switches, not
 * on every page load, so a subscriber who armed an alert at home and then
 * TRAVELLED to the park carries their home zone into it: from Berlin to
 * Orlando, 23:00-07:00 Berlin is 17:00-01:00 local, which silences the whole
 * park evening — the reverse of what this module is for. Nothing here can fix
 * that; the zone has to be refreshed where it is written, which is the
 * frontend's subscribe call (PAR-266).
 */

/**
 * 23:00 to 07:00, and the window wraps midnight.
 *
 * The obvious pair is 22:00–08:00. It was rejected (Patrick, 2026-09-15): a
 * park open until 23:00 cuts itself on a 22:00 floor, and the visitor standing
 * in it would lose the last hour of "your next block starts in ten minutes" —
 * the one hour where the notification is worth most. The 03:00 case this whole
 * module exists for is shut either way.
 */
export const QUIET_HOURS_START_HOUR = 23;
export const QUIET_HOURS_END_HOUR = 7;

/**
 * Whether it is currently quiet where this subscriber is.
 *
 * **No zone means send.** Not a guess and not a fallback to the server's clock:
 * `timezone` is nullable, the frontend writes it on every subscribe, so the
 * rows without one are old rows rather than subscribers whose zone is unknown
 * for a reason. Suppressing those would be silence with nothing to explain it —
 * a switch that is on and does nothing, which is the one failure the rest of
 * this module is arranged against. The same answer covers a zone this Node
 * build cannot resolve: `formatInTimeZone` throws on an unknown IANA name, and
 * a throw inside a five-minute job stops it notifying everybody else.
 */
export function isWithinQuietHours(
  timezone: string | null | undefined,
  nowMs: number,
): boolean {
  const hour = localHour(timezone, nowMs);
  if (hour === null) return false;
  // The window crosses midnight, so it is a union rather than a range: 23 and
  // 0..6 are quiet, 7..22 are not.
  return hour >= QUIET_HOURS_START_HOUR || hour < QUIET_HOURS_END_HOUR;
}

/**
 * The wall-clock hour at `nowMs` in `timezone`, or `null` when there is none to
 * read.
 *
 * Reading the wall clock is what makes this right across a DST change without a
 * rule for it: the same UTC instant is 06:30 in Berlin in January and 07:30 in
 * July, and the verdict follows the clock the subscriber looks at, not an
 * offset computed once.
 */
function localHour(
  timezone: string | null | undefined,
  nowMs: number,
): number | null {
  if (!timezone) return null;
  try {
    const hour = Number(formatInTimeZone(new Date(nowMs), timezone, "HH"));
    return Number.isFinite(hour) ? hour : null;
  } catch {
    return null;
  }
}
