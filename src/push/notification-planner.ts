import { formatInTimeZone } from "date-fns-tz";
import type { PushTopic } from "./push-config";

/**
 * What a stored plan and the clock together say is worth a notification.
 *
 * Pure — no database, no push service, no `Date.now()` that is not passed in —
 * so the thing that decides whether somebody's phone buzzes can be tested
 * against a plan and a timestamp rather than against a running queue.
 *
 * It reads the plan defensively at every level. The payload is stored verbatim
 * from a browser (see `trip-payload.util.ts`, which checks a skeleton and
 * deliberately not a schema), so anything below that skeleton may be missing,
 * may be the wrong type, and may be from a version of the planner this deploy
 * has never seen. A notification job that throws on one malformed trip stops
 * notifying everyone.
 */

/** How far ahead of a block the notification goes out. */
export const LEAD_MIN = 10;

/**
 * The far edge of the window.
 *
 * Wider than the job's five-minute tick on purpose: at exactly one tick's width
 * a single missed run — a deploy, a slow queue — drops the notification
 * entirely and nothing ever retries it. The overlap costs a duplicate, which
 * `dedupeKey` and the notification's `tag` between them absorb.
 */
export const LEAD_MAX_MIN = 20;

export interface PlannedNotification {
  topic: PushTopic;
  /**
   * Stable for one event across ticks.
   *
   * This is what stops the overlapping window sending the same thing twice —
   * the entry's own id plus its day, so moving a block genuinely is a new event
   * and re-running the job is not.
   */
  dedupeKey: string;
  /** Filled in per subscriber's language by `push-messages.ts`. */
  parkName: string;
  /** What is starting: a ride's name, or the label on a free block. */
  what: string;
  /** Minutes from now until it starts, already rounded to the nearest five. */
  inMinutes: number;
  /** Park-local `HH:mm`. */
  atTime: string;
  /** Where a tap lands, relative to the site's origin. */
  url: string;
}

interface PlanEntry {
  id: string;
  startMinute: number;
  attractionName?: string;
  attractionSlug?: string;
  done?: boolean;
  custom?: { label?: string };
}

/**
 * Blocks starting soon, across every park in the plan.
 *
 * "Soon" is measured in the PARK's timezone and against the park's own day,
 * which is the whole reason the plan stores a zone per park: a trip holding
 * Phantasialand and Magic Kingdom is on two different dates at 23:00 in Berlin,
 * and a job reckoning in UTC would notify about the wrong day for one of them
 * every single evening.
 *
 * A ticked-off block never notifies. Neither does one already under way — the
 * window is ahead of the start, so a block whose time has passed is behind us
 * and a banner about it is an interruption about nothing.
 */
export function dueNotifications(
  payload: unknown,
  nowMs: number,
): PlannedNotification[] {
  const parks = readParks(payload);
  const out: PlannedNotification[] = [];

  for (const park of parks) {
    const timezone = park.timezone;
    // No zone, no notification. The alternative is reckoning in the server's or
    // in UTC, and a banner at the wrong hour is worse than no banner — this is
    // the one place in the planner where being a few hours out wakes somebody.
    if (!timezone) continue;

    let today: string;
    let nowMinute: number;
    try {
      today = formatInTimeZone(new Date(nowMs), timezone, "yyyy-MM-dd");
      const hh = Number(formatInTimeZone(new Date(nowMs), timezone, "HH"));
      const mm = Number(formatInTimeZone(new Date(nowMs), timezone, "mm"));
      nowMinute = hh * 60 + mm;
    } catch {
      // An unknown IANA zone. Same answer as no zone at all.
      continue;
    }

    const day = park.days[today];
    if (!day) continue;

    for (const entry of day.entries) {
      if (entry.done) continue;
      const lead = entry.startMinute - nowMinute;
      if (lead < LEAD_MIN || lead > LEAD_MAX_MIN) continue;

      const what =
        entry.custom?.label?.trim() || entry.attractionName?.trim() || null;
      // Nothing to name. A notification reading "Als Nächstes:" with an empty
      // subject is worse than silence.
      if (!what) continue;

      out.push({
        topic: "next-up",
        dedupeKey: `next-up:${park.slug}:${today}:${entry.id}:${entry.startMinute}`,
        parkName: park.name || park.slug,
        what,
        inMinutes: Math.round(lead / 5) * 5,
        atTime: formatMinute(entry.startMinute),
        url: "/",
      });
    }
  }

  // Earliest first, so a tick that finds two blocks notifies about the nearer
  // one first and the phone's own stacking puts it on top.
  return out.sort((a, b) => a.inMinutes - b.inMinutes);
}

interface PlanPark {
  slug: string;
  name: string;
  timezone: string | null;
  days: Record<string, { entries: PlanEntry[] }>;
}

/**
 * The plan, as far as this module trusts it.
 *
 * Every level is checked and anything malformed is skipped rather than thrown
 * over: one trip written by a browser extension, an old planner version, or a
 * half-finished migration must not stop the job notifying everybody else.
 */
function readParks(payload: unknown): PlanPark[] {
  if (!isObject(payload)) return [];
  const parks = payload.parks;
  if (!isObject(parks)) return [];

  const out: PlanPark[] = [];
  for (const raw of Object.values(parks)) {
    if (!isObject(raw)) continue;
    const slug = typeof raw.slug === "string" ? raw.slug : null;
    if (!slug) continue;

    const days: PlanPark["days"] = {};
    if (isObject(raw.days)) {
      for (const [key, rawDay] of Object.entries(raw.days)) {
        if (!isObject(rawDay)) continue;
        const entries = Array.isArray(rawDay.entries) ? rawDay.entries : [];
        days[key] = { entries: entries.filter(isPlanEntry) };
      }
    }

    out.push({
      slug,
      name: typeof raw.name === "string" ? raw.name : slug,
      timezone: typeof raw.timezone === "string" ? raw.timezone : null,
      days,
    });
  }
  return out;
}

function isPlanEntry(value: unknown): value is PlanEntry {
  return (
    isObject(value) &&
    typeof value.id === "string" &&
    typeof value.startMinute === "number" &&
    Number.isFinite(value.startMinute)
  );
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Park-local minutes since midnight as `HH:mm`, past midnight folded back. */
function formatMinute(minute: number): string {
  const wrapped = ((Math.round(minute) % 1440) + 1440) % 1440;
  const hh = String(Math.floor(wrapped / 60)).padStart(2, "0");
  const mm = String(wrapped % 60).padStart(2, "0");
  return `${hh}:${mm}`;
}
