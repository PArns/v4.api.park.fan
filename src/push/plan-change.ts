import { formatInTimeZone } from "date-fns-tz";
import {
  formatMinute,
  readParks,
  type PlanEntry,
} from "./notification-planner";

/**
 * `plan-change`: something a stored plan was built around changed.
 *
 * Pure, like `notification-planner.ts`: the service hands in what it read
 * (schedule, `/plan/day`, live status) and gets back facts, changes and the
 * next baseline. The baseline is what the visitor last heard about the day —
 * the facts when watching started, then the facts of each sent digest — so a
 * slow drift adds up to a change, a blip that reverts before the next digest
 * is allowed never reaches anybody, and nothing is announced twice.
 *
 * The rules, each against that baseline:
 * - (a) **hours**: the day's published opening or closing time moves, or the
 *   day turns CLOSED (or back). Unknown hours on either side say nothing.
 * - (b) **shows**: today only, because only today has the operator's own
 *   showtimes (`source: "scheduled"`); a projection is not a promise. A
 *   planned performance no longer in the day's times has moved to the nearest
 *   one; a show that had scheduled times and dropped out of a day that still
 *   lists other scheduled shows is cancelled.
 * - (c) **rides**: today, while the park is open, a planned ride not yet ridden
 *   goes from running to not running or back. Read like a reopen alert: any
 *   non-OPERATING standby row is "down", only a fresh non-heartbeat OPERATING
 *   row is "open", anything else is unknown.
 * - (d) **waits**: up to `WAIT_DAYS_AHEAD` days out, the forecast wait at a
 *   planned ride's slot moves by at least 15 minutes AND 30 % of what it was.
 *
 * (a)–(c) may send every `URGENT_GAP_MS`; a digest of waits alone at most every
 * `WAIT_GAP_MS`, so a forecast never becomes a stream.
 */

/** How far ahead a plan day is watched at all (hours, and nothing else that far out). */
export const PLAN_CHANGE_DAYS_AHEAD = 120;
/** How far ahead (d) reads forecasts: further out a forecast moves with every retrain. */
export const WAIT_DAYS_AHEAD = 7;
export const WAIT_MIN_DELTA = 15;
export const WAIT_MIN_RATIO = 0.3;
/** Least time between two digests of the same plan day. */
export const URGENT_GAP_MS = 30 * 60_000;
/** Least time before a digest that holds only wait changes. */
export const WAIT_GAP_MS = 3 * 60 * 60_000;
/** A standby reading older than this does not prove a ride is running. */
export const FRESH_READING_MS = 15 * 60_000;
/** A planned ride whose slot is this far behind is no longer reported on. */
const PAST_SLOT_GRACE_MIN = 15;

export interface PlanDayToWatch {
  slug: string;
  name: string;
  timezone: string;
  geo: { continent: string; country: string; city: string };
  /** Park-local `yyyy-MM-dd`. */
  date: string;
  isToday: boolean;
  /** Days from the park's today. */
  daysAhead: number;
  /** Park-local minute now; only meaningful when `isToday`. */
  nowMinute: number;
  entries: PlanEntry[];
}

/**
 * The plan days worth watching: the park's today and later, up to
 * `PLAN_CHANGE_DAYS_AHEAD`, each with something in it. Like the other trip
 * topics, a park without zone or geo path is skipped.
 */
export function planDaysToWatch(
  payload: unknown,
  nowMs: number,
): PlanDayToWatch[] {
  const out: PlanDayToWatch[] = [];
  for (const park of readParks(payload)) {
    if (!park.timezone || !park.geo) continue;
    let today: string;
    let nowMinute: number;
    try {
      today = formatInTimeZone(new Date(nowMs), park.timezone, "yyyy-MM-dd");
      const [hh, mm] = formatInTimeZone(new Date(nowMs), park.timezone, "HH:mm")
        .split(":")
        .map(Number);
      nowMinute = hh * 60 + mm;
    } catch {
      continue;
    }
    for (const [date, day] of Object.entries(park.days)) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || day.entries.length === 0) {
        continue;
      }
      const daysAhead = daysBetween(today, date);
      if (daysAhead < 0 || daysAhead > PLAN_CHANGE_DAYS_AHEAD) continue;
      out.push({
        slug: park.slug,
        name: park.name || park.slug,
        timezone: park.timezone,
        geo: park.geo,
        date,
        isToday: daysAhead === 0,
        daysAhead,
        nowMinute,
        entries: day.entries,
      });
    }
  }
  return out.sort((a, b) => a.date.localeCompare(b.date));
}

function daysBetween(from: string, to: string): number {
  return Math.round(
    (Date.parse(`${to}T12:00:00Z`) - Date.parse(`${from}T12:00:00Z`)) /
      86_400_000,
  );
}

/** What was known about a plan day at one moment. A missing key is "unknown". */
export interface PlanDayFacts {
  /** `"HH:mm–HH:mm"` park-local, or `"closed"`. */
  hours?: string;
  /** Per show entry id: the nearest time the show runs (`HH:mm`), or `"cancelled"`. */
  shows: Record<string, string>;
  /** Per ride entry id. */
  rides: Record<string, "open" | "down">;
  /** Per ride entry id: forecast wait at the planned slot, minutes. */
  waits: Record<string, number>;
}

export interface ScheduleFact {
  scheduleType: string;
  openingTime: Date | string | null;
  closingTime: Date | string | null;
}

/** The parts of `/plan/day` this reads. */
export interface PlanDayInput {
  shows: Array<{ showSlug: string; times: string[]; source: string }>;
  rides: Array<{
    attractionSlug: string;
    attractionId: string;
    hours: Array<{ hour: number; wait: number }>;
    slots?: Array<{ minute: number; wait: number }>;
  }>;
}

export interface StandbyReading {
  status: string;
  isHeartbeat: boolean;
  timestampMs: number;
}

export interface FactsInput {
  day: PlanDayToWatch;
  nowMs: number;
  schedule: ScheduleFact[];
  /** Null when not read (not needed for this day, or it failed). */
  planDay: PlanDayInput | null;
  /** Standby readings by attraction id; read only for today. */
  standby: Map<string, StandbyReading>;
  previous: PlanDayFacts | null;
}

export function buildPlanDayFacts(input: FactsInput): PlanDayFacts {
  const { day, nowMs, planDay } = input;
  const facts: PlanDayFacts = { shows: {}, rides: {}, waits: {} };

  // (a) The day's own hours: earliest OPERATING opening to latest closing.
  const operating = input.schedule
    .filter((e) => e.scheduleType === "OPERATING")
    .flatMap((e) => {
      const open = toMs(e.openingTime);
      const close = toMs(e.closingTime);
      return open !== null && close !== null ? [{ open, close }] : [];
    });
  let openMs: number | null = null;
  let closeMs: number | null = null;
  if (operating.length > 0) {
    openMs = Math.min(...operating.map((o) => o.open));
    closeMs = Math.max(...operating.map((o) => o.close));
    facts.hours = `${clock(openMs, day.timezone)}–${clock(closeMs, day.timezone)}`;
  } else if (input.schedule.some((e) => e.scheduleType === "CLOSED")) {
    facts.hours = "closed";
  }

  const ahead = (e: PlanEntry) =>
    !day.isToday || e.startMinute >= day.nowMinute - PAST_SLOT_GRACE_MIN;
  const pending = day.entries.filter((e) => !e.done && ahead(e));
  const rides = pending.filter((e) => e.attractionSlug && !e.showSlug);
  const rideOf = (e: PlanEntry) =>
    planDay?.rides.find((r) => r.attractionSlug === e.attractionSlug);

  // (b) Shows, today, against the operator's own times.
  if (day.isToday && planDay) {
    const scheduled = planDay.shows.filter(
      (s) => s.source === "scheduled" && s.times.length > 0,
    );
    for (const entry of pending.filter((e) => e.showSlug)) {
      const show = scheduled.find((s) => s.showSlug === entry.showSlug);
      if (show) {
        facts.shows[entry.id] = nearestTime(show.times, entry.startMinute);
      } else if (
        scheduled.length > 0 &&
        input.previous?.shows[entry.id] !== undefined &&
        input.previous.shows[entry.id] !== "cancelled"
      ) {
        facts.shows[entry.id] = "cancelled";
      }
    }
  }

  // (c) Rides, today, while the park is open.
  const parkOpen =
    openMs !== null && closeMs !== null && nowMs >= openMs && nowMs < closeMs;
  if (day.isToday && parkOpen && planDay) {
    for (const entry of rides) {
      const ride = rideOf(entry);
      const reading = ride ? input.standby.get(ride.attractionId) : undefined;
      if (!reading) continue;
      if (reading.status !== "OPERATING") facts.rides[entry.id] = "down";
      else if (
        !reading.isHeartbeat &&
        nowMs - reading.timestampMs <= FRESH_READING_MS
      ) {
        facts.rides[entry.id] = "open";
      }
    }
  }

  // (d) Forecast waits at the planned slot.
  if (planDay && day.daysAhead <= WAIT_DAYS_AHEAD) {
    for (const entry of rides) {
      if (day.isToday && entry.startMinute <= day.nowMinute) continue;
      const ride = rideOf(entry);
      if (!ride) continue;
      const wait = waitAt(ride, entry.startMinute);
      if (wait !== null) facts.waits[entry.id] = wait;
    }
  }

  return facts;
}

function waitAt(ride: PlanDayInput["rides"][number], minute: number) {
  const slot = (ride.slots ?? [])
    .filter((s) => s.minute <= minute)
    .sort((a, b) => b.minute - a.minute)[0];
  if (slot && Number.isFinite(slot.wait)) return Math.round(slot.wait);
  const hour = ride.hours.find((h) => h.hour === Math.floor(minute / 60));
  return hour && Number.isFinite(hour.wait) ? Math.round(hour.wait) : null;
}

/** The time in `times` nearest to the planned minute, on the 24-hour circle. */
function nearestTime(times: string[], plannedMinute: number): string {
  const planned = ((plannedMinute % 1440) + 1440) % 1440;
  let best = times[0];
  let bestGap = Infinity;
  for (const t of times) {
    const [h, m] = t.split(":").map(Number);
    const raw = Math.abs(h * 60 + m - planned);
    const gap = Math.min(raw, 1440 - raw);
    if (gap < bestGap) {
      best = t;
      bestGap = gap;
    }
  }
  return best;
}

export type PlanChange =
  | { type: "hours"; from: string; to: string }
  | { type: "closed" }
  | { type: "show-moved"; name: string; to: string }
  | { type: "show-cancelled"; name: string }
  | { type: "ride-down"; name: string }
  | { type: "ride-up"; name: string }
  | { type: "wait"; name: string; at: string; from: number; to: number };

/** What changed against the baseline, in the order a digest lists it. */
export function planChanges(
  base: PlanDayFacts | null,
  cur: PlanDayFacts,
  entries: PlanEntry[],
): PlanChange[] {
  const out: PlanChange[] = [];
  const byId = new Map(entries.map((e) => [e.id, e]));
  const nameOf = (id: string) => {
    const e = byId.get(id);
    return e?.custom?.label?.trim() || e?.attractionName?.trim() || null;
  };

  if (base?.hours && cur.hours && base.hours !== cur.hours) {
    out.push(
      cur.hours === "closed"
        ? { type: "closed" }
        : { type: "hours", from: base.hours, to: cur.hours },
    );
  }

  for (const [id, now] of Object.entries(cur.shows)) {
    const before = base?.shows[id];
    const entry = byId.get(id);
    const name = nameOf(id);
    if (!entry || !name || now === before) continue;
    // First sight of a show running at the planned time is the plan, not news.
    if (before === undefined && now === formatMinute(entry.startMinute)) {
      continue;
    }
    out.push(
      now === "cancelled"
        ? { type: "show-cancelled", name }
        : { type: "show-moved", name, to: now },
    );
  }

  for (const [id, now] of Object.entries(cur.rides)) {
    const before = base?.rides[id];
    const name = nameOf(id);
    if (!name || before === undefined || before === now) continue;
    out.push({ type: now === "down" ? "ride-down" : "ride-up", name });
  }

  for (const [id, now] of Object.entries(cur.waits)) {
    const before = base?.waits[id];
    const name = nameOf(id);
    if (!name || before === undefined) continue;
    const delta = Math.abs(now - before);
    if (delta < WAIT_MIN_DELTA || delta < WAIT_MIN_RATIO * before) continue;
    const at = formatMinute(byId.get(id)!.startMinute);
    out.push({ type: "wait", name, at, from: before, to: now });
  }

  return out;
}

/** Whether a digest of these changes may go out now. */
export function isDigestDue(
  changes: PlanChange[],
  lastSentMs: number | null,
  nowMs: number,
): boolean {
  if (changes.length === 0) return false;
  if (lastSentMs === null) return true;
  const gap = changes.some((c) => c.type !== "wait")
    ? URGENT_GAP_MS
    : WAIT_GAP_MS;
  return nowMs - lastSentMs >= gap;
}

/**
 * The baseline after this tick. Sent: the current facts win, so what was just
 * announced is not announced again. Not sent: the baseline wins and the
 * current facts only fill what it did not know, so a held-back change is still
 * pending next tick. Entries no longer in the plan are dropped either way.
 */
export function nextBaseline(
  base: PlanDayFacts | null,
  cur: PlanDayFacts,
  entries: PlanEntry[],
  sent: boolean,
): PlanDayFacts {
  const ids = new Set(entries.map((e) => e.id));
  const keep = <T>(rec: Record<string, T> | undefined) =>
    Object.fromEntries(
      Object.entries(rec ?? {}).filter(([id]) => ids.has(id)),
    ) as Record<string, T>;
  const merge = <T>(b: Record<string, T> | undefined, c: Record<string, T>) =>
    sent ? { ...keep(b), ...c } : { ...c, ...keep(b) };
  const hours = sent ? (cur.hours ?? base?.hours) : (base?.hours ?? cur.hours);
  // A ride first seen down is not remembered: overnight heartbeats carry
  // CLOSED into the opening, and "running again" for every planned ride each
  // morning (or a ride that opens late) is not news. Only one seen open, then
  // down, can be announced back up.
  const rides = Object.fromEntries(
    Object.entries(cur.rides).filter(
      ([id, state]) => state === "open" || base?.rides[id] !== undefined,
    ),
  );
  return {
    ...(hours !== undefined && { hours }),
    shows: merge(base?.shows, cur.shows),
    rides: merge(base?.rides, rides),
    waits: merge(base?.waits, cur.waits),
  };
}

function toMs(value: Date | string | null): number | null {
  if (value === null || value === undefined) return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function clock(ms: number, timezone: string): string {
  return formatInTimeZone(new Date(ms), timezone, "HH:mm");
}
