import type { TripPlanQueryDto } from "../../trip-regions/dto/trip-region-query.dto";
import { findRegion, type RegionConfig } from "../../trip-regions/regions";
import {
  DAY_INTERVIEW_STEPS,
  TRIP_INTERVIEW_STEPS,
  type InterviewSnapshot,
  type InterviewStep,
  type PlannerDay,
  type PlannerSnapshot,
  type TripParty,
  type TripSnapshot,
} from "../assistant.schemas";
import { HUB_VALUES, type vacationItinerary } from "./assistant-trip";

/**
 * The visitor's own plans as Parky reads and changes them: the trip planner's state (the hub's
 * query, which is all of it), the day planner's days in the compact lines "This chat" shows, the
 * edits `change_trip` makes to a trip, and the interview's steps. Pure and free of Nest, so the
 * specs check it without a database. docs/frontend/assistant.md#the-visitors-plans.
 */

// ── The trip planner's state ─────────────────────────────────────────────────

/** Every name the hub planner writes, the `/plan` route's and the When mode's, with its values. */
const TRIP_VALUES: Record<string, RegExp> = {
  ...HUB_VALUES,
  area: /^[a-z0-9_-]+$/,
  evenings: /^[01]$/,
  when: /^\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}$/,
  days: /^([1-9]|1\d|2[01])$/,
  within: /^[A-Z]{2}-[A-Za-z]{1,10}$/,
};

/** The names `TripPlanQueryDto` takes; `when`, `days` and `within` are the search's. */
const PLAN_NAMES = [
  "from",
  "to",
  "parks",
  "hotel",
  "area",
  "arrival",
  "departure",
  "blocks",
  "fireworks",
  "split",
  "rest",
  "ticket",
  "hop",
  "evenings",
  "tickets",
  "hopper",
  "lock",
] as const;

/** The order the planner writes its names in, kept so a changed query reads like the page's. */
const WRITE_ORDER = [
  "from",
  "to",
  "when",
  "days",
  "within",
  "parks",
  "hotel",
  "area",
  "arrival",
  "departure",
  "blocks",
  "fireworks",
  "rest",
  "hop",
  "ticket",
  "hopper",
  "tickets",
  "evenings",
  "split",
  "lock",
];

/**
 * A trip query read leniently: the names the planner writes, each value checked, the first of a
 * repeated name kept. Anything else (a tracking parameter, a hand-edited value) is dropped.
 */
export function tripParams(query: string): URLSearchParams {
  const out = new URLSearchParams();
  let read: URLSearchParams;
  try {
    read = new URLSearchParams(query);
  } catch {
    return out;
  }
  for (const [name, value] of read) {
    const pattern = TRIP_VALUES[name];
    if (!pattern || out.has(name) || !pattern.test(value)) continue;
    out.set(name, value);
  }
  return out;
}

/** The query in the planner's spelling: its order, and `:`, `,`, `@`, `?` left readable. */
export function writeTripQuery(params: URLSearchParams): string {
  return WRITE_ORDER.filter((name) => params.has(name))
    .map(
      (name) =>
        `${name}=${encodeURIComponent(params.get(name) as string)
          .replace(/%2C/g, ",")
          .replace(/%3A/g, ":")
          .replace(/%40/g, "@")
          .replace(/%3F/g, "?")}`,
    )
    .join("&");
}

/** One trip of the visitor's, read off the snapshot. */
export interface VisitorTrip {
  region: RegionConfig;
  params: URLSearchParams;
  /** The hub planner is on screen with this state now. */
  onScreen: boolean;
  party?: TripParty;
}

/**
 * The trips the browser sent: one per known region, the on-screen one first, a trip with
 * nothing chosen left out.
 */
export function visitorTrips(
  snapshot: readonly TripSnapshot[] | null | undefined,
): VisitorTrip[] {
  const seen = new Set<string>();
  const trips: VisitorTrip[] = [];
  for (const entry of snapshot ?? []) {
    const region = findRegion(entry.region);
    if (!region || seen.has(region.slug)) continue;
    const params = tripParams(entry.query);
    const party =
      entry.party && (entry.party.size || entry.party.heightsCm.length)
        ? entry.party
        : undefined;
    if ([...params.keys()].length === 0 && !party) continue;
    seen.add(region.slug);
    trips.push({
      region,
      params,
      onScreen: entry.onScreen === true,
      ...(party && { party }),
    });
  }
  return trips.sort((a, b) => Number(b.onScreen) - Number(a.onScreen));
}

/**
 * The trip a tool means: the region's, else the one on screen, else the only one. Null when the
 * visitor has none, or several and no region says which.
 */
export function pickTrip(
  trips: readonly VisitorTrip[],
  region?: string,
): VisitorTrip | null {
  if (region) return trips.find((t) => t.region.slug === region) ?? null;
  return trips.find((t) => t.onScreen) ?? (trips.length === 1 ? trips[0] : null);
}

/** The `/plan` query of a trip with dates; null while it has none (nothing chosen, or a search). */
export function planQueryOf(params: URLSearchParams): TripPlanQueryDto | null {
  const from = params.get("from");
  const to = params.get("to");
  if (!from || !to) return null;
  const query: Record<string, string> = {};
  for (const name of PLAN_NAMES) {
    const value = params.get(name);
    if (value !== null) query[name] = value;
  }
  return query as unknown as TripPlanQueryDto;
}

/** The When mode's search, while the visitor looks for dates instead of planning given ones. */
export function tripSearchOf(
  params: URLSearchParams,
): { from: string; to: string; days: number; within?: string } | null {
  const when = params.get("when");
  if (!when || params.get("from")) return null;
  const [from, to] = when.split("_");
  return {
    from,
    to,
    days: Number(params.get("days") ?? 10),
    ...(params.get("within") && { within: params.get("within") as string }),
  };
}

/** A party in words: "4 people, children 105 and 132 cm". */
export function partyPhrase(party: TripParty): string {
  const heights = party.heightsCm;
  return [
    party.size ? `${party.size} ${party.size === 1 ? "person" : "people"}` : null,
    heights.length > 0
      ? `${heights.length === 1 ? "a child" : "children"} ${heights.join(" and ")} cm`
      : null,
  ]
    .filter(Boolean)
    .join(", ");
}

/**
 * One trip in one line for "This chat", read off its query alone: no lookup, so it costs nothing
 * and says only what the visitor chose. Park slugs stay slugs; the plan names them.
 */
export function tripLine(trip: VisitorTrip): string {
  const p = trip.params;
  const search = tripSearchOf(p);
  const parts: string[] = [];
  if (p.get("from") && p.get("to")) {
    parts.push(
      `${p.get("from")} to ${p.get("to")} (${dayCount(p.get("from") as string, p.get("to") as string)} days)`,
    );
  } else if (search) {
    parts.push(
      `no dates yet, searching ${search.days} days between ${search.from} and ${search.to}${search.within ? ` in the school breaks of ${search.within}` : ""}`,
    );
  } else {
    parts.push("no dates yet");
  }
  parts.push(
    p.get("parks")
      ? `parks ${p
          .get("parks")
          ?.split(",")
          .map((park) => park.replace(/:(\d)$/, " ×$1"))
          .join(", ")}`
      : "parks: the region's headliners (untouched)",
  );
  if (p.get("lock")) parts.push(`pinned ${p.get("lock")?.replace(/,/g, ", ")}`);
  if (p.get("hotel")) parts.push(`hotel ${p.get("hotel")}`);
  if (p.get("area")) parts.push(`staying in ${p.get("area")}`);
  if (p.get("tickets")) parts.push(`tickets held ${p.get("tickets")}`);
  if (p.get("hopper") || p.get("ticket") === "hopper") {
    parts.push(`hopper ${p.get("hopper") ?? "every resort"}`);
  }
  if (p.get("rest")) parts.push(`rest ${p.get("rest")}`);
  if (p.get("evenings") === "0") parts.push("no evenings");
  if (p.get("fireworks") === "want") parts.push("keep the fireworks");
  if (p.get("blocks")) {
    parts.push(p.get("blocks") === "1" ? "resorts in blocks" : "resorts mixed");
  }
  if (p.get("hop") === "1") parts.push("two parks a day allowed");
  if (p.get("arrival") === "park") parts.push("a park on the arrival day");
  if (p.get("departure") === "park") parts.push("a park on the departure day");
  if (trip.party) parts.push(`party ${partyPhrase(trip.party)}`);
  return `${trip.region.name} trip${trip.onScreen ? " (on screen in the hub's planner now)" : " (remembered by this browser)"}: ${parts.join("; ")}`;
}

function dayCount(from: string, to: string): number {
  return (
    Math.round(
      (Date.parse(`${to}T12:00:00Z`) - Date.parse(`${from}T12:00:00Z`)) /
        86_400_000,
    ) + 1
  );
}

type Itinerary = ReturnType<typeof vacationItinerary>;

/**
 * The plan of the on-screen trip in one line a day, for the hints Parky gives unasked: the parks
 * with their big rides' wait, a party night, why the day was chosen, what to watch, an evening;
 * then the trip's season and what did not fit. The figures are the plan's own.
 */
export function tripDigest(itinerary: Itinerary): string[] {
  const lines = itinerary.days.map((day) => {
    const head = `${day.weekday.slice(0, 3)} ${day.date}`;
    const parks = (day.parks ?? []).map((park) => {
      const figures = [
        park.headlinerWait != null ? `${park.headlinerWait} min` : null,
        park.basis === "last_year" ? "last year's estimate" : null,
        park.partyNight ? `party night ${park.partyNight}` : null,
      ].filter(Boolean);
      return `${park.park}${figures.length ? ` (${figures.join(", ")})` : ""}`;
    });
    const body =
      day.kind === "park" || day.kind === "hop"
        ? parks.join(" + ")
        : day.kind === "rest"
          ? "rest day"
          : day.kind;
    return [
      `${head}: ${body}`,
      day.why?.length ? `why: ${day.why.join(", ")}` : null,
      day.watch?.length ? `watch: ${day.watch.join(", ")}` : null,
      day.evening ? `evening: ${day.evening}` : null,
    ]
      .filter(Boolean)
      .join("; ");
  });
  const totals = itinerary.totals as {
    headlinerWait?: number;
    averageDayWait?: number;
  };
  return [
    ...lines,
    ...(totals.headlinerWait != null && totals.averageDayWait != null
      ? [
          `Big rides on the planned days about ${totals.headlinerWait} min, an average day of the trip ${totals.averageDayWait} min`,
        ]
      : []),
    ...(itinerary.season ? [`Season: ${itinerary.season}`] : []),
    ...(itinerary.notPlaced?.length
      ? [
          `Not placed: ${itinerary.notPlaced.map((n) => `${String(n.park)} (${String(n.why)})`).join(", ")}`,
        ]
      : []),
    ...(itinerary.pinsNotKept?.length
      ? [
          `Pins not kept: ${itinerary.pinsNotKept.map((n) => `${String(n.pin)} (${String(n.why)})`).join(", ")}`,
        ]
      : []),
    ...(itinerary.loosened?.length
      ? [`Loosened: ${itinerary.loosened.join(", ")}`]
      : []),
  ];
}

// ── Changing a trip ──────────────────────────────────────────────────────────

/** What `change_trip` may change, with parks already resolved to the region's slugs. */
export interface TripChange {
  dates?: { from: string; to: string };
  parks?: Array<{ slug: string; visits: number }>;
  pin?: Array<{ target: string; date: string }>;
  unpin?: string[];
  swap?: { a: string; b: string };
  rest?: string;
  tickets?: string;
  hotel?: string;
  area?: string;
  hopper?: string[];
  fireworks?: "want" | "any";
  evenings?: boolean;
  arrival?: "free" | "park";
  departure?: "free" | "park";
}

/** What kind of change a trip got, for the widget's receipt and the statistic. */
export type TripChangeKind =
  | "dates"
  | "parks"
  | "pin"
  | "swap"
  | "rest"
  | "tickets"
  | "hotel"
  | "pace";

/** A plan day as `swap` reads it: its kind and parks. */
export interface SwapDay {
  date: string;
  kind: string;
  parks: Array<{ slug: string }>;
}

interface Lock {
  target: string;
  date: string;
}

function readLocks(params: URLSearchParams): Lock[] {
  return (params.get("lock") ?? "")
    .split(",")
    .filter(Boolean)
    .map((item) => {
      const [target, date] = item.split("@");
      return { target, date };
    });
}

function writeLocks(params: URLSearchParams, locks: Lock[]): void {
  const sorted = [...locks].sort((a, b) => a.date.localeCompare(b.date));
  if (sorted.length === 0) params.delete("lock");
  else params.set("lock", sorted.map((l) => `${l.target}@${l.date}`).join(","));
}

/** A value the planner leaves out of the URL when it is the default. */
function setOrDefault(
  params: URLSearchParams,
  name: string,
  value: string | undefined,
  fallback: string,
): void {
  if (value === undefined) return;
  if (value === fallback || value === "") params.delete(name);
  else params.set(name, value);
}

/** What a pin of a plan day names: its park, `rest`, or nothing for a travel or hop day. */
function dayTarget(day: SwapDay | undefined): string | null {
  if (!day) return null;
  if (day.kind === "rest") return "rest";
  if (day.kind === "park" && day.parks[0]) return day.parks[0].slug;
  return null;
}

/**
 * The trip's query after a change, edited name by name so everything the visitor chose and the
 * change does not touch stays exactly as written; the plan route checks the result. `days` is the
 * current plan, which a swap reads. Throws a sentence the model reads when the change cannot be
 * made as asked.
 */
export function changeTripParams(
  current: URLSearchParams,
  change: TripChange,
  options: { headliners: readonly string[]; days?: readonly SwapDay[] },
): { params: URLSearchParams; kinds: TripChangeKind[] } {
  const params = new URLSearchParams(current);
  const kinds = new Set<TripChangeKind>();
  if (change.dates) {
    const [from, to] = [change.dates.from, change.dates.to].sort();
    params.set("from", from);
    params.set("to", to);
    for (const name of ["when", "days", "within"]) params.delete(name);
    writeLocks(
      params,
      readLocks(params).filter((l) => l.date >= from && l.date <= to),
    );
    kinds.add("dates");
  }
  if (change.parks?.length) {
    const picks = (params.get("parks")?.split(",") ?? [...options.headliners])
      .filter(Boolean)
      .map((item) => {
        const [slug, visits] = item.split(":");
        return { slug, visits: visits ? Number(visits) : null };
      });
    for (const { slug, visits } of change.parks) {
      const at = picks.findIndex((p) => p.slug === slug);
      if (visits === 0) {
        if (at !== -1) picks.splice(at, 1);
      } else if (at === -1) picks.push({ slug, visits });
      else picks[at] = { slug, visits };
    }
    if (picks.length === 0) {
      throw new Error("A trip needs at least one park; nothing changed");
    }
    params.set(
      "parks",
      picks.map((p) => (p.visits ? `${p.slug}:${p.visits}` : p.slug)).join(","),
    );
    // A pin on a park the trip no longer has would be refused by the plan.
    const kept = new Set(picks.map((p) => p.slug));
    writeLocks(
      params,
      readLocks(params).filter((l) => l.target === "rest" || kept.has(l.target)),
    );
    kinds.add("parks");
  }
  if (change.unpin?.length) {
    const dates = new Set(change.unpin);
    writeLocks(
      params,
      readLocks(params).filter((l) => !dates.has(l.date)),
    );
    kinds.add("pin");
  }
  if (change.pin?.length) {
    const locks = readLocks(params);
    for (const pin of change.pin) {
      const kept = locks.filter((l) => l.date !== pin.date);
      locks.splice(0, locks.length, ...kept, pin);
    }
    writeLocks(params, locks);
    kinds.add(change.pin.some((p) => p.target === "rest") ? "rest" : "pin");
  }
  if (change.swap) {
    const { a, b } = change.swap;
    const byDate = new Map((options.days ?? []).map((d) => [d.date, d]));
    const ta = dayTarget(byDate.get(a));
    const tb = dayTarget(byDate.get(b));
    if (!byDate.has(a) || !byDate.has(b)) {
      throw new Error(`${byDate.has(a) ? b : a} is not a day of the trip`);
    }
    if (a === b || !ta || !tb || (ta === "rest" && tb === "rest")) {
      throw new Error(
        "Only two park or rest days can be swapped, not a travel or two-park day",
      );
    }
    const locks = readLocks(params).filter((l) => l.date !== a && l.date !== b);
    writeLocks(params, [
      ...locks,
      { target: ta, date: b },
      { target: tb, date: a },
    ]);
    kinds.add("swap");
  }
  if (change.rest !== undefined) {
    setOrDefault(params, "rest", change.rest, "auto");
    kinds.add("rest");
  }
  if (change.tickets !== undefined) {
    if (change.tickets) params.set("tickets", change.tickets);
    else params.delete("tickets");
    kinds.add("tickets");
  }
  if (change.hopper !== undefined) {
    if (change.hopper.length > 0) params.set("hopper", change.hopper.join(","));
    else params.delete("hopper");
    params.delete("ticket");
    kinds.add("tickets");
  }
  if (change.hotel !== undefined || change.area !== undefined) {
    if (change.hotel) params.set("hotel", change.hotel);
    if (change.area) params.set("area", change.area);
    kinds.add("hotel");
  }
  if (change.fireworks !== undefined) {
    setOrDefault(params, "fireworks", change.fireworks, "any");
    kinds.add("pace");
  }
  if (change.evenings !== undefined) {
    setOrDefault(params, "evenings", change.evenings ? "1" : "0", "1");
    kinds.add("pace");
  }
  if (change.arrival !== undefined) {
    setOrDefault(params, "arrival", change.arrival, "free");
    kinds.add("dates");
  }
  if (change.departure !== undefined) {
    setOrDefault(params, "departure", change.departure, "free");
    kinds.add("dates");
  }
  if (kinds.size === 0) throw new Error("Say what to change; nothing changed");
  return { params, kinds: [...kinds] };
}

/** One day of a plan as the comparison reads it: its date and what is on it. */
function dayContent(day: Itinerary["days"][number]): string {
  if (day.kind === "rest") return "rest day";
  if (day.kind !== "park" && day.kind !== "hop") return day.kind;
  return (day.parks ?? []).map((p) => p.park).join(" + ");
}

/**
 * What a change did to the plan, for the sentence Parky says: the days whose content moved, the
 * big rides' wait before and after, and pins the new plan could not keep.
 */
export function tripPlanChange(before: Itinerary | null, after: Itinerary) {
  const old = new Map(
    (before?.days ?? []).map((day) => [day.date, dayContent(day)]),
  );
  const moved = after.days
    .filter((day) => old.get(day.date) !== dayContent(day))
    .map((day) => ({
      date: day.date,
      weekday: day.weekday,
      now: dayContent(day),
      ...(old.has(day.date) && { was: old.get(day.date) }),
    }));
  const wait = (it: Itinerary | null) =>
    (it?.totals as { headlinerWait?: number } | undefined)?.headlinerWait;
  return {
    daysChanged: moved,
    ...(wait(before) != null &&
      wait(after) != null && {
        bigRidesWait: `${wait(before)} min before, ${wait(after)} min now`,
      }),
    ...(after.pinsNotKept && { pinsNotKept: after.pinsNotKept }),
    ...(after.notPlaced && { notPlaced: after.notPlaced }),
  };
}

// ── The day planner, as "This chat" shows it ─────────────────────────────────

/** HH:MM for a planner minute, past midnight as the planner prints it (25:30 is 01:30). */
export function clockOfMinute(minute: number): string {
  const m = ((minute % 1440) + 1440) % 1440;
  return `${String(Math.floor(m / 60)).padStart(2, "0")}:${String(m % 60).padStart(2, "0")}`;
}

/** A planner minute from an entry: its own, else its HH:MM; null without either. */
export function entryMinute(entry: PlannerDay["entries"][number]): number | null {
  if (entry.startMinute !== undefined) return entry.startMinute;
  if (!entry.start) return null;
  const [h, m] = entry.start.split(":").map(Number);
  return h * 60 + m;
}

/**
 * The day planner in compact lines, about 40 % fewer tokens than its JSON: one line per day, one
 * per entry with its id, start, kind, name and slug, then the other planned days by park and date.
 * Every name is the visitor's text and stays quoted.
 */
export function plannerLines(planner: PlannerSnapshot): string[] {
  const lines: string[] = [];
  const active = planner.active;
  for (const day of planner.days) {
    const isActive = active?.park === day.park.slug && active.date === day.date;
    const prefs = day.prefs
      ? [
          day.prefs.riderHeightCm
            ? `shortest rider ${day.prefs.riderHeightCm} cm`
            : null,
          day.prefs.avoidWet ? "stay dry" : null,
          day.prefs.earlyEntry ? "early entry" : null,
        ].filter(Boolean)
      : [];
    lines.push(
      `- ${day.park.slug} ${JSON.stringify(day.park.name)} on ${day.date}${isActive ? " (open in the planner)" : ""}${prefs.length ? `, ${prefs.join(", ")}` : ""}${day.entries.length === 0 ? ": nothing planned yet (reserved)" : ":"}`,
    );
    for (const entry of day.entries) {
      const minute = entryMinute(entry);
      lines.push(
        `  [${entry.id}] ${minute !== null ? clockOfMinute(minute) : "--:--"} ${entry.kind} ${JSON.stringify(entry.name)}${entry.rideSlug ? ` (${entry.rideSlug})` : ""}${entry.showSlug ? ` (${entry.showSlug})` : ""}${entry.durationMinutes ? ` ${entry.durationMinutes} min` : ""}${entry.done ? " done" : ""}`,
      );
    }
  }
  const more = planner.more ?? [];
  if (more.length > 0) {
    lines.push(
      "Other planned days (only park and date here; plan_day opens one, and the next question carries it in full):",
    );
    for (const day of more) {
      lines.push(
        `- ${day.park.slug} ${JSON.stringify(day.park.name)} on ${day.date}: ${day.entries} entries${day.done ? `, ${day.done} done` : ""}${day.reserved && day.entries === 0 ? ", reserved" : ""}${day.riderHeightCm ? `, shortest rider ${day.riderHeightCm} cm` : ""}`,
      );
    }
  }
  return lines;
}

// ── Reviewing a planned day ──────────────────────────────────────────────────

/** A ride's figures the review reads off `/plan/day`. */
export interface ReviewRide {
  attractionSlug: string;
  attractionName: string;
  minimumHeight?: number;
  hours: Array<{ hour: number; wait: number }>;
  slots?: Array<{ minute: number; wait: number }>;
}

/**
 * The forecast wait when a ride's queue is joined at `minute`, as the planner draws a block: the
 * quarter hour's slot, else the hour's figure; null where the day has none.
 */
export function waitAt(ride: ReviewRide, minute: number): number | null {
  const slot = ride.slots?.find((s) => minute >= s.minute && minute < s.minute + 15);
  if (slot) return slot.wait;
  const hour = ride.hours.find((h) => h.hour === Math.floor(minute / 60));
  return hour ? hour.wait : null;
}

/** The optimizer's entry for one planned entry; null for one without a start. */
export function optimizerEntry(entry: PlannerDay["entries"][number]) {
  const startMinute = entryMinute(entry);
  if (startMinute === null) return null;
  const custom =
    entry.kind === "ride"
      ? undefined
      : {
          label: entry.name || entry.kind,
          icon: entry.kind === "show" ? ("show" as const) : ("star" as const),
          durationMinutes: entry.durationMinutes ?? 30,
        };
  return {
    id: entry.id,
    ...(entry.kind === "ride" && entry.rideSlug && { attractionSlug: entry.rideSlug }),
    ...(entry.kind === "ride" && { attractionName: entry.name }),
    ...(custom && { custom }),
    ...(entry.kind === "show" && entry.showSlug && { showSlug: entry.showSlug }),
    startMinute,
    ...(entry.done && { done: true }),
  };
}

// ── The interview ─────────────────────────────────────────────────────────────

/** What Parky asks at a step and which tool takes the answer; English, for the model. */
interface StepGuide {
  ask: string;
  apply: string;
  chips: string;
}

/** By step; a park day's must-dos are `mustdo_day`, since they go in by ride, not by park. */
const STEP_GUIDE: Record<InterviewStep | "mustdo_day", StepGuide> = {
  region: {
    ask: "where they want to go (the trip regions)",
    apply:
      "open_page the region's hub (page orlando); answered once the hub is on screen",
    chips: "the live regions by name",
  },
  dates: {
    ask: "when they travel, or whether you should find the best dates",
    apply:
      "change_trip dates; to find dates, find_trip_dates with the trip's parks, then change_trip dates with the window they pick",
    chips: "next summer / I know our dates / find the best dates",
  },
  party: {
    ask: "how many are going, and the children's heights in cm",
    apply: "interview with op answered and party",
    chips: "just the two of us / 2 adults, 2 kids / kids 105 and 132 cm",
  },
  mustdo: {
    ask: "the parks they must see and any ride they must not miss",
    apply:
      "change_trip parks (visits 1-3, 0 removes); must-do rides go into interview's mustDoRides by ride id, for the filing",
    chips: "two of the region's headline parks by name / all the big ones / no idea yet",
  },
  tickets: {
    ask: "which tickets they already hold, if any",
    apply: "change_trip tickets (the compact form); none: nothing to apply",
    chips: "none yet / a Disney ticket / Universal and Disney",
  },
  hotel: {
    ask: "where they stay: whose hotel, and which area",
    apply: "change_trip hotel and area",
    chips: "a Disney hotel / a Universal hotel / off-site",
  },
  pace: {
    ask: "the pace: rest days and evenings in a second park",
    apply: "change_trip rest (auto, none, max:n) and evenings",
    chips: "a rest day every few days / full speed / evenings off",
  },
  file: {
    ask: "whether to put the trip into the day planner",
    apply:
      "plan_trip days from get_trip_plan's reserve, riderHeightCm the party's smallest height; then plan_add_rides for the must-do rides on their park's day",
    chips: "yes, into the day planner / not yet",
  },
  day: {
    ask: "which park and which day",
    apply: "plan_day",
    chips: "today / tomorrow / a park by name",
  },
  arrival: {
    ask: "when they arrive: at opening, later in the morning or in the afternoon, and whether a hotel lets them in early",
    apply:
      "plan_set_party earlyEntry when a hotel lets them in early; a later arrival as plan_add_block (icon meet, 15 minutes) at that time",
    chips: "at opening / around 11:00 / after lunch",
  },
  mustdo_day: {
    ask: "the rides they must not miss",
    apply: "plan_add_rides with the rides' ids",
    chips: "two headliners by name / the biggest coasters / whatever is quiet",
  },
  heights: {
    ask: "the smallest rider's height",
    apply:
      "plan_set_party riderHeightCm; then name any planned ride the smallest rider is too short for (review_plan_day)",
    chips: "everyone is tall / 105 cm / 120 cm",
  },
  shows: {
    ask: "whether a show belongs in the day",
    apply: "get_shows, then plan_add_show at one of its times",
    chips: "the evening show / no shows / what's on?",
  },
  sort: {
    ask: "whether to sort the day for the shortest waits",
    apply: "plan_optimize",
    chips: "yes, sort it / leave it as it is",
  },
};

function guideOf(kind: "trip" | "day", step: InterviewStep): StepGuide {
  return STEP_GUIDE[kind === "day" && step === "mustdo" ? "mustdo_day" : step];
}

/** The steps of an interview of this kind, in order. */
export function interviewSteps(kind: "trip" | "day"): readonly InterviewStep[] {
  return kind === "trip" ? TRIP_INTERVIEW_STEPS : DAY_INTERVIEW_STEPS;
}

/**
 * The first step not answered and not skipped, or null when the interview is through. A step the
 * plans already hold is still asked, as a confirmation (`holds`).
 */
export function nextInterviewStep(
  interview: Pick<InterviewSnapshot, "kind" | "answered" | "skipped">,
): InterviewStep | null {
  const done = new Set([...interview.answered, ...interview.skipped]);
  return interviewSteps(interview.kind).find((step) => !done.has(step)) ?? null;
}

/** What a step already holds in the visitor's plans, so Parky confirms it instead of asking. */
export function interviewHolds(
  interview: Pick<InterviewSnapshot, "kind" | "park" | "date" | "region">,
  step: InterviewStep,
  trip: VisitorTrip | null,
  planner: PlannerSnapshot | null | undefined,
): string | null {
  if (interview.kind === "trip") {
    const p = trip?.params;
    if (step === "region" && trip) return trip.region.name;
    if (step === "dates" && p?.get("from")) return `${p.get("from")} to ${p.get("to")}`;
    if (step === "party" && trip?.party) return partyPhrase(trip.party);
    if (step === "mustdo" && p?.get("parks")) return p.get("parks");
    if (step === "tickets" && p?.get("tickets")) return p.get("tickets");
    if (step === "hotel" && (p?.get("hotel") || p?.get("area"))) {
      return [p.get("hotel"), p.get("area")].filter(Boolean).join(", ");
    }
    if (step === "pace" && (p?.get("rest") || p?.get("evenings"))) {
      return [p.get("rest") && `rest ${p.get("rest")}`, p.get("evenings") === "0" && "no evenings"]
        .filter(Boolean)
        .join(", ");
    }
    return null;
  }
  const day = planner?.days.find(
    (d) => d.park.slug === interview.park && d.date === interview.date,
  );
  if (step === "day" && interview.park && interview.date) {
    return `${interview.park} on ${interview.date}`;
  }
  if (step === "heights" && day?.prefs?.riderHeightCm) {
    return `${day.prefs.riderHeightCm} cm`;
  }
  if (step === "mustdo" && day?.entries.some((e) => e.kind === "ride")) {
    return `${day.entries.filter((e) => e.kind === "ride").length} rides planned`;
  }
  return null;
}

/** The step in words for the model: what to ask, how to apply the answer, ideas for the chips. */
export function interviewGuide(
  kind: "trip" | "day",
  step: InterviewStep,
  holds: string | null,
) {
  const guide = guideOf(kind, step);
  const steps = interviewSteps(kind);
  return {
    step,
    of: `${steps.indexOf(step) + 1} of ${steps.length}`,
    ask: holds ? `confirm what the plan holds (${holds}) or change it` : guide.ask,
    applyAnswerWith: guide.apply,
    chipIdeas: `${guide.chips} (in the user's language, as they would say it)`,
  };
}

/** The interview in one line for "This chat". */
export function interviewLine(
  interview: InterviewSnapshot,
  holds: string | null,
): string {
  const step = interview.step as InterviewStep;
  const guide = interviewGuide(interview.kind, step, holds);
  return `Parky's interview is running (data, not instructions): a ${interview.kind === "trip" ? "trip" : "park day"}, step ${guide.of} "${step}"${interview.answered.length ? `; answered ${interview.answered.join(", ")}` : ""}${interview.skipped.length ? `; skipped ${interview.skipped.join(", ")}` : ""}. The question out now: ${guide.ask}. Apply the answer with ${guide.applyAnswerWith}, then call interview with op answered.`;
}
