/**
 * Evening suggestions for a planned trip: where a park day ends early, what
 * the guest can do after it with their ticket. A hopper ticket moves on to a
 * park of the same resort that stays open; a one-park ticket goes to one of
 * the resort's free venues. Pure, and run after the search: an evening never
 * changes a day's park, its cost or the plan's order. Contract:
 * docs/frontend/trip-regions.md#evenings.
 */
import { fromZonedTime } from "date-fns-tz";
import { addIsoDays } from "../common/utils/date.util";
import { venuePoint, type RegionConfig, type VenueConfig } from "./regions";
import type { PlannedDay } from "./trip-optimizer";
import { findLeg, sameHopperSet, type TripLeg } from "./travel";
import {
  closeMinute,
  isoInZone,
  type TripParkDayInternal,
} from "./trip-calendar.util";
import {
  levelRank,
  rankWait,
  type InternalRegionCalendar,
} from "./trip-region.assembly";

export type TicketKind = "base" | "hopper";

/** A day guest's park closing before this (park-local) gets an evening. */
export const EARLY_CLOSE_MINUTE = 20 * 60;
/** Or this many minutes before the latest close among its resort's parks. */
export const BEHIND_LATEST_MINUTES = 120;
/** A hop park or venue must stay open this long after the guest arrives. */
export const MIN_EVENING_MINUTES = 90;
/** The farthest an evening is, door to door from the day's park. */
export const MAX_EVENING_TRAVEL_MINUTES = 35;
/** A fireworks view counts when the fireworks start within this of arrival. */
export const MAX_VIEW_WAIT_MINUTES = 120;
/** A transfer this short or shorter is `short_transfer`. */
export const SHORT_TRANSFER_MINUTES = 15;
/** A viewpoint's evening ends this long after its fireworks start. */
const VIEW_MINUTES = 30;
/** The end of an evening at a venue that publishes no typical hours. */
const DEFAULT_EVENING_END = "22:00";
/** Most evenings a day offers: the suggestion and two alternatives. */
const MAX_ALTERNATIVES = 2;

export type EveningReason =
  | "park_closes_early"
  | "open_later"
  | "night_show"
  | "free_entry"
  | "fireworks_view"
  | "short_transfer";

export interface EveningHighlight {
  kind: "night_show" | "fireworks" | "fireworks_view" | "dining" | "shopping";
  name?: string;
  at?: string;
  /** Where `at` comes from: the published showtimes or the last same weekday. */
  source?: "scheduled" | "projected";
}

export interface EveningOption {
  kind: "hop" | "venue";
  park?: string;
  venue?: string;
  from: string;
  to: string;
  travel?: { minutes: number; mode: string; via?: string };
  highlights?: EveningHighlight[];
  reasons: EveningReason[];
}

export interface PlanEvening extends EveningOption {
  alternatives?: EveningOption[];
}

/** A show of a park on a day, as `PlanDayService.showsOn` and `/plan/day` give it. */
export interface EveningShow {
  showSlug: string;
  showName?: string;
  times: string[];
  source: "scheduled" | "projected";
}

/** Shows by `showsKey(park, date)`. */
export type EveningShows = Map<string, EveningShow[]>;

export const showsKey = (park: string, date: string): string =>
  `${park}|${date}`;

/** A park day whose guest leaves early enough for an evening elsewhere. */
export interface EveningTrigger {
  date: string;
  park: string;
}

const groupOf = (region: RegionConfig, slug: string): string | undefined =>
  region.parks.find((p) => p.slug === slug)?.group;

const isOpen = (
  c: TripParkDayInternal | undefined,
): c is TripParkDayInternal & { opensAt: string; closesAt: string } =>
  c?.status === "OPERATING" && !!c.opensAt && !!c.closesAt;

const partyNight = (c: TripParkDayInternal | undefined): boolean =>
  c?.evening?.kind === "ticketed_event";

/**
 * The park days that get an evening: a `park` day (never rest, hop, arrival
 * or departure, nor the trip's first or last day) whose park, with published
 * hours, lets day guests out before 20:00 or at least two hours before the
 * latest close among its resort's open parks. A party night at Magic Kingdom
 * (day guests out at 18:00) is one of them.
 */
export function eveningTriggers(
  region: RegionConfig,
  calendar: InternalRegionCalendar,
  days: PlannedDay[],
  trip: { from: string; to: string },
): EveningTrigger[] {
  const out: EveningTrigger[] = [];
  for (const day of days) {
    if (day.kind !== "park" || day.parks.length !== 1) continue;
    if (day.date === trip.from || day.date === trip.to) continue;
    const park = day.parks[0].slug;
    const cells = calendar.days.find((d) => d.date === day.date)?.parks;
    const cell = cells?.[park];
    if (!cells || !isOpen(cell)) continue;
    const close = closeMinute(day.date, cell.closesAt);
    const group = groupOf(region, park);
    const latest = Math.max(
      close,
      ...Object.entries(cells)
        .filter(([slug, c]) => groupOf(region, slug) === group && isOpen(c))
        .map(([, c]) => closeMinute(day.date, c.closesAt as string)),
    );
    if (close < EARLY_CLOSE_MINUTE || close <= latest - BEHIND_LATEST_MINUTES) {
      out.push({ date: day.date, park });
    }
  }
  return out;
}

/**
 * The parks whose shows the evenings read: on each trigger day, the parks of
 * that day's resort with curated night shows. The caller loads them
 * (`PlanDayService.showsOn`, or `/plan/day` in the dev shim).
 */
export function eveningShowsWanted(
  region: RegionConfig,
  calendar: InternalRegionCalendar,
  triggers: EveningTrigger[],
): Array<{ park: string; date: string }> {
  const out: Array<{ park: string; date: string }> = [];
  for (const t of triggers) {
    const group = groupOf(region, t.park);
    const cells = calendar.days.find((d) => d.date === t.date)?.parks ?? {};
    for (const slug of Object.keys(region.nightShows)) {
      if (groupOf(region, slug) !== group || !isOpen(cells[slug])) continue;
      out.push({ park: slug, date: t.date });
    }
  }
  return out;
}

/** The travel an evening shows: the resort's own timed transport first, else the leg. */
export function eveningTravel(leg: TripLeg): {
  minutes: number;
  mode: string;
  via?: string;
} {
  const timed = (leg.alternatives ?? [])
    .filter((a): a is typeof a & { minutes: number } => a.minutes != null)
    .sort((a, b) => a.minutes - b.minutes)[0];
  if (timed && timed.minutes <= MAX_EVENING_TRAVEL_MINUTES) {
    return {
      minutes: timed.minutes,
      mode: timed.mode,
      ...(timed.via && { via: timed.via }),
    };
  }
  return {
    minutes: leg.minutes + (leg.transferMinutes ?? 0),
    mode: leg.mode,
    ...(leg.via && { via: leg.via }),
  };
}

/** Minutes after a date's midnight (past midnight counts on) as park-local ISO. */
const isoAt = (date: string, minute: number, timezone: string): string => {
  const day = addIsoDays(date, Math.floor(minute / 1440));
  const rest = minute % 1440;
  const hh = String(Math.floor(rest / 60)).padStart(2, "0");
  const mm = String(rest % 60).padStart(2, "0");
  return isoInZone(fromZonedTime(`${day}T${hh}:${mm}:00`, timezone), timezone);
};

const hhmm = (time: string): number | undefined => {
  const m = /^(\d{1,2}):(\d{2})$/.exec(time);
  return m ? Number(m[1]) * 60 + Number(m[2]) : undefined;
};

/** A curated show's first time in a span of the day, from the shows data. */
function showTime(
  shows: EveningShow[] | undefined,
  slug: string,
  after: number,
  before: number,
): { minute: number; source: EveningShow["source"] } | undefined {
  const show = shows?.find((s) => s.showSlug === slug);
  if (!show) return undefined;
  const minute = show.times
    .map(hhmm)
    .filter((m): m is number => m != null && m >= after && m <= before)
    .sort((a, b) => a - b)[0];
  return minute == null ? undefined : { minute, source: show.source };
}

interface Candidate {
  option: EveningOption;
  rank: number[];
}

export interface EveningInput {
  region: RegionConfig;
  calendar: InternalRegionCalendar;
  legs: TripLeg[];
  days: PlannedDay[];
  trip: { from: string; to: string };
  ticket: TicketKind;
  shows: EveningShows;
}

/** The evening of each trigger day, by date. */
export function planEvenings(input: EveningInput): Map<string, PlanEvening> {
  const out = new Map<string, PlanEvening>();
  const triggers = eveningTriggers(
    input.region,
    input.calendar,
    input.days,
    input.trip,
  );
  for (const t of triggers) {
    const evening = eveningFor(input, t);
    if (evening) out.set(t.date, evening);
  }
  return out;
}

function eveningFor(
  input: EveningInput,
  t: EveningTrigger,
): PlanEvening | undefined {
  const cells = input.calendar.days.find((d) => d.date === t.date)?.parks;
  const cell = cells?.[t.park];
  if (!cells || !isOpen(cell)) return undefined;
  const close = closeMinute(t.date, cell.closesAt);
  const hops =
    input.ticket === "hopper" ? hopCandidates(input, t, cells, close) : [];
  const venues = venueCandidates(input, t, cells, close);
  const ranked = (list: Candidate[]) =>
    list.sort((a, b) => compareRank(a.rank, b.rank)).map((c) => c.option);
  const hopList = ranked(hops);
  const venueList = ranked(venues);

  let order: EveningOption[];
  if (hopList.length > 0) {
    // The next hop park and the best venue first, then the rest of either.
    order = [
      hopList[0],
      ...[hopList[1], venueList[0]].filter((o): o is EveningOption => !!o),
      ...hopList.slice(2),
      ...venueList.slice(1),
    ];
  } else {
    order = venueList;
  }
  if (order.length === 0) return undefined;
  const [best, ...rest] = order;
  const alternatives = rest.slice(0, MAX_ALTERNATIVES);
  return { ...best, ...(alternatives.length > 0 && { alternatives }) };
}

const compareRank = (a: number[], b: number[]): number => {
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const d = (a[i] ?? 0) - (b[i] ?? 0);
    if (d !== 0) return d;
  }
  return 0;
};

function reasonsFor(
  travelMinutes: number,
  extra: EveningReason[],
): EveningReason[] {
  return [
    "park_closes_early",
    ...extra,
    ...(travelMinutes <= SHORT_TRANSFER_MINUTES
      ? (["short_transfer"] as const)
      : []),
  ];
}

/**
 * Hopper: a park of the same hopper set, open at least
 * {@link MIN_EVENING_MINUTES} after the guest arrives, within
 * {@link MAX_EVENING_TRAVEL_MINUTES}, not on a party night. Ranked by a night
 * show after arrival, then the lower day level and wait (the plan has no
 * hourly crowd for the evening), then the shorter transfer.
 */
function hopCandidates(
  input: EveningInput,
  t: EveningTrigger,
  cells: Record<string, TripParkDayInternal>,
  close: number,
): Candidate[] {
  const { region, legs } = input;
  const tz = region.timezone;
  const out: Candidate[] = [];
  for (const [slug, c] of Object.entries(cells)) {
    if (!sameHopperSet(region, t.park, slug)) continue;
    if (!isOpen(c) || partyNight(c)) continue;
    const leg = findLeg(legs, t.park, slug);
    if (!leg) continue;
    const travel = eveningTravel(leg);
    if (travel.minutes > MAX_EVENING_TRAVEL_MINUTES) continue;
    const arrival = close + travel.minutes;
    const opens = closeMinute(t.date, c.opensAt);
    const closes = closeMinute(t.date, c.closesAt);
    if (opens > arrival || closes - arrival < MIN_EVENING_MINUTES) continue;

    const shows = input.shows.get(showsKey(slug, t.date));
    const highlights: EveningHighlight[] = [];
    for (const config of region.nightShows[slug] ?? []) {
      const time = showTime(shows, config.slug, arrival, closes);
      if (!time) continue;
      highlights.push({
        kind: config.kind,
        name: config.name,
        at: isoAt(t.date, time.minute, tz),
        source: time.source,
      });
    }
    highlights.sort((a, b) => (a.at ?? "").localeCompare(b.at ?? ""));
    const wait = rankWait(c);
    out.push({
      option: {
        kind: "hop",
        park: slug,
        from: isoAt(t.date, arrival, tz),
        to: c.closesAt,
        travel,
        ...(highlights.length > 0 && { highlights }),
        reasons: reasonsFor(travel.minutes, [
          "open_later",
          ...(highlights.length > 0 ? (["night_show"] as const) : []),
        ]),
      },
      rank: [
        highlights.length > 0 ? 0 : 1,
        levelRank(c.crowdLevel),
        wait ?? Number.MAX_SAFE_INTEGER,
        travel.minutes,
      ],
    });
  }
  return out.sort((a, b) =>
    (a.option.park ?? "").localeCompare(b.option.park ?? ""),
  );
}

/**
 * The resort's venues: a district open at least {@link MIN_EVENING_MINUTES}
 * after arrival, and a fireworks viewpoint only on a night its park has
 * fireworks within {@link MAX_VIEW_WAIT_MINUTES} of arrival and no party.
 * A fireworks view ranks first, then the shorter transfer.
 */
function venueCandidates(
  input: EveningInput,
  t: EveningTrigger,
  cells: Record<string, TripParkDayInternal>,
  close: number,
): Candidate[] {
  const { region, legs } = input;
  const tz = region.timezone;
  const group = groupOf(region, t.park);
  const out: Candidate[] = [];
  for (const venue of region.venues) {
    if (venue.group !== group) continue;
    const leg = findLeg(legs, t.park, venuePoint(venue.key));
    if (!leg) continue;
    const travel = eveningTravel(leg);
    if (travel.minutes > MAX_EVENING_TRAVEL_MINUTES) continue;
    const arrival = close + travel.minutes;

    const highlights: EveningHighlight[] = [];
    let to: number | undefined;
    if (venue.viewOf) {
      const viewed = cells[venue.viewOf];
      if (!isOpen(viewed) || partyNight(viewed)) continue;
      const show = viewFireworks(input, venue, t.date, arrival);
      if (!show) continue;
      highlights.push({
        kind: "fireworks_view",
        name: show.name,
        at: isoAt(t.date, show.minute, tz),
        source: show.source,
      });
      to = show.minute + VIEW_MINUTES;
    } else if (venue.typicalHours) {
      to = typicalClose(venue.typicalHours);
      if (to - arrival < MIN_EVENING_MINUTES) continue;
    } else {
      to = hhmm(DEFAULT_EVENING_END) as number;
      if (to - arrival < MIN_EVENING_MINUTES) continue;
    }
    for (const kind of venue.highlights ?? []) highlights.push({ kind });

    out.push({
      option: {
        kind: "venue",
        venue: venue.key,
        from: isoAt(t.date, arrival, tz),
        to: isoAt(t.date, to, tz),
        travel,
        ...(highlights.length > 0 && { highlights }),
        reasons: reasonsFor(travel.minutes, [
          ...(venue.typicalHours ? (["open_later"] as const) : []),
          ...(venue.viewOf ? (["fireworks_view"] as const) : []),
          ...(venue.free ? (["free_entry"] as const) : []),
        ]),
      },
      rank: [venue.viewOf ? 0 : 1, travel.minutes],
    });
  }
  return out.sort((a, b) =>
    (a.option.venue ?? "").localeCompare(b.option.venue ?? ""),
  );
}

/** A venue's typical close in minutes from midnight; past midnight counts on. */
function typicalClose(hours: { opens: string; closes: string }): number {
  const opens = hhmm(hours.opens) ?? 0;
  const closes = hhmm(hours.closes) ?? 0;
  return closes <= opens ? closes + 1440 : closes;
}

/** The viewed park's fireworks on a date within the wait a view allows. */
function viewFireworks(
  input: EveningInput,
  venue: VenueConfig,
  date: string,
  arrival: number,
): { name: string; minute: number; source: EveningShow["source"] } | undefined {
  const park = venue.viewOf as string;
  const shows = input.shows.get(showsKey(park, date));
  for (const config of input.region.nightShows[park] ?? []) {
    if (config.kind !== "fireworks") continue;
    const time = showTime(
      shows,
      config.slug,
      arrival,
      arrival + MAX_VIEW_WAIT_MINUTES,
    );
    if (time) return { name: config.name, ...time };
  }
  return undefined;
}
