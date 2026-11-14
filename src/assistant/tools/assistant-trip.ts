import { formatInTimeZone } from "date-fns-tz";
import { getFrontendUrl } from "../../config/oauth.config";
import { nameKey } from "../../mcp/tools/park-ref";
import { addIsoDays } from "../../common/utils/date.util";
import type { NormalizedPlanQuery } from "../../trip-regions/trip-plan.service";
import type {
  InternalCalendarDay,
  InternalRegionCalendar,
} from "../../trip-regions/trip-region.service";
import { heatOf, stormWarning } from "../../trip-regions/trip-optimizer";
import type {
  PlanDayReason,
  PlannedDay,
  PlannedParkVisit,
  PlanTotals,
} from "../../trip-regions/trip-optimizer";
import { TRIP_REGIONS, type RegionConfig } from "../../trip-regions/regions";
import { stormSeasonSpan } from "../../trip-regions/trip-region.assembly";
import { travelSource, type TripLeg } from "../../trip-regions/travel";
import type {
  EveningOption,
  PlanEvening,
} from "../../trip-regions/trip-evening";
import type { TripParkDayInternal } from "../../trip-regions/trip-calendar.util";
import { ASSISTANT_LOCALES } from "../system-prompt";
import { TICKETS_PATTERN } from "../../trip-regions/dto/trip-region-query.dto";
import {
  canonicalTickets,
  productOf,
  type HopperVerdict,
  type TicketSummary,
  type TicketSuggestion,
} from "../../trip-regions/trip-tickets";
import type { WhenTicket } from "../../trip-regions/trip-when";
import type { TripWhen, WhenReason } from "../../trip-regions/trip-when";

/**
 * Parky's side of the trip regions' hubs (docs/frontend/trip-regions.md): the trip plan, the
 * region's events and its travel legs turned into what the model retells, and the hub's address
 * with a plan in its query. Pure: the tools in `assistant-tools.ts` read the services and pass
 * the results in. Every code the trip routes send becomes a short English phrase here, which the
 * model translates; nothing is left for it to decode. What differs by region (the storm season,
 * the perks' names, whose school breaks) is read off the region.
 */

/** A region park as the trip tools name and match it. */
export interface RegionParkName {
  slug: string;
  id: string;
  name: string;
  abbreviations: string[];
}

/** Several region parks match a reference; the model picks one by slug. */
export interface AmbiguousRegionPark {
  ambiguous: true;
  hint: string;
  parks: Array<{ slug: string; name: string }>;
}

/**
 * The region park a reference names: its slug, id, name or a curated abbreviation, else the one
 * park whose name or slug contains it ("Epic" → Epic Universe). Several parks containing it come
 * back as candidates; null when none matches, so the caller can ask the park search.
 */
export function regionParkFor(
  reference: string,
  parks: readonly RegionParkName[],
): RegionParkName | AmbiguousRegionPark | null {
  const ref = reference.trim().toLowerCase();
  const key = nameKey(reference);
  const exact = parks.find(
    (p) =>
      p.slug === ref ||
      p.id === ref ||
      nameKey(p.name) === key ||
      p.abbreviations.some((a) => nameKey(a) === key),
  );
  if (exact) return exact;
  // "AK" or "MK" without a curated abbreviation would match too much by containment.
  if (key.length < 4) return null;
  const partial = parks.filter(
    (p) => nameKey(p.name).includes(key) || nameKey(p.slug).includes(key),
  );
  if (partial.length === 1) return partial[0];
  if (partial.length === 0) return null;
  return {
    ambiguous: true,
    hint: "Several parks of the region match; call again with one slug",
    parks: partial.map(({ slug, name }) => ({ slug, name })),
  };
}

/** The heat warning in words, the same in every region. */
const HEAT_PHRASE = "extreme heat: hotter than a usual summer day";

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/** A `MM-DD` as a month's part: "early June", "mid-August", "late October". */
function partOfMonth(md: string): string {
  const month = MONTH_NAMES[Number(md.slice(0, 2)) - 1];
  const day = Number(md.slice(3, 5));
  if (day <= 10) return `early ${month}`;
  if (day <= 20) return `mid-${month}`;
  return `late ${month}`;
}

/**
 * The region's season phrases: its storm season and peak (Orlando's "hurricane season (June to
 * November)", Japan's typhoons) and its rainy season, by the warning code each stands for.
 */
export function seasonPhrases(region: RegionConfig): Record<string, string> {
  const out: Record<string, string> = {};
  const storm = region.weather.storm;
  if (storm) {
    const span = stormSeasonSpan(storm);
    const month = (md: string) => MONTH_NAMES[Number(md.slice(0, 2)) - 1];
    out[`${storm.kind}_season`] =
      `${storm.kind} season (${month(span.from)} to ${month(span.to)}), a fact about the season, not a forecast`;
    out[`${storm.kind}_peak`] =
      `peak of the ${storm.kind} season (${partOfMonth(span.peak.from)} to ${partOfMonth(span.peak.to)}), a fact about the season, not a forecast`;
  }
  const rainy = region.weather.notes?.find((n) => n.kind === "rainy_season");
  if (rainy) {
    out.rainy_season = `rainy season (${partOfMonth(rainy.from)} to ${partOfMonth(rainy.to)}): showers likely, a note about the season, not a forecast`;
  }
  return out;
}

/**
 * A region day's heat and storm warnings as phrases, from the calendar day's weather (forecast,
 * else the climate normal) and its date, for a tool that reads the park calendar directly
 * (`compare_parks`).
 */
export function heatAndStormWatch(
  region: RegionConfig,
  date: string,
  weather: { tempMax?: number; basis?: string } | null | undefined,
  themePark: boolean,
): string[] {
  const watch: string[] = [];
  if (
    themePark &&
    weather &&
    heatOf(
      {
        tempMax: weather.tempMax,
        basis:
          weather.basis === "climate_normal" ? "climate_normal" : "forecast",
      },
      region.weather.heat,
    )
  ) {
    watch.push(HEAT_PHRASE);
  }
  const storm = stormWarning(region.weather.storm, date);
  if (storm) watch.push(seasonPhrases(region)[storm]);
  return watch;
}

/** The plan's reasons in words; a perk's reason is the day's `perks`, named once. */
const REASONS: Record<PlanDayReason, string> = {
  quietest_in_window: "its quietest day of the trip",
  runner_up_close: "another day is nearly as good",
  weekday: "a weekday",
  holiday_ends: "a holiday or school break just ended",
  water_park_weather: "warm and dry",
  busiest_day_spared: "rests on a busy day",
  rest_after_streak: "rest after a run of park days",
  locked: "pinned by the user",
};

/** A plan day's warnings in words, the region's seasons included. */
function warningPhrases(region: RegionConfig): Record<string, string> {
  return {
    party_night_short_hours: "party night: day guests leave at the early close",
    fireworks_missed:
      "party night: day guests miss the evening show; no other day fitted",
    hours_not_published: "hours not published yet",
    estimate_last_year: "estimate from last year, not a forecast",
    long_range: "long-range forecast, less sure",
    no_forecast: "no figure yet",
    adjacent_repeat: "second visit the day after the first",
    long_streak: "over four park days in a row",
    extreme_heat: HEAT_PHRASE,
    outside_ticket_window:
      "outside the ticket's valid dates: no day inside them fitted",
    long_day_trip: "a long day trip: the travel takes hours of the park's day",
    move_day: "a move to the next city first: the park day starts later",
    ...seasonPhrases(region),
  };
}

/** A hotel perk's name on a plan day, the region's own; the prompt says what Orlando's are. */
function perkName(region: RegionConfig, perk: string): string {
  return region.perkNames[perk] ?? perk;
}

const UNPLACED: Record<string, string> = {
  no_open_day:
    "closed on every day of the trip, or open on fewer days than visits",
  no_day_left: "no day left for it",
  no_ticket_day:
    "its ticket has no day left for it (days or per-park limit used up)",
  no_base: "no city of the trip reaches it",
};

const IGNORED_LOCKS: Record<string, string> = {
  outside_trip: "the day is outside the trip",
  closed: "the park is closed that day",
  day_taken: "that day is pinned already",
  no_visit_left: "more pins than visits",
  base_unreachable: "the trip's city that day cannot reach the park",
};

const RELAXED: Record<string, string> = {
  blocks_relaxed: "the resorts could not each keep their days together",
  streak_relaxed: "too few days to rest after every four park days",
  ticket_window_relaxed:
    "a visit lies outside its ticket's valid dates: no day inside them fitted",
};

/** Why an owned ticket's days go unused, in words. */
const UNUSED: Record<string, string> = {
  parks_fewer_than_days: "the chosen parks need fewer days",
  rest_days_kept: "the rest days the trip keeps",
  window_outside_trip: "part of its valid dates lies outside the trip",
  no_open_day: "its parks are closed on the days left",
};

const TICKET_WARNINGS: Record<string, string> = {
  blockouts_unchecked: "blockout dates not checked: check the pass's calendar",
  park_reservation_needed: "passholders reserve their first park each day",
  covers_no_planned_park: "covers none of the planned parks",
};

/** The before-opening and evening slots, as get_park_events names them; others by the region's perk name. */
const SLOTS: Record<string, string> = {
  early_entry: "Early Entry (Disney resort hotel guests)",
  early_park_admission: "Early Park Admission (Universal hotel guests)",
  extended_evening: "Extended Evening Hours (Disney deluxe hotel guests)",
  ticketed_event:
    "ticketed evening event, a separate ticket; day guests leave at dayGuestsLeave",
};

/** A slot's name: Orlando's curated phrase, else the region's perk name, else its code. */
function slotName(region: RegionConfig | undefined, kind: string): string {
  return SLOTS[kind] ?? region?.perkNames[kind] ?? kind;
}

/** A season without a name of its own goes by its kind, as the site's label does. */
const SEASON_KINDS: Record<string, string> = {
  halloween: "Halloween season",
  christmas: "Christmas season",
  summer_nights: "summer nights",
  special_event: "special event",
  opening: "opening season",
  closure: "closure",
  maintenance: "maintenance",
};

/** A season's name, or its kind's when nobody gave it one. */
function seasonName(season: { name?: unknown; kind?: unknown }): string {
  return typeof season.name === "string" && season.name
    ? season.name
    : (SEASON_KINDS[String(season.kind)] ?? String(season.kind));
}

const SEASON_STATUS: Record<string, string> = {
  confirmed: "the park published the dates",
  announced: "the park announced it; details may follow",
  expected: "last year's pattern; nothing announced yet",
  cancelled: "called off",
};

/** Where a drive ends short of the gate, or how the resort's transport goes, by a leg's `via`. */
const VIA: Record<string, string> = {
  ttc: "car park at the Transportation and Ticket Center, then monorail or ferry",
  garage_shuttle: "Universal's garages, then the shuttle",
  caribbean_beach: "change at Caribbean Beach",
  international_gateway: "EPCOT's International Gateway",
  citywalk: "through CityWalk",
};

/** "Tuesday" for a YYYY-MM-DD. */
export function weekdayOf(date: string): string {
  return formatInTimeZone(new Date(`${date}T12:00:00Z`), "UTC", "EEEE");
}

/** Days in order as ranges: ["2026-11-08..2026-11-10", "2026-11-13"]. */
export function dateRuns(dates: readonly string[]): string[] {
  const sorted = [...new Set(dates)].sort();
  const runs: string[] = [];
  let start: string | null = null;
  let last: string | null = null;
  const close = () => {
    if (start) runs.push(start === last ? start : `${start}..${last}`);
  };
  for (const date of sorted) {
    if (last && addIsoDays(last, 1) === date) {
      last = date;
      continue;
    }
    close();
    start = date;
    last = date;
  }
  close();
  return runs;
}

/**
 * "HH:mm" on the park's clock, worked out here as `localizeTimes` would, so a row with its date
 * already on it, or a slot over many days, carries a time and not a second date. An end at
 * midnight reads 24:00, as the seasons write it.
 */
function clock(instant: string, timezone: string, end = false): string {
  const time = formatInTimeZone(new Date(instant), timezone, "HH:mm");
  return end && time === "00:00" ? "24:00" : time;
}

/** "09:00–22:00" on the park's clock. */
function clockRange(from: string, to: string, timezone: string): string {
  return `${clock(from, timezone)}–${clock(to, timezone, true)}`;
}

/** The /plan query names the hub reads, in the order the hub URL writes them. */
const HUB_PARAMS = [
  "from",
  "to",
  "parks",
  "hotel",
  "area",
  "blocks",
  "fireworks",
  "split",
  "rest",
  "ticket",
  "hop",
  "arrival",
  "departure",
  "lock",
  "tickets",
  "hopper",
  "bases",
  "startBase",
  "endBase",
] as const;

/** What each hub parameter may hold; anything else keeps a URL off the page. */
const HUB_VALUES: Record<(typeof HUB_PARAMS)[number], RegExp> = {
  from: /^\d{4}-\d{2}-\d{2}$/,
  to: /^\d{4}-\d{2}-\d{2}$/,
  parks: /^[a-z0-9-]+(:[1-9])?(,[a-z0-9-]+(:[1-9])?)*$/,
  hotel: /^[a-z_]+$/,
  area: /^[a-z_]+$/,
  blocks: /^[01]$/,
  fireworks: /^(want|any)$/,
  split: /^(auto|fixed)$/,
  rest: /^(auto|none|max:[1-9])$/,
  ticket: /^(base|hopper)$/,
  hop: /^[01]$/,
  arrival: /^(free|park)$/,
  departure: /^(free|park)$/,
  lock: /^[a-z0-9-]+@\d{4}-\d{2}-\d{2}(,[a-z0-9-]+@\d{4}-\d{2}-\d{2})*$/,
  tickets: TICKETS_PATTERN,
  hopper: /^[a-z0-9-]+(,[a-z0-9-]+)*$/,
  bases:
    /^[a-z0-9-]+(:\d{1,2})?(@[a-z0-9_-]+)?(,[a-z0-9-]+(:\d{1,2})?(@[a-z0-9_-]+)?)*$/,
  startBase: /^[a-z0-9-]+$/,
  endBase: /^[a-z0-9-]+$/,
};

/**
 * A region's hub on the site: `/<region slug>`, the same in every language. Orlando's,
 * `/orlando`, is the one live; each region's hub must sit at its slug.
 */
export function hubPathOf(region: Pick<RegionConfig, "slug">): string {
  return `/${region.slug}`;
}

/** Orlando's hub, `/orlando`. */
export const HUB_PATH = hubPathOf({ slug: "orlando" });

/** The plan's query string in the hub's parameter names, from the normalized plan. */
function hubQuery(q: NormalizedPlanQuery): string {
  const values: Partial<Record<(typeof HUB_PARAMS)[number], string>> = {
    from: q.from,
    to: q.to,
    parks: q.parks
      .map((p) => (p.explicit ? `${p.slug}:${p.visits}` : p.slug))
      .join(","),
    hotel: q.hotel,
    ...(q.area && { area: q.area }),
    blocks: q.blocks ? "1" : "0",
    fireworks: q.fireworks,
    split: q.split,
    rest: q.rest.mode === "max" ? `max:${q.rest.n}` : q.rest.mode,
    ...(q.ticket === "hopper" && { ticket: q.ticket }),
    hop: q.hop ? "1" : "0",
    arrival: q.arrival,
    departure: q.departure,
    ...(q.locks.length > 0 && {
      lock: q.locks.map((l) => `${l.target}@${l.date}`).join(","),
    }),
    ...(q.tickets.length > 0 && { tickets: canonicalTickets(q.tickets) }),
    ...(q.ticket !== "hopper" &&
      q.hopperResorts.length > 0 && { hopper: q.hopperResorts.join(",") }),
    ...(q.stays &&
      q.stays.bases.length > 0 && {
        bases: q.stays.bases
          .map(
            (b) =>
              `${b.base}${b.nights !== undefined ? `:${b.nights}` : ""}${b.area ? `@${b.area}` : ""}`,
          )
          .join(","),
      }),
    ...(q.stays?.startBase && { startBase: q.stays.startBase }),
    ...(q.stays?.endBase && { endBase: q.stays.endBase }),
  };
  const params = new URLSearchParams();
  for (const name of HUB_PARAMS) {
    const value = values[name];
    if (value) params.set(name, value);
  }
  return readableQuery(params);
}

/** `:`, `,` and `@` stay readable; URLSearchParams would write %3A, %2C and %40. */
function readableQuery(params: URLSearchParams): string {
  return params
    .toString()
    .replace(/%3A/g, ":")
    .replace(/%2C/g, ",")
    .replace(/%40/g, "@");
}

/** The region's hub with a plan in its query, where the planner reads it, opened at the planner. */
export function hubUrl(region: RegionConfig, q: NormalizedPlanQuery): string {
  return `${getFrontendUrl()}${hubPathOf(region)}?${hubQuery(q)}#planner`;
}

/**
 * The site path of a hub URL a tool returned (with or without a locale, which the widget adds),
 * rebuilt from the parameters the hub reads so nothing else rides along; null for anything else.
 */
export function hubPath(url: string): string | null {
  const site = new URL(getFrontendUrl());
  let parsed: URL;
  try {
    parsed = new URL(url, site);
  } catch {
    return null;
  }
  if (
    parsed.hostname.replace(/^www\./, "") !==
    site.hostname.replace(/^www\./, "")
  ) {
    return null;
  }
  if (parsed.username || parsed.password) return null;
  const segments = parsed.pathname.split("/").filter(Boolean);
  if (
    segments.length === 2 &&
    (ASSISTANT_LOCALES as readonly string[]).includes(segments[0])
  ) {
    segments.shift();
  }
  const hub = `/${segments[0]}`;
  if (
    segments.length !== 1 ||
    !Object.values(TRIP_REGIONS).some((r) => hubPathOf(r) === hub)
  ) {
    return null;
  }
  if (parsed.hash && parsed.hash !== "#planner") return null;
  const params = new URLSearchParams();
  for (const [name, value] of parsed.searchParams) {
    const pattern = (HUB_VALUES as Record<string, RegExp>)[name];
    if (!pattern || !pattern.test(value) || params.has(name)) return null;
    params.set(name, value);
  }
  const query = readableQuery(params);
  return query ? `${hub}?${query}#planner` : hub;
}

/** The plan as `TripPlanService.plan` returns it, in the optimizer's own types. */
export interface TripPlanLike {
  meta: Record<string, unknown>;
  days: Array<Record<string, unknown>>;
  totals: Record<string, unknown>;
  unplaced: Array<Record<string, unknown>>;
  ignoredLocks?: Array<Record<string, unknown>>;
}

type PlanDayWithTravel = PlannedDay & {
  travel?: TripLeg;
  between?: TripLeg;
  evening?: PlanEvening;
};

/**
 * A plan day's evening in one short phrase: where, when, how far, what is on,
 * and the alternatives by name. "projected": the show time is last same
 * weekday's, not a published one.
 */
export function eveningPhrase(
  evening: PlanEvening,
  names: ReadonlyMap<string, string>,
  region: RegionConfig,
): string {
  const placeOf = (o: EveningOption): string =>
    o.kind === "hop"
      ? `hop to ${names.get(o.park ?? "") ?? o.park}`
      : (region.venues.find((v) => v.key === o.venue)?.name ?? o.venue ?? "");
  const tz = region.timezone;
  const travel = evening.travel
    ? `, ${evening.travel.minutes} min by ${evening.travel.mode}`
    : "";
  const shows = (evening.highlights ?? [])
    .filter((h) => h.name && h.at)
    .map(
      (h) =>
        `${h.kind === "fireworks_view" ? `view of ${h.name}` : h.name} ${clock(h.at as string, tz)}${h.source === "projected" ? " (projected)" : ""}`,
    );
  const free = evening.reasons.includes("free_entry") ? ", free entry" : "";
  const others = (evening.alternatives ?? []).map(placeOf);
  return [
    `${placeOf(evening)} ${clockRange(evening.from, evening.to, tz)}${travel}${free}`,
    ...shows,
    ...(others.length > 0 ? [`or ${others.join(" or ")}`] : []),
  ].join("; ");
}

/** A leg as a plan day tells it: the minutes, the distance, how, and where the drive ends. */
function legOf(leg: TripLeg | undefined) {
  if (!leg) return undefined;
  const via = viaOf(leg.via);
  return {
    minutes: leg.minutes,
    ...(leg.km != null && { km: leg.km }),
    mode: leg.mode,
    ...(via && { via }),
    ...(leg.transferMinutes != null && { plusMinutes: leg.transferMinutes }),
  };
}

/** A leg's `via`, both ends of it when both parks have one ("ttc,garage_shuttle"). */
function viaOf(via: string | undefined): string | undefined {
  return via
    ?.split(",")
    .map((v) => VIA[v] ?? v)
    .join("; ");
}

/** One park's part of a plan day: the plan's figures beside the calendar's hours and events. */
function plannedPark(
  visit: PlannedParkVisit,
  cell: TripParkDayInternal | undefined,
  names: ReadonlyMap<string, string>,
  seasonNames: ReadonlyMap<string, string>,
  region: RegionConfig,
  hop: boolean,
) {
  const timezone = region.timezone;
  const events = (cell?.seasons ?? [])
    .map((id) => seasonNames.get(id))
    .filter((name): name is string => !!name);
  const party = cell?.evening?.kind === "ticketed_event" ? cell.evening : null;
  return {
    park: names.get(visit.slug) ?? visit.slug,
    ...(hop && { part: "half day" }),
    ...(visit.part === "evening" && { part: "evening, on an evening ticket" }),
    ...(visit.headlinerWait != null && { headlinerWait: visit.headlinerWait }),
    ...(visit.crowdLevel && { crowdLevel: visit.crowdLevel }),
    ...(visit.basis && { basis: visit.basis }),
    ...(cell?.opensAt &&
      cell.closesAt && {
        hours: clockRange(cell.opensAt, cell.closesAt, timezone),
      }),
    ...(party && {
      partyNight: clockRange(party.opensAt, party.closesAt, timezone),
    }),
    ...(visit.perks?.length && {
      perks: visit.perks.map((p) => perkName(region, p)),
    }),
    ...(events.length > 0 && { events }),
  };
}

/** The storm and rainy seasons are said once per trip (`season`), not on every day of it. */
const SEASON_WARNINGS = new Set<string>([
  "hurricane_season",
  "hurricane_peak",
  "typhoon_season",
  "typhoon_peak",
  "rainy_season",
]);

/** A plan day's warnings in words, the seasons left to the trip's `season`. */
function watch(day: PlannedDay, phrases: Record<string, string>): string[] {
  return day.warnings
    .filter((w) => !SEASON_WARNINGS.has(w))
    .map((w) => phrases[w] ?? w);
}

/**
 * The trip's season in one phrase: the storm season's peak, else the season, where a planned
 * day touches it, and the rainy season; undefined when none applies.
 */
function tripSeason(
  region: RegionConfig,
  days: readonly PlannedDay[],
): string | undefined {
  const phrases = seasonPhrases(region);
  const kind = region.weather.storm?.kind;
  const has = (code: string) =>
    days.some((d) => d.warnings.includes(code as never));
  const parts = [
    kind && has(`${kind}_peak`)
      ? phrases[`${kind}_peak`]
      : kind && has(`${kind}_season`)
        ? phrases[`${kind}_season`]
        : undefined,
    has("rainy_season") ? phrases.rainy_season : undefined,
  ].filter((p): p is string => !!p);
  return parts.length > 0 ? parts.join("; ") : undefined;
}

/** "heat and the hurricane season", or heat alone in a region without a storm season. */
function penaltiesMean(region: RegionConfig): string {
  const kind = region.weather.storm?.kind;
  return kind ? `heat and the ${kind} season` : "heat";
}

/**
 * The plan as the model retells it: each day with its weekday, kind, parks and their figures,
 * hours and events, the travel, and why it was chosen and what to watch, in words; the totals;
 * what could not be placed; the hub URL with the plan and the list `plan_trip` takes.
 */
export function vacationItinerary(input: {
  plan: TripPlanLike;
  calendar: InternalRegionCalendar;
  query: NormalizedPlanQuery;
  region: RegionConfig;
  names: ReadonlyMap<string, string>;
  ids: ReadonlyMap<string, string>;
}) {
  const { plan, calendar, query, region, names, ids } = input;
  const phrases = warningPhrases(region);
  const byDate = new Map<string, InternalCalendarDay>(
    calendar.days.map((day) => [day.date, day]),
  );
  const seasonNames = new Map(
    calendar.seasons.map((s) => {
      const status = String(s.status);
      const name = seasonName(s);
      return [
        String(s.id),
        status === "confirmed" ? name : `${name} (${status})`,
      ] as const;
    }),
  );
  const planned = plan.days as unknown as PlanDayWithTravel[];
  const owned = (plan.meta.tickets as TicketSummary[] | undefined) ?? [];
  const suggested =
    (plan.meta.ticketSuggestion as TicketSuggestion[] | undefined) ?? [];
  const mixing = plan.meta.mixing as
    { used: boolean; savedMinutesPerRide?: number } | undefined;
  const days = planned.map((day) => {
    const calendarDay = byDate.get(day.date);
    const hop = day.kind === "hop";
    const parks = day.parks.map((visit) => {
      const figures = hop
        ? visit
        : {
            ...visit,
            headlinerWait: day.headlinerWait,
            crowdLevel: day.crowdLevel,
            basis: day.basis,
            perks: day.perks,
          };
      return plannedPark(
        figures,
        calendarDay?.parks[visit.slug],
        names,
        seasonNames,
        region,
        hop,
      );
    });
    const travel = legOf(day.travel);
    const between = legOf(day.between);
    const why = day.reasons
      .filter((r): r is PlanDayReason => r in REASONS)
      .map((r) => REASONS[r]);
    const move = (day as unknown as { move?: DayMove }).move;
    return {
      date: day.date,
      weekday: weekdayOf(day.date),
      kind: day.kind,
      ...(day.base && { sleepsIn: baseName(region, day.base) }),
      ...(move && { move: movePhrase(region, move) }),
      ...(parks.length > 0 && { parks }),
      ...(calendarDay?.holidays?.length && { holidays: calendarDay.holidays }),
      ...(travel && { travel }),
      ...(between && { between }),
      ...(why.length > 0 && { why }),
      ...(watch(day, phrases).length > 0 && { watch: watch(day, phrases) }),
      ...(day.entryFrom && { entersAt: day.entryFrom }),
      ...(day.evening && {
        evening: eveningPhrase(day.evening, names, region),
      }),
      ...(ownedTicket(day, owned) && { ticket: ownedTicket(day, owned) }),
    };
  });
  const area = region.hotelAreas.find((a) => a.key === query.area);
  const totals = plan.totals as unknown as PlanTotals;
  const relaxed = (plan.meta.relaxed as string[] | undefined) ?? [];
  return {
    region: region.name,
    trip: { from: query.from, to: query.to, days: days.length },
    headlinerWaitMeans:
      "the big rides' wait at their busiest, in minutes, meaned over the park's headliners; not the wait at every hour",
    ...(area && { travelFrom: `the hotel area ${area.name}` }),
    ...(plan.meta.stays !== undefined && {
      stays: (plan.meta.stays as Array<Record<string, unknown>>).map(
        (st) =>
          `${baseName(region, String(st.base))}: ${st.nights} night${st.nights === 1 ? "" : "s"}, ${st.from}..${st.to}${st.auto ? " (chosen by the plan)" : ""}`,
      ),
    }),
    ...(plan.meta.staySuggestion !== undefined && {
      staySuggestion: staySuggestionPhrase(
        region,
        plan.meta.staySuggestion as StaySuggestionLike,
        names,
      ),
    }),
    ...(tripSeason(region, planned) && {
      season: tripSeason(region, planned),
    }),

    days,
    totals: {
      ...totals,
      means: `headlinerWait: the planned visits' mean; averageDayWait: the same parks on an average open day of the trip; savedMinutesPerRide: the difference, per headliner ride, not per day; penalties: minutes a park day that ${penaltiesMean(region)} added to the plan's cost, not to the waits`,
    },
    ...(plan.unplaced.length > 0 && {
      notPlaced: plan.unplaced.map((u) => ({
        park: names.get(String(u.slug)) ?? u.slug,
        why: UNPLACED[String(u.reason)] ?? u.reason,
      })),
    }),
    ...(plan.ignoredLocks?.length && {
      pinsNotKept: plan.ignoredLocks.map((l) => ({
        pin: l.lock,
        why: IGNORED_LOCKS[String(l.reason)] ?? l.reason,
      })),
    }),
    ...(relaxed.length > 0 && {
      loosened: relaxed.map((r) => RELAXED[r] ?? r),
    }),
    ...(plan.meta.exact === false && {
      searchCutShort: "a good plan, not proven the best",
    }),
    ...(owned.length > 0 && {
      tickets: owned.map((t) => ownedTicketSummary(t, region, names)),
    }),
    ...(mixing && {
      resortsMixed: mixing.used
        ? `yes${mixing.savedMinutesPerRide ? `, saving about ${mixing.savedMinutesPerRide} min a ride` : ""}`
        : "no, each resort's days stay together",
    }),
    ...(suggested.length > 0 && {
      ticketsToBuy: suggested.map((s, i) =>
        suggestionSummary(
          s,
          region,
          names,
          // Each shop once: a resort's second ticket shares its first's.
          suggested.findIndex((x) => x.shopUrl === s.shopUrl) === i,
        ),
      ),
      ticketsToBuyMeans:
        "what this plan needs where they hold no ticket; no prices",
    }),
    ...((plan.meta.warnings as string[] | undefined)?.includes(
      "hop_needs_hopper",
    ) && {
      hopIgnored:
        "two parks a day need a hopper ticket (ticket hopper); planned one park a day",
    }),
    hubUrl: hubUrl(region, query),
    // What plan_trip's days take to put the trip into the planner, by id: two parks can share a slug.
    reserve: (plan.days as unknown as PlannedDay[]).flatMap((day) =>
      day.parks.map((p) => ({
        park: ids.get(p.slug) ?? p.slug,
        date: day.date,
      })),
    ),
  };
}

/** A plan day's move as the route serves it. */
interface DayMove {
  from: string;
  to: string;
  minutes: number;
  mode: string;
  name?: string;
  when: "morning" | "evening" | "day";
  departAt: string;
  arriveAt: string;
}

interface StaySuggestionLike {
  nights: Record<string, number>;
  savedMinutesPerRide: number;
  park?: string;
  date?: string;
}

/** A base's name, else its key. */
function baseName(region: RegionConfig, key: string): string {
  return region.bases?.find((b) => b.key === key)?.name ?? key;
}

/** A move in one phrase: where, how, when, and what it does to the day. */
function movePhrase(region: RegionConfig, m: DayMove): string {
  const how = `${m.name ?? m.mode.replace(/_/g, " ")}, ${clock(m.departAt, region.timezone)}–${clock(m.arriveAt, region.timezone)}`;
  const route = `${baseName(region, m.from)} to ${baseName(region, m.to)}`;
  const when =
    m.when === "morning"
      ? "in the morning, the park after it"
      : m.when === "evening"
        ? "in the evening, after the park"
        : "the day's plan";
  return `${route} by ${how} (typical times), ${when}`;
}

/** The nights suggestion in one phrase. */
function staySuggestionPhrase(
  region: RegionConfig,
  s: StaySuggestionLike,
  names: ReadonlyMap<string, string>,
): string {
  const nights = Object.entries(s.nights)
    .map(
      ([base, n]) =>
        `${n} night${n === 1 ? "" : "s"} in ${baseName(region, base)}`,
    )
    .join(", ");
  const park = s.park ? (names.get(s.park) ?? s.park) : undefined;
  return `${nights} instead${park && s.date ? ` puts ${park} on ${s.date} (${weekdayOf(s.date)})` : ""}: about ${s.savedMinutesPerRide} min a ride less`;
}

/** An owned ticket's part of a plan day: "t1 day 2 of 14", or its allowance. */
function ownedTicket(
  day: PlannedDay,
  owned: readonly TicketSummary[],
): string | undefined {
  const t = owned.find((o) => o.id === day.ticket);
  if (!t) return undefined;
  if (day.ticketAllowance) return `${t.id}, water-park allowance`;
  const of = t.windowDays ?? t.days;
  return `${t.id} day ${day.ticketDay}${of ? ` of ${of}` : ""}`;
}

/** What the hopper adds, in one phrase. */
export function hopperPhrase(
  v: HopperVerdict,
  names: ReadonlyMap<string, string>,
): string {
  if (!v.recommended) return "not needed";
  const parts = [
    ...v.eveningHopList.map(
      (h) =>
        `an evening at ${names.get(h.parks[1]) ?? h.parks[1]} on ${h.date} (${weekdayOf(h.date)})`,
    ),
    ...(v.fitsOnlyWithHopper.length > 0
      ? [
          `fits ${[...new Set(v.fitsOnlyWithHopper)].map((p) => names.get(p) ?? p).join(", ")}`,
        ]
      : []),
    ...(v.savedMinutesPerRide >= 3
      ? [`saves about ${v.savedMinutesPerRide} min a ride`]
      : []),
  ];
  return `worth adding: ${parts.join("; ")}`;
}

function ownedTicketSummary(
  t: TicketSummary,
  region: RegionConfig,
  names: ReadonlyMap<string, string>,
) {
  const product = productOf(region, t.product);
  return {
    id: t.id,
    ticket: product?.name ?? t.product,
    ...(t.start && t.end && { valid: `${t.start}..${t.end}` }),
    ...(t.startChosen && { startPickedByPlan: true }),
    used: t.windowDays
      ? `${t.usedDays} park days in its ${t.windowDays}-day window`
      : t.days
        ? `${t.usedDays} of ${t.days} days`
        : `${t.usedDays} days (a pass)`,
    ...(t.unusedDays && {
      unused: `${t.unusedDays}: ${UNUSED[t.unusedReason ?? ""] ?? t.unusedReason}`,
    }),
    ...(t.hopper === "unknown" && {
      hopper: `not known, planned without; ${t.hopperVerdict ? hopperPhrase(t.hopperVerdict, names) : ""}`,
    }),
    ...(t.warnings.length > 0 && {
      watch: t.warnings.map((w) => TICKET_WARNINGS[w] ?? w),
    }),
  };
}

/** Whom an alternative is sold to, where not to everyone: "(UK/Ireland only)" in Orlando. */
function marketNote(region: RegionConfig, markets: readonly string[]): string {
  if (markets.includes(region.ticketMarkets.home)) return "";
  if (markets.includes("uk")) return " (UK/Ireland only)";
  return ` (${markets.join(", ")} only)`;
}

function suggestionSummary(
  s: TicketSuggestion,
  region: RegionConfig,
  names: ReadonlyMap<string, string>,
  withShop: boolean,
) {
  const product = productOf(region, s.product);
  return {
    ticket: product?.name ?? s.product,
    days: s.days,
    valid: `${s.start}..${s.end}`,
    ...(s.extraDays && {
      extraDays: `${s.extraDays}, so its dates reach the last park day`,
    }),
    ...(s.hopper && { hopper: hopperPhrase(s.hopper, names) }),
    ...(s.alternatives?.length && {
      alternatives: s.alternatives.map(
        (a) =>
          `${productOf(region, a.product)?.name ?? a.product}${marketNote(region, a.markets)}`,
      ),
    }),
    ...(withShop && { shop: s.shopUrl }),
  };
}

/** An owned ticket in a found window, in a phrase. */
function whenTicketPhrase(t: WhenTicket): string {
  return `${t.id}: ${t.start ? `${t.start}..${t.end}, ` : ""}${t.usedDays} days used${t.unusedDays ? `, ${t.unusedDays} unused` : ""}`;
}

/** A curated season as get_park_events gives it. */
export interface SeasonRow {
  parkSlug?: string;
  parkName: string;
  name: string | null;
  kind: string;
  status: string;
  startDate: string;
  endDate: string;
  dates: string[] | null;
  separateTicket: boolean;
  opensAt: string | null;
  closesAt: string | null;
}

/** The seasons overlapping a window, with each status's meaning once. */
export function seasonsSummary(
  seasons: readonly SeasonRow[],
  from: string,
  to: string,
) {
  const rows = seasons.map((s) => {
    const inWindow = s.dates?.filter((d) => d >= from && d <= to) ?? null;
    return {
      park: s.parkName,
      name: seasonName(s),
      kind: s.kind,
      status: s.status,
      runs:
        s.startDate === s.endDate
          ? s.startDate
          : `${s.startDate}..${s.endDate}`,
      ...(inWindow && { onlyOn: dateRuns(inWindow) }),
      ...(s.separateTicket && { separateTicket: true }),
      ...(s.opensAt && s.closesAt && { hours: `${s.opensAt}–${s.closesAt}` }),
    };
  });
  const statuses = [...new Set(seasons.map((s) => s.status))];
  return {
    seasons: rows,
    ...(statuses.length > 0 && {
      statusMeans: Object.fromEntries(
        statuses.map((s) => [s, SEASON_STATUS[s] ?? s]),
      ),
    }),
  };
}

/**
 * The early and evening slots of the region's days, per park and kind, as date ranges: an
 * early slot by its length (it moves with the opening), an evening by its times and, for a
 * ticketed night, the day guests' close.
 */
export function slotsSummary(
  days: readonly InternalCalendarDay[],
  slugs: readonly string[],
  names: ReadonlyMap<string, string>,
  timezone: string,
  region?: RegionConfig,
) {
  const groups = new Map<
    string,
    { row: Record<string, unknown>; dates: string[] }
  >();
  const add = (key: string, row: Record<string, unknown>, date: string) => {
    const group = groups.get(key) ?? { row, dates: [] };
    group.dates.push(date);
    groups.set(key, group);
  };
  for (const slug of slugs) {
    const park = names.get(slug) ?? slug;
    for (const day of days) {
      const cell = day.parks[slug];
      if (!cell || cell.status !== "OPERATING") continue;
      if (cell.early) {
        const minutes = Math.round(
          (Date.parse(cell.early.closesAt) - Date.parse(cell.early.opensAt)) /
            60_000,
        );
        add(
          `${slug}|${cell.early.kind}|${minutes}`,
          {
            park,
            slot: slotName(region, cell.early.kind),
            minutesBeforeOpening: minutes,
          },
          day.date,
        );
      }
      if (cell.evening) {
        const from = clock(cell.evening.opensAt, timezone);
        const until = clock(cell.evening.closesAt, timezone, true);
        const leave =
          cell.evening.kind === "ticketed_event" && cell.closesAt
            ? clock(cell.closesAt, timezone)
            : null;
        add(
          `${slug}|${cell.evening.kind}|${from}|${until}|${leave}`,
          {
            park,
            slot: slotName(region, cell.evening.kind),
            hours: `${from}–${until}`,
            ...(leave && { dayGuestsLeave: leave }),
          },
          day.date,
        );
      }
    }
  }
  return [...groups.values()].map(({ row, dates }) => ({
    ...row,
    days: dateRuns(dates),
  }));
}

/** A leg between two region points as get_travel_time tells it. */
export function travelSummary(
  leg: TripLeg,
  fromName: string,
  toName: string,
  region: RegionConfig,
) {
  const source = travelSource(region);
  const via = viaOf(leg.via);
  return {
    from: fromName,
    to: toName,
    minutes: leg.minutes,
    ...(leg.km != null && { km: leg.km }),
    mode: leg.mode,
    ...(leg.name && { line: leg.name }),
    ...(leg.mode === "drive" && {
      basis: "a drive with no traffic; real traffic adds to it",
    }),
    ...(via && { via }),
    ...(leg.transferMinutes != null && {
      plusMinutes: leg.transferMinutes,
      doorToDoorMinutes: leg.minutes + leg.transferMinutes,
    }),
    ...(leg.alternatives?.length && {
      alternatives: leg.alternatives.map((a) => ({
        mode: a.mode,
        ...(a.name && { name: a.name }),
        ...(a.minutes != null && { minutes: a.minutes }),
        ...(a.via && { via: VIA[a.via] ?? a.via }),
        ...(a.ticket === "park_to_park" && { needs: "a park-to-park ticket" }),
        ...(a.minutes == null && {
          note: "waits vary too much to name a time",
        }),
      })),
      alternativesMeans:
        "the resort's own transport; minutes are estimates, waiting included",
    }),
    source:
      source.source === "osrm" && leg.source !== "curated"
        ? `drive: OpenStreetMap via OSRM, ${source.attribution ?? ""}`
        : source.source === "curated"
          ? `curated from timetables and the operators' pages, checked ${source.computedOn}`
          : "curated from the operators' pages",
  };
}

// ── find_trip_dates (docs/frontend/trip-regions-when.md) ─────────────────────

/** Whose school breaks fill a region's parks, by the country its parks are in. */
const SCHOOL_OF_COUNTRY: Record<string, string> = {
  "united-states": "US",
  japan: "Japanese",
  france: "French",
};

/** "US" in Orlando: the school breaks a region's own visitors take; "local" where unknown. */
function localSchool(region: RegionConfig): string {
  return SCHOOL_OF_COUNTRY[region.parks[0]?.country ?? ""] ?? "local";
}

function whenReasons(region: RegionConfig): Record<WhenReason, string> {
  const school = localSchool(region);
  return {
    no_school_break: `no ${school} school break`,
    after_holidays: `just after a ${school} school break`,
    before_spring_break: `before the ${school} spring break`,
    festival: "a festival or holiday season runs",
    long_days: "long opening hours",
    mild_weather:
      "mild and fairly dry on average (climate normals, not a forecast)",
  };
}

function whenWarnings(region: RegionConfig): Record<string, string> {
  return {
    school_break: `${localSchool(region)} school break: busier`,
    ...seasonPhrases(region),
    extreme_heat: "extreme heat on a planned park day",
    party_nights: "party nights close a park early for day guests",
    fireworks_missed: "a planned night loses the evening show to a party",
    hours_not_published: "opening hours not published yet",
    estimate_last_year: "figures from last year, not a forecast",
    water_park_closed: "a chosen water park is closed for its season",
    ticket_window_relaxed:
      "a visit lies outside its ticket's valid dates: no day inside them fitted",
  };
}

const WHEN_BASIS: Record<string, string> = {
  forecast: "forecast",
  mixed: "partly forecast, partly last year",
  last_year: "last year's figures, not a forecast",
  none: "no figure",
};

/**
 * The best trip windows as the model retells them: dates with weekdays, the
 * plan's figure against the range's average, why and what to watch in words,
 * a `lastYear` flag where the figures are last year's, the events, and each
 * window's hub URL with the plan options; then the year strip by month.
 */
export function tripDatesSummary(input: {
  result: TripWhen;
  plan: NormalizedPlanQuery;
  /** The region with its ticket catalog, for the products' names. */
  region: RegionConfig;
  /** The region's park names, for the climate park; absent: its slug. */
  names?: ReadonlyMap<string, string>;
}) {
  const { result, plan, region } = input;
  const reasons = whenReasons(region);
  const warnings = whenWarnings(region);
  const storm = region.weather.storm?.kind;
  const climatePark =
    input.names?.get(region.weather.climatePark) ?? region.weather.climatePark;
  const seasonNames = new Map(
    result.seasons.map((s) => [String(s.id), seasonName(s)] as const),
  );
  const within = result.meta.within;
  return {
    range: { from: result.meta.from, to: result.meta.to },
    tripDays: result.meta.days,
    headlinerWaitMeans: `the planned park days' mean of the big rides' wait at their busiest, in minutes, early closings and hotel perks priced in; ${penaltiesMean(region)} rank a window lower (rankedLowerBy) but are not in it`,
    ...(within && {
      within: {
        region: within.name,
        breaks: within.periods.map((p) => `${p.from}..${p.to} ${p.name}`),
        ...(within.through && { knownUntil: within.through }),
      },
    }),
    ...(result.windows.length === 0 && {
      noWindow: within
        ? "no school break in the range is long enough for the trip"
        : "no window fits",
    }),
    windows: result.windows.map((w) => {
      const events = w.seasons
        .map((id) => seasonNames.get(id))
        .filter((name): name is string => !!name);
      return {
        from: `${w.from} (${weekdayOf(w.from)})`,
        to: `${w.to} (${weekdayOf(w.to)})`,
        ...(w.headlinerWait != null && { headlinerWait: w.headlinerWait }),
        ...(w.rangeAverageWait != null && {
          rangeAverageWait: w.rangeAverageWait,
        }),
        ...(w.headlinerWait != null &&
          w.rangeAverageWait && {
            percentBelowAverage: Math.round(
              (100 * (w.rangeAverageWait - w.headlinerWait)) /
                w.rangeAverageWait,
            ),
          }),
        basis: WHEN_BASIS[w.basis] ?? w.basis,
        ...(w.basis === "last_year" && { lastYear: true }),
        ...(w.reasons.length > 0 && {
          why: w.reasons.map((r) => reasons[r] ?? r),
        }),
        ...(w.warnings.length > 0 && {
          watch: w.warnings.map((x) => warnings[x] ?? x),
        }),
        ...(events.length > 0 && { events }),
        ...(w.penalties && {
          rankedLowerBy: [
            storm &&
              w.penalties[storm] &&
              `${storm} season, ${w.penalties[storm]} min a park day`,
            w.penalties.heat && `heat, ${w.penalties.heat} min a park day`,
          ].filter(Boolean),
        }),
        ...(w.tickets && { tickets: w.tickets.map(whenTicketPhrase) }),
        ...(w.ticketSuggestion && {
          ticketsToBuy: w.ticketSuggestion.map(
            (s) =>
              `${productOf(region, s.product)?.name ?? s.product}, ${s.days} days from ${s.start}`,
          ),
        }),
        hubUrl: hubUrl(region, { ...plan, from: w.from, to: w.to }),
      };
    }),
    months: result.months.map((m) => ({
      month: m.month,
      ...(m.headlinerWait != null && { headlinerWait: m.headlinerWait }),
      ...(m.basis === "last_year" && { lastYear: true }),
      ...(m.climate && {
        climate: `${m.climate.tempMax}/${m.climate.tempMin} °C, ${m.climate.precipitationMm} mm`,
      }),
      ...(m.localSchoolBreakDays.length > 0 && {
        schoolBreakDays: m.localSchoolBreakDays.length,
      }),
      ...(m.partyNights > 0 && { partyNights: m.partyNights }),
      ...(m.hurricaneSeason && { hurricaneSeason: true }),
      ...(m.typhoonSeason && { typhoonSeason: true }),
      ...(m.rainySeason && { rainySeason: true }),
    })),
    monthsMean: `headlinerWait: each chosen park on its best day of the week, meaned; climate: the 2015-2024 normals at ${climatePark} (high/low, month's rain), never a forecast; schoolBreakDays: days of ${localSchool(region)} school breaks`,
    ...(!result.meta.search.exact && {
      searchCutShort: "good windows, not proven the best",
    }),
  };
}
