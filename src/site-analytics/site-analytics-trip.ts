import { OVERFLOW_VALUE } from "./site-analytics.constants";
import { byCountDesc } from "./site-analytics.query";
import {
  NO_VALUE,
  matchesSpec as matches,
  tallyKey,
  tallyRows,
  type EventListSpec as ListSpec,
} from "./site-analytics-event-props";

/**
 * The trip planner's events, as the frontend sends them (`trackTripEvent` in
 * its `lib/trip-regions/plan-analytics.ts`, the list in its
 * docs/features/orlando-planner.md#analytics): every one carries `region`.
 * `ticket_link_clicked` has the eight properties the statistic allows and so
 * no region: its product's slug belongs to one region's catalog. The 2.21.0
 * planner (PAR-1309, dates first) adds the events of the tracking audit's
 * T10–T18 and T27; a name the frontend does not send yet counts nothing.
 */
export const TRIP_EVENTS = {
  started: "trip_planner_started",
  ticketsAnswered: "trip_tickets_answered",
  ticketAdded: "trip_ticket_added",
  ticketRemoved: "trip_ticket_removed",
  ticketStart: "trip_ticket_start",
  planRequested: "trip_plan_requested",
  planShown: "trip_plan_shown",
  windowPlanned: "trip_window_planned",
  ticketDetailsOpened: "trip_ticket_details_opened",
  hopperToggled: "trip_hopper_toggled",
  ticketsAdopted: "trip_tickets_adopted",
  longStayUsed: "trip_long_stay_used",
  viewChanged: "trip_view_changed",
  dayOpened: "trip_day_opened",
  dayPinned: "trip_day_pinned",
  daySwapped: "trip_day_swapped",
  daySent: "trip_day_sent",
  daysSaved: "trip_days_saved",
  icsExported: "trip_ics_exported",
  linkCopied: "trip_link_copied",
  eveningLinkClicked: "trip_evening_link_clicked",
  outsideTicketAction: "trip_outside_ticket_action",
  editOpened: "trip_edit_opened",
  optionChanged: "trip_option_changed",
  ticketsOpened: "trip_tickets_opened",
  leftOutFix: "trip_left_out_fix",
  findToggled: "trip_find_toggled",
  findPeriod: "trip_find_period",
  calendarMode: "trip_calendar_mode",
  accuracyOpened: "trip_accuracy_opened",
  staySuggestion: "trip_stay_suggestion",
  viewSeen: "trip_view_seen",
  pinsAction: "trip_pins_action",
  tileClicked: "trip_tile_clicked",
} as const;

const E = TRIP_EVENTS;

/** A purchase link of the planner's ticket catalog followed. */
export const TICKET_LINK_EVENT = "ticket_link_clicked";

/**
 * The planner tile on a region park's pages came on screen (PAR-1317): a
 * `section_seen` of its own section, which names no region; the park in its
 * path does.
 */
export const TILE_SEEN = {
  name: "section_seen",
  section: "trip_tile",
} as const;

/**
 * The events only the dates-first planner sends: the first UTC day any of
 * them was counted is the day its funnel starts (`newFunnelFrom`).
 */
export const NEW_FUNNEL_EVENTS: readonly string[] = [
  E.editOpened,
  E.optionChanged,
  E.ticketsOpened,
];

/** The funnel's steps, in the order a visitor of the dates-first planner meets them. */
export const TRIP_FUNNEL = [
  "datesPicked",
  "planShown",
  "changed",
  "ticketsEntered",
  "kept",
] as const;
export type TripFunnelStep = (typeof TRIP_FUNNEL)[number];

/** The days before the dates-first planner's first event, and the days from it on. */
export type TripFunnelEra = "before" | "since";

const KEPT = [E.daysSaved, E.icsExported, E.linkCopied];

/**
 * What each step counts in each era; null where the era has no such event.
 * Before the dates-first planner, a start was the step before the dates and
 * a ticket answer of `yes` the way into the tickets; it sent nothing when a
 * plan was changed.
 */
export const TRIP_FUNNEL_EVENTS: Record<
  TripFunnelStep,
  Record<TripFunnelEra, readonly string[] | null>
> = {
  datesPicked: { before: [E.started], since: [E.started] },
  planShown: { before: [E.planShown], since: [E.planShown] },
  changed: { before: null, since: [E.editOpened, E.optionChanged] },
  ticketsEntered: { before: [E.ticketsAnswered], since: [E.ticketsOpened] },
  kept: { before: KEPT, since: KEPT },
};

/** Whether a row counts for a step in an era, its properties included. */
function countsFor(
  step: TripFunnelStep,
  era: TripFunnelEra,
  row: TripEventRow,
): boolean {
  const events = TRIP_FUNNEL_EVENTS[step][era];
  if (!events?.includes(row.name)) return false;
  // Parky's changes are his; the funnel follows the visitor's own.
  if (row.name === E.optionChanged) return row.props.by !== "parky";
  if (row.name === E.ticketsAnswered) return row.props.answer === "yes";
  return true;
}

/** One event's count for one combination of its properties, on a UTC day and page type. */
export interface TripEventRow {
  name: string;
  props: Record<string, string>;
  count: number;
  /** The row's UTC day; absent: before every cut-over. */
  day?: string;
  pageType?: string;
  /** The page, for a tile's view: its park names the region. */
  path?: string;
}

export const TRIP_LISTS = {
  sources: { events: [E.started], props: ["source"] },
  startedVia: { events: [E.started], props: ["via"], where: { via: "*" } },
  ticketAnswers: { events: [E.ticketsAnswered], props: ["answer"] },
  requestMode: { events: [E.planRequested], props: ["mode"] },
  requestDays: { events: [E.planRequested], props: ["days"] },
  requestParks: { events: [E.planRequested], props: ["parks"] },
  requestTickets: { events: [E.planRequested], props: ["tickets"] },
  requestHopper: { events: [E.planRequested], props: ["hopper"] },
  requestBlocks: { events: [E.planRequested], props: ["blocks"] },
  requestRest: { events: [E.planRequested], props: ["rest"] },
  shown: { events: [E.planShown], props: ["mode", "outcome"] },
  relaxed: {
    events: [E.planShown],
    props: ["relaxed"],
    where: { outcome: "relaxed" },
  },
  unplaced: {
    events: [E.planShown],
    props: ["unplaced"],
    where: { mode: "dates" },
    not: { outcome: "error" },
  },
  leftOut: {
    events: [E.planShown],
    props: ["left_out"],
    where: { mode: "dates", left_out: "*" },
    not: { outcome: "error" },
  },
  stays: {
    events: [E.planShown],
    props: ["stays"],
    where: { stays: "*" },
    not: { outcome: "error" },
  },
  errors: {
    events: [E.planShown],
    props: ["mode", "status"],
    where: { outcome: "error" },
  },
  leftOutFixes: { events: [E.leftOutFix], props: ["fix", "left_out"] },
  windowsFound: {
    events: [E.planShown],
    props: ["windows"],
    where: { mode: "when" },
    not: { outcome: "error" },
  },
  windowRanks: { events: [E.windowPlanned], props: ["rank"] },
  findToggled: { events: [E.findToggled], props: ["to"] },
  findPeriods: { events: [E.findPeriod], props: ["period", "school_breaks"] },
  edits: { events: [E.editOpened], props: ["panel", "from"] },
  options: { events: [E.optionChanged], props: ["option", "value", "by"] },
  staySuggestion: { events: [E.staySuggestion], props: ["action"] },
  ticketsOpened: { events: [E.ticketsOpened], props: ["from", "first"] },
  ticketsAdded: { events: [E.ticketAdded], props: ["via", "entry"] },
  ticketStarts: { events: [E.ticketStart], props: ["start"] },
  ticketDetails: { events: [E.ticketDetailsOpened], props: ["surface"] },
  outsideTicket: { events: [E.outsideTicketAction], props: ["action"] },
  hopperToggled: { events: [E.hopperToggled], props: ["on"] },
  calendarMode: { events: [E.calendarMode], props: ["mode", "from_default"] },
  viewsChanged: { events: [E.viewChanged], props: ["view"] },
  viewsSeen: { events: [E.viewSeen], props: ["view", "chosen"] },
  dayOpened: { events: [E.dayOpened], props: ["kind", "view"] },
  dayOpenedByView: { events: [E.dayOpened], props: ["view"] },
  dayPinned: { events: [E.dayPinned], props: ["pinned"] },
  pinsActions: { events: [E.pinsAction], props: ["action"] },
  tileClicks: { events: [E.tileClicked], props: ["link"] },
  accuracyOpened: { events: [E.accuracyOpened], props: ["surface"] },
  savedDays: { events: [E.daysSaved], props: ["days"] },
  icsDays: { events: [E.icsExported], props: ["days"] },
  linksByKindAndPosition: {
    events: [TICKET_LINK_EVENT],
    props: ["kind", "position"],
  },
  linksBySurface: { events: [TICKET_LINK_EVENT], props: ["surface"] },
  linksByMode: { events: [TICKET_LINK_EVENT], props: ["ticket_mode"] },
  linksByLocaleMatch: { events: [TICKET_LINK_EVENT], props: ["locale_match"] },
  linksBySeller: {
    events: [TICKET_LINK_EVENT],
    props: ["seller", "host", "kind"],
  },
  linksByProduct: { events: [TICKET_LINK_EVENT], props: ["product"] },
} as const satisfies Record<string, ListSpec>;
export type TripListKey = keyof typeof TRIP_LISTS;

/** The counts outside the lists, and what each counts. */
export const TRIP_TOTALS = {
  planRequested: { events: [E.planRequested], props: [] },
  datesKnown: {
    events: [E.planRequested],
    props: [],
    where: { mode: "dates" },
  },
  datesSearched: {
    events: [E.planRequested],
    props: [],
    where: { mode: "when" },
  },
  parksHeadliners: {
    events: [E.planRequested],
    props: [],
    where: { parks: "0" },
  },
  shownOk: { events: [E.planShown], props: [], where: { outcome: "ok" } },
  shownRelaxed: {
    events: [E.planShown],
    props: [],
    where: { outcome: "relaxed" },
  },
  shownError: { events: [E.planShown], props: [], where: { outcome: "error" } },
  findDatesShown: {
    events: [E.planShown],
    props: [],
    where: { mode: "when" },
  },
  windowsPlanned: { events: [E.windowPlanned], props: [] },
  editOpened: { events: [E.editOpened], props: [] },
  optionChanged: {
    events: [E.optionChanged],
    props: [],
    not: { by: "parky" },
  },
  optionChangedByParky: {
    events: [E.optionChanged],
    props: [],
    where: { by: "parky" },
  },
  ticketsOpened: { events: [E.ticketsOpened], props: [] },
  ticketsAnsweredYes: {
    events: [E.ticketsAnswered],
    props: [],
    where: { answer: "yes" },
  },
  ticketRemoved: { events: [E.ticketRemoved], props: [] },
  ticketsAdopted: { events: [E.ticketsAdopted], props: [] },
  longStayUsed: { events: [E.longStayUsed], props: [] },
  viewChanged: { events: [E.viewChanged], props: [] },
  dayOpened: { events: [E.dayOpened], props: [] },
  daySwapped: { events: [E.daySwapped], props: [] },
  daySent: { events: [E.daySent], props: [] },
  eveningLinkClicked: { events: [E.eveningLinkClicked], props: [] },
  daysSaved: { events: [E.daysSaved], props: [] },
  icsExported: { events: [E.icsExported], props: [] },
  linkCopied: { events: [E.linkCopied], props: [] },
  ticketLinks: { events: [TICKET_LINK_EVENT], props: [] },
  tileClicked: { events: [E.tileClicked], props: [] },
  tilePlan: { events: [E.tileClicked], props: [], where: { link: "plan" } },
  tileCalendar: {
    events: [E.tileClicked],
    props: [],
    where: { link: "calendar" },
  },
} as const satisfies Record<string, ListSpec>;
export type TripTotalKey =
  keyof typeof TRIP_TOTALS | "unattributedTicketLinks" | "tileShown";
export type TripTotals = Record<TripTotalKey, number>;

export interface ValueCount {
  value: string;
  count: number;
}

/** What the figures are read for. */
export interface TripFigureOptions {
  /** One region's figures; null: every region's. */
  region: string | null;
  /** The region slugs park.fan has; any other value of `region` is `(other)`. */
  knownRegions: readonly string[];
  /** Each catalog product's region, for the purchase links, which name none. */
  productRegions: ReadonlyMap<string, string>;
  /**
   * Each region park's region by its page (`continent/country/city/slug`,
   * {@link parkKeyOf}), for the tile's views, which name none.
   */
  parkRegions?: ReadonlyMap<string, string>;
}

/** The park a page belongs to, as `continent/country/city/slug`; null for a page of no park. */
export function parkKeyOf(path: string | undefined): string | null {
  const [, , parks, continent, country, city, slug] = (path ?? "").split("/");
  return parks === "parks" && continent && country && city && slug
    ? `${continent}/${country}/${city}/${slug}`
    : null;
}

/** One funnel step's counts per era; null where the window has none of that era's days or the era no such event. */
export interface TripFunnelCounts {
  step: TripFunnelStep;
  before: number | null;
  since: number | null;
}

export interface TripPlannerFigures {
  region: string | null;
  /** The first UTC day the dates-first planner's events were counted; null: none yet. */
  newFunnelFrom: string | null;
  funnel: TripFunnelCounts[];
  totals: TripTotals;
  regions: Array<{
    region: string;
    datesPicked: number;
    planRequested: number;
    planShown: number;
    errors: number;
    kept: number;
    ticketLinks: number;
  }>;
  /** The tile on the region parks' pages as a way in: views and clicks per region and link. */
  tile: Array<{
    region: string;
    shown: number;
    clicked: number;
    plan: number;
    calendar: number;
  }>;
  /** Where the planner is used: its starts, requests and answers by the page they came from. */
  pageTypes: Array<{
    pageType: string;
    datesPicked: number;
    planRequested: number;
    planShown: number;
  }>;
  lists: Record<TripListKey, Array<Record<string, string> & { count: number }>>;
}

/**
 * The trip planner report's figures from the events' property counts: the
 * funnel per era (`newFunnelFrom` splits the window), the counts outside the
 * lists, every breakdown of {@link TRIP_LISTS}, the purchase links attributed
 * to a region by their product, the regions side by side and the page types
 * the planner was used from. A value of `region` park.fan does not have is
 * `(other)`; an empty property value is `(none)`.
 */
export function tripPlannerFigures(
  rows: readonly TripEventRow[],
  options: TripFigureOptions & {
    window?: { from: string; to: string };
    newFunnelFrom?: string | null;
  },
): TripPlannerFigures {
  const known = new Set(options.knownRegions);
  const cutover = options.newFunnelFrom ?? null;
  const window = options.window;
  const regionOf = (row: TripEventRow): string | null => {
    if (row.name === TICKET_LINK_EVENT) {
      return options.productRegions.get(row.props.product ?? "") ?? null;
    }
    const value = row.props.region ?? "";
    return known.has(value) ? value : OVERFLOW_VALUE;
  };
  const eraOf = (row: TripEventRow): TripFunnelEra =>
    cutover !== null && row.day !== undefined && row.day >= cutover
      ? "since"
      : "before";
  // An era the window has days of: before the cut-over, and from it on.
  const hasEra: Record<TripFunnelEra, boolean> = {
    before: cutover === null || !window || window.from < cutover,
    since: cutover !== null && (!window || window.to >= cutover),
  };

  const totals = Object.fromEntries(
    [...Object.keys(TRIP_TOTALS), "unattributedTicketLinks", "tileShown"].map(
      (k) => [k, 0],
    ),
  ) as TripTotals;
  const byTile = new Map<string, TripPlannerFigures["tile"][number]>();
  const tileOf = (region: string) => {
    const row = byTile.get(region) ?? {
      region,
      shown: 0,
      clicked: 0,
      plan: 0,
      calendar: 0,
    };
    byTile.set(region, row);
    return row;
  };
  const funnel = new Map<string, number>();
  const lists = new Map<TripListKey, Map<string, number>>();
  const byRegion = new Map<string, TripPlannerFigures["regions"][number]>();
  const byPage = new Map<string, TripPlannerFigures["pageTypes"][number]>();

  for (const row of rows) {
    const n = row.count;
    // The tile's views carry no region and are no planner event: their park names the region.
    if (row.name === TILE_SEEN.name) {
      if (row.props.section !== TILE_SEEN.section) continue;
      const key = parkKeyOf(row.path);
      const tileRegion =
        (key && options.parkRegions?.get(key)) || OVERFLOW_VALUE;
      if (options.region !== null && tileRegion !== options.region) continue;
      tileOf(tileRegion).shown += n;
      totals.tileShown += n;
      continue;
    }
    const region = regionOf(row);
    if (row.name === E.tileClicked && region !== null) {
      if (options.region === null || region === options.region) {
        const tile = tileOf(region);
        tile.clicked += n;
        if (row.props.link === "plan") tile.plan += n;
        if (row.props.link === "calendar") tile.calendar += n;
      }
    }
    if (row.name === TICKET_LINK_EVENT && region === null) {
      // Attributed to no region: counted, and listed only with every region.
      totals.unattributedTicketLinks += n;
      if (options.region !== null) continue;
    } else if (region !== null) {
      let side = byRegion.get(region);
      if (!side) {
        side = {
          region,
          datesPicked: 0,
          planRequested: 0,
          planShown: 0,
          errors: 0,
          kept: 0,
          ticketLinks: 0,
        };
        byRegion.set(region, side);
      }
      if (row.name === E.started) side.datesPicked += n;
      if (row.name === E.planRequested) side.planRequested += n;
      if (row.name === E.planShown) {
        side.planShown += n;
        if (row.props.outcome === "error") side.errors += n;
      }
      if (KEPT.includes(row.name as (typeof KEPT)[number])) side.kept += n;
      if (row.name === TICKET_LINK_EVENT) side.ticketLinks += n;
      if (options.region !== null && region !== options.region) continue;
    }

    const era = eraOf(row);
    for (const step of TRIP_FUNNEL) {
      if (countsFor(step, era, row)) {
        const k = `${step}|${era}`;
        funnel.set(k, (funnel.get(k) ?? 0) + n);
      }
    }
    for (const [key, spec] of Object.entries(TRIP_TOTALS)) {
      if (matches(spec, row)) totals[key as keyof typeof TRIP_TOTALS] += n;
    }
    for (const [key, spec] of Object.entries(TRIP_LISTS) as Array<
      [TripListKey, ListSpec]
    >) {
      if (!matches(spec, row)) continue;
      const k = tallyKey(row, spec.props);
      const tally = lists.get(key) ?? new Map<string, number>();
      tally.set(k, (tally.get(k) ?? 0) + n);
      lists.set(key, tally);
    }
    if (
      row.pageType !== undefined &&
      (row.name === E.started ||
        row.name === E.planRequested ||
        row.name === E.planShown)
    ) {
      const page = byPage.get(row.pageType) ?? {
        pageType: row.pageType || NO_VALUE,
        datesPicked: 0,
        planRequested: 0,
        planShown: 0,
      };
      if (row.name === E.started) page.datesPicked += n;
      if (row.name === E.planRequested) page.planRequested += n;
      if (row.name === E.planShown) page.planShown += n;
      byPage.set(row.pageType, page);
    }
  }

  return {
    region: options.region,
    newFunnelFrom: cutover,
    funnel: TRIP_FUNNEL.map((step) => {
      const count = (era: TripFunnelEra) =>
        hasEra[era] && TRIP_FUNNEL_EVENTS[step][era] !== null
          ? (funnel.get(`${step}|${era}`) ?? 0)
          : null;
      return { step, before: count("before"), since: count("since") };
    }),
    totals,
    tile: byCountDesc(
      [...byTile.values()],
      (row) => row.shown,
      (row) => row.region,
    ),
    regions: byCountDesc(
      [...byRegion.values()],
      (row) => row.datesPicked,
      (row) => row.region,
    ),
    pageTypes: byCountDesc(
      [...byPage.values()],
      (row) => row.datesPicked + row.planRequested,
      (row) => row.pageType,
    ),
    lists: Object.fromEntries(
      (Object.entries(TRIP_LISTS) as Array<[TripListKey, ListSpec]>).map(
        ([key, spec]) => [key, tallyRows(lists.get(key), spec.props)],
      ),
    ) as TripPlannerFigures["lists"],
  };
}
