import { OVERFLOW_VALUE } from "./site-analytics.constants";
import { byCountDesc } from "./site-analytics.query";

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
} as const;

const E = TRIP_EVENTS;

/** A purchase link of the planner's ticket catalog followed. */
export const TICKET_LINK_EVENT = "ticket_link_clicked";

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

/** The label of an empty property value. */
const NONE = "(none)";

/** One event's count for one combination of its properties, on a UTC day and page type. */
export interface TripEventRow {
  name: string;
  props: Record<string, string>;
  count: number;
  /** The row's UTC day; absent: before every cut-over. */
  day?: string;
  pageType?: string;
}

/**
 * A breakdown: the events it counts, the properties that name a row, and
 * the values the rows must have (`*` any but empty). Declared here so a new
 * event is one line.
 */
interface ListSpec {
  events: readonly string[];
  props: readonly string[];
  where?: Readonly<Record<string, string | readonly string[]>>;
  /** The values that must not be there. */
  not?: Readonly<Record<string, string>>;
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
} as const satisfies Record<string, ListSpec>;
export type TripTotalKey = keyof typeof TRIP_TOTALS | "unattributedTicketLinks";
export type TripTotals = Record<TripTotalKey, number>;

/** Whether a row matches a spec's event and property values. */
function matches(spec: ListSpec, row: TripEventRow): boolean {
  if (!spec.events.includes(row.name)) return false;
  for (const [prop, wanted] of Object.entries(spec.where ?? {})) {
    const value = row.props[prop] ?? "";
    const ok =
      wanted === "*"
        ? value !== ""
        : ([] as string[]).concat(wanted).includes(value);
    if (!ok) return false;
  }
  for (const [prop, unwanted] of Object.entries(spec.not ?? {})) {
    if ((row.props[prop] ?? "") === unwanted) return false;
  }
  return true;
}

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
    [...Object.keys(TRIP_TOTALS), "unattributedTicketLinks"].map((k) => [k, 0]),
  ) as TripTotals;
  const funnel = new Map<string, number>();
  const lists = new Map<TripListKey, Map<string, number>>();
  const byRegion = new Map<string, TripPlannerFigures["regions"][number]>();
  const byPage = new Map<string, TripPlannerFigures["pageTypes"][number]>();

  for (const row of rows) {
    const region = regionOf(row);
    const n = row.count;
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
      const k = JSON.stringify(spec.props.map((p) => row.props[p] || NONE));
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
        pageType: row.pageType || NONE,
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
        ([key, spec]) => [
          key,
          byCountDesc(
            [...(lists.get(key) ?? new Map<string, number>())].map(
              ([k, count]) => {
                const values = JSON.parse(k) as string[];
                const row = {
                  ...Object.fromEntries(
                    spec.props.map((p, i) => [p, values[i]]),
                  ),
                  count,
                } as Record<string, string> & { count: number };
                return { k, row };
              },
            ),
            (entry) => entry.row.count,
            (entry) => entry.k,
          ).map((entry) => entry.row),
        ],
      ),
    ) as TripPlannerFigures["lists"],
  };
}
