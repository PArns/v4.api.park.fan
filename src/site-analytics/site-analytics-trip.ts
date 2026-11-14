import { OVERFLOW_VALUE } from "./site-analytics.constants";
import { byCountDesc } from "./site-analytics.query";

/**
 * The trip planner's events, as the frontend sends them (`trackTripEvent` in
 * its `lib/orlando/plan-analytics.ts`, the list in its
 * docs/features/orlando-planner.md#analytics): every one carries `region`.
 * `ticket_link_clicked` has the eight properties the statistic allows and so
 * no region: its product's slug belongs to one region's catalog.
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
} as const;

/** A purchase link of the planner's ticket catalog followed. */
export const TICKET_LINK_EVENT = "ticket_link_clicked";

/** The funnel's steps, in the order a visitor meets them. */
export const TRIP_FUNNEL = [
  "started",
  "ticketsAnswered",
  "planRequested",
  "planShown",
  "viewChanged",
  "dayOpened",
  "kept",
] as const;
export type TripFunnelStep = (typeof TRIP_FUNNEL)[number];

/** The label of an empty property value. */
const NONE = "(none)";

/** One event's count for one combination of its properties. */
export interface TripEventRow {
  name: string;
  props: Record<string, string>;
  count: number;
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

/** The counts outside the lists: what a narrowed answer hides when small. */
export interface TripTotals {
  started: number;
  ticketsAnswered: number;
  planRequested: number;
  planShown: number;
  viewChanged: number;
  dayOpened: number;
  kept: number;
  datesKnown: number;
  datesSearched: number;
  parksNamed: number;
  parksHeadliners: number;
  shownOk: number;
  shownRelaxed: number;
  shownError: number;
  daysSaved: number;
  icsExported: number;
  linkCopied: number;
  findDatesRequested: number;
  findDatesShown: number;
  windowsPlanned: number;
  ticketRemoved: number;
  ticketsAdopted: number;
  longStayUsed: number;
  daySwapped: number;
  daySent: number;
  eveningLinkClicked: number;
  ticketLinks: number;
  /** Ticket link clicks on a product no region's catalog has. */
  unattributedTicketLinks: number;
}

export interface TripPlannerFigures {
  region: string | null;
  totals: TripTotals;
  regions: Array<{
    region: string;
    started: number;
    planRequested: number;
    planShown: number;
    errors: number;
    kept: number;
    ticketLinks: number;
  }>;
  sources: ValueCount[];
  ticketAnswers: ValueCount[];
  requests: Record<
    "mode" | "days" | "parks" | "tickets" | "hopper" | "blocks" | "rest",
    ValueCount[]
  >;
  shown: Array<{ mode: string; outcome: string; count: number }>;
  relaxed: ValueCount[];
  unplaced: ValueCount[];
  errors: Array<{ mode: string; status: string; count: number }>;
  windowsFound: ValueCount[];
  windowRanks: ValueCount[];
  ticketsAdded: Array<{ via: string; entry: string; count: number }>;
  ticketStarts: ValueCount[];
  ticketDetails: ValueCount[];
  outsideTicket: ValueCount[];
  hopperToggled: ValueCount[];
  viewsChanged: ValueCount[];
  dayOpened: Array<{ kind: string; view: string; count: number }>;
  dayOpenedByView: ValueCount[];
  dayPinned: ValueCount[];
  savedDays: ValueCount[];
  icsDays: ValueCount[];
  ticketLinksByKindAndPosition: Array<{
    kind: string;
    position: string;
    count: number;
  }>;
  ticketLinksBySurface: ValueCount[];
  ticketLinksByMode: ValueCount[];
  ticketLinksByLocaleMatch: ValueCount[];
  ticketLinksBySeller: Array<{
    seller: string;
    host: string;
    kind: string;
    count: number;
  }>;
  ticketLinksByProduct: ValueCount[];
}

/** Counts by a key, then listed most first. */
class Tally<K extends string> {
  private readonly counts = new Map<string, number>();

  add(key: string, count: number): void {
    this.counts.set(key, (this.counts.get(key) ?? 0) + count);
  }

  values(): ValueCount[] {
    return byCountDesc(
      [...this.counts].map(([value, count]) => ({ value, count })),
      (item) => item.count,
      (item) => item.value,
    );
  }

  /** The keys split back into the named fields they were joined from. */
  rows(fields: readonly K[]): Array<Record<K, string> & { count: number }> {
    return this.values().map(({ value, count }) => {
      const parts = JSON.parse(value) as string[];
      const row = Object.fromEntries(
        fields.map((field, i) => [field, parts[i]]),
      ) as Record<K, string>;
      return { ...row, count };
    });
  }
}

const key = (...parts: string[]) => JSON.stringify(parts);

/**
 * The trip planner report's figures from the events' property counts: the
 * funnel, its steps' details, find dates, tickets and hopper, views, what
 * was kept, the purchase links (attributed to a region by their product) and
 * the regions side by side. A value of `region` park.fan does not have is
 * `(other)`; an empty property value is `(none)`.
 */
export function tripPlannerFigures(
  rows: readonly TripEventRow[],
  options: TripFigureOptions,
): TripPlannerFigures {
  const known = new Set(options.knownRegions);
  const regionOf = (row: TripEventRow): string | null => {
    if (row.name === TICKET_LINK_EVENT) {
      return options.productRegions.get(row.props.product ?? "") ?? null;
    }
    const value = row.props.region ?? "";
    return known.has(value) ? value : OVERFLOW_VALUE;
  };
  const prop = (row: TripEventRow, name: string) => row.props[name] || NONE;

  const byRegion = new Map<string, TripPlannerFigures["regions"][number]>();
  const regionRow = (region: string) => {
    let row = byRegion.get(region);
    if (!row) {
      row = {
        region,
        started: 0,
        planRequested: 0,
        planShown: 0,
        errors: 0,
        kept: 0,
        ticketLinks: 0,
      };
      byRegion.set(region, row);
    }
    return row;
  };

  const totals: TripTotals = {
    started: 0,
    ticketsAnswered: 0,
    planRequested: 0,
    planShown: 0,
    viewChanged: 0,
    dayOpened: 0,
    kept: 0,
    datesKnown: 0,
    datesSearched: 0,
    parksNamed: 0,
    parksHeadliners: 0,
    shownOk: 0,
    shownRelaxed: 0,
    shownError: 0,
    daysSaved: 0,
    icsExported: 0,
    linkCopied: 0,
    findDatesRequested: 0,
    findDatesShown: 0,
    windowsPlanned: 0,
    ticketRemoved: 0,
    ticketsAdopted: 0,
    longStayUsed: 0,
    daySwapped: 0,
    daySent: 0,
    eveningLinkClicked: 0,
    ticketLinks: 0,
    unattributedTicketLinks: 0,
  };
  const t = {
    sources: new Tally(),
    ticketAnswers: new Tally(),
    mode: new Tally(),
    days: new Tally(),
    parks: new Tally(),
    tickets: new Tally(),
    hopper: new Tally(),
    blocks: new Tally(),
    rest: new Tally(),
    shown: new Tally<"mode" | "outcome">(),
    relaxed: new Tally(),
    unplaced: new Tally(),
    errors: new Tally<"mode" | "status">(),
    windowsFound: new Tally(),
    windowRanks: new Tally(),
    ticketsAdded: new Tally<"via" | "entry">(),
    ticketStarts: new Tally(),
    ticketDetails: new Tally(),
    outsideTicket: new Tally(),
    hopperToggled: new Tally(),
    viewsChanged: new Tally(),
    dayOpened: new Tally<"kind" | "view">(),
    dayOpenedByView: new Tally(),
    dayPinned: new Tally(),
    savedDays: new Tally(),
    icsDays: new Tally(),
    linkKindPosition: new Tally<"kind" | "position">(),
    linkSurface: new Tally(),
    linkMode: new Tally(),
    linkLocale: new Tally(),
    linkSeller: new Tally<"seller" | "host" | "kind">(),
    linkProduct: new Tally(),
  };

  const E = TRIP_EVENTS;
  for (const row of rows) {
    const region = regionOf(row);
    const n = row.count;
    if (row.name === TICKET_LINK_EVENT && region === null) {
      // Attributed to no region: counted, and listed only with every region.
      totals.unattributedTicketLinks += n;
      if (options.region !== null) continue;
    } else if (region !== null) {
      const side = regionRow(region);
      if (row.name === E.started) side.started += n;
      if (row.name === E.planRequested) side.planRequested += n;
      if (row.name === E.planShown) {
        side.planShown += n;
        if (row.props.outcome === "error") side.errors += n;
      }
      if (
        row.name === E.daysSaved ||
        row.name === E.icsExported ||
        row.name === E.linkCopied
      ) {
        side.kept += n;
      }
      if (row.name === TICKET_LINK_EVENT) side.ticketLinks += n;
      if (options.region !== null && region !== options.region) continue;
    }

    switch (row.name) {
      case E.started:
        totals.started += n;
        t.sources.add(prop(row, "source"), n);
        break;
      case E.ticketsAnswered:
        totals.ticketsAnswered += n;
        t.ticketAnswers.add(prop(row, "answer"), n);
        break;
      case E.planRequested: {
        totals.planRequested += n;
        const mode = prop(row, "mode");
        if (mode === "dates") totals.datesKnown += n;
        if (mode === "when") {
          totals.datesSearched += n;
          totals.findDatesRequested += n;
        }
        if (row.props.parks === "0") totals.parksHeadliners += n;
        else if (row.props.parks) totals.parksNamed += n;
        t.mode.add(mode, n);
        t.days.add(prop(row, "days"), n);
        t.parks.add(prop(row, "parks"), n);
        t.tickets.add(prop(row, "tickets"), n);
        t.hopper.add(prop(row, "hopper"), n);
        t.blocks.add(prop(row, "blocks"), n);
        t.rest.add(prop(row, "rest"), n);
        break;
      }
      case E.planShown: {
        totals.planShown += n;
        const mode = prop(row, "mode");
        const outcome = prop(row, "outcome");
        if (outcome === "ok") totals.shownOk += n;
        if (outcome === "relaxed") totals.shownRelaxed += n;
        if (outcome === "error") totals.shownError += n;
        if (mode === "when") totals.findDatesShown += n;
        t.shown.add(key(mode, outcome), n);
        if (outcome === "relaxed") t.relaxed.add(prop(row, "relaxed"), n);
        if (outcome === "error")
          t.errors.add(key(mode, prop(row, "status")), n);
        if (mode === "dates" && outcome !== "error") {
          t.unplaced.add(prop(row, "unplaced"), n);
        }
        if (mode === "when" && outcome !== "error") {
          t.windowsFound.add(prop(row, "windows"), n);
        }
        break;
      }
      case E.windowPlanned:
        totals.windowsPlanned += n;
        t.windowRanks.add(prop(row, "rank"), n);
        break;
      case E.ticketAdded:
        t.ticketsAdded.add(key(prop(row, "via"), prop(row, "entry")), n);
        break;
      case E.ticketRemoved:
        totals.ticketRemoved += n;
        break;
      case E.ticketStart:
        t.ticketStarts.add(prop(row, "start"), n);
        break;
      case E.ticketDetailsOpened:
        t.ticketDetails.add(prop(row, "surface"), n);
        break;
      case E.ticketsAdopted:
        totals.ticketsAdopted += n;
        break;
      case E.longStayUsed:
        totals.longStayUsed += n;
        break;
      case E.outsideTicketAction:
        t.outsideTicket.add(prop(row, "action"), n);
        break;
      case E.hopperToggled:
        t.hopperToggled.add(prop(row, "on"), n);
        break;
      case E.viewChanged:
        totals.viewChanged += n;
        t.viewsChanged.add(prop(row, "view"), n);
        break;
      case E.dayOpened:
        totals.dayOpened += n;
        t.dayOpened.add(key(prop(row, "kind"), prop(row, "view")), n);
        t.dayOpenedByView.add(prop(row, "view"), n);
        break;
      case E.dayPinned:
        t.dayPinned.add(prop(row, "pinned"), n);
        break;
      case E.daySwapped:
        totals.daySwapped += n;
        break;
      case E.daySent:
        totals.daySent += n;
        break;
      case E.daysSaved:
        totals.daysSaved += n;
        totals.kept += n;
        t.savedDays.add(prop(row, "days"), n);
        break;
      case E.icsExported:
        totals.icsExported += n;
        totals.kept += n;
        t.icsDays.add(prop(row, "days"), n);
        break;
      case E.linkCopied:
        totals.linkCopied += n;
        totals.kept += n;
        break;
      case E.eveningLinkClicked:
        totals.eveningLinkClicked += n;
        break;
      case TICKET_LINK_EVENT:
        totals.ticketLinks += n;
        t.linkKindPosition.add(
          key(prop(row, "kind"), prop(row, "position")),
          n,
        );
        t.linkSurface.add(prop(row, "surface"), n);
        t.linkMode.add(prop(row, "ticket_mode"), n);
        t.linkLocale.add(prop(row, "locale_match"), n);
        t.linkSeller.add(
          key(prop(row, "seller"), prop(row, "host"), prop(row, "kind")),
          n,
        );
        t.linkProduct.add(prop(row, "product"), n);
        break;
    }
  }

  return {
    region: options.region,
    totals,
    regions: byCountDesc(
      [...byRegion.values()],
      (row) => row.started,
      (row) => row.region,
    ),
    sources: t.sources.values(),
    ticketAnswers: t.ticketAnswers.values(),
    requests: {
      mode: t.mode.values(),
      days: t.days.values(),
      parks: t.parks.values(),
      tickets: t.tickets.values(),
      hopper: t.hopper.values(),
      blocks: t.blocks.values(),
      rest: t.rest.values(),
    },
    shown: t.shown.rows(["mode", "outcome"]),
    relaxed: t.relaxed.values(),
    unplaced: t.unplaced.values(),
    errors: t.errors.rows(["mode", "status"]),
    windowsFound: t.windowsFound.values(),
    windowRanks: t.windowRanks.values(),
    ticketsAdded: t.ticketsAdded.rows(["via", "entry"]),
    ticketStarts: t.ticketStarts.values(),
    ticketDetails: t.ticketDetails.values(),
    outsideTicket: t.outsideTicket.values(),
    hopperToggled: t.hopperToggled.values(),
    viewsChanged: t.viewsChanged.values(),
    dayOpened: t.dayOpened.rows(["kind", "view"]),
    dayOpenedByView: t.dayOpenedByView.values(),
    dayPinned: t.dayPinned.values(),
    savedDays: t.savedDays.values(),
    icsDays: t.icsDays.values(),
    ticketLinksByKindAndPosition: t.linkKindPosition.rows(["kind", "position"]),
    ticketLinksBySurface: t.linkSurface.values(),
    ticketLinksByMode: t.linkMode.values(),
    ticketLinksByLocaleMatch: t.linkLocale.values(),
    ticketLinksBySeller: t.linkSeller.rows(["seller", "host", "kind"]),
    ticketLinksByProduct: t.linkProduct.values(),
  };
}
