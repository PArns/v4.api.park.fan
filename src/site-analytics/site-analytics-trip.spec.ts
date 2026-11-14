import {
  TICKET_LINK_EVENT,
  TRIP_EVENTS,
  tripPlannerFigures,
  type TripEventRow,
} from "./site-analytics-trip";

const ev = (
  name: string,
  props: Record<string, string>,
  count = 1,
): TripEventRow => ({ name, props, count });
const orlando = (name: string, props: Record<string, string> = {}, n = 1) =>
  ev(name, { region: "orlando", ...props }, n);

const ROWS: TripEventRow[] = [
  orlando(TRIP_EVENTS.started, { source: "hero" }, 10),
  orlando(TRIP_EVENTS.started, { source: "link" }, 2),
  ev(TRIP_EVENTS.started, { region: "japan", source: "hero" }, 3),
  // A region value nobody sends: a script, or a region not built yet.
  ev(TRIP_EVENTS.started, { region: "atlantis", source: "hero" }, 1),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "yes" }, 4),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "not_yet" }, 5),
  orlando(
    TRIP_EVENTS.planRequested,
    { mode: "dates", days: "5-7", parks: "0", tickets: "none", hopper: "none" },
    9,
  ),
  orlando(
    TRIP_EVENTS.planRequested,
    { mode: "when", days: "8-10", parks: "3", tickets: "owned", hopper: "all" },
    3,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "ok", unplaced: "0" },
    6,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "relaxed", relaxed: "blocks", unplaced: "1" },
    2,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "error", status: "503" },
    1,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "when", outcome: "ok", windows: "3" },
    3,
  ),
  orlando(TRIP_EVENTS.windowPlanned, { rank: "1" }, 2),
  orlando(
    TRIP_EVENTS.hopperToggled,
    { resort: "walt-disney-world", on: "true" },
    2,
  ),
  orlando(TRIP_EVENTS.viewChanged, { view: "week" }, 4),
  orlando(TRIP_EVENTS.dayOpened, { kind: "park", view: "list" }, 7),
  orlando(TRIP_EVENTS.daysSaved, { days: "4" }, 2),
  orlando(TRIP_EVENTS.icsExported, { days: "4" }, 1),
  orlando(TRIP_EVENTS.linkCopied, {}, 1),
  ev(
    TICKET_LINK_EVENT,
    {
      product: "wdw-date-based",
      kind: "official",
      position: "1",
      seller: "Disney",
      host: "disneyworld.disney.go.com",
      surface: "suggestion",
      ticket_mode: "plan_first",
      locale_match: "true",
    },
    5,
  ),
  ev(
    TICKET_LINK_EVENT,
    { product: "tdr-1-day", kind: "plain", position: "2" },
    2,
  ),
  ev(
    TICKET_LINK_EVENT,
    { product: "gone-product", kind: "sponsored", position: "1" },
    1,
  ),
];

const OPTIONS = {
  knownRegions: ["orlando", "japan"],
  productRegions: new Map([
    ["wdw-date-based", "orlando"],
    ["tdr-1-day", "japan"],
  ]),
};

describe("tripPlannerFigures", () => {
  it("counts the funnel's steps and reads dates and parks off the requests", () => {
    const figures = tripPlannerFigures(ROWS, { ...OPTIONS, region: "orlando" });
    expect(figures.totals).toMatchObject({
      started: 12,
      ticketsAnswered: 9,
      planRequested: 12,
      datesKnown: 9,
      datesSearched: 3,
      parksHeadliners: 9,
      parksNamed: 3,
      planShown: 12,
      shownOk: 9,
      shownRelaxed: 2,
      shownError: 1,
      findDatesRequested: 3,
      findDatesShown: 3,
      windowsPlanned: 2,
      viewChanged: 4,
      dayOpened: 7,
      kept: 4,
      daysSaved: 2,
      icsExported: 1,
      linkCopied: 1,
      ticketLinks: 5,
      unattributedTicketLinks: 1,
    });
    expect(figures.sources).toEqual([
      { value: "hero", count: 10 },
      { value: "link", count: 2 },
    ]);
    expect(figures.relaxed).toEqual([{ value: "blocks", count: 2 }]);
    expect(figures.errors).toEqual([
      { mode: "dates", status: "503", count: 1 },
    ]);
    expect(figures.unplaced).toEqual([
      { value: "0", count: 6 },
      { value: "1", count: 2 },
    ]);
    expect(figures.windowsFound).toEqual([{ value: "3", count: 3 }]);
    expect(figures.hopperToggled).toEqual([{ value: "true", count: 2 }]);
    expect(figures.dayOpened).toEqual([
      { kind: "park", view: "list", count: 7 },
    ]);
  });

  it("attributes a purchase link to the region whose catalog has its product", () => {
    const orlandoOnly = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
    });
    expect(orlandoOnly.ticketLinksByProduct).toEqual([
      { value: "wdw-date-based", count: 5 },
    ]);
    expect(orlandoOnly.ticketLinksByKindAndPosition).toEqual([
      { kind: "official", position: "1", count: 5 },
    ]);
    expect(orlandoOnly.ticketLinksBySeller).toEqual([
      {
        seller: "Disney",
        host: "disneyworld.disney.go.com",
        kind: "official",
        count: 5,
      },
    ]);

    // Every region's: the unattributed one is listed too.
    const all = tripPlannerFigures(ROWS, { ...OPTIONS, region: null });
    expect(all.totals.ticketLinks).toBe(8);
    expect(all.totals.unattributedTicketLinks).toBe(1);
    expect(all.ticketLinksByProduct.map((row) => row.value)).toEqual([
      "wdw-date-based",
      "tdr-1-day",
      "gone-product",
    ]);
  });

  it("sets the regions side by side, an unknown region value as (other)", () => {
    const all = tripPlannerFigures(ROWS, { ...OPTIONS, region: null });
    expect(all.regions).toEqual([
      expect.objectContaining({
        region: "orlando",
        started: 12,
        planRequested: 12,
        planShown: 12,
        errors: 1,
        kept: 4,
        ticketLinks: 5,
      }),
      expect.objectContaining({ region: "japan", started: 3, ticketLinks: 2 }),
      expect.objectContaining({ region: "(other)", started: 1 }),
    ]);
    expect(all.totals.started).toBe(16);
    // One region's figures leave the others out.
    expect(
      tripPlannerFigures(ROWS, { ...OPTIONS, region: "japan" }).totals,
    ).toMatchObject({ started: 3, planRequested: 0, ticketLinks: 2 });
  });

  it("names an empty value (none) rather than dropping it", () => {
    const figures = tripPlannerFigures([orlando(TRIP_EVENTS.started, {}, 6)], {
      ...OPTIONS,
      region: null,
    });
    expect(figures.sources).toEqual([{ value: "(none)", count: 6 }]);
  });
});
