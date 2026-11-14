import {
  TICKET_LINK_EVENT,
  TRIP_EVENTS,
  TRIP_FUNNEL,
  parkKeyOf,
  tripPlannerFigures,
  type TripEventRow,
} from "./site-analytics-trip";

const ev = (
  name: string,
  props: Record<string, string>,
  count = 1,
  day = "2026-11-20",
  pageType?: string,
): TripEventRow => ({
  name,
  props,
  count,
  day,
  ...(pageType && { pageType }),
});
const orlando = (
  name: string,
  props: Record<string, string> = {},
  n = 1,
  day = "2026-11-20",
  pageType?: string,
) => ev(name, { region: "orlando", ...props }, n, day, pageType);

const OPTIONS = {
  knownRegions: ["orlando", "japan"],
  productRegions: new Map([
    ["wdw-date-based", "orlando"],
    ["tdr-1-day", "japan"],
  ]),
};

/** The old planner's days (to the 17th) and the dates-first planner's (from the 18th). */
const ACROSS = { from: "2026-11-10", to: "2026-11-24" };
const CUTOVER = "2026-11-18";
const OLD = "2026-11-12";
const NEW = "2026-11-20";

const ROWS: TripEventRow[] = [
  // The old planner: a start, a ticket answer of yes, an answer, a saved trip.
  orlando(TRIP_EVENTS.started, { source: "hero" }, 10, OLD, "other"),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "yes" }, 4, OLD),
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "not_yet" }, 5, OLD),
  orlando(TRIP_EVENTS.planShown, { mode: "dates", outcome: "ok" }, 8, OLD),
  orlando(TRIP_EVENTS.daysSaved, { days: "4" }, 2, OLD),
  // The dates-first planner.
  orlando(
    TRIP_EVENTS.started,
    { source: "dates", via: "trip_band" },
    20,
    NEW,
    "trip_hub",
  ),
  orlando(TRIP_EVENTS.started, { source: "link" }, 2, NEW, "trip_month"),
  orlando(
    TRIP_EVENTS.planRequested,
    { mode: "dates", days: "5-7", parks: "0", tickets: "none", hopper: "none" },
    30,
    NEW,
    "trip_hub",
  ),
  orlando(
    TRIP_EVENTS.planRequested,
    { mode: "when", days: "8-10", parks: "3", tickets: "owned", hopper: "all" },
    6,
    NEW,
    "trip_best_time",
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "ok", unplaced: "0", left_out: "0" },
    20,
    NEW,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    {
      mode: "dates",
      outcome: "relaxed",
      relaxed: "blocks",
      unplaced: "1",
      left_out: "1",
    },
    4,
    NEW,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "dates", outcome: "error", status: "503" },
    1,
    NEW,
  ),
  orlando(
    TRIP_EVENTS.planShown,
    { mode: "when", outcome: "ok", windows: "3" },
    3,
    NEW,
  ),
  orlando(TRIP_EVENTS.editOpened, { panel: "parks", from: "answer" }, 6, NEW),
  orlando(
    TRIP_EVENTS.optionChanged,
    { option: "rest", value: "none", by: "visitor" },
    3,
    NEW,
  ),
  orlando(TRIP_EVENTS.optionChanged, { option: "parks", by: "parky" }, 2, NEW),
  orlando(TRIP_EVENTS.ticketsOpened, { from: "card", first: "true" }, 5, NEW),
  // Still sent once per browser after a plan: not the dates-first funnel's step.
  orlando(TRIP_EVENTS.ticketsAnswered, { answer: "yes" }, 1, NEW),
  orlando(TRIP_EVENTS.leftOutFix, { fix: "longer", left_out: "1" }, 2, NEW),
  orlando(TRIP_EVENTS.windowPlanned, { rank: "1" }, 2, NEW),
  orlando(
    TRIP_EVENTS.hopperToggled,
    { resort: "walt-disney-world", on: "true" },
    2,
    NEW,
  ),
  orlando(TRIP_EVENTS.dayOpened, { kind: "park", view: "list" }, 7, NEW),
  orlando(TRIP_EVENTS.daysSaved, { days: "4" }, 2, NEW),
  orlando(TRIP_EVENTS.icsExported, { days: "4" }, 1, NEW),
  orlando(TRIP_EVENTS.linkCopied, {}, 1, NEW),
  ev(TRIP_EVENTS.started, { region: "japan", source: "hero" }, 3, NEW),
  // A region value nobody sends: a script, or a region not built yet.
  ev(TRIP_EVENTS.started, { region: "atlantis", source: "hero" }, 1, NEW),
  ev(
    TICKET_LINK_EVENT,
    {
      product: "wdw-date-based",
      kind: "official",
      position: "1",
      seller: "Disney",
      host: "disneyworld.disney.go.com",
    },
    5,
    NEW,
  ),
  ev(
    TICKET_LINK_EVENT,
    { product: "tdr-1-day", kind: "plain", position: "2" },
    2,
    NEW,
  ),
  ev(
    TICKET_LINK_EVENT,
    { product: "gone-product", kind: "sponsored", position: "1" },
    1,
    NEW,
  ),
];

const step = (figures: ReturnType<typeof tripPlannerFigures>, name: string) =>
  figures.funnel.find((s) => s.step === name);

describe("tripPlannerFigures", () => {
  it("counts each step by the old planner's events before the cut-over and the new ones from it", () => {
    const figures = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
      window: ACROSS,
      newFunnelFrom: CUTOVER,
    });
    expect(figures.funnel.map((s) => s.step)).toEqual([...TRIP_FUNNEL]);
    expect(figures.newFunnelFrom).toBe(CUTOVER);
    expect(step(figures, "started")).toEqual({
      step: "started",
      before: 10,
      since: 22,
    });
    expect(step(figures, "requested")).toMatchObject({ before: 0, since: 36 });
    expect(step(figures, "shown")).toMatchObject({ before: 8, since: 28 });
    // The old planner sent nothing when a plan was changed; Parky's changes are his.
    expect(step(figures, "changed")).toMatchObject({ before: null, since: 9 });
    // A yes before the cut-over; the ticket step's openings after it.
    expect(step(figures, "tickets")).toMatchObject({
      before: 4,
      since: 5,
    });
    expect(step(figures, "kept")).toMatchObject({ before: 2, since: 4 });
  });

  it("leaves an era out of a window that has none of its days", () => {
    const after = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
      window: { from: CUTOVER, to: "2026-11-24" },
      newFunnelFrom: CUTOVER,
    });
    expect(step(after, "started")).toMatchObject({
      before: null,
      since: 22,
    });
    // No dates-first event counted yet: all of it is the old planner's.
    const before = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
      window: ACROSS,
      newFunnelFrom: null,
    });
    expect(step(before, "changed")).toMatchObject({
      before: null,
      since: null,
    });
    expect(step(before, "started")).toMatchObject({
      before: 32,
      since: null,
    });
  });

  it("totals what the steps leave out, and lists every breakdown by its properties", () => {
    const figures = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
      window: ACROSS,
      newFunnelFrom: CUTOVER,
    });
    expect(figures.totals).toMatchObject({
      planRequested: 36,
      datesKnown: 30,
      datesSearched: 6,
      parksHeadliners: 30,
      shownOk: 31,
      shownRelaxed: 4,
      shownError: 1,
      findDatesShown: 3,
      windowsPlanned: 2,
      editOpened: 6,
      optionChanged: 3,
      optionChangedByParky: 2,
      ticketsOpened: 5,
      ticketsAnsweredYes: 5,
      ticketLinks: 5,
      unattributedTicketLinks: 1,
    });
    expect(figures.lists.sources).toEqual([
      { source: "dates", count: 20 },
      { source: "hero", count: 10 },
      { source: "link", count: 2 },
    ]);
    expect(figures.lists.startedVia).toEqual([{ via: "trip_band", count: 20 }]);
    expect(figures.lists.leftOut).toEqual([
      { left_out: "0", count: 20 },
      { left_out: "1", count: 4 },
    ]);
    expect(figures.lists.leftOutFixes).toEqual([
      { fix: "longer", left_out: "1", count: 2 },
    ]);
    expect(figures.lists.options).toEqual([
      { option: "rest", value: "none", by: "visitor", count: 3 },
      { option: "parks", value: "(none)", by: "parky", count: 2 },
    ]);
    expect(figures.lists.errors).toEqual([
      { mode: "dates", status: "503", count: 1 },
    ]);
    expect(figures.lists.windowsFound).toEqual([{ windows: "3", count: 3 }]);
    expect(figures.lists.ticketsOpened).toEqual([
      { from: "card", first: "true", count: 5 },
    ]);
  });

  it("names the page types the planner was used from", () => {
    const figures = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
      window: ACROSS,
      newFunnelFrom: CUTOVER,
    });
    expect(figures.pageTypes).toEqual([
      {
        pageType: "trip_hub",
        started: 20,
        requested: 30,
        shown: 0,
      },
      { pageType: "other", started: 10, requested: 0, shown: 0 },
      {
        pageType: "trip_best_time",
        started: 0,
        requested: 6,
        shown: 0,
      },
      {
        pageType: "trip_month",
        started: 2,
        requested: 0,
        shown: 0,
      },
    ]);
  });

  it("attributes a purchase link to the region whose catalog has its product", () => {
    const orlandoOnly = tripPlannerFigures(ROWS, {
      ...OPTIONS,
      region: "orlando",
    });
    expect(orlandoOnly.lists.linksByProduct).toEqual([
      { product: "wdw-date-based", count: 5 },
    ]);
    expect(orlandoOnly.lists.linksBySeller).toEqual([
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
    expect(all.lists.linksByProduct.map((row) => row.product)).toEqual([
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
        started: 32,
        requested: 36,
        shown: 36,
        errors: 1,
        kept: 6,
        ticketLinks: 5,
      }),
      expect.objectContaining({
        region: "japan",
        started: 3,
        ticketLinks: 2,
      }),
      expect.objectContaining({ region: "(other)", started: 1 }),
    ]);
    expect(
      tripPlannerFigures(ROWS, { ...OPTIONS, region: "japan" }).totals,
    ).toMatchObject({ planRequested: 0, ticketLinks: 2 });
  });

  it("reads the tile on the region parks' pages: its views by the park in the page, its clicks by region and link", () => {
    const MK = "/parks/north-america/united-states/orlando/magic-kingdom-park";
    const seen = (path: string, count: number, section = "trip_tile") => ({
      name: "section_seen",
      props: { section },
      count,
      path,
    });
    const rows: TripEventRow[] = [
      seen(`/de${MK}`, 20),
      // The tile stands on a park's calendar page too.
      seen(
        "/en/parks/north-america/united-states/orlando/epcot/crowd-calendar",
        6,
      ),
      // No region park: the region is (other).
      seen("/de/parks/europe/germany/bruehl/phantasialand", 2),
      // Another section of a park page says nothing about the tile.
      seen(`/de${MK}`, 50, "main_end"),
      orlando(TRIP_EVENTS.tileClicked, { link: "plan" }, 5),
      orlando(TRIP_EVENTS.tileClicked, { link: "calendar" }, 2),
    ];
    const parkRegions = new Map([
      ["north-america/united-states/orlando/magic-kingdom-park", "orlando"],
      ["north-america/united-states/orlando/epcot", "orlando"],
    ]);
    const all = tripPlannerFigures(rows, {
      ...OPTIONS,
      region: null,
      parkRegions,
    });
    expect(all.tile).toEqual([
      { region: "orlando", shown: 26, clicked: 7, plan: 5, calendar: 2 },
      { region: "(other)", shown: 2, clicked: 0, plan: 0, calendar: 0 },
    ]);
    expect(all.totals).toMatchObject({
      tileShown: 28,
      tileClicked: 7,
      tilePlan: 5,
      tileCalendar: 2,
    });
    expect(all.lists.tileClicks).toEqual([
      { link: "plan", count: 5 },
      { link: "calendar", count: 2 },
    ]);
    // A view is no planner event: it adds no region to the side-by-side table.
    expect(all.regions.map((row) => row.region)).toEqual(["orlando"]);

    const one = tripPlannerFigures(rows, {
      ...OPTIONS,
      region: "orlando",
      parkRegions,
    });
    expect(one.tile).toEqual([
      { region: "orlando", shown: 26, clicked: 7, plan: 5, calendar: 2 },
    ]);
    expect(one.totals.tileShown).toBe(26);
    expect(parkKeyOf(`/de${MK}/wartezeiten-kalender`)).toBe(
      "north-america/united-states/orlando/magic-kingdom-park",
    );
    expect(parkKeyOf("/de/orlando")).toBeNull();
  });

  it("counts the day planner opened from the trip planner by the region and suffix of its source", () => {
    const opened = (source: string, count: number) =>
      ev("planner_opened", { source }, count);
    const rows = [
      opened("orlando-trip-day", 4),
      opened("orlando-trip-first", 2),
      opened("orlando-hub", 3),
      opened("japan-trip-day", 5),
      // The day planner's own ways in, and a region nobody has.
      opened("header", 40),
      opened("atlantis-trip-day", 9),
    ];
    const one = tripPlannerFigures(rows, { ...OPTIONS, region: "orlando" });
    expect(one.totals).toMatchObject({
      dayPlannerTripDay: 4,
      dayPlannerTripFirst: 2,
      dayPlannerHub: 3,
    });
    // No planner event: it adds no row to the regions side by side.
    expect(one.regions).toEqual([]);
    expect(
      tripPlannerFigures(rows, { ...OPTIONS, region: null }).totals
        .dayPlannerTripDay,
    ).toBe(9);
  });

  it("names an empty value (none) rather than dropping it", () => {
    const figures = tripPlannerFigures([orlando(TRIP_EVENTS.started, {}, 6)], {
      ...OPTIONS,
      region: null,
    });
    expect(figures.lists.sources).toEqual([{ source: "(none)", count: 6 }]);
  });
});
