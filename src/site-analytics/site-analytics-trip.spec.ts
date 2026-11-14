import {
  TICKET_LINK_EVENT,
  TRIP_EVENTS,
  TRIP_FUNNEL,
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
    expect(step(figures, "datesPicked")).toEqual({
      step: "datesPicked",
      before: 10,
      since: 22,
    });
    expect(step(figures, "planShown")).toMatchObject({ before: 8, since: 28 });
    // The old planner sent nothing when a plan was changed; Parky's changes are his.
    expect(step(figures, "changed")).toMatchObject({ before: null, since: 9 });
    // A yes before the cut-over; the ticket step's openings after it.
    expect(step(figures, "ticketsEntered")).toMatchObject({
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
    expect(step(after, "datesPicked")).toMatchObject({
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
    expect(step(before, "datesPicked")).toMatchObject({
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
        datesPicked: 20,
        planRequested: 30,
        planShown: 0,
      },
      { pageType: "other", datesPicked: 10, planRequested: 0, planShown: 0 },
      {
        pageType: "trip_best_time",
        datesPicked: 0,
        planRequested: 6,
        planShown: 0,
      },
      {
        pageType: "trip_month",
        datesPicked: 2,
        planRequested: 0,
        planShown: 0,
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
        datesPicked: 32,
        planRequested: 36,
        planShown: 36,
        errors: 1,
        kept: 6,
        ticketLinks: 5,
      }),
      expect.objectContaining({
        region: "japan",
        datesPicked: 3,
        ticketLinks: 2,
      }),
      expect.objectContaining({ region: "(other)", datesPicked: 1 }),
    ]);
    expect(
      tripPlannerFigures(ROWS, { ...OPTIONS, region: "japan" }).totals,
    ).toMatchObject({ planRequested: 0, ticketLinks: 2 });
  });

  it("names an empty value (none) rather than dropping it", () => {
    const figures = tripPlannerFigures([orlando(TRIP_EVENTS.started, {}, 6)], {
      ...OPTIONS,
      region: null,
    });
    expect(figures.lists.sources).toEqual([{ source: "(none)", count: 6 }]);
  });
});
