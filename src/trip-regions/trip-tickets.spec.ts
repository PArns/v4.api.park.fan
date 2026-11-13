import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import type { OptimizedPlan, PlannedDay } from "./trip-optimizer";
import {
  normalizePlanQuery,
  optimizePlan,
  planCacheQuery,
  planResponse,
} from "./trip-plan.assembly";
import {
  canonicalTickets,
  hopperVerdict,
  parseTickets,
  suggestTickets,
  type SuggestionInput,
} from "./trip-tickets";
import type { TripParkDayInternal } from "./trip-calendar.util";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";

const TODAY = "2026-11-12";
const FROM = "2027-02-01";
const KNOWN = ORLANDO.parks.map((p) => p.slug);
const TRIP = { from: FROM, to: addIsoDays(FROM, 13) };
const DISNEY = [
  "magic-kingdom-park",
  "epcot",
  "disneys-hollywood-studios",
  "disneys-animal-kingdom-theme-park",
];
const USF = "universal-studios-florida";
const IOA = "universal-islands-of-adventure";
const EPIC = "universal-epic-universe";

function calendar(
  from: string,
  to: string,
  cellFor: (slug: string, i: number) => TripParkDayInternal = (slug, i) => ({
    status: "OPERATING",
    hoursPublished: true,
    crowdLevel: "moderate",
    headlinerWait: 40,
    waitExact: 30 + ((slug.length * 7 + i * 11) % 30),
    basis: "forecast",
    dayHours: 12,
  }),
): InternalRegionCalendar {
  const days = [];
  for (let d = from, i = 0; d <= to; d = addIsoDays(d, 1), i++) {
    days.push({
      date: d,
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(KNOWN.map((s) => [s, cellFor(s, i)])),
    });
  }
  return {
    meta: {
      region: "orlando",
      timezone: "America/New_York",
      from,
      to,
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: KNOWN,
    days,
    seasons: [],
  };
}

const query = (over: Partial<TripPlanQueryDto>): TripPlanQueryDto => ({
  from: TRIP.from,
  to: TRIP.to,
  ...over,
});

/** The plan route's answer for a query, on the default calendar or the given cells. */
function answer(
  over: Partial<TripPlanQueryDto>,
  cellFor?: (slug: string, i: number) => TripParkDayInternal,
) {
  const q = normalizePlanQuery(ORLANDO, KNOWN, query(over), TODAY);
  const cal = calendar(q.from, q.to, cellFor);
  const trip = optimizePlan(ORLANDO, q, cal);
  const body = planResponse(ORLANDO, q, cal, trip, {
    computedAt: "t",
    today: TODAY,
    leadMae: new Map(),
  });
  return { q, trip, body, meta: body.meta as Record<string, any> };
}

describe("tickets= in the query", () => {
  const parse = (raw: string, starts = true) =>
    parseTickets(ORLANDO, raw, TRIP, starts);

  it("reads lengths, hoppers and starts, in the order given", () => {
    expect(
      parse(
        "wdw-uk-14-day-magic@2027-02-01,uor-date-based.3.h,wdw-date-based.4.h?,wdw-annual-pass",
      ),
    ).toEqual([
      {
        id: "t1",
        product: "wdw-uk-14-day-magic",
        days: 14,
        hopper: true,
        start: "2027-02-01",
      },
      { id: "t2", product: "uor-date-based", days: 3, hopper: true },
      { id: "t3", product: "wdw-date-based", days: 4, hopper: "unknown" },
      {
        id: "t4",
        product: "wdw-annual-pass",
        days: "unlimited",
        hopper: true,
      },
    ]);
  });

  it("refuses what the catalog does not sell, with the reason", () => {
    expect(() => parse("nope")).toThrow(/not a ticket/);
    expect(() => parse("wdw-date-based")).toThrow(/needs its length/);
    expect(() => parse("wdw-date-based.11")).toThrow(/sold for 1–10 days/);
    expect(() => parse("seaworld-1-day.h")).toThrow(/no hopper option/);
    expect(() => parse("seaworld-1-day.h?")).toThrow(/no hopper option/);
    expect(() => parse("wdw-annual-pass@2027-02-02")).toThrow(/pass/);
    expect(() => parse("wdw-annual-pass.3")).toThrow(/no length/);
    expect(() => parse("wdw-date-based.4@2027-02-20")).toThrow(
      /after the trip/,
    );
    expect(() => parse("wdw-4-park-magic@2027-06-01", true)).toThrow();
    expect(() => parse(Array(7).fill("legoland-1-day").join(","))).toThrow(
      /at most 6/,
    );
    // Sold for 2027-01-03 … 2027-05-22 only.
    expect(() =>
      parseTickets(
        ORLANDO,
        "wdw-4-park-magic",
        { from: "2027-06-01", to: "2027-06-10" },
        true,
      ),
    ).toThrow(/none in the trip/);
  });

  it("takes a start before the trip while the window reaches into it", () => {
    expect(parse("wdw-date-based.4@2027-01-27")[0].start).toBe("2027-01-27");
    expect(() => parse("wdw-date-based.4@2027-01-25")).toThrow(
      /end before the trip/,
    );
    // Discovery Cove's reserved day must be a trip day.
    expect(() => parse("discovery-cove@2027-01-31")).toThrow(/reserved day/);
  });

  it("refuses a start on /when, where windows are searched", () => {
    expect(() => parse("wdw-date-based.4@2027-02-02", false)).toThrow(
      /leave out @2027-02-02/,
    );
    expect(parse("wdw-date-based.4", false)).toHaveLength(1);
  });

  it("caches by the canonical form, the same tickets written two ways being one key", () => {
    const a = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ tickets: "wdw-uk-14-day-magic.h,uor-date-based.3" }),
      TODAY,
    );
    const b = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ tickets: "wdw-uk-14-day-magic.14,uor-date-based.3" }),
      TODAY,
    );
    expect(canonicalTickets(a.tickets)).toBe(
      "wdw-uk-14-day-magic.14.h,uor-date-based.3",
    );
    expect(planCacheQuery(a)).toBe(planCacheQuery(b));
    const none = normalizePlanQuery(ORLANDO, KNOWN, query({}), TODAY);
    expect(planCacheQuery(none)).not.toBe(planCacheQuery(a));
    // A plan without tickets keeps the key it had, plus the catalog's version.
    const parts = planCacheQuery(none).split("|");
    expect(parts).toHaveLength(16);
    expect(parts[15]).toBe(`catalog:${none.catalogVersion}`);
    // Another catalog version is another key: an edited rule never serves an old plan.
    expect(planCacheQuery({ ...none, catalogVersion: "edited" })).not.toBe(
      planCacheQuery(none),
    );
  });

  it("keeps ticket=hopper as plan first with every resort's hopper, and hopper= per resort", () => {
    const legacy = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ ticket: "hopper", hop: "1" }),
      TODAY,
    );
    expect(legacy.hopperResorts).toEqual([
      "walt-disney-world",
      "universal-orlando",
      "seaworld-orlando",
    ]);
    expect(legacy.hop).toBe(true);
    const disney = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ hopper: "walt-disney-world" }),
      TODAY,
    );
    expect(disney.hopperResorts).toEqual(["walt-disney-world"]);
    expect(() =>
      normalizePlanQuery(ORLANDO, KNOWN, query({ hopper: "day-trips" }), TODAY),
    ).toThrow(/not a resort/);
    // hop=1 with an owned ticket that hops needs no ticket=hopper.
    const owned = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ tickets: "wdw-uk-14-day-magic", hop: "1" }),
      TODAY,
    );
    expect(owned.hop).toBe(true);
    expect(owned.warnings).toEqual([]);
  });
});

describe("plan first: the suggestion", () => {
  it("suggests a ticket per resort with the shape of the planned days", () => {
    const { meta, body } = answer({
      parks: [...DISNEY, USF, IOA, EPIC].join(","),
      split: "fixed",
    });
    const byResort = new Map(
      (meta.ticketSuggestion as any[]).map((s) => [s.resort, s]),
    );
    const disney = byResort.get("walt-disney-world");
    expect(disney).toMatchObject({
      id: "suggested:walt-disney-world",
      product: "wdw-date-based",
      shopUrl: "https://disneyworld.disney.go.com/admission/tickets/",
    });
    // Four park days; rest days inside the block may ask a longer window.
    expect(disney.days - (disney.extraDays ?? 0)).toBe(4);
    expect(disney.parks).toEqual([...DISNEY].sort());
    expect(disney.hopper).toMatchObject({
      recommended: false,
      why: ["no_gain"],
    });
    // The window from its first day reaches its last.
    const days = (body.days as any[]).filter(
      (d) => d.ticket === "suggested:walt-disney-world",
    );
    expect(days.map((d) => d.ticketDay)).toEqual([1, 2, 3, 4]);
    expect(disney.start).toBe(days[0].date);
    expect(disney.end >= days[3].date).toBe(true);
    // Three Universal days include Epic Universe on the date-based ticket.
    expect(byResort.get("universal-orlando")).toMatchObject({
      product: "uor-date-based",
      days: 3,
    });
    // A 14-day trip: the UK tickets as alternatives, UK and Ireland only.
    expect(disney.alternatives).toContainEqual({
      product: "wdw-uk-14-day-magic",
      days: 14,
      markets: ["uk"],
      reason: "long_stay",
    });
  });

  it("asks for the shortest length whose window reaches the last day, and says the extra", () => {
    const plan = {
      days: [
        parkDay("2027-02-02", "magic-kingdom-park"),
        parkDay("2027-02-03", "epcot"),
        parkDay("2027-02-07", "disneys-hollywood-studios"),
      ],
      unplaced: [],
    } as unknown as OptimizedPlan;
    const [s] = suggest(plan).suggestions;
    // 3 days in 5 does not reach the 6th day: 4 in 7 does.
    expect(s).toMatchObject({
      product: "wdw-date-based",
      days: 4,
      extraDays: 1,
    });
    expect(s.end).toBe("2027-02-08");
  });

  it("gives two Universal days with Epic Universe the 1-Day + 1-Day Epic, and Epic alone its day ticket", () => {
    const two = suggest({
      days: [parkDay("2027-02-02", USF), parkDay("2027-02-03", EPIC)],
      unplaced: [],
    } as unknown as OptimizedPlan).suggestions[0];
    expect(two).toMatchObject({ product: "uor-1-day-plus-epic", days: 2 });
    const epic = suggest({
      days: [parkDay("2027-02-02", EPIC)],
      unplaced: [],
    } as unknown as OptimizedPlan).suggestions[0];
    expect(epic).toMatchObject({ product: "uor-epic-1-day", days: 1 });
    expect(epic.hopper).toBeUndefined();
  });

  it("covers a Disney water park with the Plus option's allowance", () => {
    const { suggestions, days } = suggest({
      days: [
        parkDay("2027-02-02", "magic-kingdom-park"),
        parkDay("2027-02-03", "disneys-typhoon-lagoon-water-park"),
        parkDay("2027-02-04", "epcot"),
      ],
      unplaced: [],
    } as unknown as OptimizedPlan);
    expect(suggestions[0]).toMatchObject({
      product: "wdw-date-based-plus",
      days: 2,
    });
    expect(days.get("2027-02-03")).toEqual({
      ticket: "suggested:walt-disney-world",
      ticketAllowance: true,
    });
  });
});

describe("the hopper verdict", () => {
  it("recommends the hopper for an evening it opens, and lists it", () => {
    const plan = {
      days: [parkDay("2027-02-02", "magic-kingdom-park")],
      unplaced: [],
    } as unknown as OptimizedPlan;
    const v = hopperVerdict(plan, plan, new Set(DISNEY), [
      { date: "2027-02-02", parks: ["magic-kingdom-park", "epcot"] },
    ]);
    expect(v).toMatchObject({
      recommended: true,
      eveningHops: 1,
      hopDays: 0,
      why: ["evening_hops"],
    });
  });

  it("counts the hop days, the visits only the hopper fits and the minutes it saves per ride", () => {
    const withHopper = {
      days: [
        hopDay("2027-02-02", ["magic-kingdom-park", 30], ["epcot", 40]),
        parkDay("2027-02-03", "disneys-hollywood-studios", 40),
      ],
      unplaced: [],
    } as unknown as OptimizedPlan;
    const without = {
      days: [
        parkDay("2027-02-02", "magic-kingdom-park", 50),
        parkDay("2027-02-03", "disneys-hollywood-studios", 60),
      ],
      unplaced: [{ slug: "epcot", reason: "no_day_left" }],
    } as unknown as OptimizedPlan;
    const v = hopperVerdict(withHopper, without, new Set(DISNEY), []);
    expect(v.hopDayList).toEqual([
      { date: "2027-02-02", parks: ["magic-kingdom-park", "epcot"] },
    ]);
    expect(v.fitsOnlyWithHopper).toEqual(["epcot"]);
    expect(v.savedMinutes).toBe(40);
    expect(v.savedMinutesPerRide).toBe(20);
    expect(v.why).toEqual(["hop_days", "fits_more_parks", "saves_minutes"]);
  });

  it("does not recommend hop days that fit nothing more and save under 3 minutes a ride", () => {
    const withHopper = {
      days: [
        hopDay("2027-02-02", ["magic-kingdom-park", 40], ["epcot", 40]),
        parkDay("2027-02-03", "disneys-hollywood-studios", 40),
      ],
      unplaced: [],
    } as unknown as OptimizedPlan;
    const without = {
      days: [
        parkDay("2027-02-02", "magic-kingdom-park", 45),
        parkDay("2027-02-03", "epcot", 40),
        parkDay("2027-02-04", "disneys-hollywood-studios", 40),
      ],
      unplaced: [],
    } as unknown as OptimizedPlan;
    const v = hopperVerdict(withHopper, without, new Set(DISNEY), []);
    expect(v).toMatchObject({
      recommended: false,
      hopDays: 1,
      savedMinutes: 5,
      savedMinutesPerRide: 2,
      why: ["no_gain"],
    });
  });

  it("finds no gain where nothing differs", () => {
    const plan = {
      days: [parkDay("2027-02-02", "epcot", 40)],
      unplaced: [],
    } as unknown as OptimizedPlan;
    expect(hopperVerdict(plan, plan, new Set(DISNEY), [])).toMatchObject({
      recommended: false,
      savedMinutes: 0,
      why: ["no_gain"],
    });
  });
});

describe("owned tickets in the answer", () => {
  it("lists each owned ticket, its days and why some are unused", () => {
    const { meta, body } = answer({
      parks: DISNEY.join(","),
      split: "fixed",
      tickets: "wdw-uk-14-day-magic@2027-02-01",
    });
    expect(meta.tickets).toEqual([
      {
        id: "t1",
        product: "wdw-uk-14-day-magic",
        days: 14,
        hopper: true,
        start: "2027-02-01",
        end: "2027-02-18",
        usedDays: 4,
        unusedDays: 10,
        unusedReason: "parks_fewer_than_days",
        warnings: [],
        links: [
          {
            seller: "Disney Holidays UK",
            url: "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/",
            official: true,
            sponsored: false,
          },
        ],
        shopUrl:
          "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/",
      },
    ]);
    expect(meta.ticketCatalog).toMatch(/^[0-9a-f]{12}$/);
    expect(meta.ticketSuggestion).toBeUndefined();
    expect(
      (body.days as any[])
        .filter((d) => d.ticket === "t1")
        .map((d) => d.ticketDay),
    ).toEqual([1, 2, 3, 4]);
  });

  it("plans Epic Universe plan first beside a 2-day Universal ticket, with its day ticket suggested", () => {
    const { meta, body } = answer({
      parks: [USF, IOA, EPIC].join(","),
      tickets: "uor-date-based.2",
    });
    expect(body.unplaced).toEqual([]);
    const [s] = meta.ticketSuggestion as any[];
    expect(s).toMatchObject({
      id: "suggested:universal-orlando",
      product: "uor-epic-1-day",
      notCoveredBy: ["t1"],
    });
    expect(s.alternatives).toBeUndefined();
    const epicDay = (body.days as any[]).find((d) => d.parks[0]?.slug === EPIC);
    expect(epicDay.ticket).toBe("suggested:universal-orlando");
  });

  it("says a pass's blockouts are unchecked, once in meta and on the ticket", () => {
    const { meta } = answer({
      parks: DISNEY.join(","),
      tickets: "wdw-annual-pass",
    });
    expect(meta.warnings).toEqual(["blockouts_unchecked"]);
    expect(meta.tickets[0]).toMatchObject({
      hopper: true,
      warnings: ["blockouts_unchecked", "park_reservation_needed"],
    });
    expect(meta.tickets[0].start).toBeUndefined();
    expect(meta.tickets[0].unusedDays).toBeUndefined();
  });

  it("fills an owned ticket with second visits and names it", () => {
    const { meta } = answer({
      parks: DISNEY.join(","),
      tickets: "wdw-date-based.6",
    });
    const split = meta.split as Array<{ slug: string; ticket?: string }>;
    expect(split.filter((s) => s.ticket === "t1")).toHaveLength(2);
    expect(meta.tickets[0]).toMatchObject({ usedDays: 6 });
    expect(meta.tickets[0].unusedDays).toBeUndefined();
  });

  it("gives a .h? ticket the hopper verdict and plans it without hopping", () => {
    const { meta } = answer({
      parks: DISNEY.join(","),
      tickets: "wdw-date-based.4.h?",
    });
    expect(meta.tickets[0].hopper).toBe("unknown");
    expect(meta.tickets[0].hopperVerdict).toMatchObject({
      recommended: false,
      why: ["no_gain"],
    });
  });
});

describe("owned tickets' resorts mix only when it pays", () => {
  const UNIVERSAL = [USF, IOA, EPIC];
  const both =
    "wdw-uk-14-day-magic@2027-02-01,uor-uk-three-park-adventure@2027-02-01";
  /** Disney quiet on even days and Universal on odd ones, by `gap` minutes. */
  const alternating =
    (gap: number) =>
    (slug: string, i: number): TripParkDayInternal => {
      const disney = DISNEY.includes(slug);
      const quiet = disney ? i % 2 === 0 : i % 2 === 1;
      return {
        status: "OPERATING",
        hoursPublished: true,
        crowdLevel: "moderate",
        headlinerWait: 40,
        waitExact: quiet ? 30 : 30 + gap,
        basis: "forecast",
        dayHours: 12,
      };
    };
  // Nine visits in twelve park days: blocks cannot put them all on quiet days.
  const parks = [
    "magic-kingdom-park:2",
    "epcot:2",
    "disneys-hollywood-studios",
    "disneys-animal-kingdom-theme-park",
    ...UNIVERSAL,
  ].join(",");

  it("mixes where that saves at least 3 minutes a ride", () => {
    const { meta } = answer(
      { parks, split: "fixed", tickets: both },
      alternating(30),
    );
    expect(meta.mixing.used).toBe(true);
    expect(meta.mixing.savedMinutesPerRide).toBeGreaterThanOrEqual(3);
  });

  it("keeps the resorts in blocks where mixing saves less", () => {
    const { meta, body } = answer(
      { parks, split: "fixed", tickets: both },
      alternating(1),
    );
    expect(meta.mixing.used).toBe(false);
    expect(meta.mixing.savedMinutesPerRide).toBeLessThan(3);
    const universal = (body.days as any[])
      .map((d, i) =>
        d.parks.some((p: any) => UNIVERSAL.includes(p.slug)) ? i : -1,
      )
      .filter((i) => i >= 0);
    const disney = (body.days as any[])
      .map((d, i) =>
        d.parks.some((p: any) => DISNEY.includes(p.slug)) ? i : -1,
      )
      .filter((i) => i >= 0);
    expect(
      universal.every(
        (i) => i < Math.min(...disney) || i > Math.max(...disney),
      ),
    ).toBe(true);
  });

  it("follows blocks written in the query either way", () => {
    expect(
      answer(
        { parks, split: "fixed", tickets: both, blocks: "1" },
        alternating(30),
      ).meta.mixing,
    ).toEqual({ used: false });
    expect(
      answer(
        { parks, split: "fixed", tickets: both, blocks: "0" },
        alternating(1),
      ).meta.mixing,
    ).toEqual({ used: true });
  });

  it("numbers an every-day ticket's days by its window and gives its window", () => {
    const { meta, body } = answer({
      parks: UNIVERSAL.join(","),
      split: "fixed",
      tickets: "uor-uk-three-park-adventure@2027-02-01",
    });
    expect(meta.tickets[0]).toMatchObject({ usedDays: 3, windowDays: 14 });
    expect(meta.tickets[0].unusedDays).toBeUndefined();
    expect(meta.tickets[0].unusedReason).toBeUndefined();
    for (const d of (body.days as any[]).filter((x) => x.ticket === "t1")) {
      const day =
        Math.round(
          (Date.parse(`${d.date}T00:00:00Z`) -
            Date.parse("2027-02-01T00:00:00Z")) /
            86_400_000,
        ) + 1;
      expect(d.ticketDay).toBe(day);
    }
  });
});

function parkDay(date: string, slug: string, wait = 40): PlannedDay {
  return {
    date,
    kind: "park",
    parks: [{ slug, part: "full" }],
    headlinerWait: wait,
    reasons: [],
    warnings: [],
  };
}

function hopDay(date: string, ...parks: Array<[string, number]>): PlannedDay {
  return {
    date,
    kind: "hop",
    parks: parks.map(([slug, wait]) => ({
      slug,
      part: "half",
      headlinerWait: wait,
    })),
    reasons: [],
    warnings: [],
  };
}

function suggest(plan: OptimizedPlan) {
  const input: SuggestionInput = {
    region: ORLANDO,
    plan,
    trip: { from: FROM, to: addIsoDays(FROM, 6) },
    owned: [],
    hopperResorts: [],
    eveningsWith: () => new Map(),
    replan: () => null,
  };
  return suggestTickets(input);
}
