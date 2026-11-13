import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import { extraVisitRoom, type PlanInput } from "./trip-optimizer";
import {
  autoSplit,
  normalizePlanQuery,
  optimizePlan,
  planCacheQuery,
  planInput,
  plannedLeads,
  planResponse,
} from "./trip-plan.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";

const TODAY = "2026-11-12";
const FROM = "2026-12-01";
const KNOWN = ORLANDO.parks.map((p) => p.slug);

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
  from: FROM,
  to: addIsoDays(FROM, 13),
  ...over,
});

describe("normalizePlanQuery", () => {
  it("defaults to the headliners, blocks, any fireworks and split=auto", () => {
    const q = normalizePlanQuery(ORLANDO, KNOWN, query({}), TODAY);
    expect(q.parks).toHaveLength(7);
    expect(q.parks.every((p) => p.visits === 1 && !p.explicit)).toBe(true);
    expect(q).toMatchObject({
      blocks: true,
      fireworks: "any",
      split: "auto",
      hotel: "offsite",
      arrival: "free",
      departure: "free",
    });
  });

  it("marks a written count explicit, and keeps it out of the cache key otherwise", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ parks: "epcot,magic-kingdom-park:1", blocks: "0" }),
      TODAY,
    );
    expect(q.parks).toEqual([
      { slug: "epcot", visits: 1, explicit: false },
      { slug: "magic-kingdom-park", visits: 1, explicit: true },
    ]);
    expect(planCacheQuery(q)).toContain("epcot,magic-kingdom-park:1");
    expect(q.blocks).toBe(false);
  });
});

describe("extraVisitRoom", () => {
  const input = (days: number, visits: number, over = {}): PlanInput => ({
    days: Array.from({ length: days }, (_, i) => ({
      date: addIsoDays(FROM, i),
      weekday: 1,
      isHoliday: false,
      isSchoolVacation: false,
    })),
    parks: [
      { slug: "epcot", group: "walt-disney-world", type: "theme_park", visits },
    ],
    cells: {},
    options: {
      arrival: "free",
      departure: "free",
      blocks: true,
      blockGroups: [],
      fireworks: "any",
      rest: { mode: "auto" },
      hop: false,
      hopPairs: [],
      perkRules: [],
      locks: [],
      ...over,
    },
  });

  it("keeps a rest day per five days on a long trip, and runs to four", () => {
    // 14 days: 12 free of travel; a run of 12 needs 2 rests, 14 / 5 → 2.
    expect(extraVisitRoom(input(14, 7))).toBe(3);
    // 7 days: 5 free of travel, one run of 5 needs a rest; under 8 days no ratio.
    expect(extraVisitRoom(input(7, 3))).toBe(1);
  });

  it("counts a pinned rest day, which also splits the runs, and keeps none with rest=none", () => {
    // Pinned on day 6: runs of 5 and 6 each still need a rest; 11 − 7 − 2.
    expect(
      extraVisitRoom(
        input(14, 7, {
          locks: [{ target: "rest", date: addIsoDays(FROM, 6) }],
        }),
      ),
    ).toBe(2);
    expect(extraVisitRoom(input(14, 7, { rest: { mode: "none" } }))).toBe(5);
  });
});

describe("autoSplit", () => {
  it("gives spare days to second visits in the curated order, never to explicit counts", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({
        parks:
          "magic-kingdom-park:1,epcot,universal-epic-universe,disneys-hollywood-studios,universal-studios-florida",
      }),
      TODAY,
    );
    const visits = autoSplit(ORLANDO, q, calendar(q.from, q.to));
    // 14 days → 12 travel-free, 2 rest, 5 visits: room for 5 more, but only
    // three parks listed without a count have a rank (Hollywood Studios 5,
    // Universal Studios 6 come after EPCOT 2 and Epic 3).
    expect(Object.fromEntries(visits)).toEqual({
      "magic-kingdom-park": 1,
      epcot: 2,
      "universal-epic-universe": 2,
      "disneys-hollywood-studios": 2,
      "universal-studios-florida": 2,
    });
  });

  it("does nothing with split=fixed or without room", () => {
    const fixed = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ split: "fixed" }),
      TODAY,
    );
    expect([
      ...autoSplit(ORLANDO, fixed, calendar(fixed.from, fixed.to)).values(),
    ]).toEqual(fixed.parks.map(() => 1));
    const short = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ to: addIsoDays(FROM, 4), parks: "epcot,magic-kingdom-park" }),
      TODAY,
    );
    // 5 days → 3 travel-free, 2 visits, a run of 3 needs no rest: one spare.
    expect(
      Object.fromEntries(
        autoSplit(ORLANDO, short, calendar(short.from, short.to)),
      ),
    ).toEqual({ epcot: 1, "magic-kingdom-park": 2 });
  });

  it("skips a park with fewer than two open days", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ to: addIsoDays(FROM, 6), parks: "magic-kingdom-park,epcot" }),
      TODAY,
    );
    const cal = calendar(q.from, q.to, (slug, i) => ({
      status: slug === "magic-kingdom-park" && i !== 2 ? "CLOSED" : "OPERATING",
      hoursPublished: true,
      crowdLevel: "moderate",
      headlinerWait: 40,
      basis: "forecast",
    }));
    expect(Object.fromEntries(autoSplit(ORLANDO, q, cal))).toEqual({
      "magic-kingdom-park": 1,
      epcot: 2,
    });
  });
});

describe("planResponse", () => {
  it("reports the split, the measured error at the planned leads and whether the order matters", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ to: addIsoDays(FROM, 6), parks: "magic-kingdom-park,epcot" }),
      TODAY,
    );
    const cal = calendar(q.from, q.to);
    const trip = optimizePlan(ORLANDO, q, cal);
    // 7 days → 5 travel-free, a run of 5 keeps a rest: room for two more.
    expect(trip.split).toEqual([
      { slug: "epcot", visits: 2, auto: true },
      { slug: "magic-kingdom-park", visits: 2, auto: true },
    ]);
    const leads = plannedLeads(trip, TODAY);
    expect(leads.length).toBe(4);
    const answer = planResponse(ORLANDO, q, cal, trip, {
      computedAt: "t",
      today: TODAY,
      leadMae: new Map(leads.map((l, i) => [l, i === 0 ? null : 15])),
    });
    expect(answer.meta).toMatchObject({ split: trip.split, exact: true });
    expect(answer.totals).toMatchObject({
      expectedError: 15,
      visits: 4,
      orderMatters: expect.any(Boolean),
    });
    const none = planResponse(ORLANDO, q, cal, trip, {
      computedAt: "t",
      today: TODAY,
      leadMae: new Map(),
    });
    expect(none.totals).not.toHaveProperty("expectedError");
  });

  it("feeds the optimizer the unrounded wait", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ parks: "epcot" }),
      TODAY,
    );
    const input = planInput(ORLANDO, q, calendar(q.from, q.to), new Map());
    const first = input.cells.epcot[q.from];
    expect(first.headlinerWait).toBe(40);
    expect(first.exactWait).toBeDefined();
  });
});
