import {
  BadRequestException,
  NotFoundException,
  ServiceUnavailableException,
} from "@nestjs/common";
import { addIsoDays } from "../../common/utils/date.util";
import { JAPAN, ORLANDO } from "../regions";
import { productOf } from "../trip-tickets";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "../trip-plan.assembly";
import type { InternalRegionCalendar } from "../trip-region.assembly";
import { normalizeWhenQuery } from "../trip-when.assembly";
import type { TripPlanQueryDto } from "../dto/trip-region-query.dto";
import type { TripPlanDto } from "../dto/trip-region-response.dto";
import type { TripWhen } from "../trip-when";
import {
  TRIP_ERROR_CODES,
  answerFactsOf,
  UNKNOWN_REGION,
  errorOf,
  hourOf,
  metricRowOf,
  planAnswerFacts,
  planShapeOf,
  regionOf,
  ticketKindOf,
  whenAnswerFacts,
  whenShapeOf,
} from "./trip-request-facts";

const TODAY = "2026-11-12";
const FROM = "2027-02-01";
const KNOWN = ORLANDO.parks.map((p) => p.slug);
const query = (over: Partial<TripPlanQueryDto>): TripPlanQueryDto => ({
  from: FROM,
  to: addIsoDays(FROM, 9),
  parks: "epcot,magic-kingdom-park",
  ...over,
});

/** The code of what normalizing `over` throws. */
const codeOf = (over: Partial<TripPlanQueryDto>) => {
  try {
    normalizePlanQuery(ORLANDO, KNOWN, query(over), TODAY);
  } catch (error) {
    return errorOf(error);
  }
  throw new Error("the query was valid");
};

describe("errorOf", () => {
  it.each([
    [{ from: "2027-02-30" }, "bad_date"],
    [{ from: "2026-11-01" }, "window_in_past"],
    [{ to: "2027-01-20" }, "window_reversed"],
    [{ to: addIsoDays(FROM, 25) }, "window_too_long"],
    [{ parks: "europa-park" }, "unknown_park"],
    [{ parks: "epcot,epcot" }, "park_listed_twice"],
    [{ parks: "epcot:4" }, "visits_per_park"],
    [{ to: FROM, parks: "epcot:2,magic-kingdom-park:2" }, "visits_exceed_days"],
    [{ hopper: "atlantis" }, "bad_hopper"],
    [{ area: "atlantis" }, "unknown_area"],
    [{ lock: `universal-epic-universe@${FROM}` }, "bad_lock"],
    [{ lock: `epcot@${addIsoDays(FROM, 30)}` }, "bad_lock"],
    [{ tickets: "nope" }, "bad_tickets"],
    [{ tickets: "wdw-date-based.4@2027-02-30" }, "bad_tickets"],
  ])("tells a plan refused for %j as %s", (over, code) => {
    expect(codeOf(over)).toEqual({ status: 400, code });
  });

  it("tells a search range shorter than the trip", () => {
    try {
      normalizeWhenQuery(
        ORLANDO,
        KNOWN,
        { from: FROM, to: addIsoDays(FROM, 2), days: 7, parks: "epcot" },
        TODAY,
      );
      throw new Error("the query was valid");
    } catch (error) {
      expect(errorOf(error)).toEqual({
        status: 400,
        code: "range_shorter_than_trip",
      });
    }
  });

  it("reads the validation pipe's list of failed constraints as one code", () => {
    expect(
      errorOf(
        new BadRequestException([
          "property budget should not exist",
          "from must be a date",
        ]),
      ),
    ).toEqual({ status: 400, code: "invalid_query" });
  });

  it("names a missing region, an outage and anything that is no HTTP error", () => {
    expect(errorOf(new NotFoundException('No trip region "x"'))).toEqual({
      status: 404,
      code: "unknown_region",
    });
    expect(errorOf(new ServiceUnavailableException())).toEqual({
      status: 503,
      code: "unavailable",
    });
    expect(errorOf(new Error("relation does not exist"))).toEqual({
      status: 500,
      code: "internal",
    });
    expect(errorOf(new BadRequestException("something new"))).toEqual({
      status: 400,
      code: "bad_request",
    });
  });

  it("answers only codes of its own list", () => {
    for (const error of [
      new BadRequestException("hotel must be one of disney, offsite"),
      new BadRequestException("within: no school-holiday regions for XX"),
    ]) {
      expect(TRIP_ERROR_CODES).toContain(errorOf(error).code);
    }
  });
});

describe("a region with bases", () => {
  const JAPAN_TODAY = "2027-01-04";
  const japan = (over: Record<string, string>) =>
    normalizePlanQuery(
      JAPAN,
      JAPAN.parks.map((p) => p.slug),
      {
        from: "2027-04-10",
        to: "2027-04-19",
        parks: "tokyo-disneyland,universal-studios-japan",
        ...over,
      } as TripPlanQueryDto,
      JAPAN_TODAY,
    );
  const codeIn = (fn: () => unknown) => {
    try {
      fn();
    } catch (error) {
      return errorOf(error).code;
    }
    throw new Error("the query was valid");
  };

  it.each([
    [{ bases: "atlantis" }, "bad_bases"],
    [{ bases: "tokyo,tokyo" }, "bad_bases"],
    [{ bases: "tokyo@atlantis" }, "bad_bases"],
    [{ startBase: "atlantis" }, "bad_bases"],
    [{ bases: "tokyo:5,osaka:5" }, "stay_nights_exceed_trip"],
    [{ area: "maihama" }, "area_needs_base"],
  ])("tells a stay refused for %j as %s", (over, code) => {
    expect(codeIn(() => japan(over))).toBe(code);
  });

  it("tells bases asked of a region without them", () => {
    expect(codeOf({ bases: "tokyo" })).toEqual({
      status: 400,
      code: "no_bases_in_region",
    });
  });

  it("counts the stays and moves of a real answer", () => {
    const q = japan({
      parks:
        "tokyo-disneyland,tokyo-disneysea,fuji-q-highland,legoland-japan,universal-studios-japan",
      split: "fixed",
      startBase: "tokyo",
      endBase: "osaka",
      bases: "tokyo:4,fujikawaguchiko:1,nagoya:1,osaka:3",
    });
    const days: InternalRegionCalendar["days"] = [];
    for (let i = 0; i < 10; i++) {
      const date = addIsoDays(q.from, i);
      days.push({
        date,
        isHoliday: false,
        isSchoolVacation: false,
        parks: Object.fromEntries(
          JAPAN.parks.map((p, k) => [
            p.slug,
            {
              status: "OPERATING",
              hoursPublished: true,
              crowdLevel: "moderate",
              headlinerWait: 30,
              waitExact: 25 + ((k * 11 + i * 9) % 30),
              basis: "forecast",
              dayHours: 12,
              usualHours: 12,
              opensAt: `${date}T09:00:00+09:00`,
              closesAt: `${date}T21:00:00+09:00`,
            },
          ]),
        ),
      });
    }
    const calendar: InternalRegionCalendar = {
      meta: {
        region: "japan",
        timezone: "Asia/Tokyo",
        from: q.from,
        to: q.to,
        computedAt: "t",
        publishedHoursUntil: {},
      },
      parks: JAPAN.parks.map((p) => p.slug),
      days,
      seasons: [],
    };
    const answer = planResponse(
      JAPAN,
      q,
      calendar,
      optimizePlan(JAPAN, q, calendar),
      {
        computedAt: "t",
        today: JAPAN_TODAY,
        leadMae: new Map(),
      },
    );
    expect(planAnswerFacts(answer)).toMatchObject({
      bases: 4,
      moves: 3,
      leftOut: 0,
      staySuggested: false,
    });
    // Both ends fixed, the region's own move buffer.
    expect(planShapeOf(JAPAN, q, true)).toMatchObject({
      gateways: "both",
      moveBuffer: undefined,
    });
    expect(
      planShapeOf(
        JAPAN,
        japan({ startBase: "tokyo", moveBuffer: 60 } as never),
        true,
      ),
    ).toMatchObject({ gateways: "start", moveBuffer: 60 });
  });
});

describe("the shape of a query", () => {
  it("counts parks, visits, locks and tickets, and says a hopper anywhere", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({
        parks: "epcot:2,magic-kingdom-park,universal-epic-universe",
        lock: `epcot@${addIsoDays(FROM, 2)}`,
        tickets: "uor-date-based.3.h,wdw-annual-pass,discovery-cove",
      }),
      TODAY,
    );
    expect(planShapeOf(ORLANDO, q, true)).toEqual({
      tripDays: 10,
      parks: 3,
      parksNamed: true,
      visits: 4,
      locks: 1,
      ticketKinds: ["multi_day", "pass", "reserved_day"],
      hopper: true,
    });
  });

  it("counts the headliners when no park was named", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      KNOWN,
      query({ parks: undefined }),
      TODAY,
    );
    const shape = planShapeOf(ORLANDO, q, false);
    expect(shape).toMatchObject({ parksNamed: false, hopper: false });
    expect(shape.parks).toBe(ORLANDO.parks.filter((p) => p.headliner).length);
  });

  it("takes a search's trip length and its range, never a date", () => {
    const q = normalizeWhenQuery(
      ORLANDO,
      KNOWN,
      { from: FROM, to: addIsoDays(FROM, 59), days: 8, parks: "epcot" },
      TODAY,
    );
    const shape = whenShapeOf(ORLANDO, q, true);
    expect(shape).toMatchObject({
      tripDays: 8,
      rangeDays: 60,
      within: false,
      locks: 0,
      parks: 1,
    });
    expect(JSON.stringify(shape)).not.toContain("2027");
  });

  it("tells an owned ticket by its product", () => {
    const kind = (id: string, days: number | "unlimited" = 2) =>
      ticketKindOf(productOf(ORLANDO, id), { days });
    expect(kind("wdw-annual-pass", "unlimited")).toBe("pass");
    expect(kind("seaworld-fun-card")).toBe("pass");
    expect(kind("discovery-cove", 1)).toBe("reserved_day");
    expect(kind("wdw-date-based", 1)).toBe("one_day");
    expect(kind("wdw-date-based", 4)).toBe("multi_day");
    expect(ticketKindOf(undefined, { days: 3 })).toBe("unknown");
  });
});

describe("the facts of an answer", () => {
  const plan = (over: Partial<TripPlanDto>): TripPlanDto => ({
    meta: { exact: true },
    days: [],
    parks: [],
    totals: {},
    unplaced: [],
    parksLeftOut: [],
    ...over,
  });

  it("reads a plan's search, its loosened rules and the visits it left out", () => {
    expect(
      planAnswerFacts(
        plan({
          meta: { exact: false, relaxed: ["streak_relaxed", "blocks_relaxed"] },
          unplaced: [
            { slug: "epcot", reason: "no_open_day" },
            { slug: "epcot", reason: "no_day_left" },
          ],
        }),
      ),
    ).toEqual({
      exact: false,
      relaxed: ["blocks_relaxed", "streak_relaxed"],
      unplacedReasons: ["no_day_left", "no_open_day"],
      bases: null,
      moves: null,
      windows: null,
      windowsPlanned: null,
      leftOut: 0,
      staySuggested: null,
    });
  });

  it("counts the stays and moves of a region with bases, and only there", () => {
    const facts = planAnswerFacts(
      plan({
        meta: { exact: true, stays: [{ base: "tokyo" }, { base: "osaka" }] },
        days: [
          { kind: "park", base: "tokyo" },
          { kind: "travel", base: "osaka", move: { from: "tokyo" } },
        ],
      }),
    );
    expect(facts).toMatchObject({ bases: 2, moves: 1 });
    expect(
      planAnswerFacts(
        plan({ totals: { moves: 3 }, meta: { exact: true, stays: [] } }),
      ),
    ).toMatchObject({ bases: 0, moves: 3 });
    expect(planAnswerFacts(plan({}))).toMatchObject({
      bases: null,
      moves: null,
    });
  });

  it("keeps only codes of the API's own shape", () => {
    expect(
      planAnswerFacts(
        plan({ meta: { relaxed: ["blocks_relaxed", "<script>", 7] } }),
      ).relaxed,
    ).toEqual(["blocks_relaxed"]);
  });

  it("reads a search's proof, how much it planned and the best window's gaps", () => {
    const when = {
      meta: { search: { exact: false, planned: 41 } },
      windows: [
        {
          warnings: [],
          plan: {
            days: [],
            unplaced: ["epcot"],
            stays: [{ base: "tokyo" }, { base: "osaka" }],
          },
        },
        { warnings: ["ticket_window_relaxed"], plan: { days: [] } },
      ],
    } as unknown as TripWhen;
    expect(whenAnswerFacts(when)).toEqual({
      exact: false,
      relaxed: ["ticket_window_relaxed"],
      unplacedReasons: ["unknown"],
      bases: 2,
      moves: null,
      windows: 2,
      windowsPlanned: 41,
      leftOut: 0,
    });
  });
});

describe("the parks left out and the stay suggestion", () => {
  it("counts the chosen parks with no visit, on /plan and in /when's best window", () => {
    const plan = {
      meta: { exact: true },
      days: [],
      parks: [],
      totals: {},
      unplaced: [],
      parksLeftOut: [
        { park: "epcot", reason: "no_day_left" },
        { park: "universal-epic-universe", reason: "no_day_left" },
      ],
    } as unknown as TripPlanDto;
    expect(planAnswerFacts(plan)).toMatchObject({
      leftOut: 2,
      staySuggested: null,
    });
    expect(answerFactsOf("plan", plan)).toMatchObject({ leftOut: 2 });
    const when = {
      meta: { search: { exact: true, planned: 3 } },
      windows: [
        { warnings: [], plan: { days: [], parksLeftOut: [{ park: "epcot" }] } },
      ],
    } as unknown as TripWhen;
    expect(whenAnswerFacts(when)).toMatchObject({ leftOut: 1 });
    expect(whenAnswerFacts({ meta: {}, windows: [] } as never)).toMatchObject({
      leftOut: null,
    });
    // The best days add nothing; an answer of another shape is read as none.
    expect(answerFactsOf("best_days", {})).toBeUndefined();
    expect(answerFactsOf("plan", null)).toBeUndefined();
  });

  it("says a suggestion only in a region with bases", () => {
    const facts = planAnswerFacts({
      meta: {
        exact: true,
        stays: [{ base: "tokyo" }],
        staySuggestion: { base: "osaka" },
      },
      days: [],
      parks: [],
      totals: {},
      unplaced: [],
    } as unknown as TripPlanDto);
    expect(facts.staySuggested).toBe(true);
  });
});

describe("a metric row", () => {
  it("is the site's unless Parky asked", () => {
    const of = (caller?: "site" | "parky") =>
      metricRowOf({
        route: "plan",
        caller,
        region: "orlando",
        at: new Date("2026-11-14T17:42:31Z"),
        latencyMs: 5,
        trace: { shape: { moveBuffer: 90, gateways: "end" } },
        status: 200,
        facts: {
          exact: true,
          relaxed: [],
          unplacedReasons: [],
          bases: 2,
          moves: 1,
          windows: null,
          windowsPlanned: null,
          leftOut: 1,
          staySuggested: true,
        },
      });
    expect(of()).toMatchObject({
      caller: "site",
      leftOut: 1,
      moveBuffer: 90,
      gateways: "end",
      staySuggested: true,
    });
    expect(of("parky").caller).toBe("parky");
  });

  it("keeps the hour, never the moment, and a known region only", () => {
    expect(hourOf(new Date("2026-11-14T17:42:31.123Z")).toISOString()).toBe(
      "2026-11-14T17:00:00.000Z",
    );
    expect(regionOf("orlando")).toBe("orlando");
    expect(regionOf("atlantis")).toBe(UNKNOWN_REGION);
    expect(regionOf("__proto__")).toBe(UNKNOWN_REGION);
    expect(regionOf(undefined)).toBe(UNKNOWN_REGION);
  });

  it("takes the calendar's cache for the best days, and clamps the counts", () => {
    const row = metricRowOf({
      route: "best_days",
      region: "orlando",
      at: new Date("2026-11-14T17:42:31Z"),
      latencyMs: 12.6,
      trace: { calendarCache: "hit", shape: { rangeDays: 99999 } },
      status: 200,
    });
    expect(row).toMatchObject({
      bucket: new Date("2026-11-14T17:00:00Z"),
      route: "best_days",
      cache: "hit",
      calendarCache: "hit",
      latencyMs: 13,
      rangeDays: 32767,
      relaxed: [],
      unplacedReasons: [],
      ticketKinds: [],
      errorCode: null,
      exact: null,
    });
  });
});
