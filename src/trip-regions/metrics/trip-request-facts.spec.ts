import {
  BadRequestException,
  NotFoundException,
  ServiceUnavailableException,
} from "@nestjs/common";
import { addIsoDays } from "../../common/utils/date.util";
import { ORLANDO } from "../regions";
import { productOf } from "../trip-tickets";
import { normalizePlanQuery } from "../trip-plan.assembly";
import { normalizeWhenQuery } from "../trip-when.assembly";
import type { TripPlanQueryDto } from "../dto/trip-region-query.dto";
import type { TripPlanDto } from "../dto/trip-region-response.dto";
import type { TripWhen } from "../trip-when";
import {
  TRIP_ERROR_CODES,
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
    [{ to: FROM }, "visits_exceed_days"],
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
        { warnings: [], plan: { days: [], unplaced: ["epcot"] } },
        { warnings: ["ticket_window_relaxed"], plan: { days: [] } },
      ],
    } as unknown as TripWhen;
    expect(whenAnswerFacts(when)).toEqual({
      exact: false,
      relaxed: ["ticket_window_relaxed"],
      unplacedReasons: ["unknown"],
      bases: null,
      moves: null,
      windows: 2,
      windowsPlanned: 41,
    });
  });
});

describe("a metric row", () => {
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
