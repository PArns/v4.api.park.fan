import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import {
  dayCost,
  dayCostParts,
  heatOf,
  hurricaneMinutes,
  hurricaneWeight,
  isHurricanePeak,
  optimizeTrip,
  type PlanCell,
} from "./trip-optimizer";
import { regionBestDays } from "./trip-best-days";
import { normalizePlanQuery } from "./trip-plan.assembly";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { findTripDates } from "./trip-when";

/**
 * Extreme heat and the hurricane season in the one day cost every route
 * shares (`dayCost`), and what each route reports of them: the plan's and
 * the windows' `penalties`, their warnings, the best days' reason. The plain
 * figures (`headlinerWait`) never carry them.
 */

const hot = (tempMax: number, basis: "forecast" | "climate_normal") => ({
  tempMax,
  precipitationMm: 0,
  basis,
});
const cell = (wait: number, extra: Partial<PlanCell> = {}): PlanCell => ({
  status: "OPERATING",
  headlinerWait: wait,
  exactWait: wait,
  basis: "forecast",
  dayHours: 12,
  ...extra,
});

describe("the heat term", () => {
  it("makes a 36 °C forecast day dearer at a theme park and not at a water park", () => {
    const mild = cell(40, { weather: hot(30, "forecast") });
    const scorching = cell(40, { weather: hot(36, "forecast") });
    expect(dayCost(mild, { type: "theme_park" })).toBe(40);
    const parts = dayCostParts(scorching, { type: "theme_park" });
    expect(parts.cost).toBeCloseTo(40 * 1.08);
    expect(parts.heat).toBeCloseTo(3.2);
    // A water park keeps its warm-and-dry bonus and gets nothing on top.
    expect(dayCost(scorching, { type: "water_park" })).toBe(
      dayCost(mild, { type: "water_park" }),
    );
    expect(dayCostParts(scorching, { type: "water_park" }).heat).toBe(0);
  });

  it("counts a climate normal from 31.5 °C at half the weight, and nothing below", () => {
    expect(heatOf(hot(34.9, "forecast"))).toBeNull();
    expect(heatOf(hot(31.4, "climate_normal"))).toBeNull();
    expect(heatOf(hot(32.5, "climate_normal"))?.factor).toBeCloseTo(1.04);
    expect(heatOf(hot(50, "forecast"))?.factor).toBe(1.4);
    expect(heatOf(undefined)).toBeNull();
  });
});

describe("the hurricane season", () => {
  it("weighs the dates to NHC's climatology, peak mid-August to mid-October", () => {
    expect(hurricaneWeight("2027-05-31")).toBe(0);
    expect(hurricaneWeight("2027-06-01")).toBe(0.05);
    expect(hurricaneWeight("2027-09-10")).toBe(1);
    expect(hurricaneWeight("2027-11-30")).toBe(0.05);
    expect(hurricaneWeight("2027-12-01")).toBe(0);
    expect(isHurricanePeak("2027-08-14")).toBe(false);
    expect(isHurricanePeak("2027-08-15")).toBe(true);
    expect(isHurricanePeak("2027-10-15")).toBe(true);
    expect(isHurricanePeak("2027-10-16")).toBe(false);
    expect(hurricaneMinutes("2027-09-10")).toBe(8);
  });

  it("adds the same minutes to every park on a date, so a day's pick is unchanged", () => {
    const ctx = { type: "theme_park" as const, date: "2027-09-10" };
    expect(dayCost(cell(40), ctx) - dayCost(cell(30), ctx)).toBe(10);
    expect(dayCostParts(cell(40), ctx).hurricane).toBe(8);
    expect(dayCost(cell(40), { type: "theme_park" })).toBe(40);
  });
});

describe("the plan", () => {
  it("reports heat and hurricane as penalties beside the plain headlinerWait", () => {
    const dates = ["2027-09-09", "2027-09-10", "2027-09-11"];
    const plan = optimizeTrip({
      days: dates.map((date) => ({
        date,
        weekday: new Date(`${date}T12:00:00Z`).getUTCDay(),
        isHoliday: false,
        isSchoolVacation: false,
      })),
      parks: [
        {
          slug: "epcot",
          group: "walt-disney-world",
          type: "theme_park",
          visits: 1,
        },
      ],
      cells: {
        epcot: Object.fromEntries(
          dates.map((d) => [d, cell(40, { weather: hot(37, "forecast") })]),
        ),
      },
      options: {
        arrival: "park",
        departure: "park",
        blocks: false,
        blockGroups: [],
        fireworks: "any",
        rest: { mode: "none" },
        hop: false,
        hopPairs: [],
        perkRules: [],
        locks: [],
      },
    });
    expect(plan.totals.headlinerWait).toBe(40);
    expect(plan.totals.penalties).toEqual({ heat: 6.4, hurricane: 8 });
    const day = plan.days.find((d) => d.kind === "park");
    expect(day?.warnings).toEqual(
      expect.arrayContaining(["extreme_heat", "hurricane_peak"]),
    );
  });
});

describe("best days", () => {
  it("names extreme heat and ranks a peak-season day behind an equal one before it", () => {
    const start = "2027-08-12";
    const n = 6;
    const days = Array.from({ length: n }, (_, i) => {
      const date = addIsoDays(start, i);
      const park: TripParkDayInternal = {
        status: "OPERATING",
        hoursPublished: true,
        crowdLevel: "moderate",
        headlinerWait: 40,
        waitExact: 40,
        basis: "forecast",
        dayHours: 12,
        usualHours: 12,
        ...(i === 1 && { weather: hot(37, "forecast") }),
      };
      return {
        date,
        isHoliday: false,
        isSchoolVacation: false,
        parks: { epcot: park },
      };
    });
    const calendar: InternalRegionCalendar = {
      meta: {
        region: "orlando",
        timezone: "America/New_York",
        from: days[0].date,
        to: days[n - 1].date,
        computedAt: "t",
        publishedHoursUntil: {},
      },
      parks: ["epcot"],
      days,
      seasons: [],
    };
    const best = regionBestDays(ORLANDO, calendar, 6);
    const quiet = best.parks[0].quiet;
    // 12–14 August carry the early season's 3.2 minutes, 15 August on the
    // peak's 6.4; the 37 °C day (40 × 1.16 + 3.2) is dearer than any of them.
    expect(quiet[0].date).toBe("2027-08-12");
    expect(quiet[quiet.length - 1].date).toBe("2027-08-13");
    expect(quiet.find((d) => d.date === "2027-08-13")?.reasons).toContain(
      "extreme_heat",
    );
    expect(quiet.every((d) => d.headlinerWait === 40)).toBe(true);
  });
});

describe("/when", () => {
  const parks = ["epcot", "magic-kingdom-park"];

  /** Every day alike: the same waits and hours, so only the season tells windows apart. */
  function flat(
    from: string,
    n: number,
    basisOf: (i: number) => TripParkDayInternal["basis"] = () => "forecast",
  ): InternalRegionCalendar {
    const days = Array.from({ length: n }, (_, i) => ({
      date: addIsoDays(from, i),
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(
        parks.map((slug) => [
          slug,
          {
            status: "OPERATING",
            hoursPublished: true,
            crowdLevel: "moderate",
            headlinerWait: 40,
            waitExact: 40,
            basis: basisOf(i),
            dayHours: 12,
          } as TripParkDayInternal,
        ]),
      ),
    }));
    return {
      meta: {
        region: "orlando",
        timezone: "America/New_York",
        from,
        to: days[n - 1].date,
        computedAt: "t",
        publishedHoursUntil: {},
      },
      parks,
      days,
      seasons: [],
    };
  }

  const run = (cal: InternalRegionCalendar, limit = 3) =>
    findTripDates({
      region: ORLANDO,
      calendar: cal,
      query: {
        from: cal.days[0].date,
        to: cal.days[cal.days.length - 1].date,
        days: 5,
        limit,
        gap: 5,
        plan: normalizePlanQuery(
          ORLANDO,
          ORLANDO.parks.map((p) => p.slug),
          {
            from: cal.days[0].date,
            to: addIsoDays(cal.days[0].date, 4),
            parks: parks.join(","),
          },
          "2027-01-01",
        ),
      },
    });

  it("puts peak-season windows behind equal ones outside the season, plain figures alike", () => {
    const result = run(flat("2027-05-10", 140), 10);
    const first = result.windows[0];
    expect(first.to < "2027-06-01").toBe(true);
    expect(first.penalties).toBeUndefined();
    const peak = result.windows.filter((w) =>
      w.warnings.includes("hurricane_peak"),
    );
    for (const w of peak) {
      expect(w.rank).toBeGreaterThan(first.rank);
      expect(w.headlinerWait).toBe(first.headlinerWait);
      expect(w.penalties?.hurricane).toBeGreaterThan(0);
      expect(w.warnings).toContain("hurricane_season");
    }
  });

  it("prefers a forecast window over a last-year one when their figures are equal", () => {
    // Out of the hurricane season; the first 20 days read last year.
    const result = run(
      flat("2027-01-04", 40, (i) => (i < 20 ? "last_year" : "forecast")),
      2,
    );
    expect(result.windows[0].basis).toBe("forecast");
    expect(result.windows[0].headlinerWait).toBe(40);
  });
});
