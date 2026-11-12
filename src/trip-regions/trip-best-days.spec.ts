import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import { regionBestDays } from "./trip-best-days";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { bestOfDay, type InternalRegionCalendar } from "./trip-region.assembly";

const START = "2026-12-07"; // a Monday

function cell(
  wait: number,
  exact: number,
  over: Partial<TripParkDayInternal> = {},
): TripParkDayInternal {
  return {
    status: "OPERATING",
    hoursPublished: true,
    crowdLevel: "moderate",
    headlinerWait: wait,
    waitExact: exact,
    basis: "forecast",
    dayHours: 12,
    usualHours: 12,
    ...over,
  };
}

/** A region calendar of `n` days; `parks[slug](i)` gives each day. */
function calendar(
  n: number,
  parks: Record<string, (i: number) => TripParkDayInternal>,
  flags: (i: number) => {
    isHoliday?: boolean;
    isSchoolVacation?: boolean;
  } = () => ({}),
): InternalRegionCalendar {
  const bare = Array.from({ length: n }, (_, i) => ({
    date: addIsoDays(START, i),
    isHoliday: flags(i).isHoliday ?? false,
    isSchoolVacation: flags(i).isSchoolVacation ?? false,
    parks: Object.fromEntries(
      Object.entries(parks).map(([slug, f]) => [slug, f(i)]),
    ),
  }));
  const days = bare.map((d) => {
    const { best, why } = bestOfDay(ORLANDO, d.parks);
    return { ...d, best, bestWhy: why };
  });
  return {
    meta: {
      region: "orlando",
      timezone: "America/New_York",
      from: days[0].date,
      to: days[n - 1].date,
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: Object.keys(parks),
    days,
    seasons: [],
  };
}

describe("regionBestDays", () => {
  it("ranks on the unrounded wait, puts party nights behind full days, names the busiest", () => {
    // Days 0–6. Day 2 rounds to 30 like day 3 but is quieter unrounded; day 1
    // is the quietest of all but a party night that sends day guests home early.
    const exact = [44, 25, 29.4, 30.6, 50, 61, 70];
    const cal = calendar(7, {
      "magic-kingdom-park": (i) =>
        cell(Math.round(exact[i] / 5) * 5, exact[i], {
          ...(i === 1 && {
            dayHours: 9,
            evening: {
              kind: "ticketed_event",
              opensAt: "x",
              closesAt: "y",
            },
          }),
        }),
    });
    const result = regionBestDays(ORLANDO, cal, 3);
    const mk = result.parks[0];
    expect(mk.quiet.map((d) => d.date)).toEqual([
      addIsoDays(START, 2),
      addIsoDays(START, 3),
      addIsoDays(START, 0),
    ]);
    expect(mk.quiet[0].reasons).toEqual(["weekday", "quietest_in_window"]);
    expect(mk.busy.map((d) => d.date)).toEqual([
      addIsoDays(START, 6),
      addIsoDays(START, 5),
    ]);
    expect(mk.basis).toBe("forecast");
    expect(mk.flat).toBeUndefined();
    const withParty = regionBestDays(ORLANDO, cal, 7).parks[0].quiet;
    expect(withParty[withParty.length - 1]).toMatchObject({
      date: addIsoDays(START, 1),
      reasons: expect.arrayContaining(["party_night_short_hours"]),
    });
  });

  it("calls a window flat when its rounded waits spread less than ten minutes", () => {
    const cal = calendar(4, {
      epcot: (i) => cell([40, 45, 45, 40][i], [40, 44, 46, 41][i]),
    });
    expect(regionBestDays(ORLANDO, cal, 5).parks[0].flat).toEqual({
      min: 40,
      max: 45,
    });
  });

  it("keeps last-year days with their basis and reasons, and the majority basis", () => {
    const cal = calendar(
      3,
      {
        epcot: (i) =>
          cell(40 + i * 5, 40 + i * 5, {
            basis: i === 0 ? "forecast" : "last_year",
            status: i === 0 ? "OPERATING" : "UNKNOWN",
          }),
      },
      (i) => ({ isSchoolVacation: i === 0 }),
    );
    const epcot = regionBestDays(ORLANDO, cal, 5).parks[0];
    expect(epcot.basis).toBe("last_year");
    expect(epcot.quiet[1].reasons).toEqual(
      expect.arrayContaining(["estimate_last_year", "holiday_ends"]),
    );
    expect(epcot.quiet[0].reasons).toContain("school_break");
  });

  it("names per resort the three dates with the quietest best park", () => {
    const cal = calendar(5, {
      "magic-kingdom-park": (i) => cell(40, [50, 30, 45, 35, 60][i]),
      epcot: (i) => cell(40, [40, 70, 32, 70, 70][i]),
      "universal-studios-florida": (i) => cell(30, 30 + i),
    });
    const groups = regionBestDays(ORLANDO, cal, 5).groups;
    expect(groups.map((g) => g.key)).toEqual([
      "walt-disney-world",
      "universal-orlando",
      "seaworld-orlando",
    ]);
    expect(groups[0].quiet).toEqual([
      {
        date: addIsoDays(START, 1),
        slug: "magic-kingdom-park",
        headlinerWait: 40,
      },
      { date: addIsoDays(START, 2), slug: "epcot", headlinerWait: 40 },
      {
        date: addIsoDays(START, 3),
        slug: "magic-kingdom-park",
        headlinerWait: 40,
      },
    ]);
    expect(groups[2].quiet).toEqual([]);
  });

  it("ranks days whose hours are not published on their forecast", () => {
    const cal = calendar(5, {
      "magic-kingdom-park": (i) =>
        cell(40 + i * 5, 40 + i * 5, {
          status: "UNKNOWN",
          hoursPublished: false,
          basis: "long_range",
          dayHours: undefined,
        }),
      "disneys-animal-kingdom-theme-park": (i) =>
        cell(35 + i * 5, 35 + i * 5, {
          status: "UNKNOWN",
          hoursPublished: false,
          basis: "long_range",
          dayHours: undefined,
        }),
    });
    const result = regionBestDays(ORLANDO, cal, 3);
    expect(result.parks[0].quiet.map((d) => d.headlinerWait)).toEqual([
      40, 45, 50,
    ]);
    expect(
      result.groups.find((g) => g.key === "walt-disney-world")?.quiet,
    ).toHaveLength(3);
    expect(cal.days[0].best?.["walt-disney-world"]).toBe(
      "disneys-animal-kingdom-theme-park",
    );
  });
});
