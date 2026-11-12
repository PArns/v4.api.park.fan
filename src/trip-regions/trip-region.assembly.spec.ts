import { ORLANDO } from "./regions";
import type { TripParkDayInternal } from "./trip-calendar.util";
import {
  assembleRegionCalendar,
  bestOfDay,
  publicCalendar,
  type ParkCalendarSource,
  type RegionCalendarDayLike,
  type RegionParkRecord,
} from "./trip-region.assembly";
import fixture from "./__fixtures__/disney-2026-11-20-30.json";

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
    ...over,
  };
}

describe("bestOfDay", () => {
  it("picks per resort the lowest unrounded wait on full days, never a day trip", () => {
    const { best, why } = bestOfDay(
      ORLANDO,
      {
        "magic-kingdom-park": cell(40, 41.2),
        epcot: cell(40, 38.9),
        "universal-studios-florida": cell(30, 30),
        "legoland-florida": cell(10, 10),
      },
      new Map(),
    );
    expect(best).toEqual({
      "walt-disney-world": "epcot",
      "universal-orlando": "universal-studios-florida",
    });
    expect(why).toEqual({
      "walt-disney-world": "shortest_wait",
      "universal-orlando": "shortest_wait",
    });
  });

  it("prices a short day up, so a park closing early loses to a fuller one", () => {
    // MK: 40 over 9 h against a usual 13 h → 40 × 13/9 = 57.8; AK 45 over a full day.
    const { best, why } = bestOfDay(
      ORLANDO,
      {
        "magic-kingdom-park": cell(40, 40, { dayHours: 9 }),
        "disneys-animal-kingdom-theme-park": cell(45, 45, { dayHours: 12 }),
      },
      new Map([
        ["magic-kingdom-park", 13],
        ["disneys-animal-kingdom-theme-park", 12],
      ]),
    );
    expect(best["walt-disney-world"]).toBe("disneys-animal-kingdom-theme-park");
    expect(why["walt-disney-world"]).toBe("closes_early_elsewhere");
  });

  it("breaks a tie on the longer day, then the lower level", () => {
    expect(
      bestOfDay(
        ORLANDO,
        {
          "magic-kingdom-park": cell(40, 40, { dayHours: 11 }),
          epcot: cell(40, 40, { dayHours: 12 }),
        },
        new Map(),
      ).best["walt-disney-world"],
    ).toBe("epcot");
    expect(
      bestOfDay(
        ORLANDO,
        {
          "magic-kingdom-park": cell(40, 40, { crowdLevel: "low" }),
          epcot: cell(40, 40, { crowdLevel: "high" }),
        },
        new Map(),
      ).best["walt-disney-world"],
    ).toBe("magic-kingdom-park");
  });

  it("leaves a resort out on last-year days and where no park has a figure", () => {
    expect(
      bestOfDay(
        ORLANDO,
        {
          "magic-kingdom-park": cell(40, 40, { basis: "last_year" }),
          epcot: cell(40, 40, { status: "UNKNOWN" }),
          "universal-studios-florida": {
            status: "OPERATING",
            hoursPublished: true,
            crowdLevel: "unknown",
            basis: "none",
          },
        },
        new Map(),
      ).best,
    ).toEqual({});
  });
});

describe("assembleRegionCalendar on production's answers (fetched 2026-11-12)", () => {
  type FixturePark = {
    calendar: ParkCalendarSource["calendar"];
    schedule: ParkCalendarSource["rows"];
  };
  const parks = fixture.parks as unknown as Record<string, FixturePark>;

  const calendar = assembleRegionCalendar({
    region: ORLANDO,
    from: "2026-11-20",
    to: "2026-11-30",
    today: "2026-11-12",
    computedAt: "t",
    sources: Object.entries(parks).map(([slug, data]) => {
      const config = ORLANDO.parks.find((p) => p.slug === slug)!;
      const park: RegionParkRecord = {
        config,
        id: `id-${slug}`,
        slug,
        name: slug,
        path: `/parks/x/${slug}`,
        latitude: 0,
        longitude: 0,
        timezone: "America/New_York",
      };
      return {
        park,
        calendar: {
          meta: data.calendar.meta,
          days: data.calendar.days as RegionCalendarDayLike[],
        },
        rows: data.schedule,
        lastYear: new Map(),
      };
    }),
    seasons: [],
    forecastUntil: [],
  });
  const day24 = calendar.days.find((d) => d.date === "2026-11-24")!;

  it("does not pick Magic Kingdom's party night (40 min, closes 18:00) over Animal Kingdom's full day (45 min)", () => {
    expect(day24.parks["magic-kingdom-park"]).toMatchObject({
      headlinerWait: 40,
      closesAt: "2026-11-24T18:00:00-05:00",
      closesEarly: true,
      dayGuestCloseAt: "2026-11-24T18:00:00-05:00",
    });
    expect(day24.parks["disneys-animal-kingdom-theme-park"]).toMatchObject({
      headlinerWait: 45,
    });
    expect(day24.best?.["walt-disney-world"]).toBe(
      "disneys-animal-kingdom-theme-park",
    );
    expect(day24.bestWhy?.["walt-disney-world"]).toBe("closes_early_elsewhere");
  });

  it("serves closesEarly and bestWhy on the public calendar", () => {
    const pub = publicCalendar(calendar).days.find(
      (d) => d.date === "2026-11-24",
    )!;
    expect(pub.parks["magic-kingdom-park"]).toMatchObject({
      closesEarly: true,
    });
    expect(pub.parks["magic-kingdom-park"]).not.toHaveProperty("dayHours");
    expect(pub.bestWhy).toBeDefined();
  });
});
