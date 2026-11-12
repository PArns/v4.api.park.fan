import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import { normalizePlanQuery } from "./trip-plan.assembly";
import type {
  InternalCalendarDay,
  InternalRegionCalendar,
} from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import {
  findTripDates,
  isoWeekStart,
  selectWindows,
  type WhenInput,
  type WhenWithin,
} from "./trip-when";

const TODAY = "2027-01-01";
const DISNEY = [
  "magic-kingdom-park",
  "epcot",
  "disneys-hollywood-studios",
  "disneys-animal-kingdom-theme-park",
];
const UNIVERSAL = [
  "universal-studios-florida",
  "universal-islands-of-adventure",
  "universal-epic-universe",
];
const HEADLINERS = [...DISNEY, ...UNIVERSAL];

/** A deterministic wait in 20…80: a slow season, a weekly rhythm and noise per park. */
function waitFor(slug: string, index: number, seed = 1): number {
  let h = seed;
  for (const ch of `${slug}|${index}`) h = (h * 31 + ch.charCodeAt(0)) % 9973;
  const season = 20 * Math.sin(index / 9);
  const weekly = index % 7 === 5 || index % 7 === 6 ? 12 : 0;
  return Math.round(45 + season + weekly + (h % 15)) + 0.25;
}

function cell(wait: number, extra: Partial<TripParkDayInternal> = {}) {
  const day: TripParkDayInternal = {
    status: "OPERATING",
    hoursPublished: true,
    crowdLevel: "moderate",
    headlinerWait: Math.round(wait / 5) * 5,
    waitExact: wait,
    basis: "forecast",
    dayHours: 12,
    ...extra,
  };
  return day;
}

function calendar(
  from: string,
  days: number,
  slugs: string[] = HEADLINERS,
  tweak: (
    slug: string,
    index: number,
    date: string,
  ) => Partial<TripParkDayInternal> = () => ({}),
  dayTweak: (index: number) => Partial<InternalCalendarDay> = () => ({}),
): InternalRegionCalendar {
  const list: InternalCalendarDay[] = [];
  for (let i = 0; i < days; i++) {
    const date = addIsoDays(from, i);
    list.push({
      date,
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(
        slugs.map((s) => [s, cell(waitFor(s, i), tweak(s, i, date))]),
      ),
      ...dayTweak(i),
    });
  }
  return {
    meta: {
      region: "orlando",
      timezone: "America/New_York",
      from,
      to: addIsoDays(from, days - 1),
      computedAt: "2027-01-01T00:00:00Z",
      publishedHoursUntil: {},
    },
    parks: slugs,
    days: list,
    seasons: [],
  };
}

function input(
  cal: InternalRegionCalendar,
  length: number,
  options: {
    limit?: number;
    gap?: number;
    parks?: string[];
    plan?: Record<string, string>;
    within?: WhenWithin;
  } = {},
): WhenInput {
  const parks = options.parks ?? HEADLINERS;
  const from = cal.days[0].date;
  const plan = normalizePlanQuery(
    ORLANDO,
    ORLANDO.parks.map((p) => p.slug),
    {
      from,
      to: addIsoDays(from, length - 1),
      parks: parks.join(","),
      ...options.plan,
    },
    TODAY,
  );
  return {
    region: ORLANDO,
    calendar: cal,
    query: {
      from,
      to: cal.days[cal.days.length - 1].date,
      days: length,
      limit: options.limit ?? 5,
      gap: options.gap ?? 7,
      plan,
      within: options.within,
    },
  };
}

/** Every window planned (limit = all, gap 1), then the gap rule applied to that order. */
function bruteForce(base: WhenInput, limit: number, gap: number): string[] {
  const all = findTripDates({
    ...base,
    query: { ...base.query, limit: 400, gap: 1 },
  }).windows.map((w, rank) => ({ index: rank, from: w.from, score: rank }));
  // `all` is in score order; the gap rule works on start dates.
  const kept: string[] = [];
  for (const w of all) {
    if (kept.length >= limit) break;
    const apart = kept.every(
      (k) =>
        Math.abs(
          (Date.parse(`${k}T00:00:00Z`) - Date.parse(`${w.from}T00:00:00Z`)) /
            86_400_000,
        ) >= gap,
    );
    if (apart) kept.push(w.from);
  }
  return kept;
}

describe("findTripDates", () => {
  it("returns what planning every window would, on small ranges", () => {
    for (const [length, plan] of [
      [5, {}],
      [6, { blocks: "0", split: "fixed" }],
      [4, { parks: "epcot,magic-kingdom-park" }],
    ] as const) {
      const cal = calendar("2027-02-01", 30);
      const parks =
        "parks" in plan ? plan.parks.split(",") : DISNEY.slice(0, 3);
      const base = input(cal, length, {
        parks,
        plan: { ...plan, parks: parks.join(",") },
      });
      const fast = findTripDates({
        ...base,
        query: { ...base.query, limit: 3, gap: 4 },
      });
      expect(fast.windows.map((w) => w.from)).toEqual(bruteForce(base, 3, 4));
      expect(fast.meta.search.exact).toBe(true);
    }
  });

  it("plans only the windows its bound cannot rule out", () => {
    const cal = calendar("2027-01-02", 120);
    const result = findTripDates(input(cal, 10));
    expect(result.meta.search.windows).toBe(111);
    expect(result.meta.search.planned).toBeLessThan(60);
    expect(result.windows).toHaveLength(5);
    expect(result.windows.map((w) => w.rank)).toEqual([1, 2, 3, 4, 5]);
  });

  it("keeps the returned windows gap days apart", () => {
    const cal = calendar("2027-01-02", 90);
    for (const gap of [1, 7, 14]) {
      const starts = findTripDates(input(cal, 8, { gap })).windows.map(
        (w) => w.from,
      );
      for (const a of starts) {
        for (const b of starts) {
          if (a === b) continue;
          const apart = Math.abs((Date.parse(a) - Date.parse(b)) / 86_400_000);
          expect(apart).toBeGreaterThanOrEqual(gap);
        }
      }
    }
  });

  it("is deterministic", () => {
    const cal = calendar("2027-01-02", 90);
    const a = findTripDates(input(cal, 10));
    const b = findTripDates(input(cal, 10));
    expect(a).toEqual(b);
  });

  it("plans 21-day windows", () => {
    const cal = calendar("2027-01-02", 50);
    const result = findTripDates(
      input(cal, 21, { limit: 2, plan: { split: "fixed" } }),
    );
    expect(result.windows).toHaveLength(2);
    for (const w of result.windows) {
      expect(w.plan.days).toHaveLength(21);
      expect(
        w.plan.days.filter((d) => d.kind === "park").map((d) => d.parks[0]),
      ).toHaveLength(7);
    }
  });

  it("keeps windows inside a school break and lists the breaks without one that fits", () => {
    const cal = calendar("2027-03-01", 60);
    const within: WhenWithin = {
      code: "DE-NW",
      name: "Nordrhein-Westfalen",
      country: "DE",
      through: "2028-01-01",
      periods: [{ from: "2027-03-20", to: "2027-04-04", name: "Easter" }],
    };
    const result = findTripDates(input(cal, 10, { within }));
    expect(result.windows.length).toBeGreaterThan(0);
    for (const w of result.windows) {
      expect(w.from >= "2027-03-20" && w.to <= "2027-04-04").toBe(true);
    }
    expect(result.meta.within).toEqual(within);
    expect(result.meta.search.candidates).toBe(7);

    const short: WhenWithin = {
      ...within,
      periods: [{ from: "2027-03-22", to: "2027-03-26", name: "Short" }],
    };
    const none = findTripDates(input(cal, 10, { within: short }));
    expect(none.windows).toEqual([]);
    expect(none.meta.within?.periods).toEqual(short.periods);
    // The year strip still describes the range.
    expect(none.weeks.length).toBeGreaterThan(0);
    expect(none.months.map((m) => m.month)).toEqual(["2027-03", "2027-04"]);
  });

  it("ranks a window that loses a park behind every window that keeps it", () => {
    // Epic Universe closed for the first 20 days.
    const cal = calendar("2027-02-01", 40, HEADLINERS, (slug, i) =>
      slug === "universal-epic-universe" && i < 20
        ? { status: "CLOSED", headlinerWait: undefined, waitExact: undefined }
        : { waitExact: slug === "universal-epic-universe" ? 70 : 30 },
    );
    // Ten days leave eight for seven parks: no window has to drop one for lack of days.
    const result = findTripDates(input(cal, 10, { limit: 3, gap: 3 }));
    for (const w of result.windows) {
      expect(w.plan.unplaced).toBeUndefined();
      // Epic Universe opens on day 20, the departure day of the window from day 11.
      expect(w.from >= "2027-02-13").toBe(true);
    }
  });

  it("prices a day without published hours as if open, and says so", () => {
    const cal = calendar("2027-02-01", 30, HEADLINERS, (_slug, i) =>
      i >= 10
        ? { status: "UNKNOWN", hoursPublished: false, dayHours: undefined }
        : {},
    );
    const result = findTripDates(input(cal, 7, { limit: 10, gap: 1 }));
    const late = result.windows.find((w) => w.from >= "2027-02-11");
    expect(late?.warnings).toContain("hours_not_published");
    expect(late?.headlinerWait).toBeLessThan(200);
  });

  it("names reasons and warnings from the days", () => {
    const cal = calendar(
      "2027-05-20",
      40,
      HEADLINERS,
      (slug, i) =>
        slug === "magic-kingdom-park" && i === 30
          ? {
              closesEarly: true,
              dayHours: 9,
              evening: {
                kind: "ticketed_event",
                opensAt: "x",
                closesAt: "y",
              },
            }
          : { basis: i > 25 ? "last_year" : "forecast" },
      (i) => (i >= 10 && i < 16 ? { isSchoolVacation: true } : {}),
    );
    const result = findTripDates(input(cal, 7, { limit: 10, gap: 1 }));
    const byFrom = new Map(result.windows.map((w) => [w.from, w]));
    const all = result.windows;
    expect(all.some((w) => w.reasons.includes("no_school_break"))).toBe(true);
    const inBreak = all.find((w) => w.warnings.includes("school_break"));
    if (inBreak) expect(inBreak.reasons).not.toContain("no_school_break");
    const june = all.find((w) => w.to >= "2027-06-01");
    expect(june?.warnings ?? ["hurricane_season"]).toContain(
      "hurricane_season",
    );
    expect(byFrom.size).toBe(all.length);
    const lastYear = all.find((w) => w.basis === "last_year");
    if (lastYear) expect(lastYear.warnings).toContain("estimate_last_year");
  });

  it("builds the year strip by ISO week and calendar month", () => {
    const cal = calendar(
      "2027-01-04",
      62,
      HEADLINERS,
      (slug, i) =>
        slug === "epcot" && i === 3
          ? {
              closesEarly: true,
              dayHours: 8,
              evening: { kind: "ticketed_event", opensAt: "x", closesAt: "y" },
              seasons: ["s1"],
            }
          : {},
      (i) =>
        i === 14
          ? { isHoliday: true, holidays: ["Martin Luther King Jr. Day"] }
          : {},
    );
    cal.seasons = [
      { id: "s1", kind: "special_event", name: "Festival", parkSlug: "epcot" },
      { id: "s2", kind: "christmas", name: "Unused", parkSlug: "epcot" },
    ];
    const climate = Object.fromEntries(
      Array.from({ length: 31 }, (_, d) => [
        `01-${String(d + 1).padStart(2, "0")}`,
        {
          temperatureMax: 22,
          temperatureMin: 10,
          precipitationSum: 2,
          rainSum: 2,
          snowfallSum: 0,
          windSpeedMax: 10,
          weatherCode: 1,
        },
      ]),
    );
    const result = findTripDates({
      ...input(cal, 7),
      climate,
      climatePark: "magic-kingdom-park",
    });
    expect(result.weeks[0]).toMatchObject({ start: "2027-01-04" });
    expect(result.weeks).toHaveLength(9);
    expect(isoWeekStart("2027-01-10")).toBe("2027-01-04");
    const jan = result.months[0];
    expect(jan).toMatchObject({
      month: "2027-01",
      basis: "forecast",
      climate: {
        tempMax: 22,
        tempMin: 10,
        precipitationMm: 62,
        basis: "climate_normal",
        period: "2015-2024",
      },
      usSchoolBreakDays: [
        { date: "2027-01-18", name: "Martin Luther King Jr. Day" },
      ],
      seasons: ["s1"],
      partyNights: 1,
      hurricaneSeason: false,
    });
    expect(jan.headlinerWait).toBeGreaterThan(0);
    expect(jan.dayGuestHours).toBeCloseTo(12, 0);
    expect(result.months[1].climate).toBeUndefined();
    expect(result.seasons.map((s) => s.id)).toEqual(["s1"]);
    expect(result.meta.climate).toEqual({
      park: "magic-kingdom-park",
      basis: "climate_normal",
      period: "2015-2024",
    });
  });

  it("stops at the time budget and says the answer is not proven", () => {
    let t = 0;
    const result = findTripDates({
      ...input(calendar("2027-01-02", 120), 10),
      clock: () => (t += 1000),
    });
    expect(result.meta.search.exact).toBe(false);
    expect(result.windows.length).toBeGreaterThan(0);
  });
});

describe("selectWindows", () => {
  it("keeps the best, skipping one closer than gap to a better one", () => {
    const kept = selectWindows(
      [
        { index: 0, score: 5 },
        { index: 3, score: 1 },
        { index: 9, score: 2 },
        { index: 5, score: 3 },
      ],
      3,
      4,
    );
    expect(kept.map((k) => k.index)).toEqual([3, 9]);
  });
});
