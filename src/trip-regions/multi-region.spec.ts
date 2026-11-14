import { addIsoDays } from "../common/utils/date.util";
import {
  TEST_PARIS,
  TEST_TOKYO,
  TEST_TOKYO_TICKETS,
} from "./__fixtures__/test-regions";
import { ORLANDO, type RegionConfig } from "./regions";
import { withCatalog } from "./ticket-catalog";
import { validateTicketProduct } from "./ticket-validation";
import { buildLegs, travelSource } from "./travel";
import { regionBestDays } from "./trip-best-days";
import {
  dayCostParts,
  heatOf,
  penaltiesOf,
  stormWarning,
} from "./trip-optimizer";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "./trip-plan.assembly";
import {
  describeRegion,
  summarizeRegion,
  type InternalRegionCalendar,
  type RegionParkRecord,
} from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { findTripDates } from "./trip-when";
import {
  resolveWithin,
  schoolBreakRegions,
  SCHOOL_BREAK_REGIONS,
} from "./trip-school-breaks";

/**
 * What a region beyond Orlando brings, on two small regions
 * (`__fixtures__/test-regions.ts`): its own hotel kinds, perks and early-slot
 * names, its storm and rainy seasons and heat thresholds, curated travel,
 * ticket markets and an evening ticket, and the list route. Orlando's own
 * answers are pinned by `orlando-identity.spec.ts`.
 */

const TODAY = "2027-01-04";

/** Every park of a region open every day with a wait that differs by park and day. */
function calendarOf(
  region: RegionConfig,
  from: string,
  days: number,
  tweak: (slug: string, date: string, cell: TripParkDayInternal) => void = () =>
    undefined,
  holidays: string[] = [],
): InternalRegionCalendar {
  const out: InternalRegionCalendar["days"] = [];
  for (let i = 0; i < days; i++) {
    const date = addIsoDays(from, i);
    const parks: Record<string, TripParkDayInternal> = {};
    region.parks.forEach((p, k) => {
      const wait = 30 + ((k * 7 + i * 11) % 25);
      const cell: TripParkDayInternal = {
        status: "OPERATING",
        hoursPublished: true,
        crowdLevel: "moderate",
        headlinerWait: Math.round(wait / 5) * 5,
        waitExact: wait,
        basis: "forecast",
        dayHours: 12,
        usualHours: 12,
        opensAt: `${date}T09:00:00+09:00`,
        closesAt: `${date}T21:00:00+09:00`,
        early: {
          kind: region.groups.find((g) => g.key === p.group)?.earlyKind ?? "",
          opensAt: `${date}T08:30:00+09:00`,
          closesAt: `${date}T09:00:00+09:00`,
        },
      };
      tweak(p.slug, date, cell);
      parks[p.slug] = cell;
    });
    out.push({
      date,
      isHoliday: holidays.includes(date),
      isSchoolVacation: false,
      parks,
    });
  }
  return {
    meta: {
      region: region.slug,
      timezone: region.timezone,
      from,
      to: addIsoDays(from, days - 1),
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: region.parks.map((p) => p.slug),
    days: out,
    seasons: [],
  };
}

const known = (region: RegionConfig) => region.parks.map((p) => p.slug);

function plan(
  region: RegionConfig,
  query: Record<string, string>,
  calendar?: InternalRegionCalendar,
) {
  const q = normalizePlanQuery(region, known(region), query as never, TODAY);
  const cal =
    calendar ??
    calendarOf(
      region,
      q.from,
      Math.round(
        (Date.parse(`${q.to}T00:00:00Z`) - Date.parse(`${q.from}T00:00:00Z`)) /
          86_400_000,
      ) + 1,
    );
  const trip = optimizePlan(region, q, cal);
  return planResponse(region, q, cal, trip, {
    computedAt: "t",
    today: TODAY,
    leadMae: new Map(),
  });
}

const records = (region: RegionConfig): RegionParkRecord[] =>
  region.parks.map((config, i) => ({
    config,
    id: `id-${i}`,
    slug: config.slug,
    name: config.slug,
    path: `/parks/${config.continent}/${config.country}/${config.city}/${config.slug}`,
    latitude: 0,
    longitude: 0,
    timezone: region.timezone,
  }));

describe("a region's own vocabulary", () => {
  it("validates hotel= against the region's hotel kinds", () => {
    expect(() =>
      normalizePlanQuery(
        TEST_TOKYO,
        known(TEST_TOKYO),
        { from: "2027-02-01", to: "2027-02-05", hotel: "disney" },
        TODAY,
      ),
    ).toThrow("hotel must be one of tdr_hotel, offsite");
    expect(() =>
      normalizePlanQuery(
        ORLANDO,
        known(ORLANDO),
        {
          from: "2027-02-01",
          to: "2027-02-05",
          parks: "epcot",
          hotel: "tdr_hotel",
        },
        TODAY,
      ),
    ).toThrow(
      "hotel must be one of disney, disney_deluxe, disney_springs, universal, universal_premier, offsite",
    );
    // An area's hotel is the default.
    const q = normalizePlanQuery(
      TEST_TOKYO,
      known(TEST_TOKYO),
      { from: "2027-02-01", to: "2027-02-05", area: "maihama" },
      TODAY,
    );
    expect(q.hotel).toBe("tdr_hotel");
  });

  it("names the region's perk and its early slot on the plan and the best days", () => {
    const answer = plan(TEST_TOKYO, {
      from: "2027-02-01",
      to: "2027-02-05",
      parks: "test-land,test-sea",
      hotel: "tdr_hotel",
      split: "fixed",
    });
    const parkDays = answer.days.filter((d) => d.kind === "park");
    expect(parkDays).toHaveLength(2);
    for (const day of parkDays) {
      expect(day.perks).toEqual(["happy_entry"]);
      expect(day.reasons).toContain("happy_entry");
    }
    const best = regionBestDays(
      TEST_TOKYO,
      calendarOf(TEST_TOKYO, "2027-02-01", 7),
      3,
    );
    expect(best.parks[0].quiet[0].reasons).toContain("happy_entry");
  });

  it("serves the hotel kinds, markets and weather on the region route", () => {
    const region = describeRegion(TEST_TOKYO, records(TEST_TOKYO));
    expect(region.hotels).toEqual([
      { key: "tdr_hotel", perks: ["happy_entry"] },
      { key: "offsite", perks: [] },
    ]);
    expect(region.ticketMarkets).toEqual({ home: "jp", codes: ["jp", "intl"] });
    expect(region.weather).toEqual({
      storm: {
        kind: "typhoon",
        from: "08-01",
        to: "10-31",
        peak: { from: "09-01", to: "09-30" },
      },
      notes: [{ kind: "rainy_season", from: "06-05", to: "07-19" }],
      climatePark: "test-land",
    });
    const orlando = describeRegion(ORLANDO, records(ORLANDO));
    expect(orlando.weather.storm).toEqual({
      kind: "hurricane",
      from: "06-01",
      to: "11-30",
      peak: { from: "08-15", to: "10-15" },
    });
    expect(orlando.hotels.map((h) => h.key)).toEqual([
      "disney",
      "disney_deluxe",
      "disney_springs",
      "universal",
      "universal_premier",
      "offsite",
    ]);
  });
});

describe("weather per region", () => {
  const cell = {
    status: "OPERATING" as const,
    headlinerWait: 40,
    exactWait: 40,
    basis: "forecast" as const,
    dayHours: 12,
  };

  it("prices Japan's typhoon season under its own kind and nothing in Paris", () => {
    const sept = "2027-09-10";
    const tokyo = dayCostParts(cell, {
      type: "theme_park",
      date: sept,
      weather: TEST_TOKYO.weather,
    });
    expect(tokyo.storm).toBe(6);
    expect(tokyo.hurricane).toBe(0);
    expect(stormWarning(TEST_TOKYO.weather.storm, sept)).toBe("typhoon_peak");
    expect(stormWarning(TEST_TOKYO.weather.storm, "2027-08-20")).toBe(
      "typhoon_season",
    );
    expect(stormWarning(TEST_TOKYO.weather.storm, "2027-06-20")).toBe(
      undefined,
    );
    const paris = dayCostParts(cell, {
      type: "theme_park",
      date: sept,
      weather: TEST_PARIS.weather,
    });
    expect(paris).toEqual({ cost: 40, heat: 0, storm: 0, hurricane: 0 });
    expect(penaltiesOf(0, 12, 2, "typhoon")).toEqual({ typhoon: 6 });
  });

  it("reads heat against the region's own thresholds", () => {
    const warm = { tempMax: 33, basis: "forecast" as const };
    expect(heatOf(warm, ORLANDO.weather.heat)).toBeNull();
    expect(heatOf(warm, TEST_PARIS.weather.heat)?.factor).toBeCloseTo(1.08);
  });

  it("warns of the typhoon peak and the rainy season on the plan's days", () => {
    const sept = plan(TEST_TOKYO, {
      from: "2027-09-06",
      to: "2027-09-10",
      parks: "test-land",
      split: "fixed",
    });
    const day = sept.days.find((d) => d.kind === "park");
    expect(day?.warnings).toContain("typhoon_peak");
    expect(day?.warnings).not.toContain("hurricane_peak");
    const june = plan(TEST_TOKYO, {
      from: "2027-06-14",
      to: "2027-06-18",
      parks: "test-land",
      split: "fixed",
    });
    expect(june.days.find((d) => d.kind === "park")?.warnings).toContain(
      "rainy_season",
    );
    const paris = plan(TEST_PARIS, {
      from: "2027-09-06",
      to: "2027-09-10",
      parks: "test-park",
      split: "fixed",
    });
    const parisDay = paris.days.find((d) => d.kind === "park");
    expect(parisDay?.warnings).toEqual([]);
  });

  it("gives /when's windows and months the region's seasons, and its own school breaks", () => {
    const from = "2027-05-03";
    const cal = calendarOf(TEST_TOKYO, from, 180, undefined, ["2027-07-19"]);
    cal.days.forEach((d) => {
      if (d.date >= "2027-07-20" && d.date <= "2027-08-31") {
        d.isSchoolVacation = true;
      }
    });
    const days = 5;
    const result = findTripDates({
      region: TEST_TOKYO,
      calendar: cal,
      query: {
        from,
        to: cal.meta.to,
        days,
        limit: 10,
        gap: 3,
        plan: normalizePlanQuery(
          TEST_TOKYO,
          known(TEST_TOKYO),
          { from, to: addIsoDays(from, days - 1), parks: "test-land" },
          TODAY,
        ),
      },
    });
    const months = new Map(result.months.map((m) => [m.month, m]));
    expect(months.get("2027-09")?.typhoonSeason).toBe(true);
    expect(months.get("2027-05")?.typhoonSeason).toBe(false);
    expect(months.get("2027-06")?.rainySeason).toBe(true);
    expect(months.get("2027-09")?.rainySeason).toBe(false);
    for (const m of result.months) {
      expect(m).not.toHaveProperty("hurricaneSeason");
      expect(m).not.toHaveProperty("usSchoolBreakDays");
    }
    expect(months.get("2027-08")?.localSchoolBreakDays.length).toBe(31);
    for (const w of result.windows) {
      const touches = (lo: string, hi: string) => w.from <= hi && w.to >= lo;
      expect(w.warnings.includes("typhoon_peak")).toBe(
        touches("2027-09-01", "2027-09-30"),
      );
      expect(w.warnings.includes("rainy_season")).toBe(
        touches("2027-06-05", "2027-07-19"),
      );
      expect(w.warnings).not.toContain("hurricane_season");
      if (w.penalties) expect(w.penalties).not.toHaveProperty("hurricane");
    }
    // The typhoon peak ranks behind equal windows outside it.
    const peak = result.windows.find((w) =>
      w.warnings.includes("typhoon_peak"),
    );
    if (peak) expect(peak.penalties?.typhoon).toBeGreaterThan(0);
  });
});

describe("curated travel", () => {
  it("serves a curated region's legs as they are written, with their source", () => {
    expect(travelSource(TEST_TOKYO)).toEqual({
      source: "curated",
      computedOn: "2026-11-13",
      basis: "timetable",
    });
    const legs = buildLegs(TEST_TOKYO);
    expect(legs).toContainEqual({
      from: "test-land",
      to: "test-sea",
      minutes: 15,
      mode: "monorail",
      name: "Disney Resort Line",
      alternatives: [{ mode: "walk", minutes: 20 }],
    });
    expect(legs.find((l) => l.to === "test-fuji")?.mode).toBe("highway_bus");
    const region = describeRegion(TEST_TOKYO, records(TEST_TOKYO));
    expect(region.travel.source).toBe("curated");
    expect(region.travel.basis).toBe("timetable");
  });

  it("marks Orlando's curated legs in its OSRM matrix", () => {
    const legs = buildLegs(ORLANDO);
    const walk = legs.find(
      (l) =>
        [l.from, l.to].sort().join("|") ===
        "universal-islands-of-adventure|universal-studios-florida",
    );
    expect(walk).toMatchObject({ mode: "walk", source: "curated" });
    const drive = legs.find(
      (l) => [l.from, l.to].sort().join("|") === "epcot|magic-kingdom-park",
    );
    expect(drive?.mode).toBe("drive");
    expect(drive).not.toHaveProperty("source");
  });
});

describe("tickets per region", () => {
  const tokyo = withCatalog(TEST_TOKYO, {
    region: TEST_TOKYO.slug,
    version: "t",
    products: TEST_TOKYO_TICKETS,
  });

  const draft = (extra: Record<string, unknown> = {}) => ({
    slug: "test-weeknight",
    name: "Weeknight Passport",
    resort: "tokyo-disney-resort",
    markets: ["jp"],
    parks: ["test-land", "test-sea"],
    days: { min: 1, max: 1 },
    windowTable: [{ days: 1, window: 1 }],
    anchor: "start",
    hopping: "none",
    confidence: "high",
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: "https://www.tokyodisneyresort.jp/en/tdr/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    entryFrom: "17:00",
    validDays: ["weekday"],
    ...extra,
  });

  it("checks markets against the region's and an evening ticket's fields", () => {
    const ok = validateTicketProduct(tokyo, draft(), {
      creating: true,
      slugs: [],
    });
    expect(ok.problems).toEqual([]);
    expect(ok.valid?.product).toMatchObject({
      entryFrom: "17:00",
      validDays: ["weekday"],
    });
    const bad = validateTicketProduct(
      tokyo,
      draft({
        markets: ["us"],
        entryFrom: "5pm",
        validDays: ["sunday"],
      }),
      { creating: true, slugs: [] },
    );
    expect(bad.problems.map((p) => p.path).sort()).toEqual([
      "entryFrom",
      "markets[0]",
      "validDays[0]",
    ]);
    const suggested = validateTicketProduct(tokyo, draft({ suggestRank: 2 }), {
      creating: true,
      slugs: [],
    });
    expect(suggested.problems.map((p) => p.path)).toEqual(["suggestRank"]);
  });

  it("plans an owned evening ticket on its kind of day, from its entry time", () => {
    // 2027-02-01 is a Monday; 6 and 7 February a weekend.
    const answer = plan(tokyo, {
      from: "2027-02-01",
      to: "2027-02-08",
      parks: "test-sea",
      tickets: "test-early-evening@2027-02-06",
      split: "fixed",
    });
    const day = answer.days.find((d) => d.kind === "park");
    expect(day?.date).toBe("2027-02-06");
    expect(day?.parks).toEqual([{ slug: "test-sea", part: "evening" }]);
    expect(day?.entryFrom).toBe("15:00");
    expect(day?.ticket).toBe("t1");
    // Without a date, any weekend day or holiday of the trip; never a weekday.
    const free = plan(tokyo, {
      from: "2027-02-01",
      to: "2027-02-05",
      parks: "test-sea",
      tickets: "test-early-evening",
      split: "fixed",
    });
    expect(free.unplaced).toEqual([
      { slug: "test-sea", reason: "no_ticket_day" },
    ]);
  });

  it("suggests whole-day tickets of any market, never an evening ticket", () => {
    const query = {
      from: "2027-02-01",
      to: "2027-02-05",
      parks: "test-land",
      split: "fixed",
    };
    const suggested = (answer: ReturnType<typeof plan>) =>
      (
        answer.meta as { ticketSuggestion?: Array<{ product: string }> }
      ).ticketSuggestion?.map((s) => s.product);
    expect(suggested(plan(tokyo, query))).toEqual(["test-1-day"]);
    // Sold only abroad, it is suggested all the same.
    const abroad = withCatalog(TEST_TOKYO, {
      region: TEST_TOKYO.slug,
      version: "abroad",
      products: TEST_TOKYO_TICKETS.map((p) =>
        p.id === "test-1-day" ? { ...p, markets: ["intl"] } : p,
      ),
    });
    expect(suggested(plan(abroad, query))).toEqual(["test-1-day"]);
  });
});

describe("the region list", () => {
  it("summarizes a region for the navigation", () => {
    expect(summarizeRegion(TEST_TOKYO, records(TEST_TOKYO), 21)).toEqual({
      slug: "test-tokyo",
      name: "Test Tokyo",
      timezone: "Asia/Tokyo",
      parkCount: 3,
      maxTripDays: 21,
      groups: [
        {
          key: "tokyo-disney-resort",
          name: "Tokyo Disney Resort",
          kind: "resort",
        },
        { key: "day-trips", name: "Day trips", kind: "day_trip" },
      ],
    });
  });
});

describe("school breaks of the six locales' markets", () => {
  it("offers Spain and Italy by their stored codes", () => {
    expect(Object.keys(SCHOOL_BREAK_REGIONS)).toEqual(
      expect.arrayContaining(["DE", "NL", "GB", "BE", "FR", "AT", "ES", "IT"]),
    );
    const rows = [
      {
        date: "2027-04-01",
        name: "Easter holidays",
        country: "ES",
        region: "ES-MD",
        holidayType: "school",
        isNationwide: false,
      },
      {
        date: "2027-04-01",
        name: "Easter holidays",
        country: "IT",
        region: "IT-LO",
        holidayType: "school",
        isNationwide: false,
      },
    ];
    const coverage = {
      regional: new Map([
        ["ES|MD", "2027-06-20"],
        ["IT|LO", "2027-06-10"],
      ]),
      national: new Map<string, string>(),
      countryAny: new Map([
        ["ES", "2027-06-20"],
        ["IT", "2027-06-10"],
      ]),
    };
    const breaks = schoolBreakRegions(
      ["ES", "IT"],
      new Map([
        ["ES", rows.filter((r) => r.country === "ES")],
        ["IT", rows.filter((r) => r.country === "IT")],
      ]),
      coverage as never,
      "2027-01-01",
      "2027-12-31",
    );
    const madrid = resolveWithin("ES-MD", breaks);
    expect(madrid.name).toBe("Madrid");
    expect(madrid.periods[0]).toMatchObject({ name: "Easter holidays" });
    expect(madrid.through).toBe("2027-06-20");
    expect(resolveWithin("IT-LO", breaks).name).toBe("Lombardia");
    expect(breaks.regions.find((r) => r.code === "ES-AN-SE")?.name).toBe(
      "Andalucía (Sevilla)",
    );
  });
});
