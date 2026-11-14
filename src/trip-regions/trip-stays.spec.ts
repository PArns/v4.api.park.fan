import { addIsoDays } from "../common/utils/date.util";
import { TEST_JAPAN, TEST_PARIS } from "./__fixtures__/test-regions";
import { ORLANDO, stayRulesOf, type RegionConfig } from "./regions";
import {
  normalizePlanQuery,
  optimizePlan,
  planCacheQuery,
  planResponse,
  staysOption,
} from "./trip-plan.assembly";
import {
  describeRegion,
  type InternalRegionCalendar,
  type RegionParkRecord,
} from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { findTripDates } from "./trip-when";

/**
 * A trip across several cities (docs/frontend/trip-regions.md#bases-a-trip-across-several-cities)
 * on a four-city test region by train: the stays the plan picks or the
 * visitor fixes, moves as a morning, an evening or a whole day, long day
 * trips, the trip's ends, the hotel's perks after the first night, the
 * nights suggestion, `/when`, the region route and the 400s.
 */

const TODAY = "2027-01-04";
type Tweak = (slug: string, i: number, cell: TripParkDayInternal) => void;

function calendarOf(
  region: RegionConfig,
  from: string,
  days: number,
  tweak: Tweak = () => undefined,
): InternalRegionCalendar {
  const out: InternalRegionCalendar["days"] = [];
  for (let i = 0; i < days; i++) {
    const date = addIsoDays(from, i);
    const parks: Record<string, TripParkDayInternal> = {};
    region.parks.forEach((p, k) => {
      const wait = 30 + ((k * 13 + i * 7) % 25);
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
      };
      tweak(p.slug, i, cell);
      parks[p.slug] = cell;
    });
    out.push({ date, isHoliday: false, isSchoolVacation: false, parks });
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

const daysOf = (from: string, to: string) =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  ) + 1;

function plan(
  query: Record<string, string>,
  tweak?: Tweak,
  region: RegionConfig = TEST_JAPAN,
) {
  const q = normalizePlanQuery(region, known(region), query as never, TODAY);
  const calendar = calendarOf(region, q.from, daysOf(q.from, q.to), tweak);
  const trip = optimizePlan(region, q, calendar);
  return planResponse(region, q, calendar, trip, {
    computedAt: "t",
    today: TODAY,
    leadMae: new Map(),
  }) as unknown as {
    meta: Record<string, unknown> & {
      stays?: Array<Record<string, unknown>>;
      exact: boolean;
    };
    days: Array<Record<string, unknown> & { parks: Array<{ slug: string }> }>;
    totals: Record<string, number>;
    unplaced: Array<{ slug: string; reason: string }>;
    ignoredLocks?: Array<{ lock: string; reason: string }>;
  };
}

const ALL = "j-land,j-sea,j-fuji,j-lego,j-usj";
const TRIP = { from: "2027-04-10", to: "2027-04-19", split: "fixed" };

describe("the stays of a trip across cities", () => {
  it("picks the bases, their order and nights, and every park on a day its base reaches", () => {
    const answer = plan({
      ...TRIP,
      parks: ALL,
      startBase: "tokyo",
      endBase: "osaka",
    });
    const stays = answer.meta.stays ?? [];
    expect(stays[0]).toMatchObject({ base: "tokyo", from: "2027-04-10" });
    expect(stays[stays.length - 1]).toMatchObject({
      base: "osaka",
      to: "2027-04-19",
    });
    expect(stays.reduce((n, s) => n + (s.nights as number), 0)).toBe(9);
    expect(stays.every((s) => s.auto === true)).toBe(true);
    expect(answer.unplaced).toEqual([]);
    expect(answer.meta.exact).toBe(true);
    const reach = new Map(
      (TEST_JAPAN.bases ?? []).map((b) => [
        b.key,
        new Set(b.parks.map((p) => p.slug)),
      ]),
    );
    for (const day of answer.days) {
      expect(day.base).toBeDefined();
      for (const p of day.parks) {
        const move = day.move as
          { from: string; to: string; when: string } | undefined;
        const from = move
          ? move.when === "evening"
            ? move.from
            : move.to
          : (day.base as string);
        expect(reach.get(from)?.has(p.slug)).toBe(true);
      }
    }
    expect(answer.totals.moves).toBe(stays.length - 1);
  });

  it("follows the route line one way: landing in Osaka and leaving from Tokyo reverses it", () => {
    const answer = plan({
      ...TRIP,
      parks: ALL,
      startBase: "osaka",
      endBase: "tokyo",
    });
    const bases = (answer.meta.stays ?? []).map((s) => s.base);
    expect(bases[0]).toBe("osaka");
    expect(bases[bases.length - 1]).toBe("tokyo");
    const order = new Map(
      (TEST_JAPAN.bases ?? []).map((b) => [b.key, b.order as number]),
    );
    for (let i = 1; i < bases.length; i++) {
      expect(order.get(bases[i] as string)).toBeLessThan(
        order.get(bases[i - 1] as string) as number,
      );
    }
  });

  it("keeps the nights the visitor fixed, rules a base out with :0 and makes its park a long day trip", () => {
    const answer = plan({
      ...TRIP,
      parks: ALL,
      startBase: "tokyo",
      endBase: "osaka",
      bases: "tokyo:4@maihama,fuji:0,nagoya:1",
    });
    const stays = answer.meta.stays ?? [];
    expect(stays.find((s) => s.base === "tokyo")).toMatchObject({
      nights: 4,
      area: "maihama",
    });
    expect(stays.find((s) => s.base === "tokyo")).not.toHaveProperty("auto");
    expect(stays.find((s) => s.base === "nagoya")?.nights).toBe(1);
    expect(stays.some((s) => s.base === "fuji")).toBe(false);
    const fuji = answer.days.find((d) =>
      d.parks.some((p) => p.slug === "j-fuji"),
    );
    expect(fuji?.base).toBe("tokyo");
    expect(fuji?.warnings).toContain("long_day_trip");
  });

  it("goes back to where the flight leaves on the departure day of a round trip", () => {
    const answer = plan({
      ...TRIP,
      parks: "j-land,j-usj",
      startBase: "tokyo",
      endBase: "tokyo",
    });
    const stays = answer.meta.stays ?? [];
    const last = answer.days[answer.days.length - 1];
    if (stays[stays.length - 1].base !== "tokyo") {
      expect(last.kind).toBe("departure");
      expect(last.move).toMatchObject({ to: "tokyo", when: "day" });
    }
    expect(stays[0].base).toBe("tokyo");
    expect(stays.filter((s) => s.base === "tokyo")).toHaveLength(1);
  });

  it("puts a park after a morning move, priced by the hours left, and says when the guest gets in", () => {
    // Tokyo one night, then Nagoya: LEGOLAND is cheap only on the move day.
    const answer = plan(
      {
        from: "2027-04-10",
        to: "2027-04-12",
        parks: "j-land,j-lego",
        startBase: "tokyo",
        endBase: "nagoya",
        arrival: "park",
        departure: "park",
        split: "fixed",
        rest: "none",
      },
      (slug, i, cell) => {
        if (slug === "j-lego") cell.waitExact = i === 1 ? 10 : 90;
        if (slug === "j-land") cell.waitExact = i === 0 ? 20 : 90;
      },
    );
    const day = answer.days[1];
    expect(day.parks).toEqual([{ slug: "j-lego", part: "full" }]);
    expect(day.move).toMatchObject({
      from: "tokyo",
      to: "nagoya",
      when: "morning",
      departAt: "2027-04-11T08:00:00+09:00",
      arriveAt: "2027-04-11T10:30:00+09:00",
    });
    // 08:00 + 150 on the train + 60 for the luggage + 40 to the park.
    expect(day.startAt).toBe("2027-04-11T12:10:00+09:00");
    expect(day.warnings).toContain("move_day");
    expect(day.base).toBe("nagoya");
  });

  it("puts a park before an evening move where the train still arrives in time", () => {
    const answer = plan(
      {
        from: "2027-04-10",
        to: "2027-04-12",
        parks: "j-lego,j-usj",
        startBase: "nagoya",
        endBase: "osaka",
        arrival: "park",
        departure: "park",
        split: "fixed",
        rest: "none",
      },
      (slug, i, cell) => {
        if (slug === "j-lego") {
          cell.waitExact = i === 1 ? 10 : 90;
          cell.closesAt = cell.closesAt?.replace("21:00", "18:00");
          cell.dayHours = 9;
        }
        if (slug === "j-usj") cell.waitExact = i === 2 ? 10 : 90;
      },
    );
    const day = answer.days[1];
    expect(day.parks).toEqual([{ slug: "j-lego", part: "full" }]);
    expect(day.move).toMatchObject({
      from: "nagoya",
      to: "osaka",
      when: "evening",
      departAt: "2027-04-11T18:40:00+09:00",
      arriveAt: "2027-04-11T20:10:00+09:00",
    });
    expect(day.warnings).not.toContain("move_day");
    expect(day).not.toHaveProperty("startAt");
  });

  it("makes a move too long for a park a travel day", () => {
    const answer = plan({
      from: "2027-04-10",
      to: "2027-04-13",
      parks: "j-fuji,j-lego",
      bases: "fuji:1,nagoya:2",
      arrival: "park",
      departure: "park",
      split: "fixed",
      rest: "none",
    });
    const day = answer.days[1];
    expect(day.kind).toBe("travel");
    expect(day.move).toMatchObject({ from: "fuji", to: "nagoya", when: "day" });
    expect(answer.totals.travelDays).toBe(1);
  });

  it("counts the stay hotel's perk from the morning after check-in", () => {
    const answer = plan({
      from: "2027-04-10",
      to: "2027-04-12",
      parks: "j-land:3",
      bases: "tokyo@maihama",
      arrival: "park",
      departure: "park",
      split: "fixed",
      rest: "none",
    });
    const perks = answer.days.map((d) => d.perks);
    expect(perks[0]).toBeUndefined();
    expect(perks[2]).toEqual(["happy_entry"]);
  });

  it("suggests one night more or less in a fixed stay where that saves waiting", () => {
    // USJ is cheap only on the 14th: a Tokyo stay fixed at 5 nights keeps the
    // trip in Tokyo that day; one night less there reaches it.
    const answer = plan(
      {
        from: "2027-04-10",
        to: "2027-04-17",
        parks: "j-land,j-usj",
        bases: "tokyo:5",
        startBase: "tokyo",
        endBase: "osaka",
        split: "fixed",
      },
      (slug, i, cell) => {
        if (slug === "j-usj") cell.waitExact = i === 4 ? 10 : 80;
      },
    );
    expect(answer.meta.staySuggestion).toMatchObject({
      nights: { tokyo: 4 },
      park: "j-usj",
      date: "2027-04-14",
    });
    expect(
      (answer.meta.staySuggestion as { savedMinutesPerRide: number })
        .savedMinutesPerRide,
    ).toBeGreaterThanOrEqual(3);
    const free = plan({ ...TRIP, parks: ALL, startBase: "tokyo" });
    expect(free.meta).not.toHaveProperty("staySuggestion");
  });

  it("takes the visitor's own buffer for a hotel change, and names it", () => {
    const base = {
      ...TRIP,
      parks: ALL,
      startBase: "tokyo",
      endBase: "osaka",
    };
    const normalize = (moveBuffer?: number) =>
      normalizePlanQuery(
        TEST_JAPAN,
        known(TEST_JAPAN),
        { ...base, ...(moveBuffer !== undefined && { moveBuffer }) } as never,
        TODAY,
      );
    expect(plan(base).meta.moveBuffer).toBe(
      stayRulesOf(TEST_JAPAN).moveOverheadMinutes,
    );
    expect(plan({ ...base, moveBuffer: 120 } as never).meta.moveBuffer).toBe(
      120,
    );
    const slow = normalize(120);
    expect(
      staysOption(TEST_JAPAN, slow.stays!, []).rules.moveOverheadMinutes,
    ).toBe(120);
    expect(planCacheQuery(normalize(30))).not.toBe(planCacheQuery(slow));
    expect(() =>
      normalizePlanQuery(
        ORLANDO,
        ["epcot"],
        {
          from: "2027-04-10",
          to: "2027-04-19",
          parks: "epcot",
          moveBuffer: 30,
        } as never,
        TODAY,
      ),
    ).toThrow("Orlando has no bases");
  });

  it("refuses stays where they do not belong, and bad ones", () => {
    const refuse =
      (region: RegionConfig, query: Record<string, string>) => () =>
        normalizePlanQuery(
          region,
          known(region),
          {
            from: "2027-04-10",
            to: "2027-04-19",
            parks: "j-land",
            ...query,
          } as never,
          TODAY,
        );
    expect(refuse(ORLANDO, { parks: "epcot", bases: "tokyo" })).toThrow(
      "Orlando has no bases",
    );
    expect(refuse(TEST_JAPAN, { area: "maihama" })).toThrow(
      "bases=<base>@<area>",
    );
    expect(refuse(TEST_JAPAN, { bases: "kyoto:2" })).toThrow(
      "kyoto is not a base of Test Japan",
    );
    expect(refuse(TEST_JAPAN, { bases: "tokyo@universal_city" })).toThrow(
      "universal_city is not a hotel area of Tokyo",
    );
    expect(refuse(TEST_JAPAN, { bases: "tokyo:6,osaka:4" })).toThrow(
      "Your stays add up to 10 nights; the trip has 9",
    );
    expect(refuse(TEST_JAPAN, { startBase: "atlantis" })).toThrow(
      "atlantis is not a base",
    );
  });

  it("leaves a park out that no open base reaches", () => {
    const answer = plan({
      ...TRIP,
      parks: "j-fuji,j-land",
      bases: "tokyo:0,fuji:0",
    });
    expect(answer.unplaced).toEqual(
      expect.arrayContaining([
        { slug: "j-fuji", reason: "no_base" },
        { slug: "j-land", reason: "no_base" },
      ]),
    );
  });
});

describe("bases off a line", () => {
  const PARIS: RegionConfig = {
    ...TEST_PARIS,
    parks: [
      ...TEST_PARIS.parks,
      {
        slug: "test-asterix",
        continent: "europe",
        country: "france",
        city: "plailly",
        group: "disneyland-paris",
        type: "theme_park",
        headliner: false,
      },
    ],
    hotelAreas: [
      {
        key: "val-d-europe",
        name: "Val d'Europe",
        latitude: 48.85,
        longitude: 2.78,
        hotel: "disney_hotel",
        note: "",
      },
      {
        key: "asterix-hotels",
        name: "Parc Astérix hotels",
        latitude: 49.13,
        longitude: 2.57,
        hotel: "offsite",
        note: "",
      },
    ],
    bases: [
      {
        key: "disneyland-paris",
        name: "Disneyland Paris",
        hotelAreas: ["val-d-europe"],
        gateways: [
          { code: "XED", name: "Marne-la-Vallée–Chessy", kind: "station" },
        ],
        parks: [
          { slug: "test-park", minutes: 10, mode: "walk" },
          { slug: "test-studios", minutes: 10, mode: "walk" },
          { slug: "test-asterix", minutes: 60, mode: "drive" },
        ],
      },
      {
        key: "parc-asterix",
        name: "Parc Astérix",
        hotelAreas: ["asterix-hotels"],
        gateways: [
          { code: "CDG", name: "Paris-Charles de Gaulle", kind: "airport" },
        ],
        parks: [
          { slug: "test-asterix", minutes: 5, mode: "walk" },
          { slug: "test-park", minutes: 60, mode: "drive" },
        ],
      },
    ],
    baseLegs: [
      {
        from: "disneyland-paris",
        to: "parc-asterix",
        minutes: 60,
        mode: "drive",
      },
    ],
  };

  it("lets the stays come in either order where the bases lie on no line", () => {
    const query = {
      from: "2027-05-03",
      to: "2027-05-07",
      parks: "test-park,test-studios,test-asterix",
      split: "fixed",
    };
    const there = plan(
      { ...query, startBase: "parc-asterix", endBase: "disneyland-paris" },
      undefined,
      PARIS,
    );
    expect((there.meta.stays ?? []).map((s) => s.base)).toEqual([
      "parc-asterix",
      "disneyland-paris",
    ]);
    const back = plan(
      { ...query, startBase: "disneyland-paris", endBase: "parc-asterix" },
      undefined,
      PARIS,
    );
    expect((back.meta.stays ?? []).map((s) => s.base)).toEqual([
      "disneyland-paris",
      "parc-asterix",
    ]);
    // One night at Parc Astérix beats a day trip there only when its day is worth the move.
    const stay = plan({ ...query, bases: "parc-asterix:1" }, undefined, PARIS);
    expect(
      (stay.meta.stays ?? []).find((s) => s.base === "parc-asterix")?.nights,
    ).toBe(1);
  });
});

describe("/when across cities", () => {
  it("plans each window with its stays", () => {
    const from = "2027-04-05";
    const cal = calendarOf(TEST_JAPAN, from, 40);
    const result = findTripDates({
      region: TEST_JAPAN,
      calendar: cal,
      query: {
        from,
        to: cal.meta.to,
        days: 8,
        limit: 3,
        gap: 3,
        plan: normalizePlanQuery(
          TEST_JAPAN,
          known(TEST_JAPAN),
          {
            from,
            to: addIsoDays(from, 7),
            parks: "j-land,j-usj",
            startBase: "tokyo",
            endBase: "osaka",
          },
          TODAY,
        ),
      },
    });
    expect(result.windows.length).toBeGreaterThan(0);
    for (const w of result.windows) {
      expect(w.plan.stays?.[0].base).toBe("tokyo");
      expect(w.plan.days.every((d) => d.base)).toBe(true);
    }
  });
});

describe("the region route of a region with bases", () => {
  it("serves its bases with their gateways, the base legs, the stay rules and the parks' codes", () => {
    const records: RegionParkRecord[] = TEST_JAPAN.parks.map((config, i) => ({
      config,
      id: `id-${i}`,
      slug: config.slug,
      name: config.slug,
      path: "",
      latitude: 0,
      longitude: 0,
      timezone: "Asia/Tokyo",
    }));
    const region = describeRegion(TEST_JAPAN, records);
    expect(region.parks.map((p) => p.code)).toEqual([
      "DL",
      "DS",
      "FQ",
      "LJ",
      "US",
    ]);
    expect(region.bases?.[0]).toMatchObject({
      key: "tokyo",
      order: 1,
      defaultArea: "maihama",
      gateways: [
        { code: "HND", name: "Haneda", kind: "airport" },
        { code: "NRT", name: "Narita", kind: "airport" },
      ],
    });
    expect(region.baseLegs?.find((l) => l.from === "fuji")).toMatchObject({
      to: "nagoya",
      parkDay: false,
    });
    expect(
      region.baseLegs?.find((l) => l.to === "nagoya" && l.from === "tokyo")
        ?.parkDay,
    ).toBe(true);
    expect(region.stayRules).toEqual({
      dayTripFreeMinutes: 45,
      moveOverheadMinutes: 60,
      parkDayMaxLegMinutes: 150,
      moveCost: 20,
      moveStartsAt: "08:00",
      lastArrivalAt: "23:00",
      moveBufferOptions: [30, 60, 90, 120],
    });
  });
});
