import { DataSource } from "typeorm";
import { fromZonedTime } from "date-fns-tz";
import type { Redis } from "ioredis";
import { runAssistantTool, type AssistantToolContext } from "./assistant-tools";
import { visitorTrips } from "./assistant-plans";
import { localizePageUrls } from "./assistant-actions";
import {
  dateRuns,
  eveningPhrase,
  hubPath,
  LAST_YEAR_PHRASE,
  leftOutFixes,
  regionParkFor,
  type RegionParkName,
} from "./assistant-trip";
import { TripAccuracyService } from "../../trip-regions/trip-accuracy.service";
import { ParksService } from "../../parks/parks.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import type { CalendarService } from "../../parks/services/calendar.service";
import {
  TripRegionService,
  type InternalRegionCalendar,
} from "../../trip-regions/trip-region.service";
import { TripPlanService } from "../../trip-regions/trip-plan.service";
import { TripWhenService } from "../../trip-regions/trip-when.service";
import type { HolidaysService } from "../../holidays/holidays.service";
import type { WeatherService } from "../../parks/weather.service";
import type { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import type { PlanDayService } from "../../parks/services/plan-day.service";
import { ORLANDO } from "../../trip-regions/regions";
import { isoInZone } from "../../trip-regions/trip-calendar.util";
import type { TripParkDayInternal } from "../../trip-regions/trip-calendar.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";

const TZ = "America/New_York";
const today = getCurrentDateInTimezone(TZ);
const day = (n: number) => addIsoDays(today, n);
const at = (date: string, clock: string) =>
  isoInZone(fromZonedTime(`${date}T${clock}:00`, TZ), TZ);

const NAMES: Record<string, string> = {
  "magic-kingdom-park": "Magic Kingdom Park",
  epcot: "EPCOT",
  "disneys-hollywood-studios": "Disney's Hollywood Studios",
  "disneys-animal-kingdom-theme-park": "Disney's Animal Kingdom Theme Park",
  "universal-studios-florida": "Universal Studios Florida",
  "universal-islands-of-adventure": "Islands of Adventure",
  "universal-epic-universe": "Epic Universe",
  "universal-volcano-bay": "Volcano Bay",
  "seaworld-orlando": "SeaWorld Orlando",
};
const idOf = (slug: string) =>
  `00000000-0000-4000-8000-${String(
    ORLANDO.parks.findIndex((p) => p.slug === slug),
  ).padStart(12, "0")}`;
const regionParks = ORLANDO.parks.map((config) => ({
  config,
  park: {
    id: idOf(config.slug),
    slug: config.slug,
    name: NAMES[config.slug] ?? config.slug,
    continentSlug: config.continent,
    countrySlug: config.country,
    citySlug: config.city,
    timezone: TZ,
    retiredAt: null,
    curatedAbbreviations: config.slug === "magic-kingdom-park" ? "MK" : null,
  },
}));

/** A park day as the region calendar holds it, open 09:00 until `close`. */
function open(
  date: string,
  wait: number,
  basis: TripParkDayInternal["basis"] = "forecast",
  close = "22:00",
): TripParkDayInternal {
  return {
    status: "OPERATING",
    hoursPublished: true,
    opensAt: at(date, "09:00"),
    closesAt: at(date, close),
    crowdLevel: "low",
    headlinerWait: wait,
    basis,
    dayHours: Number(close.slice(0, 2)) - 9,
  };
}

const PARTY = "s-party";
const FOOD = "s-food";

/** Five trip days: MK with Early Entry and a party night, EPCOT, Epic past the forecast. */
function calendar(): InternalRegionCalendar {
  const dates = [10, 11, 12, 13, 14].map(day);
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from: dates[0],
      to: dates[4],
      computedAt: "",
      publishedHoursUntil: {},
    },
    parks: ["magic-kingdom-park", "epcot", "universal-epic-universe"],
    days: dates.map((date, i) => {
      const mk = open(date, [50, 50, 30, 60, 60][i], "forecast", "22:00");
      mk.early = {
        kind: "early_entry",
        opensAt: at(date, "08:30"),
        closesAt: at(date, "09:00"),
      };
      if (i === 2) {
        Object.assign(mk, {
          closesAt: at(date, "18:00"),
          dayHours: 9,
          evening: {
            kind: "ticketed_event",
            opensAt: at(date, "19:00"),
            closesAt: at(addIsoDays(date, 1), "00:00"),
          },
          seasons: [PARTY],
        });
      }
      const epcot = open(date, [35, 35, 40, 45, 45][i]);
      epcot.seasons = [FOOD];
      const epic = open(
        date,
        [70, 70, 75, 55, 55][i],
        i >= 3 ? "last_year" : "forecast",
      );
      return {
        date,
        isHoliday: false,
        isSchoolVacation: false,
        parks: {
          "magic-kingdom-park": mk,
          epcot,
          "universal-epic-universe": epic,
        },
      };
    }),
    seasons: [
      {
        id: PARTY,
        parkSlug: "magic-kingdom-park",
        kind: "christmas",
        name: "Mickey's Very Merry Christmas Party",
        startDate: day(0),
        endDate: day(40),
        dates: [day(12), day(14), day(16), day(60)],
        status: "confirmed",
        separateTicket: true,
        opensAt: "19:00",
        closesAt: "24:00",
      },
      {
        id: FOOD,
        parkSlug: "epcot",
        kind: "special_event",
        name: "EPCOT International Food & Wine Festival",
        startDate: day(0),
        endDate: day(30),
        dates: null,
        status: "expected",
        separateTicket: false,
        opensAt: null,
        closesAt: null,
      },
    ],
  };
}

const PHL = {
  id: "11111111-1111-4111-8111-111111111111",
  slug: "phantasialand",
  name: "Phantasialand",
  timezone: "Europe/Berlin",
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};

/** Every day from `from` to `to` open at each of `slugs`, with waits that differ by park and day. */
function wideCalendar(
  from: string,
  to: string,
  slugs: readonly string[],
): InternalRegionCalendar {
  const dates: string[] = [];
  for (let d = from; d <= to; d = addIsoDays(d, 1)) dates.push(d);
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from,
      to,
      computedAt: "",
      publishedHoursUntil: {},
    },
    parks: [...slugs],
    days: dates.map((date, i) => ({
      date,
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(
        slugs.map((slug, p) => [
          slug,
          {
            ...open(date, 30 + ((p * 7 + i * 13) % 40)),
            seasons: [FOOD],
          },
        ]),
      ),
    })),
    seasons: calendar().seasons,
  };
}

function setup(
  build: (from: string, to: string) => InternalRegionCalendar = () =>
    calendar(),
) {
  const store = new Map<string, string>();
  const redis = {
    get: jest.fn(async (k: string) => store.get(k) ?? null),
    set: jest.fn(async (k: string, v: string) => {
      store.set(k, v);
      return "OK";
    }),
  } as unknown as Redis;
  const parks = {
    findById: jest.fn(async (id: string) => {
      if (id === PHL.id) return PHL;
      const found = regionParks.find((p) => p.park.id === id);
      return found?.park ?? null;
    }),
  };
  const regions = new TripRegionService(
    parks as unknown as ParksService,
    {} as CalendarService,
    {} as ParkSeasonService,
    redis,
  );
  jest.spyOn(regions, "resolveParks").mockResolvedValue(regionParks as never);
  const internalCalendar = jest
    .spyOn(regions, "internalCalendar")
    .mockImplementation(async (_region, from, to) => build(from, to));
  const plans = new TripPlanService(
    regions,
    {
      getLeadTimeMae: jest.fn(async () => null),
    } as unknown as PredictionLeadSnapshotService,
    { showsOn: jest.fn(async () => []) } as unknown as PlanDayService,
    redis,
  );
  const seasons = {
    forParkInRange: jest.fn(async () => [
      {
        id: "s-hw",
        parkId: PHL.id,
        kind: "halloween",
        name: null,
        startDate: day(1),
        endDate: day(20),
        dates: null,
        status: "announced",
        separateTicket: false,
        opensAt: null,
        closesAt: null,
      },
    ]),
  };
  const whens = new TripWhenService(
    regions,
    {
      getHolidays: jest.fn(async (country: string) =>
        country === "DE"
          ? Array.from({ length: 11 }, (_, k) => ({
              date: day(20 + k),
              name: "Summer Holidays",
              country: "DE",
              region: "DE-NW",
              holidayType: "school",
              isNationwide: false,
            }))
          : [],
      ),
      getSchoolHolidayCoverage: jest.fn(async () => ({
        regional: new Map([["DE|NW", day(300)]]),
        national: new Map(),
        countryAny: new Map([["DE", day(300)]]),
      })),
    } as unknown as HolidaysService,
    {
      getClimateNormals: jest.fn(async () => null),
    } as unknown as WeatherService,
    redis,
  );
  const providers = new Map<unknown, unknown>([
    [TripRegionService, regions],
    [TripPlanService, plans],
    [TripWhenService, whens],
    [
      TripAccuracyService,
      {
        accuracy: jest.fn(async (region: string) => ({
          region,
          forecastAccuracy: {
            days: 30,
            from: "2026-10-14",
            to: "2026-11-12",
            leadDays: 7,
            maeMinutes: 11.4,
            parks: 7,
            sampleDays: 30,
          },
        })),
      },
    ],
    [ParksService, parks],
    [ParkSeasonService, seasons],
    [
      DataSource,
      {
        getRepository: () => ({
          find: async ({ where }: { where: { slug: string } }) =>
            where.slug === PHL.slug ? [PHL] : [],
        }),
      },
    ],
  ]);
  const call = async (
    name: string,
    input: unknown,
    locale = "de",
    extra: Partial<AssistantToolContext> = {},
  ) => {
    const onError = jest.fn();
    const result = await runAssistantTool(
      name,
      input,
      {
        get: <T>(token: unknown) => providers.get(token) as T,
        locale,
        ...extra,
      },
      onError,
    );
    expect(onError).not.toHaveBeenCalled();
    return {
      ...result,
      json: result.isError
        ? null
        : (JSON.parse(result.content) as Record<string, any>),
    };
  };
  return { call, internalCalendar, seasons };
}

describe("eveningPhrase", () => {
  it("tells the evening in one phrase: where, when, how far, the show and the alternatives", () => {
    const names = new Map([
      ["disneys-hollywood-studios", "Disney's Hollywood Studios"],
      ["epcot", "EPCOT"],
    ]);
    const phrase = eveningPhrase(
      {
        kind: "hop",
        park: "disneys-hollywood-studios",
        from: "2026-12-03T18:17:00-05:00",
        to: "2026-12-03T21:00:00-05:00",
        travel: { minutes: 17, mode: "drive" },
        highlights: [
          {
            kind: "night_show",
            name: "Fantasmic!",
            at: "2026-12-03T20:00:00-05:00",
            source: "projected",
          },
        ],
        reasons: ["park_closes_early", "open_later", "night_show"],
        alternatives: [
          {
            kind: "hop",
            park: "epcot",
            from: "2026-12-03T18:16:00-05:00",
            to: "2026-12-03T21:00:00-05:00",
            reasons: ["park_closes_early"],
          },
          {
            kind: "venue",
            venue: "disney-springs",
            from: "2026-12-03T18:19:00-05:00",
            to: "2026-12-03T23:00:00-05:00",
            reasons: ["park_closes_early", "free_entry"],
          },
        ],
      },
      names,
      ORLANDO,
    );
    expect(phrase).toBe(
      "hop to Disney's Hollywood Studios 18:17–21:00, 17 min by drive; Fantasmic! 20:00 (projected); or hop to EPCOT or Disney Springs",
    );
  });
});

describe("regionParkFor", () => {
  const parks: RegionParkName[] = regionParks.map(({ config, park }) => ({
    slug: config.slug,
    id: park.id,
    name: park.name,
    abbreviations: park.curatedAbbreviations ? ["MK"] : [],
  }));

  it("finds a park by slug, name, abbreviation or the one name that contains the words", () => {
    expect(regionParkFor("epcot", parks)).toMatchObject({ slug: "epcot" });
    expect(regionParkFor("mk", parks)).toMatchObject({
      slug: "magic-kingdom-park",
    });
    expect(regionParkFor("Epic", parks)).toMatchObject({
      slug: "universal-epic-universe",
    });
    expect(regionParkFor("Hollywood Studios", parks)).toMatchObject({
      slug: "disneys-hollywood-studios",
    });
  });

  it("hands back the candidates for a word several parks share, and nothing for a park elsewhere", () => {
    expect(regionParkFor("Universal", parks)).toMatchObject({
      ambiguous: true,
      parks: expect.arrayContaining([
        {
          slug: "universal-studios-florida",
          name: "Universal Studios Florida",
        },
      ]),
    });
    expect(regionParkFor("Phantasialand", parks)).toBeNull();
  });
});

describe("dateRuns", () => {
  it("writes consecutive days as one range", () => {
    expect(
      dateRuns(["2026-12-03", "2026-12-01", "2026-12-02", "2026-12-05"]),
    ).toEqual(["2026-12-01..2026-12-03", "2026-12-05"]);
  });
});

describe("the hub's address", () => {
  it("takes a hubUrl with or without a locale and keeps only the plan's parameters", () => {
    expect(
      hubPath(
        "https://park.fan/de/trip-regions/orlando?from=2027-03-03&to=2027-03-10&parks=epcot:1,magic-kingdom-park:2&hop=0#planner",
      ),
    ).toBe(
      "/trip-regions/orlando?from=2027-03-03&to=2027-03-10&parks=epcot:1,magic-kingdom-park:2&hop=0#planner",
    );
    expect(hubPath("https://park.fan/trip-regions/orlando")).toBe(
      "/trip-regions/orlando",
    );
    // An older answer's link at the region's slug lands on the route folder.
    expect(hubPath("https://park.fan/de/orlando?from=2027-03-03")).toBe(
      "/trip-regions/orlando?from=2027-03-03#planner",
    );
  });

  it("refuses another host, another page, an unknown parameter or value and another anchor", () => {
    for (const url of [
      "https://evil.example/orlando?from=2027-03-03",
      "https://park.fan/de/orlando/2027/3?from=2027-03-03",
      "https://park.fan/trip-regions/atlantis?from=2027-03-03",
      "https://park.fan/orlando?from=2027-03-03&next=https://evil.example",
      "https://park.fan/orlando?parks=<script>",
      "https://park.fan/orlando?from=2027-03-03#top",
    ]) {
      expect([url, hubPath(url)]).toEqual([url, null]);
    }
  });

  it("puts the chat's language in front of the hub, not of a page that only starts like it", () => {
    expect(
      localizePageUrls(
        {
          hubUrl:
            "https://park.fan/trip-regions/orlando?from=2027-03-03#planner",
          other: "https://park.fan/trip-regions/orlandox",
        },
        "nl",
      ),
    ).toEqual({
      hubUrl:
        "https://park.fan/nl/trip-regions/orlando?from=2027-03-03#planner",
      other: "https://park.fan/trip-regions/orlandox",
    });
  });
});

describe("the visitor's own trip", () => {
  const onScreen = (
    query: string,
    party?: { size?: number; heightsCm: number[] },
  ) => ({
    trips: visitorTrips([
      { region: "orlando", query, onScreen: true, ...(party && { party }) },
    ]),
    answer: { round: 0, done: new Set<string>() },
  });
  const planned = `from=${day(10)}&to=${day(14)}&parks=magic-kingdom-park,epcot,universal-epic-universe`;

  it("reads the trip on screen as plan_vacation would plan it, with the party", async () => {
    const { call } = setup();
    const { json } = await call(
      "get_trip_plan",
      {},
      "de",
      onScreen(planned, { size: 4, heightsCm: [105] }),
    );
    expect(json?.shown).toBe("on screen in the hub's planner now");
    expect(json?.party).toEqual({ size: 4, heightsCm: [105] });
    expect(json?.days.map((d: { kind: string }) => d.kind)).toEqual([
      "arrival",
      "park",
      "park",
      "park",
      "departure",
    ]);
  });

  it("says what is missing instead of guessing a plan", async () => {
    const { call } = setup();
    const none = await call("get_trip_plan", {}, "de", { trips: [] });
    expect(none.isError).toBe(true);
    expect(none.content).toMatch(/no trip in the trip planner/);
    const searching = await call(
      "get_trip_plan",
      {},
      "de",
      onScreen(`when=${day(10)}_${day(90)}&days=7&within=DE-NW`),
    );
    expect(searching.content).toMatch(
      /searching 7 days .* find_trip_dates answers that/,
    );
  });

  it("pins a park through the planner's own query and says which days moved", async () => {
    const { call } = setup();
    const result = await call(
      "change_trip",
      { pin: [{ park: "Magic Kingdom", date: day(13) }] },
      "de",
      onScreen(planned),
    );
    expect(result.action).toEqual({
      type: "trip_set",
      region: "orlando",
      query: `${planned}&lock=magic-kingdom-park@${day(13)}`,
      change: ["pin"],
    });
    expect(result.json?.shownIn).toBe("the planner on screen, which re-plans");
    const moved = result.json?.daysChanged as Array<{
      date: string;
      now: string;
    }>;
    expect(moved.find((d) => d.date === day(13))?.now).toBe(
      "Magic Kingdom Park",
    );
  });

  it("swaps two park days and refuses a trip the plan route refuses, sending nothing", async () => {
    const { call } = setup();
    const before = await call("get_trip_plan", {}, "de", onScreen(planned));
    const [a, b] = (
      before.json?.days as Array<{
        kind: string;
        date: string;
        parks: Array<{ park: string }>;
      }>
    ).filter((d) => d.kind === "park");
    const swapped = await call(
      "change_trip",
      { swap: { a: a.date, b: b.date } },
      "de",
      onScreen(planned),
    );
    expect(swapped.action).toMatchObject({
      type: "trip_set",
      change: ["swap"],
    });
    const days = swapped.json?.daysChanged as Array<{
      date: string;
      now: string;
      was: string;
    }>;
    expect(days.find((d) => d.date === a.date)).toMatchObject({
      now: b.parks[0].park,
      was: a.parks[0].park,
    });
    const tooLong = await call(
      "change_trip",
      { dates: { from: day(10), to: day(40) } },
      "de",
      onScreen(planned),
    );
    expect(tooLong.isError).toBe(true);
    expect(tooLong.action).toBeUndefined();
  });

  it("starts a trip from dates when the visitor has none yet", async () => {
    const { call } = setup();
    const result = await call(
      "change_trip",
      { dates: { from: day(10), to: day(14) } },
      "de",
      { trips: [], answer: { round: 0, done: new Set<string>() } },
    );
    expect(result.action).toEqual({
      type: "trip_set",
      region: "orlando",
      query: `from=${day(10)}&to=${day(14)}`,
      change: ["dates"],
    });
    expect(result.json?.shownIn).toBe("the hub, which opens with it");
    // Seven headliners on three park days: the plan fills the days and names the parks it left
    // out, which Parky reads as the next thing to settle.
    expect(result.json?.notPlannedYet).toBeUndefined();
    expect(result.json?.parksLeftOut).toEqual(
      expect.arrayContaining([
        { park: expect.any(String), why: "no day left for it" },
      ]),
    );
    expect(result.json?.leftOutFixes).toEqual([
      "the arrival or departure day as a park day: change_trip arrival park or departure park",
      "fewer parks: change_trip parks with visits 0 for one they can do without",
      expect.stringMatching(
        /^a longer trip: change_trip dates, \d+ more days?$/,
      ),
    ]);
  });
});

describe("plan_vacation", () => {
  const trip = {
    from: day(10),
    to: day(14),
    parks: [
      { park: "Magic Kingdom" },
      { park: "epcot" },
      { park: "Epic", visits: 1 },
    ],
  };

  it("retells the plan day by day in park time, with the hub and the planner's list", async () => {
    const { call } = setup();
    const { json } = await call("plan_vacation", trip);
    expect(json?.timesIn).toMatch(/^park time, America\/New_York/);
    expect(json?.days.map((d: { kind: string }) => d.kind)).toEqual([
      "arrival",
      "park",
      "park",
      "park",
      "departure",
    ]);
    const parkDays = json?.days.filter(
      (d: { kind: string }) => d.kind === "park",
    );
    for (const d of parkDays) {
      expect(d.weekday).toMatch(/day$/);
      expect(d.parks[0].hours).toMatch(/^09:00–(22|18):00$/);
      expect(d.why.length).toBeGreaterThan(0);
    }
    const epic = parkDays.find(
      (d: { parks: Array<{ park: string }> }) =>
        d.parks[0].park === "Epic Universe",
    );
    expect(epic.parks[0].basis).toBe("estimated from last year");
    expect(epic.watch).toContain(LAST_YEAR_PHRASE);
    const epcot = parkDays.find(
      (d: { parks: Array<{ park: string }> }) => d.parks[0].park === "EPCOT",
    );
    expect(epcot.parks[0].events).toEqual([
      "EPCOT International Food & Wine Festival (expected)",
    ]);
    expect(json?.hubUrl).toBe(
      `https://park.fan/de/trip-regions/orlando?from=${day(10)}&to=${day(14)}&parks=epcot:1,magic-kingdom-park:1,universal-epic-universe:1&hotel=offsite&blocks=1&fireworks=any&split=auto&rest=auto&hop=0&arrival=free&departure=free#planner`,
    );
    expect(json?.reserve).toHaveLength(3);
    expect(json?.reserve).toContainEqual({
      park: idOf("universal-epic-universe"),
      date: epic.date,
    });
    expect(json?.totals.means).toMatch(/per headliner ride, not per day/);
  });

  it("plans with the tickets they hold and names the one to buy for the rest", async () => {
    const { call } = setup();
    const { json } = await call("plan_vacation", {
      ...trip,
      tickets: "wdw-date-based.2.h?",
    });
    expect(json?.tickets).toEqual([
      expect.objectContaining({
        id: "t1",
        ticket: "Walt Disney World date-based theme park ticket",
        used: "2 of 2 days",
        hopper: expect.stringMatching(/^not known, planned without; /),
      }),
    ]);
    const disneyDays = json?.days.filter((d: { ticket?: string }) =>
      d.ticket?.startsWith("t1 day"),
    );
    expect(disneyDays).toHaveLength(2);
    expect(json?.ticketsToBuy).toEqual([
      expect.objectContaining({
        ticket: "Universal Epic Universe 1-Day ticket",
      }),
    ]);
    expect(json?.hubUrl).toContain("&tickets=wdw-date-based.2.h%3F");
  });

  it("names a hotel's perk and the travel from its area", async () => {
    const { call } = setup();
    const { json } = await call("plan_vacation", {
      ...trip,
      area: "disney",
    });
    const mk = json?.days.find(
      (d: { parks?: Array<{ park: string }> }) =>
        d.parks?.[0].park === "Magic Kingdom Park",
    );
    expect(mk.parks[0].perks).toEqual(["Early Entry"]);
    expect(json?.travelFrom).toBe("the hotel area Walt Disney World");
    expect(mk.travel).toMatchObject({ mode: "drive", plusMinutes: 15 });
    expect(mk.travel.via).toMatch(/Transportation and Ticket Center/);
    // The perk is named once, in perks, not again among the reasons.
    expect(mk.why.join(" ")).not.toMatch(/Early Entry/);
  });

  it("keeps the longest trip with every visit under the result's size limit", async () => {
    const slugs = [
      "magic-kingdom-park",
      "epcot",
      "disneys-hollywood-studios",
      "disneys-animal-kingdom-theme-park",
      "universal-studios-florida",
      "universal-islands-of-adventure",
      "universal-epic-universe",
    ];
    const { call } = setup((from, to) => wideCalendar(from, to, slugs));
    const { json, content } = await call("plan_vacation", {
      from: day(10),
      to: day(30),
      area: "universal",
      hotel: "universal_premier",
      parks: slugs.map((park, i) => ({ park, visits: i < 2 ? 3 : 2 })),
    });
    expect(json?.days).toHaveLength(21);
    expect(json?.reserve).toHaveLength(16);
    // Tickets to buy included; a result is cut at 12,000.
    expect(content.length).toBeLessThan(10_000);
  });

  it("refuses a trip over three weeks, a park outside Orlando and a plan the route refuses", async () => {
    const { call } = setup();
    const long = await call("plan_vacation", {
      from: day(10),
      to: day(40),
    });
    expect(long.isError).toBe(true);
    expect(long.content).toMatch(/at most 21 days/);
    const elsewhere = await call("plan_vacation", {
      ...trip,
      parks: [{ park: "phantasialand" }],
    });
    expect(elsewhere.isError).toBe(true);
    expect(elsewhere.content).toMatch(/Orlando parks only/);
    const crowded = await call("plan_vacation", {
      ...trip,
      parks: [
        { park: "epcot", visits: 3 },
        { park: "Magic Kingdom", visits: 3 },
      ],
    });
    expect(crowded.isError).toBe(true);
    expect(crowded.content).toMatch(/More park visits than trip days/);
  });
});

/** Every day of a range open at both parks, the waits rising and falling over the weeks. */
function longCalendar(from: string, to: string): InternalRegionCalendar {
  const days = [];
  for (let date = from, i = 0; date <= to; date = addIsoDays(date, 1), i++) {
    const cell = (wait: number): TripParkDayInternal => ({
      status: "OPERATING",
      hoursPublished: true,
      crowdLevel: "moderate",
      headlinerWait: Math.round(wait / 5) * 5,
      waitExact: wait,
      basis: i > 30 ? "last_year" : "forecast",
      dayHours: 12,
    });
    days.push({
      date,
      isHoliday: false,
      isSchoolVacation: false,
      parks: {
        "magic-kingdom-park": cell(50 + 15 * Math.sin(i / 5)),
        epcot: cell(40 + 10 * Math.cos(i / 4)),
      },
    });
  }
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from,
      to,
      computedAt: "",
      publishedHoursUntil: {},
    },
    parks: ["magic-kingdom-park", "epcot"],
    days,
    seasons: [],
  };
}

describe("find_trip_dates", () => {
  const trip = {
    from: day(1),
    to: day(45),
    days: 5,
    parks: [{ park: "Magic Kingdom" }, { park: "epcot" }],
  };

  it("retells the best windows with their figures, phrases and hub links, and the months", async () => {
    const { call } = setup(longCalendar);
    const { json } = await call("find_trip_dates", { ...trip, limit: 3 });
    expect(json?.tripDays).toBe(5);
    expect(json?.windows).toHaveLength(3);
    for (const w of json?.windows ?? []) {
      expect(w.from).toMatch(/^\d{4}-\d{2}-\d{2} \(\w+day\)$/);
      expect(typeof w.headlinerWait).toBe("number");
      expect(typeof w.rangeAverageWait).toBe("number");
      expect(w.hubUrl).toMatch(
        /\/orlando\?from=\d{4}-\d{2}-\d{2}&to=\d{4}-\d{2}-\d{2}&parks=epcot,magic-kingdom-park&.*#planner$/,
      );
      expect(hubPath(w.hubUrl)).not.toBeNull();
      for (const phrase of [...(w.why ?? []), ...(w.watch ?? [])]) {
        expect(phrase).not.toMatch(/_/);
      }
    }
    expect(json?.months.length).toBeGreaterThan(0);
    expect(json?.months[0].month).toMatch(/^\d{4}-\d{2}$/);
    expect(json?.windows.some((w: { lastYear?: boolean }) => w.lastYear)).toBe(
      json?.windows.some((w: { basis: string }) => w.basis.startsWith("last")),
    );
  });

  it("keeps the windows in the school break it is given", async () => {
    const { call } = setup(longCalendar);
    const { json } = await call("find_trip_dates", {
      ...trip,
      within: "DE-NW",
    });
    expect(json?.within.region).toBe("Nordrhein-Westfalen");
    for (const w of json?.windows ?? []) {
      expect(w.from.slice(0, 10) >= json?.within.breaks[0].slice(0, 10)).toBe(
        true,
      );
    }
  });

  it("refuses an unknown school-holiday region with the route's sentence", async () => {
    const { call } = setup(longCalendar);
    const result = await call("find_trip_dates", { ...trip, within: "DE-XX" });
    expect(result.isError).toBe(true);
    expect(result.content).toContain("DE-NW");
  });
});

describe("get_park_events", () => {
  it("lists Orlando's seasons with their status and the slots as date ranges", async () => {
    const { call } = setup();
    const { json } = await call("get_park_events", {
      from: day(10),
      to: day(20),
    });
    expect(json?.scope).toBe("Orlando");
    expect(json?.seasons).toContainEqual({
      park: "Magic Kingdom Park",
      name: "Mickey's Very Merry Christmas Party",
      kind: "christmas",
      status: "confirmed",
      runs: `${day(0)}..${day(40)}`,
      onlyOn: [day(12), day(14), day(16)],
      separateTicket: true,
      hours: "19:00–24:00",
    });
    expect(json?.statusMeans.expected).toMatch(/last year's pattern/);
    expect(json?.slots).toContainEqual({
      park: "Magic Kingdom Park",
      slot: "Early Entry (guests of a Walt Disney World resort hotel, a Disney Deluxe or Deluxe Villa resort hotel or a Disney Springs Resort Area hotel)",
      minutesBeforeOpening: 30,
      days: [`${day(10)}..${day(14)}`],
    });
    expect(json?.slots).toContainEqual(
      expect.objectContaining({
        park: "Magic Kingdom Park",
        hours: "19:00–24:00",
        dayGuestsLeave: "18:00",
        days: [day(12)],
      }),
    );
  });

  it("gives a park elsewhere its seasons only, a nameless one by its kind", async () => {
    const { call, seasons, internalCalendar } = setup();
    const { json } = await call("get_park_events", { park: "phantasialand" });
    expect(seasons.forParkInRange).toHaveBeenCalled();
    expect(internalCalendar).not.toHaveBeenCalled();
    expect(json?.seasons).toEqual([
      expect.objectContaining({
        park: "Phantasialand",
        name: "Halloween season",
        status: "announced",
      }),
    ]);
    expect(json?.slots).toMatch(/known for the Orlando.* parks only/);
  });

  it("refuses a window over 92 days", async () => {
    const { call } = setup();
    const result = await call("get_park_events", {
      from: day(1),
      to: day(120),
    });
    expect(result.isError).toBe(true);
  });
});

describe("get_travel_time", () => {
  it("drives to Magic Kingdom's car park and names the transfer and the source", async () => {
    const { call } = setup();
    const { json } = await call("get_travel_time", {
      from: "Epic Universe",
      to: "Magic Kingdom",
    });
    expect(json).toMatchObject({
      from: "Epic Universe",
      to: "Magic Kingdom Park",
      mode: "drive",
      plusMinutes: 15,
      basis: expect.stringMatching(/no traffic/),
      source: expect.stringMatching(/OpenStreetMap/),
    });
    expect(json?.doorToDoorMinutes).toBe(json?.minutes + 15);
  });

  it("walks from a Universal hotel through CityWalk", async () => {
    const { call } = setup();
    const { json } = await call("get_travel_time", {
      area: "universal",
      to: "Islands of Adventure",
    });
    expect(json?.alternatives).toContainEqual({
      mode: "walk",
      minutes: 15,
      via: "through CityWalk",
    });
  });

  it("says it has nothing for a park outside Orlando", async () => {
    const { call } = setup();
    const result = await call("get_travel_time", {
      from: "epcot",
      to: "phantasialand",
    });
    expect(result.isError).toBe(true);
    expect(result.content).toMatch(/Orlando/);
  });
});

describe("the trip in the planner and the hub", () => {
  it("reserves a plan's days across parks once each, in order", async () => {
    const { call } = setup();
    const result = await call("plan_trip", {
      days: [
        { park: idOf("epcot"), date: day(12) },
        { park: idOf("magic-kingdom-park"), date: day(11) },
        { park: idOf("epcot"), date: day(12) },
      ],
    });
    expect(result.action).toEqual({
      type: "planner_reserve_trip",
      days: [
        {
          park: expect.objectContaining({ slug: "magic-kingdom-park" }),
          date: day(11),
        },
        { park: expect.objectContaining({ slug: "epcot" }), date: day(12) },
      ],
    });
    expect((await call("plan_trip", { park: "epcot", days: [] })).isError).toBe(
      true,
    );
  });

  it("opens the hub, empty or with a plan", async () => {
    const { call } = setup();
    expect((await call("open_page", { page: "trip_hub" })).action).toEqual({
      type: "navigate",
      path: "/trip-regions/orlando",
    });
    expect(
      (
        await call("open_page", {
          url: `https://park.fan/de/trip-regions/orlando?from=${day(10)}&to=${day(14)}#planner`,
        })
      ).action,
    ).toEqual({
      type: "navigate",
      path: `/trip-regions/orlando?from=${day(10)}&to=${day(14)}#planner`,
    });
    expect(
      (await call("open_page", { url: "https://park.fan/orlando?x=1" }))
        .isError,
    ).toBe(true);
  });
});

describe("2.21.0: live regions, the route, parks left out, a region's facts", () => {
  const onlyOrlando = { liveRegions: new Set(["orlando"]) };

  it("plans, changes, opens and interviews only the regions the site has live", async () => {
    const { call } = setup();
    const coming =
      /Japan's trip planner is not on park.fan yet; it opens later/;
    for (const [name, input] of [
      ["plan_vacation", { region: "japan", from: day(10), to: day(14) }],
      [
        "change_trip",
        { region: "japan", dates: { from: day(10), to: day(14) } },
      ],
      ["get_trip_region", { region: "japan" }],
      ["open_page", { page: "trip_hub", region: "japan" }],
      [
        "open_page",
        {
          url: "https://park.fan/trip-regions/japan?from=2027-04-01&to=2027-04-05",
        },
      ],
      ["interview", { op: "start", kind: "trip", region: "japan" }],
    ] as const) {
      const result = await call(name, input, "de", {
        ...onlyOrlando,
        answer: { round: 0, done: new Set<string>() },
      });
      expect(result.isError).toBe(true);
      expect(result.content).toMatch(coming);
    }
    expect(
      (await call("open_page", { page: "trip_hub" }, "de", onlyOrlando)).action,
    ).toEqual({ type: "navigate", path: "/trip-regions/orlando" });
    // An older widget sends no list: every region, as before.
    expect(
      (await call("open_page", { page: "trip_hub", region: "japan" })).action,
    ).toEqual({ type: "navigate", path: "/trip-regions/japan" });
  });

  it("starts a trip from parks alone, or from a date search, without dates", async () => {
    const { call } = setup();
    const empty = { trips: [], answer: { round: 0, done: new Set<string>() } };
    const parks = await call(
      "change_trip",
      { parks: [{ park: "EPCOT", visits: 1 }] },
      "de",
      empty,
    );
    expect(parks.action).toEqual({
      type: "trip_set",
      region: "orlando",
      query: "parks=epcot:1",
      change: ["parks"],
    });
    expect(parks.json?.plan).toBe("no dates yet, so nothing is planned");
    const search = await call(
      "change_trip",
      { search: { from: day(30), to: day(120), days: 7 } },
      "de",
      empty,
    );
    expect(search.action).toMatchObject({
      query: `when=${day(30)}_${day(120)}&days=7`,
      change: ["dates"],
    });
    const both = await call(
      "change_trip",
      {
        dates: { from: day(10), to: day(14) },
        search: { from: day(30), to: day(120), days: 7 },
      },
      "de",
      empty,
    );
    expect(both.content).toBe("Give dates or a search, not both");
  });

  it("takes no route in a region with one base", async () => {
    const { call } = setup();
    const result = await call(
      "change_trip",
      { dates: { from: day(10), to: day(14) }, moveBuffer: 60 },
      "de",
      { trips: [], answer: { round: 0, done: new Set<string>() } },
    );
    expect(result.isError).toBe(true);
    expect(result.content).toMatch(/^Orlando has one base for the whole trip/);
  });

  it("names the ways to fit parks the plan left out, by why they are out", () => {
    expect(
      leftOutFixes([{ reason: "no_day_left" }, { reason: "no_day_left" }], {
        arrival: "park",
        departure: "free",
      }),
    ).toEqual([
      "the departure day as a park day: change_trip departure park",
      "fewer parks: change_trip parks with visits 0 for one they can do without",
      "a longer trip: change_trip dates, 2 more days",
    ]);
    expect(
      leftOutFixes([{ reason: "no_base" }, { reason: "no_open_day" }], {
        arrival: "free",
        departure: "free",
      }),
    ).toEqual([
      "other dates, when the park is open: change_trip dates",
      "a city that reaches it: change_trip bases",
    ]);
  });

  it("gives a region's facts: resorts, hotels and perks, tickets, storm season, hub", async () => {
    const { call } = setup();
    const { json } = await call("get_trip_region", {}, "de", onlyOrlando);
    expect(json?.name).toBe("Orlando");
    expect(json?.hubUrl).toMatch(/\/trip-regions\/orlando$/);
    const disney = json?.resorts.find(
      (r: { key: string }) => r.key === "walt-disney-world",
    );
    expect(disney.parks).toContain("Magic Kingdom Park (headliner)");
    expect(json?.hotels.length).toBeGreaterThan(2);
    expect(json?.tickets.join(" ")).toMatch(/wdw-date-based/);
    expect(json?.stormSeason).toMatch(
      /^hurricane season, its peak 08-15 to 10-15/,
    );
    expect(json?.bases).toBeUndefined();
  });

  it("shows each resort's park of the day over a span, quietest and busiest first", async () => {
    const { call } = setup(() => {
      const base = calendar();
      return {
        ...base,
        days: base.days.map((d, i) => ({
          ...d,
          best: { "walt-disney-world": i % 2 ? "epcot" : "magic-kingdom-park" },
        })),
      };
    });
    const { json } = await call(
      "get_trip_days",
      { from: day(10), to: day(14), resort: "walt-disney-world" },
      "de",
      onlyOrlando,
    );
    expect(json?.resorts).toHaveLength(1);
    expect(json?.resorts[0].days).toHaveLength(5);
    expect(json?.resorts[0].quietest[0]).toMatch(
      /: (Magic Kingdom Park|EPCOT) \d+ min/,
    );
    const unknown = await call(
      "get_trip_days",
      { resort: "tokyo-disney-resort" },
      "de",
      onlyOrlando,
    );
    expect(unknown.content).toMatch(
      /^No resort tokyo-disney-resort in Orlando: /,
    );
  });

  it("says how accurate the region's forecast was, as a typical miss", async () => {
    const { call } = setup();
    const { json } = await call("get_forecast_accuracy", {}, "de", onlyOrlando);
    expect(json?.accuracy).toBe(
      "a week ahead, the big rides' peak wait was off by 11.4 minutes on average, over 7 parks and the 30 days 2026-10-14 to 2026-11-12",
    );
    expect(json?.means).toMatch(/estimated from last year/);
  });
});
